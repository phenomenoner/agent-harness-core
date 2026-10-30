"""Native RLM submission with explicit host-selected route and grant scope."""

import json

from ahc.domain.sessions import ContractError
from ahc.domain.skill_values import canonical_json

from .aar_native_binding import _IDENTITY
from .aar_native_operations import NativeAarOperations, NativeOperationReceipt
from .aar_native_route import verify_native_model_route

_COUNTERS = {"budget_model_requests", "budget_input_tokens", "budget_output_tokens",
             "budget_child_operations", "budget_artifact_bytes"}


class NativeAarRlm:
    """No implicit route/grant selection, retries, or start_only safety assumption.

    The supplied MCP client owns parent admission and durable call reservation.
    AHC composition must supply grants already approved for this child and its
    parent-limited budgets; this adapter does not manufacture that authority.
    """

    def __init__(self, client, binding, *, profile_id, catalog_digest, profile_digest,
                 approved_grants):
        if (type(approved_grants) is not tuple or not approved_grants
                or any(type(grant) is not str or not _IDENTITY.fullmatch(grant)
                       for grant in approved_grants)
                or len(set(approved_grants)) != len(approved_grants)):
            raise ContractError("native RLM approved grants invalid")
        self.client, self.binding = client, binding
        self.route = {"profile_id": profile_id, "catalog_digest": catalog_digest,
                      "profile_digest": profile_digest}
        self.approved_grants = frozenset(approved_grants)

    def prepare(self, report, context, *, query, strategy, max_steps, start_only=False):
        verify_native_model_route(self.binding, report, **self.route)
        if type(context) is not dict:
            raise ContractError("native RLM context missing")
        expected = self.binding.read_context(deadline_unix_ms=context.get("deadline_unix_ms"))
        fields = set(expected) | _COUNTERS | {
            "request_id", "idempotency_key", "grant_ids", "budget_wall_time_ms"}
        if (set(context) != fields or any(type(context[key]) is not type(value)
                or context[key] != value for key, value in expected.items())):
            raise ContractError("native RLM context binding mismatch")
        for key in ("request_id",):
            if type(context[key]) is not str or not _IDENTITY.fullmatch(context[key]):
                raise ContractError("native RLM request identity invalid")
        key = context["idempotency_key"]
        if type(key) is not str or not 8 <= len(key) <= 128:
            raise ContractError("native RLM idempotency key invalid")
        grants = context["grant_ids"]
        if (type(grants) is not list or any(type(item) is not str for item in grants)
                or grants != sorted(set(grants)) or not grants
                or not set(grants) <= self.approved_grants):
            raise ContractError("native RLM grant scope mismatch")
        for key in _COUNTERS:
            if type(context[key]) is not int or not 0 <= context[key] <= 9223372036854775807:
                raise ContractError("native RLM budget invalid")
        if context["budget_model_requests"] > 16:
            raise ContractError("native RLM model-request budget unsupported")
        wall = context["budget_wall_time_ms"]
        remaining = context["deadline_unix_ms"] - self.binding.server_now_unix_ms
        if type(wall) is not int or not remaining <= wall <= 60000:
            raise ContractError("native RLM wall budget invalid")
        if (type(query) is not str or not 1 <= len(query) <= 65536
                or strategy not in ("baseline", "evidence_synthesis")
                or type(max_steps) is not int or not 1 <= max_steps <= 32
                or (strategy == "evidence_synthesis" and max_steps < 2)
                or type(start_only) is not bool):
            raise ContractError("native RLM specification invalid")
        arguments = {"context": context, "query": query, "strategy": strategy,
                     "max_steps": max_steps, "start_only": start_only}
        encoded = canonical_json(arguments)
        if len(encoded) > 65536:
            raise ContractError("native RLM request exceeds MCP bound")
        return json.loads(encoded)

    def submit(self, call_id, report, context, *, query, strategy, max_steps,
               deadline, start_only=False, cancelled=lambda: False):
        arguments = self.prepare(report, context, query=query, strategy=strategy,
                                 max_steps=max_steps, start_only=start_only)
        try:
            observed = self.client.invoke(self.client.session, call_id, "aar_rlm_execute",
                arguments, deadline=deadline, cancelled=cancelled)
            if observed.get("status") == "rejected":
                return {"status": "rejected", "operation": None, "receipt": None}
            if observed.get("status") != "observed":
                raise ContractError("native RLM response unverified")
            payload = observed["result"]["structuredContent"]
            operation_id = payload["operation"]["value"]
            receipt = NativeOperationReceipt.parse(payload, operation_id)
            operation = NativeAarOperations(self.client, self.binding, operation_id)
            operation.last_receipt = receipt
            return {"status": "observed", "operation": operation, "receipt": receipt}
        except Exception:
            # No returned ID means unknown, not permission to guess an ID or replay.
            return {"status": "unknown", "operation": None, "receipt": None}
