"""Native operation lifecycle over an independently admitted MCP client."""

import json

from ahc.domain.native_operation import NativeOperationReceipt as NativeOperationReceipt
from ahc.domain.sessions import ContractError
from ahc.domain.skill_values import canonical_json

from .aar_native_binding import _IDENTITY, NativeAarBinding, _positive


class NativeAarOperations:
    """One bound native operation, distinct from its AHC child and parent IDs.

    Context grant IDs are supplied by the trusted composition. This adapter does
    not discover grants, authorize effects, retry calls, or start model work.
    The MCP client retains transport admission and durable call reservations.
    """

    def __init__(self, client, binding, operation_id):
        if type(binding) is not NativeAarBinding:
            raise ContractError("native binding required")
        if type(operation_id) is not str or not _IDENTITY.fullmatch(operation_id):
            raise ContractError("native operation identifier invalid")
        self.client, self.binding, self.operation_id = client, binding, operation_id
        self.last_receipt = None

    def status(self, call_id, *, deadline_unix_ms, deadline, cancelled=lambda: False):
        context = self.binding.read_context(deadline_unix_ms=deadline_unix_ms)
        return self._invoke("aar_operation_status", call_id, context,
                            deadline=deadline, cancelled=cancelled)

    def cancel(self, call_id, context, *, deadline, cancelled=lambda: False):
        self._mutation_context(context)
        return self._invoke("aar_operation_cancel", call_id, context,
                            deadline=deadline, cancelled=cancelled)

    def reconcile(self, call_id, context, *, deadline, cancelled=lambda: False):
        self._mutation_context(context)
        return self._invoke("aar_operation_reconcile", call_id, context,
                            deadline=deadline, cancelled=cancelled)

    def _mutation_context(self, context):
        if type(context) is not dict:
            raise ContractError("native mutation context required")
        expected = self.binding.read_context(deadline_unix_ms=context.get("deadline_unix_ms"))
        if (set(context) != set(expected) | {
                "request_id", "idempotency_key", "grant_id", "budget_wall_time_ms"}
                or any(type(context.get(key)) is not type(value) or context[key] != value
                       for key, value in expected.items())):
            raise ContractError("native mutation binding mismatch")
        for key in ("request_id", "grant_id"):
            if type(context[key]) is not str or not _IDENTITY.fullmatch(context[key]):
                raise ContractError("native mutation identity invalid")
        key = context["idempotency_key"]
        if type(key) is not str or not 8 <= len(key) <= 128:
            raise ContractError("native idempotency key invalid")
        budget = _positive(context["budget_wall_time_ms"])
        remaining = context["deadline_unix_ms"] - self.binding.server_now_unix_ms
        if budget > 60000 or budget < remaining:
            raise ContractError("native mutation budget must cover deadline within server bound")

    def _invoke(self, tool, call_id, context, *, deadline, cancelled):
        # Copy before crossing transport so the caller cannot mutate retained args.
        arguments = json.loads(canonical_json({"context": context,
                                              "operation_id": self.operation_id}))
        try:
            observed = self.client.invoke(self.client.session, call_id, tool, arguments,
                                           deadline=deadline, cancelled=cancelled)
        except Exception:
            return {"status": "unknown", "receipt": None}
        if observed.get("status") == "rejected":
            return {"status": "rejected", "receipt": None}
        if observed.get("status") != "observed":
            return {"status": "unknown", "receipt": None}
        try:
            payload = observed["result"]["structuredContent"]
            receipt = NativeOperationReceipt.parse(payload, self.operation_id)
            previous = self.last_receipt
            if previous is not None:
                if receipt.revision < previous.revision:
                    raise ContractError("native receipt revision regressed")
                if (receipt.revision == previous.revision
                        and receipt.record_payload() != previous.record_payload()):
                    raise ContractError("native receipt revision conflict")
                if previous.terminal and (receipt.state != previous.state or not receipt.terminal):
                    raise ContractError("native terminal receipt regressed")
            self.last_receipt = receipt
            return {"status": "observed", "receipt": receipt}
        except (KeyError, TypeError, ContractError):
            return {"status": "unknown", "receipt": None}
