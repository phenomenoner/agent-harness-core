"""Single-use native RLM child port with durable reservation before submission."""

import json
import math
import time

from ahc.domain.child_work import ChildWorkEnvelope, child_output
from ahc.domain.sessions import ContractError
from ahc.domain.skill_values import canonical_json, digest_json


class NativeAarChildBackend:
    def __init__(
        self,
        envelope,
        native_store,
        rlm,
        report,
        context,
        *,
        token,
        strategy="baseline",
        max_steps=1,
        deadline,
        max_status_reads=256,
        status_interval=0.25,
    ):
        children = native_store.children
        if (
            type(envelope) is not ChildWorkEnvelope
            or envelope.execution_kind != "minion"
            or envelope.parent != children.parent
            or rlm.client.session != envelope.parent.session
            or type(deadline) not in (int, float)
            or not math.isfinite(deadline)
            or deadline > children.request.effective_deadline
            or context.get("deadline_unix_ms", 0) > envelope.deadline_unix_ms
            or type(max_status_reads) is not int
            or not 0 <= max_status_reads <= 256
            or type(status_interval) not in (int, float)
            or not math.isfinite(status_interval)
            or not 0.001 <= status_interval <= 0.25
        ):
            raise ContractError("native child composition mismatch")
        self.envelope, self.native, self.rlm = envelope, native_store, rlm
        self.report = json.loads(canonical_json(report))
        self.context = json.loads(canonical_json(context))
        self.token, self.strategy, self.max_steps, self.deadline = (
            token,
            strategy,
            max_steps,
            deadline,
        )
        self.used = self.closed = False
        self.operation = None
        self.max_status_reads, self.status_interval = max_status_reads, status_interval

    def run(self, envelope, *, cancelled):
        if self.used or self.closed or envelope != self.envelope or not callable(cancelled):
            raise ContractError("native child port cannot be reused")
        self.used = True
        children = self.native.children

        def stopped():
            return (
                cancelled()
                or children.should_stop(envelope.parent, envelope.child_id, self.token)
                or children._monotonic_now() >= self.deadline
            )

        if stopped():
            raise ContractError("native child interrupted before submission")
        arguments = self.rlm.prepare(
            self.report,
            self.context,
            query=envelope.task_text,
            strategy=self.strategy,
            max_steps=self.max_steps,
            start_only=True,
        )
        bound = self.rlm.binding
        identity = {
            "server_id": self.rlm.client.server_id,
            "runtime_generation": bound.runtime_generation,
            "capability_digest": bound.pin.capability_digest,
            "tool_surface_digest": bound.pin.tool_surface_digest,
            "schema_bundle_digest": bound.pin.schema_bundle_digest,
            "catalog_digest": self.rlm.route["catalog_digest"],
            "profile_digest": self.rlm.route["profile_digest"],
            "submission_digest": "sha256:" + digest_json(arguments),
            "principal_id": bound.principal_id,
            "session_id": bound.session_id,
        }
        self.native.reserve(envelope.parent, envelope.child_id, self.token, identity)
        if stopped():
            raise ContractError("native child interrupted after reservation")
        result = self.rlm.submit(
            "native-" + digest_json(envelope.to_dict())[:40],
            self.report,
            self.context,
            query=envelope.task_text,
            strategy=self.strategy,
            max_steps=self.max_steps,
            start_only=True,
            deadline=self.deadline,
            cancelled=stopped,
        )
        if result["status"] != "observed" or result["receipt"] is None:
            raise ContractError("native child submission outcome unknown")
        receipt = result["receipt"]
        payload = json.loads(receipt.payload_json)
        self.native.observe(envelope.parent, envelope.child_id, self.token, payload)
        operation = result["operation"]
        self.operation = operation
        for index in range(self.max_status_reads):
            if receipt.terminal or receipt.reconciliation_required or stopped():
                break
            remaining = self.deadline - children._monotonic_now()
            time.sleep(min(self.status_interval, max(0, remaining)))
            if stopped():
                break
            observed = operation.status(
                "native-status-" + digest_json(envelope.to_dict())[:40] + f"-{index}",
                deadline_unix_ms=self.context["deadline_unix_ms"],
                deadline=self.deadline,
                cancelled=stopped,
            )
            if observed["status"] != "observed" or observed["receipt"] is None:
                raise ContractError("native child status outcome unknown")
            receipt = observed["receipt"]
            payload = json.loads(receipt.payload_json)
            self.native.observe(envelope.parent, envelope.child_id, self.token, payload)
        if stopped() or not receipt.terminal or receipt.state != "succeeded":
            raise ContractError("native child has no verified success")
        return self._answer(payload.get("result"), receipt.operation_id)

    def _answer(self, result, operation_id):
        fields = {"operation", "strategy", "answer", "steps", "usage", "broker_trace"}
        if (
            type(result) is not dict
            or set(result) != fields | {"schema_version", "trace_digest"}
            or result["schema_version"] != "aar.rlm.v1"
            or result["operation"] != {"type": "operation", "value": operation_id}
            or result["strategy"] != self.strategy
            or result["trace_digest"]
            != "sha256:" + digest_json({key: result[key] for key in fields})
            or type(result["steps"]) is not list
            or len(result["steps"]) > self.max_steps
            or type(result["usage"]) is not dict
        ):
            raise ContractError("native child result identity or trace mismatch")
        for name in (
            "model_requests",
            "input_tokens",
            "output_tokens",
            "child_operations",
            "artifact_bytes",
        ):
            value = result["usage"].get(name)
            if type(value) is not int or not 0 <= value <= self.context["budget_" + name]:
                raise ContractError("native child result exceeds admitted budget")
        return child_output(result["answer"])

    def close(self):
        if not self.closed:
            self.rlm.client.close()
            self.closed = True
