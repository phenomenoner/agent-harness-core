"""Explicit, deadline-bound artifact retention after verified native child success."""

import json
from uuid import uuid4

from ahc.adapters.aar_native_artifacts import resolve_native_artifact
from ahc.domain.sessions import ContractError
from ahc.infrastructure.native_artifact_import import NativeArtifactImport


def retain_artifacts(backend, children, envelope, token, producer, grants, *, cancelled):
    operation = backend.operation
    if operation is None or not grants or cancelled():
        raise ContractError("native artifact retention unavailable")
    context = backend.context
    reconciled = operation.reconcile("native-artifacts-" + uuid4().hex, {
        **operation.binding.read_context(deadline_unix_ms=context["deadline_unix_ms"]),
        "request_id": "artifacts-" + uuid4().hex,
        "idempotency_key": "artifacts-" + uuid4().hex,
        "grant_id": grants[0], "budget_wall_time_ms": context["budget_wall_time_ms"],
    }, deadline=backend.deadline, cancelled=cancelled)
    if reconciled["status"] != "observed" or reconciled["receipt"] is None:
        raise ContractError("native artifact reconciliation unverified")
    receipt = reconciled["receipt"]
    payload = json.loads(receipt.payload_json)
    if not receipt.terminal or receipt.state != "succeeded":
        raise ContractError("native artifact success unavailable")
    backend.native.observe(envelope.parent, envelope.child_id, token, payload)
    references = payload.get("artifacts", [])
    if type(references) is not list or len(references) > 8:
        raise ContractError("native artifact count exceeds retention bound")
    importer = NativeArtifactImport(children)
    for reference in references:
        result = resolve_native_artifact(operation, "native-artifact-" + uuid4().hex, reference,
            max_bytes=min(16384, context["budget_artifact_bytes"]),
            deadline_unix_ms=context["deadline_unix_ms"], deadline=backend.deadline,
            cancelled=cancelled)
        if result["status"] != "observed" or result["artifact"] is None or cancelled():
            raise ContractError("native artifact disclosure unverified")
        importer.publish(envelope.parent, envelope.child_id, token, producer, result["artifact"])
