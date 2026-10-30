"""Normalize an explicit ordinary-child batch against the current workspace."""

import json
import math

from ahc.domain.child_work import ChildWorkEnvelope
from ahc.domain.sessions import ContractError, _identifier
from ahc.domain.skill_values import digest_json
from ahc.domain.workspace import WorkspaceIdentity
from ahc.infrastructure.child_batch_runner import ChildBatchRunner


def invoke_batch(owner, identity, arguments):
    try:
        if (not owner.batch_configured or owner.children is None
                or identity != owner.workspace.identity or type(arguments) is not dict
                or set(arguments) != {"expected", "children", "concurrency"}
                or WorkspaceIdentity.from_dict(arguments["expected"]) != identity
                or type(arguments["children"]) is not list
                or not arguments["children"]
                or type(arguments["concurrency"]) is not int
                or not 1 <= arguments["concurrency"] <= owner.max_concurrency):
            raise ContractError("child batch input invalid")
        ledger = owner.children
        records = {row["child_id"]: row for row in ledger.read(ledger.parent)}
        remaining = math.floor((ledger.request.effective_deadline - ledger._monotonic_now()) * 1000) - 1
        wall, envelopes, seen = ledger._now(), [], set()
        for item in arguments["children"]:
            if (type(item) is not dict or set(item) != {"child_id", "task_text", "timeout_ms"}
                    or type(item["timeout_ms"]) is not int
                    or item["timeout_ms"] < 1):
                raise ContractError("child batch item invalid")
            local_id = _identifier(item["child_id"], "child identifier")
            child_id = digest_json({"parent": ledger.parent.to_dict(), "child": local_id})
            if child_id in seen:
                raise ContractError("child batch identifier duplicated")
            seen.add(child_id)
            existing = records.get(child_id)
            if existing is None and remaining <= 1:
                raise ContractError("child parent deadline exhausted")
            deadline = (existing["deadline_unix_ms"] if existing else
                        wall + min(item["timeout_ms"], remaining))
            envelope = ChildWorkEnvelope(child_id, ledger.parent, item["task_text"],
                                         identity.workspace_id, deadline)
            if existing and envelope.request_digest != existing["request_digest"]:
                raise ContractError("child identifier task mismatch")
            envelopes.append(envelope)
        if len(seen | records.keys()) > owner.max_children:
            raise ContractError("child parent call limit reached")
    except Exception:
        return {"disposition": "rejected", "reason": "invalid-child-batch",
                "unresolved_effects": False}
    if any(service.cleanup_pending for service in owner.services.values()):
        return _unknown()
    try:
        owner.prepare_artifacts(envelopes)
        if owner.batch is None:
            owner.batch = ChildBatchRunner(ledger)
        results = owner.batch.dispatch(envelopes,
            lambda worker, item, token: owner._port(item, token, children=worker),
            cancelled=owner.cancelled, concurrency=arguments["concurrency"])
        values = [owner._result(record) if record is not None else _unknown()
                  for record in results]
        for value in values:
            record = value.get("result")
            output = None if record is None else record.get("result")
            if output:
                record["result"] = project_child_result(output)
        unresolved = any(value["unresolved_effects"] for value in values)
        return {"disposition": "unknown" if unresolved else "observed",
                "unresolved_effects": unresolved, "result": values}
    except Exception:
        return _unknown()


def _unknown():
    return {"disposition": "unknown", "reason": "child-batch-unverified",
            "unresolved_effects": True}


def project_child_result(output):
    """Keep artifact IDs discoverable within the batch bridge's aggregate bound."""
    result = dict(output)
    if len(json.dumps(result["output_text"])) > 1024:
        result.update(output_text=result["output_text"][:128], output_truncated=True)
    if len(json.dumps(result)) > 1600 and result.get("artifacts"):
        result["artifact_ids"] = [item["artifact_id"] for item in result.pop("artifacts")]
        result["artifacts_compacted"] = True
        # Full filenames remain in the durable child result, retrievable individually.
    return result
