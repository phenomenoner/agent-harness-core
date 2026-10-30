"""One backend-return snapshot per exact claimed child, separate from output truth."""

import json

from ahc.domain.agent_backend import AgentTurnObservation
from ahc.domain.sessions import ContractError, _identifier
from ahc.domain.token_observation import TokenCounts, TokenObservation, durable_token_snapshot

from .working_repository import WorkingSetConflict, WorkingSetCorrupt


def read(connection, envelope):
    if connection.execute("SELECT 1 FROM sqlite_master WHERE type='table' "
                          "AND name='runtime_child_token_observations'").fetchone() is None:
        return None
    row = connection.execute("SELECT envelope_digest,snapshot_json FROM "
        "runtime_child_token_observations WHERE child_id=?", (envelope.child_id,)).fetchone()
    if row is None:
        return None
    try:
        value = json.loads(row[1])
        if (row[0] != envelope.request_digest or type(value) is not dict
                or not {"last", "total", "scope", "complete"} <= set(value)
                or set(value) - {"last", "total", "scope", "complete", "model",
                                 "model_context_window", "context_compaction",
                                 "compaction_threshold", "compaction_threshold_source"}
                or value["scope"] != "backend-notification-snapshot"
                or value["complete"] is not False):
            raise ValueError()
        for name in ("last", "total"):
            if type(value[name]) is not dict or TokenCounts(**value[name]).to_dict() != value[name]:
                raise ValueError()
        compaction = value.get("context_compaction", {})
        if (type(compaction) is not dict or compaction and set(compaction) != {
                "count", "before_tokens", "after_tokens"}):
            raise ValueError()
        observed = TokenObservation("bound-thread", "bound-turn", TokenCounts(**value["last"]),
            TokenCounts(**value["total"]), model=value.get("model"),
            model_context_window=value.get("model_context_window"),
            compaction_count=compaction.get("count", 0),
            compaction_before_tokens=compaction.get("before_tokens"),
            compaction_after_tokens=compaction.get("after_tokens"),
            compaction_threshold=value.get("compaction_threshold"),
            compaction_threshold_source=value.get("compaction_threshold_source"))
        canonical = observed.to_dict()
        del canonical["backend_thread"], canonical["backend_turn"]
        if canonical != value:
            raise ValueError()
        return value
    except (ValueError, TypeError, ContractError):
        raise WorkingSetCorrupt("child token observation invalid") from None


def record(children, envelope, token, observed):
    children._bound(envelope.parent)
    _identifier(token, "child claim token")
    if (type(observed) is not AgentTurnObservation
            or observed.request.session != envelope.parent.session
            or observed.request.operation_id != envelope.child_id):
        raise ContractError("child token observation owner invalid")
    snapshot = durable_token_snapshot(observed.token_observation,
                                     observed.backend_thread, observed.backend_turn)
    if not snapshot:
        return
    payload = snapshot["token_observation"]

    def save(connection):
        children._require_parent(connection, envelope.parent)
        row, actual, _ = children._row(connection, envelope.parent, envelope.child_id)
        if actual != envelope or row["state"] != "claimed" or row["claim_token"] != token:
            raise WorkingSetConflict("child token observation claim changed")
        previous = read(connection, envelope)
        if previous is not None:
            if previous != payload:
                raise WorkingSetConflict("child token observation already recorded")
            return
        connection.execute("""CREATE TABLE IF NOT EXISTS runtime_child_token_observations (
            child_id TEXT PRIMARY KEY REFERENCES runtime_children(child_id),
            envelope_digest TEXT NOT NULL, snapshot_json TEXT NOT NULL)""")
        connection.execute("INSERT INTO runtime_child_token_observations VALUES(?,?,?)",
            (envelope.child_id, envelope.request_digest,
             json.dumps(payload, sort_keys=True, separators=(",", ":"))))

    children.store._mutate(save)
