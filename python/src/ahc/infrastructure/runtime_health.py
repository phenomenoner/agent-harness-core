"""Persisted domain-state counts; never infer process or provider liveness."""

import sqlite3

from ahc.domain.sessions import ContractError, VirtualSessionIdentity
from ahc.domain.skill_values import canonical_json

_DOMAINS = {
    "schedules": ("runtime_schedules", "state", ("active", "paused", "removed")),
    "turns": ("runtime_turns", "status", ("submitted", "terminal")),
    "workspaces": ("workspace_heads", "state", ("ready", "executing")),
    "children": ("runtime_children", "state",
                 ("queued", "claimed", "cancelled", "terminal", "unknown")),
}


def inspect_runtime_health(store, session):
    if type(session) is not VirtualSessionIdentity:
        raise ContractError("runtime health session invalid")
    session_json = canonical_json(session.to_dict()).decode()
    with store._read() as connection:
        connection.execute("BEGIN")
        tables = {row[0] for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table'")}

        def count(required, sql, states):
            if not set(required) <= tables:
                return {"status": "schema-absent"}
            try:
                rows = connection.execute(sql, (session_json,)).fetchall()
                counts = {row[0]: row[1] for row in rows}
                if any(state not in states for state in counts):
                    return {"status": "unreadable"}
                return {"status": "observed", "state_counts": counts}
            except sqlite3.DatabaseError:
                return {"status": "unreadable"}

        domains = {name: count((table,),
            f"SELECT {column},count(*) FROM {table} WHERE session_json=? GROUP BY {column}", states)
            for name, (table, column, states) in _DOMAINS.items()}
        domains["turn_effects"] = count(("runtime_turns",),
            "SELECT CASE WHEN status='submitted' THEN 'not-terminal' "
            "WHEN json_type(result_json,'$.unresolved_effects')='true' THEN 'unresolved' "
            "WHEN json_type(result_json,'$.unresolved_effects')='false' THEN 'resolved' "
            "ELSE 'unclassified' END,count(*) FROM runtime_turns "
            "WHERE session_json=? GROUP BY 1",
            ("not-terminal", "unresolved", "resolved", "unclassified"))
        domains["memory_proposals"] = count(("memory_store_proposals",),
            "SELECT state,count(*) FROM memory_store_proposals "
            "WHERE json_extract(payload,'$.session')=? GROUP BY state",
            ("pending", "approved", "in-flight", "rejected", "succeeded", "failed", "unknown"))
        domains["channel_outbox"] = count(("runtime_channel_outbox", "runtime_turns"),
            "SELECT o.state,count(*) FROM runtime_channel_outbox o JOIN runtime_turns t "
            "ON t.operation_id=o.operation_id WHERE t.session_json=? GROUP BY o.state",
            ("pending", "claimed", "sent", "unknown"))
        domains["batch_receipts"] = count(("runtime_batch_receipts", "runtime_turns"),
            "SELECT CASE WHEN b.receipt_json IS NULL THEN 'completion-unverified' "
            "ELSE 'recorded' END,count(*) FROM runtime_batch_receipts b JOIN runtime_turns t "
            "ON t.operation_id=b.operation_id WHERE t.session_json=? GROUP BY 1",
            ("completion-unverified", "recorded"))
        domains["mcp_receipts"] = count(("runtime_mcp_receipts", "runtime_turns"),
            "SELECT COALESCE(m.status,'completion-unverified'),count(*) "
            "FROM runtime_mcp_receipts m JOIN runtime_turns t "
            "ON t.operation_id=m.operation_id WHERE t.session_json=? GROUP BY 1",
            ("completion-unverified", "observed", "tool-error", "unknown"))
        from .artifact_health import inspect_artifact_counts

        domains["artifacts"] = inspect_artifact_counts(connection, session_json, tables)
    return {"observation_scope": "persisted-state-counts", "domains": domains}
