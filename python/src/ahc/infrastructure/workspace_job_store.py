"""Small job receipts in the existing runtime database; no executable code retained."""

import json
from dataclasses import replace

from ahc.domain.sessions import ContractError
from ahc.domain.skill_values import canonical_json

TABLE = """CREATE TABLE IF NOT EXISTS runtime_workspace_jobs (
    session_json TEXT NOT NULL, workspace_id TEXT NOT NULL, generation INTEGER NOT NULL,
    job_id TEXT NOT NULL, request_digest TEXT NOT NULL, origin_json TEXT NOT NULL,
    successor_json TEXT NOT NULL, state TEXT NOT NULL, result_json TEXT,
    PRIMARY KEY(session_json, workspace_id, generation, job_id))"""


def key(identity, job_id):
    return (canonical_json(identity.session.to_dict()).decode(),
            identity.workspace_id, identity.generation, job_id)


def initialize(store):
    store._mutate(lambda connection: connection.execute(TABLE))


def read(store, identity, job_id):
    with store._read() as connection:
        current = store._load_current(connection, identity.session.ref.virtual_session_id)
        if current.session != identity.session or current.status != "active":
            raise ContractError("workspace job session is stale")
        row = connection.execute(
            "SELECT * FROM runtime_workspace_jobs WHERE session_json=? AND workspace_id=? "
            "AND generation=? AND job_id=?", key(identity, job_id)).fetchone()
        return dict(row) if row is not None else None


def reserve(store, identity, job_id, request_digest):
    def operation(connection):
        row = connection.execute(
            "SELECT * FROM runtime_workspace_jobs WHERE session_json=? AND workspace_id=? "
            "AND generation=? AND job_id=?", key(identity, job_id)).fetchone()
        if row is not None:
            current = store._load_current(connection, identity.session.ref.virtual_session_id)
            if current.session != identity.session or current.status != "active":
                raise ContractError("workspace job session is stale")
            if row["request_digest"] != request_digest:
                raise ContractError("workspace job id bound to another request")
            return dict(row), False
        store._require_workspace(connection, identity, "ready")
        if identity.revision >= 2**63 - 1:
            raise ContractError("workspace revision exhausted")
        active = connection.execute(
            "SELECT 1 FROM runtime_workspace_jobs WHERE session_json=? AND workspace_id=? "
            "AND generation=? AND state IN ('admitted','running')", key(identity, job_id)[:3]
        ).fetchone()
        if active is not None:
            raise ContractError("workspace already has an active job")
        successor = replace(identity, revision=identity.revision + 1)
        connection.execute(
            "UPDATE workspace_heads SET revision=?, state='executing' "
            "WHERE session_json=? AND workspace_id=?",
            (successor.revision, *key(identity, job_id)[:2]))
        connection.execute(
            "INSERT INTO runtime_workspace_jobs VALUES(?,?,?,?,?,?,?,?,NULL)",
            (*key(identity, job_id), request_digest,
             canonical_json(identity.to_dict()).decode(),
             canonical_json(successor.to_dict()).decode(), "admitted"))
        row = connection.execute(
            "SELECT * FROM runtime_workspace_jobs WHERE session_json=? AND workspace_id=? "
            "AND generation=? AND job_id=?", key(identity, job_id)).fetchone()
        return dict(row), True
    return store._mutate(operation)


def finish(store, identity, job_id, result, *, state="complete"):
    encoded = json.dumps(result, allow_nan=False, ensure_ascii=False)
    if len(encoded.encode("utf-8")) > 2**20:
        raise ContractError("workspace job result receipt too large")
    def operation(connection):
        row = connection.execute(
            "SELECT * FROM runtime_workspace_jobs WHERE session_json=? AND workspace_id=? "
            "AND generation=? AND job_id=?", key(identity, job_id)).fetchone()
        if row is None:
            raise ContractError("workspace job unavailable")
        if row["state"] not in ("admitted", "running"):
            return False  # A terminal unknown/cancelled receipt cannot become success later.
        if state == "complete":
            store._require_workspace(connection, identity, "executing")
            connection.execute(
                "UPDATE workspace_heads SET state='ready' WHERE session_json=? AND workspace_id=?",
                key(identity, job_id)[:2])
        connection.execute(
            "UPDATE runtime_workspace_jobs SET state=?,result_json=? WHERE session_json=? "
            "AND workspace_id=? AND generation=? AND job_id=?",
            (state, encoded, *key(identity, job_id)))
        return True
    return store._mutate(operation)


def update_cancellation(store, identity, job_id, details):
    """Improve only this exact terminal cancellation's cleanup evidence."""
    def operation(connection):
        connection.execute(
            "UPDATE runtime_workspace_jobs SET result_json=? WHERE session_json=? "
            "AND workspace_id=? AND generation=? AND job_id=? AND state='cancelled' "
            "AND successor_json=?",
            (canonical_json(details).decode(), *key(identity, job_id),
             canonical_json(identity.to_dict()).decode()))
    store._mutate(operation)
