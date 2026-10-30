"""Discover exact retained child owners; never infer a unit from its name."""

import json

from ahc.domain.sessions import VirtualSessionIdentity, _identifier
from ahc.domain.skill_values import canonical_json, digest_json
from ahc.domain.workspace import WorkspaceIdentity

from .backend_custody import BackendCustodyUnavailable
from .child_backend_custody import ChildBackendCustody
from .child_work_repository import ChildWorkRepository
from .runtime_recovery_access import verified_recovery_edges
from .working_repository import WorkingSetCorrupt


def recover_child_backends(store, session, workspace_id, runner):
    if type(session) is not VirtualSessionIdentity:
        raise WorkingSetCorrupt("child recovery session invalid")
    _identifier(workspace_id, "workspace")
    sessions = [session]
    with store._read() as connection:
        current = store._load_current(connection, session.ref.virtual_session_id)
        if current.session != session or current.status != "active":
            raise BackendCustodyUnavailable("child recovery requires current active session")
        if connection.execute("SELECT 1 FROM sqlite_master WHERE type='table' "
                              "AND name='runtime_recovery_transitions'").fetchone():
            edges = verified_recovery_edges(connection, session.ref.virtual_session_id)
            cursor = session
            for _ in range(len(edges)):
                ancestor = edges.get(cursor)
                if ancestor is None:
                    break
                if ancestor in sessions:
                    raise WorkingSetCorrupt("child recovery ancestry cycle")
                sessions.append(ancestor)
                cursor = ancestor
    recovered = []
    for selected in sessions:
        recovered.extend(_recover_exact_scope(store, selected, workspace_id, runner))
    return tuple(recovered)


def _recover_exact_scope(store, session, workspace_id, runner):
    with store._read() as connection:
        tables = {row[0] for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table'")}
        rows = (connection.execute("SELECT * FROM runtime_children WHERE session_json=?",
            (canonical_json(session.to_dict()).decode(),)).fetchall()
            if "runtime_children" in tables else ())
        envelopes = []
        for row in rows:
            envelope, _ = ChildWorkRepository._decode(row)
            if envelope.execution_kind == "subagent" and envelope.artifact_scope == workspace_id:
                envelopes.append(envelope)
        bindings = {digest_json({"session": session.to_dict(), "workspace": workspace_id,
                    "resource_role": "backend", "child": {"parent": item.parent.to_dict(),
                    "child_id": item.child_id, "envelope_digest": item.request_digest}})
                    for item in envelopes}
        if "backend_custody" in tables:
            for row in connection.execute("SELECT * FROM backend_custody WHERE state='held'"):
                record = json.loads(row["record_json"])
                if "child" not in record:
                    continue
                workspace = WorkspaceIdentity.from_dict(record["workspace"])
                if workspace.session != session or workspace.workspace_id != workspace_id:
                    continue
                if digest_json(record) != row["record_digest"]:
                    raise WorkingSetCorrupt("child recovery custody digest mismatch")
                if row["owner_key"] not in bindings:
                    raise BackendCustodyUnavailable("held child lacks retained ledger binding")
        if "backend_creation_intents" in tables:
            base = {"session": session.to_dict(), "workspace": workspace_id}
            known = bindings | {digest_json(base),
                                digest_json({**base, "resource_role": "python-workspace"})}
            for row in connection.execute(
                    "SELECT * FROM backend_creation_intents WHERE state='pending'"):
                if row["owner_key"] in known:
                    continue
                workspace = WorkspaceIdentity.from_dict(json.loads(row["workspace_json"]))
                if workspace.session == session and workspace.workspace_id == workspace_id:
                    raise BackendCustodyUnavailable("pending owner lacks retained ledger binding")
    recovered = []
    for envelope in envelopes:
        custody = ChildBackendCustody(store, envelope)
        key = digest_json({"session": session.to_dict(), "workspace": workspace_id,
                           **custody._role_binding()})
        with store._read() as connection:
            held = connection.execute("SELECT 1 FROM backend_custody "
                "WHERE owner_key=? AND state='held' LIMIT 1", (key,)).fetchone()
            pending = connection.execute("SELECT 1 FROM backend_creation_intents "
                "WHERE owner_key=? AND state='pending'", (key,)).fetchone()
        if held is None and pending is None:
            continue
        # Discovery is advisory: lease acquisition and recover re-read current custody.
        lease = custody.acquire(session, workspace_id)
        try:
            recovered.extend(custody.recover(lease, runner))
        finally:
            lease.close()
    return tuple(recovered)
