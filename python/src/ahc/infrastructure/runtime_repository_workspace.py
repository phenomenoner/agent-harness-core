"""Workspace checkpoint and execution-state persistence for ``RuntimeRepository``."""

import json
from dataclasses import replace

from ahc.domain.sessions import ContractError, VirtualSessionIdentity, _identifier
from ahc.domain.skill_values import canonical_json
from ahc.domain.turns import _sha256_hex
from ahc.domain.workspace import WorkspaceCheckpoint, WorkspaceIdentity

from .runtime_recovery_access import historical_session_allows
from .working_repository import WorkingSetConflict, WorkingSetCorrupt


def save_workspace_checkpoint(repository, expected, checkpoint, *, fail_before_commit=False):
    """Persist portable values only; callers own live interpreter revision checks."""
    if (
        type(checkpoint) is not WorkspaceCheckpoint
        or checkpoint.origin.session != expected.session
    ):
        raise ContractError("checkpoint session mismatch")
    origin = checkpoint.origin
    owner = canonical_json(origin.session.to_dict()).decode()

    def operation(connection):
        current = repository._load_current(connection, origin.session.ref.virtual_session_id)
        if current != expected or current.status != "active":
            raise WorkingSetConflict("checkpoint lost current session authority")
        head = connection.execute(
            "SELECT * FROM workspace_heads WHERE session_json=? AND workspace_id=?",
            (owner, origin.workspace_id),
        ).fetchone()
        if head is not None:
            repository._require_workspace(connection, origin, "ready")
        row = connection.execute(
            "SELECT * FROM workspace_checkpoints WHERE session_json=? AND workspace_id=? "
            "AND generation=? AND revision=?",
            (owner, origin.workspace_id, origin.generation, origin.revision),
        ).fetchone()
        if row is not None:
            if row["digest"] != checkpoint.digest:
                raise ContractError("checkpoint revision already bound to different values")
            if (
                row["origin_json"] != canonical_json(origin.to_dict()).decode()
                or row["values_json"] != checkpoint.values_json
            ):
                raise WorkingSetCorrupt("stored checkpoint binding is invalid")
            return checkpoint.digest
        connection.execute(
            "INSERT INTO workspace_checkpoints VALUES(?,?,?,?,?,?,?)",
            (
                owner,
                origin.workspace_id,
                origin.generation,
                origin.revision,
                checkpoint.digest,
                canonical_json(origin.to_dict()).decode(),
                checkpoint.values_json,
            ),
        )
        return checkpoint.digest

    return repository._mutate(operation, fail_before_commit=fail_before_commit)


def read_workspace_checkpoint(repository, session, workspace_id, digest):
    """Exact digest lookup; verified recovery returns a transient owner projection."""
    if type(session) is not VirtualSessionIdentity:
        raise ContractError("checkpoint read owner invalid")
    _identifier(workspace_id, "workspace")
    _sha256_hex(digest)
    with repository._read() as connection:
        current = repository._load_current(connection, session.ref.virtual_session_id)
        if current.session != session or current.status != "active":
            raise WorkingSetConflict("checkpoint read lost session authority")
        row = connection.execute(
            "SELECT * FROM workspace_checkpoints WHERE digest=?", (digest,)
        ).fetchone()
        if row is None:
            return None
        if row["workspace_id"] != workspace_id:
            raise ContractError("checkpoint read owner mismatch")
        try:
            checkpoint = WorkspaceCheckpoint(
                WorkspaceIdentity.from_dict(json.loads(row["origin_json"])), row["values_json"]
            )
            if (
                checkpoint.digest != digest
                or row["session_json"] != canonical_json(
                    checkpoint.origin.session.to_dict()).decode()
                or checkpoint.origin.workspace_id != workspace_id
                or checkpoint.origin.generation != row["generation"]
                or checkpoint.origin.revision != row["revision"]
            ):
                raise ValueError()
        except (ValueError, TypeError, KeyError, RecursionError):
            raise WorkingSetCorrupt("stored checkpoint binding is invalid") from None
        if checkpoint.origin.session != session:
            if not historical_session_allows(repository, connection, session, checkpoint.origin.session):
                raise ContractError("checkpoint read owner mismatch")
            # Original bytes and digest remain stored under the historical producer.
            # Only the startup value is projected, through a verified historical read edge.
            return WorkspaceCheckpoint(replace(checkpoint.origin, session=session),
                                       checkpoint.values_json)
        return checkpoint


def reserve_workspace_generation(repository, expected, workspace_id, checkpoint=None):
    _identifier(workspace_id, "workspace")
    if checkpoint is not None and (
        type(checkpoint) is not WorkspaceCheckpoint
        or checkpoint.origin.session != expected.session
        or checkpoint.origin.workspace_id != workspace_id
    ):
        raise ContractError("workspace restore checkpoint owner mismatch")
    owner = canonical_json(expected.session.to_dict()).decode()

    def operation(connection):
        current = repository._load_current(connection, expected.session.ref.virtual_session_id)
        if current != expected or current.status != "active":
            raise WorkingSetConflict("workspace creation lost session authority")
        row = connection.execute(
            "SELECT generation FROM workspace_heads WHERE session_json=? AND workspace_id=?",
            (owner, workspace_id),
        ).fetchone()
        generation = max(
            row["generation"] if row else 0,
            checkpoint.origin.generation if checkpoint else 0,
        ) + 1
        if generation >= 2**63:
            raise ContractError("workspace generation exhausted")
        connection.execute(
            "INSERT INTO workspace_heads VALUES(?,?,?,0,'ready') "
            "ON CONFLICT(session_json,workspace_id) DO UPDATE SET "
            "generation=excluded.generation,revision=0,state='ready'",
            (owner, workspace_id, generation),
        )
        return WorkspaceIdentity(expected.session, workspace_id, generation, 0)

    return repository._mutate(operation)


def require_workspace(repository, connection, identity, state):
    if type(identity) is not WorkspaceIdentity:
        raise ContractError("workspace identity invalid")
    current = repository._load_current(connection, identity.session.ref.virtual_session_id)
    row = connection.execute(
        "SELECT * FROM workspace_heads WHERE session_json=? AND workspace_id=?",
        (canonical_json(identity.session.to_dict()).decode(), identity.workspace_id),
    ).fetchone()
    if (
        current.session != identity.session
        or current.status != "active"
        or row is None
        or row["generation"] != identity.generation
        or row["revision"] != identity.revision
        or row["state"] != state
    ):
        raise WorkingSetConflict("workspace owner, revision or execution state is stale")
    return current


def require_workspace_ready(repository, identity):
    with repository._read() as connection:
        return repository._require_workspace(connection, identity, "ready")


def observe_workspace_step(repository, identity):
    """Read one exact active owner/revision without replaying a workspace effect."""
    with repository._read() as connection:
        row = connection.execute(
            "SELECT state FROM workspace_heads WHERE session_json=? AND workspace_id=?",
            (canonical_json(identity.session.to_dict()).decode(), identity.workspace_id),
        ).fetchone()
        if row is None or row["state"] not in ("ready", "executing"):
            raise WorkingSetConflict("workspace step observation unavailable")
        repository._require_workspace(connection, identity, row["state"])
        return row["state"]


def begin_workspace_step(repository, identity):
    def operation(connection):
        repository._require_workspace(connection, identity, "ready")
        if identity.revision >= 2**63 - 1:
            raise ContractError("workspace revision exhausted")
        successor = WorkspaceIdentity(
            identity.session, identity.workspace_id, identity.generation, identity.revision + 1
        )
        connection.execute(
            "UPDATE workspace_heads SET revision=?,state='executing' "
            "WHERE session_json=? AND workspace_id=?",
            (
                successor.revision,
                canonical_json(identity.session.to_dict()).decode(),
                identity.workspace_id,
            ),
        )
        return successor

    return repository._mutate(operation)


def finish_workspace_step(repository, identity):
    def operation(connection):
        repository._require_workspace(connection, identity, "executing")
        connection.execute(
            "UPDATE workspace_heads SET state='ready' WHERE session_json=? AND workspace_id=?",
            (canonical_json(identity.session.to_dict()).decode(), identity.workspace_id),
        )

    return repository._mutate(operation)
