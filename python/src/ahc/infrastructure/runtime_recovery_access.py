"""Verify recovery and ordinary-continuation edges before historical reads."""

import json

from ahc.domain.runtime_recovery import RecoveryTransition
from ahc.domain.sessions import ContractError
from ahc.domain.skill_values import canonical_json
from ahc.domain.working_sets import WorkingSetSnapshot, validate_working_set_transition

from .working_db import WorkingDatabaseError, decode_snapshot
from .working_repository import WorkingSetCorrupt


def recovery_allows(repository, connection, session, original):
    """Keep recovery-only callers restricted to verified restore transitions."""
    return _session_allows(repository, connection, session, original, ordinary=False)


def historical_session_allows(repository, connection, session, original):
    """Read ancestry may also follow validated ordinary continuation edges."""
    return _session_allows(repository, connection, session, original, ordinary=True)


def _session_allows(repository, connection, session, original, *, ordinary):
    if (session.ref.virtual_session_id != original.ref.virtual_session_id
            or session.agent_id != original.agent_id or session.route_id != original.route_id):
        return False
    current = repository._load_current(connection, session.ref.virtual_session_id)
    if current.session != session or current.status != "active":
        return False
    has_recovery = connection.execute("SELECT 1 FROM sqlite_master WHERE type='table' "
        "AND name='runtime_recovery_transitions'").fetchone() is not None
    edges = (verified_recovery_edges(connection, session.ref.virtual_session_id)
             if has_recovery else {})
    if ordinary:
        for successor, predecessor in verified_continuation_edges(
                connection, session.ref.virtual_session_id).items():
            if successor in edges and edges[successor] != predecessor:
                raise WorkingSetCorrupt("historical session ancestry conflicts")
            edges[successor] = predecessor
    cursor = session
    for _ in range(len(edges)):
        cursor = edges.get(cursor)
        if cursor is None:
            return False
        if cursor == original:
            return True
    return False


def verified_recovery_edges(connection, virtual_session_id):
    """Reconstruct persisted edges without requiring live repository ownership."""
    rows = connection.execute("SELECT * FROM runtime_recovery_transitions "
                              "WHERE virtual_session_id=?",
                              (virtual_session_id,)).fetchall()
    edges = {}
    for row in rows:
        try:
            data = json.loads(row["transition_json"])
            saved_row = connection.execute("SELECT snapshot_json FROM snapshots "
                "WHERE snapshot_digest=?", (data["saved_snapshot_digest"],)).fetchone()
            if saved_row is None:
                raise ValueError()
            saved = decode_snapshot(saved_row["snapshot_json"])
            source = WorkingSetSnapshot.from_dict(data["source_snapshot"])
            transition = RecoveryTransition(saved, source, data["backup_digest"],
                                            data["source_database_digest"])
            if (data != transition.to_dict() or row["transition_digest"] != transition.digest
                    or row["transition_json"] != canonical_json(data).decode()
                    or saved.session.ref.virtual_session_id != row["virtual_session_id"]):
                raise ValueError()
            recovered = transition.recovered
            proof = connection.execute("SELECT snapshot_json FROM snapshots "
                "WHERE snapshot_digest=?", (recovered.snapshot_digest,)).fetchone()
            if proof is None or decode_snapshot(proof["snapshot_json"]) != recovered:
                raise ValueError()
            if recovered.session in edges and edges[recovered.session] != saved.session:
                raise ValueError()
            edges[recovered.session] = saved.session
        except (ValueError, TypeError, KeyError, ContractError, WorkingDatabaseError):
            raise WorkingSetCorrupt("recovery content provenance corrupted") from None
    return edges


def verified_continuation_edges(connection, virtual_session_id):
    """Only validated, idle, exact-domain snapshot transitions confer read ancestry."""
    edges = {}
    rows = connection.execute(
        "SELECT snapshot_json FROM snapshots WHERE virtual_session_id=? AND revision=1",
        (virtual_session_id,),
    ).fetchall()
    for row in rows:
        successor = decode_snapshot(row[0])
        if successor.continuation_reason not in {"compaction", "backend-rollover"}:
            continue
        saved = connection.execute(
            "SELECT snapshot_json FROM snapshots WHERE snapshot_digest=?",
            (successor.predecessor_digest,),
        ).fetchone()
        if saved is None:
            raise WorkingSetCorrupt("continuation predecessor snapshot missing")
        predecessor = decode_snapshot(saved[0])
        try:
            validate_working_set_transition(predecessor, successor)
            if (predecessor.active_turn_id is not None or successor.active_turn_id is not None
                    or successor.session == predecessor.session):
                raise ContractError("continuation must cross idle generations")
        except ContractError:
            raise WorkingSetCorrupt("continuation read provenance corrupted") from None
        edges[successor.session] = predecessor.session
    return edges
