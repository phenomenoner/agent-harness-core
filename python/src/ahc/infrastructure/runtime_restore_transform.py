"""Transform a prepared copy; source snapshots are inputs, never a live-source fence."""

import json
import os
import sqlite3
from contextlib import closing
from pathlib import Path

from ahc.domain.runtime_recovery import RecoveryTransition
from ahc.domain.sessions import ContractError
from ahc.domain.skill_values import canonical_json
from ahc.domain.turns import _sha256_hex
from ahc.domain.working_sets import WorkingSetSnapshot

from .runtime_backup import LIMIT, _digest, _inspect_database
from .runtime_restore import inspect_runtime_restore
from .working_db import decode_snapshot, insert_snapshot, require_real_directory, snapshot_pointer


def transform_runtime_restore(candidate_root, expected_backup_digest, source_snapshots,
                              source_database_digest, destination, *, fail_before_commit=False):
    _sha256_hex(source_database_digest)
    if (type(source_snapshots) is not tuple or not 1 <= len(source_snapshots) <= 128
            or any(type(item) is not WorkingSetSnapshot for item in source_snapshots)):
        raise ContractError("restore source snapshots invalid")
    source = {item.session.ref.virtual_session_id: item for item in source_snapshots}
    if len(source) != len(source_snapshots):
        raise ContractError("duplicate restore source session")
    receipt = inspect_runtime_restore(candidate_root, expected_backup_digest)
    candidate_root = Path(candidate_root).absolute()
    destination = Path(destination).absolute()
    require_real_directory(destination.parent, allow_absent=False)
    if destination == candidate_root or candidate_root in destination.parents:
        raise ContractError("transformed candidate must be separate")
    destination.mkdir(exist_ok=False)
    database = destination / "recovered-candidate.sqlite"
    total = 0
    original_path = candidate_root / receipt["database"]
    with original_path.open("rb") as original, database.open("xb") as output:
        while block := original.read(1024 * 1024):
            total += len(block)
            if total > LIMIT or total > receipt["byte_count"]:
                raise ContractError("restore transformation copy exceeds bound")
            output.write(block)
        output.flush()
        os.fsync(output.fileno())
    if _digest(database) != expected_backup_digest:
        raise ContractError("prepared candidate changed during copy")
    with closing(sqlite3.connect(database)) as connection:
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("BEGIN IMMEDIATE")
        try:
            rows = connection.execute("SELECT s.snapshot_json FROM current_working_sets c "
                "JOIN snapshots s ON s.snapshot_digest=c.snapshot_digest").fetchall()
            saved = [decode_snapshot(row[0]) for row in rows]
            if {item.session.ref.virtual_session_id for item in saved} != set(source):
                raise ContractError("restore source and backup session sets differ")
            connection.execute("CREATE TABLE IF NOT EXISTS runtime_recovery_transitions ("
                "virtual_session_id TEXT NOT NULL, transition_digest TEXT PRIMARY KEY, "
                "transition_json TEXT NOT NULL)")
            transitions = []
            for snapshot in saved:
                name = snapshot.session.ref.virtual_session_id
                current = source[name]
                if (snapshot.session.agent_id != current.session.agent_id
                        or snapshot.session.route_id != current.session.route_id):
                    raise ContractError("restore owner mismatch")
                if snapshot.status in {"completed", "cancelled"}:
                    continue
                transition = RecoveryTransition(snapshot, current, expected_backup_digest,
                                                source_database_digest)
                recovered = transition.recovered
                insert_snapshot(connection, recovered)
                pointer = snapshot_pointer(recovered)
                connection.execute("UPDATE current_working_sets SET generation=?,revision=?,"
                    "agent_id=?,route_id=?,snapshot_digest=? WHERE virtual_session_id=?",
                    (*pointer[1:], pointer[0]))
                connection.execute("INSERT INTO runtime_recovery_transitions VALUES(?,?,?)",
                    (name, transition.digest, canonical_json(transition.to_dict()).decode()))
                transitions.append(transition.to_dict())
            if fail_before_commit:
                raise ContractError("restore transformation interrupted before commit")
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
    _inspect_database(database)
    result = {"schema": "ahc.runtime-restore-transformation.v1", "database": database.name,
              "backup_digest": expected_backup_digest,
              "source_database_digest": source_database_digest,
              "sha256": _digest(database), "state": "transformed", "activation_allowed": False,
              "transitions": transitions}
    with (destination / "transformation.json").open("x", encoding="utf-8") as output:
        json.dump(result, output, sort_keys=True, indent=2)
        output.flush()
        os.fsync(output.fileno())
    return result
