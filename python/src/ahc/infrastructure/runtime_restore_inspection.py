"""Read-only, caller-pinned verification of a transformed recovery candidate."""

import json
import sqlite3
from contextlib import closing
from pathlib import Path

from ahc.domain.sessions import ContractError, VirtualSessionIdentity
from ahc.domain.skill_values import canonical_json, digest_json
from ahc.domain.turns import _sha256_hex

from .runtime_backup import LIMIT, _digest, _inspect_database
from .runtime_recovery_access import verified_recovery_edges
from .working_db import decode_snapshot, require_real_directory, require_regular_file


def inspect_transformed_restore(root, expected_digest, expected_backup_digest,
                                expected_source_digest):
    for digest in (expected_digest, expected_backup_digest, expected_source_digest):
        _sha256_hex(digest)
    root = require_real_directory(Path(root), allow_absent=False)
    receipt_path, database = root / "transformation.json", root / "recovered-candidate.sqlite"
    for path in (receipt_path, database):
        require_regular_file(path, allow_absent=False)
    if receipt_path.stat().st_size > 16 * 1048576 or database.stat().st_size > LIMIT:
        raise ContractError("transformed candidate exceeds inspection bound")
    try:
        receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
        if (type(receipt) is not dict or set(receipt) != {
                "schema", "database", "backup_digest", "source_database_digest", "sha256",
                "state", "activation_allowed", "transitions"}
                or receipt["schema"] != "ahc.runtime-restore-transformation.v1"
                or receipt["database"] != database.name or receipt["state"] != "transformed"
                or receipt["activation_allowed"] is not False
                or receipt["sha256"] != expected_digest
                or receipt["backup_digest"] != expected_backup_digest
                or receipt["source_database_digest"] != expected_source_digest
                or type(receipt["transitions"]) is not list
                or len(receipt["transitions"]) > 128
                or _digest(database) != expected_digest):
            raise ValueError()
        _inspect_database(database)
        uri = database.absolute().as_uri() + "?mode=ro"
        with closing(sqlite3.connect(uri, uri=True)) as connection:
            connection.row_factory = sqlite3.Row
            current = {}
            for row in connection.execute("SELECT s.snapshot_json FROM current_working_sets c "
                    "JOIN snapshots s ON s.snapshot_digest=c.snapshot_digest"):
                snapshot = decode_snapshot(row["snapshot_json"])
                if snapshot.status in {"active", "interrupted"}:
                    current[snapshot.session.ref.virtual_session_id] = snapshot
            seen = set()
            for transition in receipt["transitions"]:
                recovered = VirtualSessionIdentity.from_dict(transition["recovered_session"])
                saved = VirtualSessionIdentity.from_dict(transition["saved_session"])
                name = recovered.ref.virtual_session_id
                if name in seen:
                    raise ValueError()
                seen.add(name)
                row = connection.execute("SELECT transition_json FROM runtime_recovery_transitions "
                    "WHERE transition_digest=?", (digest_json(transition),)).fetchone()
                if (row is None or row[0] != canonical_json(transition).decode()
                        or transition["backup_digest"] != expected_backup_digest
                        or transition["source_database_digest"] != expected_source_digest
                        or current[name].session != recovered
                        or current[name].snapshot_digest != transition["recovered_snapshot_digest"]
                        or verified_recovery_edges(connection, name).get(recovered) != saved):
                    raise ValueError()
            if seen != set(current):
                raise ValueError()
        if _digest(database) != expected_digest:
            raise ValueError()
        return {"schema": "ahc.runtime-restore-inspection.v1", "sha256": expected_digest,
                "backup_digest": expected_backup_digest,
                "source_database_digest": expected_source_digest,
                "recovered_sessions": len(seen), "state": "verified-transformed",
                "activation_allowed": False}
    except (ValueError, TypeError, KeyError, UnicodeError, sqlite3.Error):
        raise ContractError("transformed candidate verification failed") from None
