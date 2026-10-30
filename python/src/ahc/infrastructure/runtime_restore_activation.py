"""Explicit offline source replacement with retained custody and no automatic replay."""

import json
import os
import sqlite3
import time
import uuid
from contextlib import closing
from pathlib import Path

from ahc.domain.sessions import ContractError

from .runtime_backup import LIMIT, _digest
from .runtime_restore_guard import offline_restore_source
from .runtime_restore_inspection import inspect_transformed_restore
from .working_db import WORKING_DB_FILENAME, require_real_directory


class RestoreIndeterminate(RuntimeError):
    """Pending marker and retained attempt require explicit reconciliation."""

    def __init__(self, message, attempt):
        super().__init__(message)
        self.attempt = attempt


def _write_json(path, value):
    with path.open("x", encoding="utf-8") as stream:
        json.dump(value, stream, sort_keys=True, indent=2)
        stream.flush()
        os.fsync(stream.fileno())


def _copy(source, target, digest):
    deadline, size = time.monotonic() + 30, 0
    with source.open("rb") as incoming, target.open("xb") as outgoing:
        while block := incoming.read(1048576):
            size += len(block)
            if size > LIMIT or time.monotonic() > deadline:
                raise ContractError("activation copy exceeds bound")
            outgoing.write(block)
        outgoing.flush()
        os.fsync(outgoing.fileno())
    if _digest(target) != digest:
        raise ContractError("activation copy identity mismatch")


def activate_runtime_restore(store, supervisor_lock, candidate_root, candidate_digest,
                             backup_digest, source_digest, *, fail_after_replace=False):
    """Development API: caller must explicitly authorize the named offline runtime effect."""
    candidate_root = require_real_directory(Path(candidate_root), allow_absent=False)
    inspection = inspect_transformed_restore(candidate_root, candidate_digest,
                                            backup_digest, source_digest)
    with offline_restore_source(store, supervisor_lock, source_digest) as snapshots:
        receipt = json.loads((candidate_root / "transformation.json").read_text(encoding="utf-8"))
        current = {item.session.ref.virtual_session_id: item for item in snapshots}
        candidate_db = candidate_root / "recovered-candidate.sqlite"
        with closing(sqlite3.connect(candidate_db.absolute().as_uri() + "?mode=ro",
                                     uri=True)) as connection:
            names = {row[0] for row in connection.execute(
                "SELECT virtual_session_id FROM current_working_sets")}
        if names != set(current):
            raise ContractError("activation source session set mismatch")
        for transition in receipt["transitions"]:
            name = transition["source_session"]["ref"]["virtual_session_id"]
            if current[name].to_dict() != transition["source_snapshot"]:
                raise ContractError("activation source snapshot mismatch")
        # Keep copies and intent below the exact guarded root on the same filesystem.
        attempts = store.root / "restore-attempts"
        require_real_directory(attempts, allow_absent=True)
        attempts.mkdir(exist_ok=True)
        attempt = attempts / uuid.uuid4().hex
        attempt.mkdir(exist_ok=False)
        active = store.root / WORKING_DB_FILENAME
        _copy(active, attempt / "rollback.sqlite", source_digest)
        _copy(candidate_root / "recovered-candidate.sqlite", attempt / "replacement.sqlite",
              candidate_digest)
        intent = {"schema": "ahc.runtime-restore-activation.v1", "attempt": attempt.name,
                  "source_digest": source_digest, "candidate_digest": candidate_digest,
                  "backup_digest": backup_digest, "state": "pending"}
        _write_json(attempt / "intent.json", intent)
        pending = store.root / "restore-pending.json"
        _write_json(pending, intent)
        try:
            if _digest(active) != source_digest:
                raise ContractError("activation source changed before replacement")
            os.replace(attempt / "replacement.sqlite", active)
            if fail_after_replace:
                raise OSError("injected replacement response loss")
            if _digest(active) != candidate_digest:
                raise ContractError("activation readback differs")
            result = {**intent, "state": "activated",
                      "recovered_sessions": inspection["recovered_sessions"]}
            _write_json(attempt / "completed.json", result)
            pending.unlink()
            return result
        except BaseException as exc:
            raise RestoreIndeterminate(
                "restore pending; inspect retained attempt before resuming", attempt.name) from exc
