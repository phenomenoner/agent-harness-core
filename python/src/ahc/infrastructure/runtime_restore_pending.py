"""Diagnose and explicitly accept a retained replacement outcome without replay."""

import json
import re

from ahc.domain.sessions import ContractError
from ahc.domain.turns import _sha256_hex

from .runtime_backup import LIMIT, _digest
from .runtime_restore_activation import _write_json
from .runtime_restore_guard import offline_restore_source
from .working_db import (
    WORKING_DB_FILENAME,
    _without_duplicates,
    require_real_directory,
    require_regular_file,
)


def _read_json(path):
    require_regular_file(path, allow_absent=False)
    if path.stat().st_size > 16384:
        raise ContractError("restore receipt exceeds bound")
    try:
        return json.loads(path.read_text(encoding="utf-8"), object_pairs_hook=_without_duplicates)
    except (ValueError, UnicodeError):
        raise ContractError("restore receipt invalid") from None


def _inspect(store, expected_attempt):
    if type(expected_attempt) is not str or not re.fullmatch(r"[0-9a-f]{32}", expected_attempt):
        raise ContractError("restore attempt identity invalid")
    pending = _read_json(store.root / "restore-pending.json")
    if (type(pending) is not dict or set(pending) != {
            "schema", "attempt", "source_digest", "candidate_digest", "backup_digest", "state"}
            or pending["schema"] != "ahc.runtime-restore-activation.v1"
            or pending["attempt"] != expected_attempt or pending["state"] != "pending"):
        raise ContractError("restore pending identity mismatch")
    for key in ("source_digest", "candidate_digest", "backup_digest"):
        _sha256_hex(pending[key])
    attempt = store.root / "restore-attempts" / expected_attempt
    require_real_directory(attempt, allow_absent=False)
    if _read_json(attempt / "intent.json") != pending:
        raise ContractError("restore intent differs from pending")
    active, rollback = store.root / WORKING_DB_FILENAME, attempt / "rollback.sqlite"
    for path in (active, rollback):
        require_regular_file(path, allow_absent=False)
        if path.stat().st_size > LIMIT:
            raise ContractError("restore database exceeds bound")
    if _digest(rollback) != pending["source_digest"]:
        raise ContractError("restore rollback identity mismatch")
    observed = _digest(active)
    outcome = ("candidate" if observed == pending["candidate_digest"] else
               "source" if observed == pending["source_digest"] else "unrecognized")
    return {**pending, "observed_digest": observed, "outcome": outcome}


def inspect_pending_restore(store, expected_attempt):
    with store.execution_lock(restore=True), store.writer_lock():
        return _inspect(store, expected_attempt)


def reconcile_pending_restore(store, supervisor_lock, expected_attempt, expected_digest, decision):
    if decision not in {"accept-candidate", "retain-source"}:
        raise ContractError("restore reconciliation decision invalid")
    _sha256_hex(expected_digest)
    with offline_restore_source(store, supervisor_lock, expected_digest, reconcile=True):
        observed = _inspect(store, expected_attempt)
        required = "candidate" if decision == "accept-candidate" else "source"
        if observed["observed_digest"] != expected_digest or observed["outcome"] != required:
            raise ContractError("restore outcome does not match explicit decision")
        resolution = {**observed, "state": "reconciled", "decision": decision}
        path = store.root / "restore-attempts" / expected_attempt / "resolution.json"
        if path.exists():
            if _read_json(path) != resolution:
                raise ContractError("restore resolution already differs")
        else:
            _write_json(path, resolution)
        # No database replacement or provider replay; retain all attempt/rollback bytes.
        (store.root / "restore-pending.json").unlink()
        return resolution
