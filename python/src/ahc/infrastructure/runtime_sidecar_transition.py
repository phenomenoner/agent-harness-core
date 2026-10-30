"""Guarded normalization of an offline original bundle, with resumable custody."""

import os
import re
import uuid
from pathlib import Path

from ahc.domain.sessions import ContractError
from ahc.domain.skill_values import digest_json
from ahc.domain.turns import _sha256_hex

from .runtime_backup import _digest, _inspect_database
from .runtime_restore_activation import _copy, _write_json
from .runtime_restore_guard import offline_restore_ownership
from .runtime_restore_pending import _read_json
from .runtime_sidecar_capture import SUFFIXES
from .runtime_sidecar_inspection import inspect_sidecar_capture
from .working_db import WORKING_DB_FILENAME, require_real_directory, require_regular_file


def _inventory(root):
    result = {}
    for suffix in SUFFIXES:
        path = root / (WORKING_DB_FILENAME + suffix)
        require_regular_file(path, allow_absent=True)
        if path.exists():
            result[path.name] = {"byte_count": path.stat().st_size, "sha256": _digest(path)}
    return result


def normalize_runtime_source(store, supervisor_lock, capture_root, capture_digest,
                             normalized_digest):
    capture_root = Path(capture_root).absolute()
    inspect_sidecar_capture(capture_root, capture_digest, normalized_digest)
    with offline_restore_ownership(store, supervisor_lock):
        files = _read_json(capture_root / "capture.json")["files"]
        if _inventory(store.root) != files or digest_json(files) != capture_digest:
            raise ContractError("sidecar source no longer matches capture")
        base = store.root / "sidecar-attempts"
        require_real_directory(base, allow_absent=True)
        base.mkdir(exist_ok=True)
        attempt = base / uuid.uuid4().hex
        attempt.mkdir()
        (attempt / "original").mkdir()
        _copy(capture_root / "normalized.sqlite", attempt / "replacement.sqlite", normalized_digest)
        intent = {"schema": "ahc.sidecar-transition.v1", "attempt": attempt.name,
                  "files": files, "capture_digest": capture_digest,
                  "normalized_digest": normalized_digest}
        _write_json(attempt / "intent.json", intent)
        _write_json(store.root / "sidecar-pending.json", intent)
        return _finish(store, intent)


def resume_sidecar_transition(store, supervisor_lock, expected_attempt):
    if type(expected_attempt) is not str or not re.fullmatch(r"[0-9a-f]{32}", expected_attempt):
        raise ContractError("sidecar attempt invalid")
    with offline_restore_ownership(store, supervisor_lock, reconcile=True):
        intent = _read_json(store.root / "sidecar-pending.json")
        if (type(intent) is not dict or set(intent) != {
                "schema", "attempt", "files", "capture_digest", "normalized_digest"}
                or intent["schema"] != "ahc.sidecar-transition.v1"
                or intent["attempt"] != expected_attempt):
            raise ContractError("sidecar pending identity mismatch")
        _sha256_hex(intent["normalized_digest"])
        files = intent["files"]
        if (type(files) is not dict or WORKING_DB_FILENAME not in files
                or not set(files) <= {WORKING_DB_FILENAME + s for s in SUFFIXES}
                or digest_json(files) != intent["capture_digest"]):
            raise ContractError("sidecar pending inventory invalid")
        return _finish(store, intent)


def _finish(store, intent):
    attempt = store.root / "sidecar-attempts" / intent["attempt"]
    require_real_directory(attempt / "original", allow_absent=False)
    if _read_json(attempt / "intent.json") != intent:
        raise ContractError("sidecar retained intent differs")
    files = intent["files"]
    for path in store.root.iterdir():
        if path.name in {WORKING_DB_FILENAME + s for s in SUFFIXES} and path.name not in files:
            raise ContractError("unexpected source sidecar appeared")
    # Retire sidecars first, then database. The pending marker prevents SQLite
    # from opening a partially retired bundle through repository entrypoints.
    for name in sorted(files, key=lambda name: name == WORKING_DB_FILENAME):
        source, retained = store.root / name, attempt / "original" / name
        require_regular_file(source, allow_absent=True)
        require_regular_file(retained, allow_absent=True)
        digest = files[name]["sha256"]
        if retained.exists():
            if _digest(retained) != digest:
                raise ContractError("retained sidecar bytes changed")
            if source.exists() and not (name == WORKING_DB_FILENAME
                    and _digest(source) == intent["normalized_digest"]):
                raise ContractError("unexpected replacement at source path")
        else:
            if not source.exists() or _digest(source) != digest:
                raise ContractError("source sidecar identity unavailable")
            os.rename(source, retained)
    active = store.root / WORKING_DB_FILENAME
    if not active.exists():
        replacement = attempt / "replacement.sqlite"
        require_regular_file(replacement, allow_absent=False)
        if _digest(replacement) != intent["normalized_digest"]:
            raise ContractError("normalized replacement changed")
        os.replace(replacement, active)
    if _digest(active) != intent["normalized_digest"]:
        raise ContractError("normalized source readback differs")
    _inspect_database(active)
    result = {"schema": "ahc.sidecar-transition-result.v1", "attempt": intent["attempt"],
              "capture_digest": intent["capture_digest"], "sha256": intent["normalized_digest"],
              "state": "normalized", "restored_generation": False}
    completed = attempt / "completed.json"
    if completed.exists():
        if _read_json(completed) != result:
            raise ContractError("sidecar completion differs")
    else:
        _write_json(completed, result)
    (store.root / "sidecar-pending.json").unlink()
    return result
