"""Bounded offline diagnosis; never opens SQLite or resumes a pending transition."""

import re

from ahc.domain.sessions import ContractError
from ahc.domain.skill_values import digest_json
from ahc.domain.turns import _sha256_hex

from .runtime_backup import LIMIT, _digest
from .runtime_restore_guard import offline_restore_ownership
from .runtime_restore_pending import _read_json
from .runtime_sidecar_capture import SUFFIXES
from .working_db import WORKING_DB_FILENAME, require_real_directory, require_regular_file


def inspect_sidecar_transition(store, supervisor_lock, *, expected_attempt=None):
    if expected_attempt is not None and (
            type(expected_attempt) is not str
            or not re.fullmatch(r"[0-9a-f]{32}", expected_attempt)):
        raise ContractError("sidecar attempt invalid")
    with offline_restore_ownership(store, supervisor_lock, reconcile=True):
        marker = store.root / "sidecar-pending.json"
        require_regular_file(marker, allow_absent=True)
        if not marker.exists():
            if expected_attempt is not None:
                raise ContractError("requested sidecar attempt is not pending")
            return {"state": "no-pending-transition", "resumed": False}
        intent = _read_json(marker)
        if (type(intent) is not dict or set(intent) != {
                "schema", "attempt", "files", "capture_digest", "normalized_digest"}
                or intent["schema"] != "ahc.sidecar-transition.v1"
                or type(intent["attempt"]) is not str
                or not re.fullmatch(r"[0-9a-f]{32}", intent["attempt"])
                or expected_attempt not in (None, intent["attempt"])):
            raise ContractError("sidecar pending identity mismatch")
        _sha256_hex(intent["normalized_digest"])
        files = intent["files"]
        if (type(files) is not dict or WORKING_DB_FILENAME not in files
                or not set(files) <= {WORKING_DB_FILENAME + suffix for suffix in SUFFIXES}
                or digest_json(files) != intent["capture_digest"]):
            raise ContractError("sidecar pending inventory invalid")
        total = 0
        for metadata in files.values():
            if (type(metadata) is not dict or set(metadata) != {"byte_count", "sha256"}
                    or type(metadata["byte_count"]) is not int or metadata["byte_count"] < 0):
                raise ContractError("sidecar pending file metadata invalid")
            total += metadata["byte_count"]
            _sha256_hex(metadata["sha256"])
        if total > LIMIT:
            raise ContractError("sidecar pending inventory exceeds bound")
        attempt = store.root / "sidecar-attempts" / intent["attempt"]
        require_real_directory(attempt / "original", allow_absent=False)
        if _read_json(attempt / "intent.json") != intent:
            raise ContractError("sidecar retained intent differs")
        budget = 0

        def fingerprint(path):
            nonlocal budget
            require_regular_file(path, allow_absent=True)
            if not path.exists():
                return None
            budget += path.stat().st_size
            if budget > 3 * LIMIT:
                raise ContractError("sidecar diagnosis bytes exceed bound")
            return _digest(path)

        observed = {}
        conflict = False
        active = None
        for name in sorted({WORKING_DB_FILENAME + suffix for suffix in SUFFIXES}):
            source = fingerprint(store.root / name)
            if name == WORKING_DB_FILENAME:
                active = source
            retained = fingerprint(attempt / "original" / name)
            metadata = files.get(name)
            normalized = name == WORKING_DB_FILENAME and source == intent["normalized_digest"]
            if metadata is None:
                valid = source is None and retained is None
            else:
                valid = ((retained == metadata["sha256"] and (source is None or normalized))
                         or (retained is None and source == metadata["sha256"]))
            conflict |= not valid
            observed[name] = {"source_present": source is not None,
                              "retained_present": retained is not None, "consistent": valid}
        replacement = fingerprint(attempt / "replacement.sqlite")
        installed = active == intent["normalized_digest"]
        conflict |= not installed and replacement != intent["normalized_digest"]
        return {"state": "conflict" if conflict else "pending",
                "attempt": intent["attempt"], "files": observed, "normalized_installed": installed,
                "replacement_verified": replacement == intent["normalized_digest"],
                "resumed": False, "next_command": None if conflict else "runtime-sidecar-resume"}
