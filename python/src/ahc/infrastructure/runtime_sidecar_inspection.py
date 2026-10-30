"""Caller-pinned verification of preserved sidecar bundles and normalized output."""

from pathlib import Path

from ahc.domain.sessions import ContractError
from ahc.domain.skill_values import digest_json
from ahc.domain.turns import _sha256_hex

from .runtime_backup import LIMIT, _digest, _inspect_database
from .runtime_restore_pending import _read_json
from .runtime_sidecar_capture import SUFFIXES
from .working_db import WORKING_DB_FILENAME, require_real_directory, require_regular_file


def inspect_sidecar_capture(root, expected_capture_digest, expected_normalized_digest):
    _sha256_hex(expected_capture_digest)
    _sha256_hex(expected_normalized_digest)
    root = require_real_directory(Path(root), allow_absent=False)
    raw = require_real_directory(root / "raw", allow_absent=False)
    capture, normalized = _read_json(root / "capture.json"), _read_json(root / "normalization.json")
    if (type(capture) is not dict or set(capture) != {"schema", "files"}
            or capture["schema"] != "ahc.sidecar-capture.v1"
            or type(normalized) is not dict or set(normalized) != {
                "schema", "files", "capture_digest", "database", "sha256", "activation_allowed"}
            or normalized["schema"] != "ahc.sidecar-normalization.v1"
            or normalized["database"] != "normalized.sqlite"
            or normalized["activation_allowed"] is not False
            or normalized["capture_digest"] != expected_capture_digest
            or normalized["sha256"] != expected_normalized_digest
            or normalized["files"] != capture["files"]):
        raise ContractError("sidecar receipt binding invalid")
    inventory = capture["files"]
    allowed = {WORKING_DB_FILENAME + suffix for suffix in SUFFIXES}
    if (type(inventory) is not dict or WORKING_DB_FILENAME not in inventory
            or not set(inventory) <= allowed or digest_json(inventory) != expected_capture_digest
            or {path.name for path in raw.iterdir()} != set(inventory)):
        raise ContractError("sidecar inventory binding invalid")
    total = 0
    for name, item in inventory.items():
        if (type(item) is not dict or set(item) != {"byte_count", "sha256"}
                or type(item["byte_count"]) is not int or item["byte_count"] < 0):
            raise ContractError("sidecar inventory entry invalid")
        _sha256_hex(item["sha256"])
        total += item["byte_count"]
        path = raw / name
        require_regular_file(path, allow_absent=False)
        if total > LIMIT or path.stat().st_size != item["byte_count"]:
            raise ContractError("sidecar capture size mismatch")
        if _digest(path) != item["sha256"]:
            raise ContractError("sidecar capture bytes changed")
    database = root / "normalized.sqlite"
    require_regular_file(database, allow_absent=False)
    if database.stat().st_size > LIMIT or _digest(database) != expected_normalized_digest:
        raise ContractError("normalized database identity mismatch")
    if any((root / ("normalized.sqlite" + suffix)).exists() for suffix in SUFFIXES[1:]):
        raise ContractError("normalized output has sidecars")
    _inspect_database(database)
    return {"schema": "ahc.sidecar-inspection.v1", "capture_digest": expected_capture_digest,
            "normalized_digest": expected_normalized_digest, "files": len(inventory),
            "state": "verified", "activation_allowed": False}
