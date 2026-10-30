"""Prepare a verified restore candidate without activating copied execution authority."""

import json
import os
import time
from pathlib import Path

from ahc.domain.sessions import ContractError
from ahc.domain.turns import _sha256_hex

from .runtime_backup import LIMIT, _digest, _inspect_database, inspect_runtime_backup
from .runtime_restore_assessment import assess_database, assess_runtime_restore
from .working_db import require_real_directory, require_regular_file


def prepare_runtime_restore(backup_root, destination):
    backup_root = require_real_directory(Path(backup_root), allow_absent=False)
    manifest = inspect_runtime_backup(backup_root)
    assessment = assess_runtime_restore(backup_root)
    if assessment["backup_digest"] != manifest["sha256"]:
        raise ContractError("restore backup identity changed")
    destination = Path(destination).absolute()
    require_real_directory(destination.parent, allow_absent=False)
    if destination == backup_root or backup_root in destination.parents:
        raise ContractError("restore candidate must be outside backup root")
    destination.mkdir(exist_ok=False)
    # This filename is deliberately not the active runtime database filename.
    candidate = destination / "restore-candidate.sqlite"
    deadline, total = time.monotonic() + 30, 0
    with (backup_root / "runtime.sqlite").open("rb") as source, candidate.open("xb") as target:
        while block := source.read(1024 * 1024):
            total += len(block)
            if total > LIMIT or total > manifest["byte_count"] or time.monotonic() > deadline:
                raise ContractError("restore copy bound exceeded")
            target.write(block)
        target.flush()
        os.fsync(target.fileno())
    if (total != manifest["byte_count"] or _digest(candidate) != manifest["sha256"]
            or _inspect_database(candidate) != manifest["tables"]):
        raise ContractError("restore candidate differs from verified backup")
    receipt = {"schema": "ahc.runtime-restore-candidate.v1",
               "database": candidate.name, "sha256": manifest["sha256"],
               "byte_count": total, "state": "prepared", "activation_allowed": False,
               "assessment": assessment}
    with (destination / "restore.json").open("x", encoding="utf-8") as output:
        json.dump(receipt, output, sort_keys=True, indent=2)
        output.flush()
        os.fsync(output.fileno())
    return receipt


def inspect_runtime_restore(root, expected_backup_digest):
    _sha256_hex(expected_backup_digest)
    root = require_real_directory(Path(root), allow_absent=False)
    receipt_path, database = root / "restore.json", root / "restore-candidate.sqlite"
    require_regular_file(receipt_path, allow_absent=False)
    require_regular_file(database, allow_absent=False)
    if receipt_path.stat().st_size > 1048576 or database.stat().st_size > LIMIT:
        raise ContractError("restore candidate exceeds inspection bound")
    try:
        receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
        if (type(receipt) is not dict or set(receipt) != {
                "schema", "database", "sha256", "byte_count", "state",
                "activation_allowed", "assessment"}
                or receipt["schema"] != "ahc.runtime-restore-candidate.v1"
                or receipt["database"] != database.name or receipt["state"] != "prepared"
                or receipt["activation_allowed"] is not False
                or type(receipt["byte_count"]) is not int
                or receipt["byte_count"] != database.stat().st_size
                or receipt["sha256"] != expected_backup_digest
                or _digest(database) != expected_backup_digest):
            raise ValueError()
        tables = _inspect_database(database)
        if receipt["assessment"] != assess_database(database, expected_backup_digest, tables):
            raise ValueError()
        return receipt
    except (ValueError, TypeError, KeyError, UnicodeError):
        raise ContractError("restore candidate verification failed") from None
