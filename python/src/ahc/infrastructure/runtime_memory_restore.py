"""Stage both recovery databases while retaining all activation boundaries."""

import json
import os
import time
from pathlib import Path

from ahc.domain.sessions import ContractError

from .runtime_memory_backup import inspect_runtime_memory_backup
from .runtime_restore import prepare_runtime_restore
from .runtime_restore_transform import transform_runtime_restore
from .working_db import require_real_directory, require_regular_file


def prepare_runtime_memory_restore(
    backup_root,
    destination,
    *,
    expected_digest,
    expected_binding_digest,
    source_snapshots,
    source_database_digest,
    provider_inspect,
):
    backup_root = require_real_directory(Path(backup_root).absolute(), allow_absent=False)
    destination = Path(destination).absolute()
    require_real_directory(destination.parent, allow_absent=False)
    if destination == backup_root or backup_root in destination.parents:
        raise ContractError("joint restore must be outside backup")
    verified = inspect_runtime_memory_backup(
        backup_root,
        expected_digest=expected_digest,
        expected_binding_digest=expected_binding_digest,
        provider_inspect=provider_inspect,
    )
    memory = verified["memory"]
    database_name = ("memory.db" if memory.get("schema_version") == "ahc.memory-backup.v1"
                     else "memory.sqlite")
    if (
        type(memory.get("byte_count")) is not int
        or not 0 < memory["byte_count"] <= 2 * 1024**3
        or memory.get("database") != database_name
    ):
        raise ContractError("memory restore bounds invalid")
    destination.mkdir(exist_ok=False)
    prepare_runtime_restore(backup_root / "runtime", destination / "runtime-prepared")
    runtime = transform_runtime_restore(
        destination / "runtime-prepared",
        verified["runtime"]["sha256"],
        source_snapshots,
        source_database_digest,
        destination / "runtime-transformed",
    )
    copied = destination / "memory"
    copied.mkdir(exist_ok=False)
    original = backup_root / "memory" / database_name
    require_regular_file(original, allow_absent=False)
    total, deadline = 0, time.monotonic() + 30
    with original.open("rb") as source, (copied / database_name).open("xb") as target:
        while block := source.read(1024 * 1024):
            total += len(block)
            if total > memory["byte_count"] or time.monotonic() > deadline:
                raise ContractError("memory restore copy bound exceeded")
            target.write(block)
        target.flush()
        os.fsync(target.fileno())
    if total != memory["byte_count"]:
        raise ContractError("memory restore size mismatch")
    with (copied / "manifest.json").open("x", encoding="utf-8") as output:
        json.dump(memory, output, sort_keys=True)
    if provider_inspect(copied, expected_digest=memory["sha256"]) != memory:
        raise ContractError("copied memory restore differs")
    result = {
        "schema": "ahc.runtime-memory-restore.v1",
        "backup_digest": expected_digest,
        "runtime": runtime,
        "memory": memory,
        "activation_allowed": False,
        "original_binding_digest": expected_binding_digest,
        "state": "prepared",
        "binding_reconfiguration_required": True,
    }
    with (destination / "restore.json").open("x", encoding="utf-8") as output:
        json.dump(result, output, sort_keys=True, indent=2)
    return result
