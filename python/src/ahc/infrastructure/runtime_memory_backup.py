"""Coordinated backup under AHC write exclusion; never runtime activation."""

import hashlib
import json
import re
from pathlib import Path

from ahc.domain.sessions import ContractError

from .runtime_backup import _backup_locked, inspect_runtime_backup
from .working_db import require_real_directory, require_regular_file


def require_reconciled_memory_effects(connection):
    """Check all admitted memory writers under the caller's write exclusion.

    This is read-only and does not initialize optional operation tables. Ordinary
    runtime-only evidence backups may preserve unknown custody; a coordinated
    runtime/provider snapshot or restore cannot claim a settled boundary from it.
    """
    for table in ("memory_store_proposals", "memory_revision_operations"):
        exists = connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)
        ).fetchone()
        if exists and connection.execute(
            f"SELECT 1 FROM {table} WHERE state IN ('in-flight','unknown') LIMIT 1"
        ).fetchone():
            raise ContractError("memory effects must be reconciled before joint backup or restore")


def backup_runtime_memory(store, destination, *, binding_digest, provider_backup, provider_inspect):
    if type(binding_digest) is not str or not re.fullmatch(r"[0-9a-f]{64}", binding_digest):
        raise ContractError("joint backup binding digest invalid")
    destination = Path(destination).absolute()
    require_real_directory(destination.parent, allow_absent=False)
    if destination == store.root or store.root in destination.parents:
        raise ContractError("joint backup must be outside runtime")
    with store.execution_lock(), store.writer_lock(), store._read() as source:
        require_reconciled_memory_effects(source)
        destination.mkdir(exist_ok=False)
        memory = provider_backup(destination / "memory")
        if (
            type(memory) is not dict
            or type(memory.get("sha256")) is not str
            or memory.get("activation_allowed") is not False
        ):
            raise ContractError("provider backup receipt invalid")
        checked = provider_inspect(destination / "memory", expected_digest=memory["sha256"])
        if checked != memory:
            raise ContractError("provider backup readback mismatch")
        runtime = _backup_locked(source, destination / "runtime")
        result = {
            "schema": "ahc.runtime-memory-backup.v2",
            "runtime": runtime,
            "memory": memory,
            "binding_digest": binding_digest,
            "activation_allowed": False,
            "scope": "one-ahc-runtime-and-bound-provider-snapshot",
        }
        with (destination / "manifest.json").open("x", encoding="utf-8") as output:
            json.dump(result, output, sort_keys=True, indent=2)
        return result


def inspect_runtime_memory_backup(
    destination, *, expected_digest, expected_binding_digest, provider_inspect
):
    root = require_real_directory(Path(destination).absolute(), allow_absent=False)
    path = root / "manifest.json"
    require_regular_file(path, allow_absent=False)
    if (
        type(expected_digest) is not str
        or not re.fullmatch(r"[0-9a-f]{64}", expected_digest)
        or path.stat().st_size > 131072
    ):
        raise ContractError("joint manifest bound invalid")
    raw = path.read_bytes()
    if hashlib.sha256(raw).hexdigest() != expected_digest:
        raise ContractError("joint manifest digest mismatch")

    def unique(pairs):
        value = {}
        for key, item in pairs:
            if key in value:
                raise ContractError("joint manifest duplicate key")
            value[key] = item
        return value

    value = json.loads(raw, object_pairs_hook=unique)
    if (
        type(value) is not dict
        or set(value)
        != {"schema", "runtime", "memory", "activation_allowed", "scope", "binding_digest"}
        or value["schema"] != "ahc.runtime-memory-backup.v2"
        or value["scope"] != "one-ahc-runtime-and-bound-provider-snapshot"
        or type(expected_binding_digest) is not str
        or not re.fullmatch(r"[0-9a-f]{64}", expected_binding_digest)
        or value["binding_digest"] != expected_binding_digest
        or value["activation_allowed"] is not False
        or type(value["memory"]) is not dict
        or type(value["memory"].get("sha256")) is not str
    ):
        raise ContractError("joint manifest shape invalid")
    if inspect_runtime_backup(root / "runtime") != value["runtime"]:
        raise ContractError("joint runtime receipt mismatch")
    memory = provider_inspect(root / "memory", expected_digest=value["memory"]["sha256"])
    if memory != value["memory"]:
        raise ContractError("joint memory receipt mismatch")
    return value
