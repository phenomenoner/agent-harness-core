"""Create an explicit successor binding after verifying both restore candidates."""

import hashlib
import json
import os
from dataclasses import asdict
from pathlib import Path

from ahc.domain.sessions import ContractError
from ahc.infrastructure.runtime_restore_inspection import inspect_transformed_restore
from ahc.infrastructure.working_db import require_real_directory, require_regular_file

from .native_memory_binding import parse_native_memory_binding


def prepare_memory_binding(root, destination, *, expected_digest, binding, registry,
                           provider_inspect):
    root = require_real_directory(Path(root).absolute(), allow_absent=False)
    receipt_path = root / "restore.json"
    require_regular_file(receipt_path, allow_absent=False)
    if receipt_path.stat().st_size > 16 * 1024**2:
        raise ContractError("joint restore receipt too large")
    raw = receipt_path.read_bytes()
    if hashlib.sha256(raw).hexdigest() != expected_digest:
        raise ContractError("joint restore receipt changed")
    receipt = json.loads(raw)
    if (type(receipt) is not dict or receipt.get("schema") != "ahc.runtime-memory-restore.v1"
            or receipt.get("original_binding_digest") != binding.digest
            or receipt.get("state") != "prepared"
            or receipt.get("activation_allowed") is not False):
        raise ContractError("joint restore binding mismatch")
    runtime, memory = receipt["runtime"], receipt["memory"]
    inspect_transformed_restore(root / "runtime-transformed", runtime["sha256"],
                                runtime["backup_digest"], runtime["source_database_digest"])
    if provider_inspect(root / "memory", expected_digest=memory["sha256"]) != memory:
        raise ContractError("restored memory changed")
    row = asdict(binding)
    row.pop("resource_id")
    from .mempalace_memory_binding import MemPalaceMemoryBinding
    if isinstance(binding, MemPalaceMemoryBinding):
        from ahc.adapters.mempalace_memory_worker import LazyEmbedding
        from ahc.infrastructure.memory_journal import MemoryJournal
        restored_root = root / "memory-restored"
        MemoryJournal.restore(root / "memory", restored_root,
                              embedder=LazyEmbedding(binding.model_root), projection=None)
        row["root"] = str(restored_root)
        row["palace_id"] = binding.palace_id[:40] + "-restore-" + expected_digest[:15]
        schema = "ahc.mempalace-memory.bindings.v1"
    else:
        row["database"] = str(root / "memory" / "memory.sqlite")
        schema = "ahc.native-memory.bindings.v1" if os.name == "nt" else "ahc.native-memory.bindings.v2"
    document = {"schema_version": schema,
                "bindings": {binding.resource_id: row}}
    updated = parse_native_memory_binding(document, registry=registry, agent_id=binding.agent_id)
    destination = Path(destination).absolute()
    require_real_directory(destination.parent, allow_absent=False)
    with destination.open("x", encoding="utf-8") as output:
        json.dump(document, output, sort_keys=True, indent=2)
    return {"binding_file": str(destination), "binding_digest": updated.digest,
            "original_binding_digest": binding.digest, "activation_allowed": False}


def recovered_memory_reference(original, reference, registry):
    from .foreground_config_native import parse_trusted_foreground_config
    from .foreground_native_memory import read_native_binding

    if "native_memory" not in original.to_dict():
        if reference is not None:
            raise ContractError("recovery cannot introduce a new memory binding")
        return None
    if reference is None:
        raise ContractError("recovery requires explicit successor memory binding")
    prior = read_native_binding(original, registry)
    document = original.to_dict()
    document["native_memory"] = reference
    updated = read_native_binding(parse_trusted_foreground_config(document), registry)
    old_fields, new_fields = asdict(prior), asdict(updated)
    from .mempalace_memory_binding import MemPalaceMemoryBinding
    if isinstance(prior, MemPalaceMemoryBinding) != isinstance(updated, MemPalaceMemoryBinding):
        raise ContractError("recovery cannot change memory provider kind")
    if isinstance(prior, MemPalaceMemoryBinding) and isinstance(updated, MemPalaceMemoryBinding):
        old_path, new_path = old_fields.pop("root"), new_fields.pop("root")
        old_palace, new_palace = old_fields.pop("palace_id"), new_fields.pop("palace_id")
        if old_palace == new_palace:
            raise ContractError("memory restore requires a distinct search projection")
        database = Path(new_path) / "memory.db"
    else:
        old_path, new_path = old_fields.pop("database"), new_fields.pop("database")
        database = Path(new_path)
    if old_fields != new_fields or Path(old_path).resolve() == Path(new_path).resolve():
        raise ContractError("recovery memory binding must only change database")
    require_regular_file(database, allow_absent=False)
    return dict(reference)


def recovery_memory_options(args):
    from ahc.domain.profile_registry import AgentProfileRegistry

    values = (args.native_memory_binding_file, args.native_memory_binding_digest,
              args.profile_registry)
    if all(value is None for value in values):
        return {}
    if any(value is None for value in values):
        raise ContractError("recovery memory options must be supplied together")
    if args.profile_registry.stat().st_size > 1024**2:
        raise ContractError("recovery profile registry exceeds bound")
    registry = AgentProfileRegistry.from_dict(
        json.loads(args.profile_registry.read_text(encoding="utf-8")))
    return {"profile_registry": registry, "native_memory_reference": {
        "binding_file": str(args.native_memory_binding_file),
        "binding_digest": args.native_memory_binding_digest}}
