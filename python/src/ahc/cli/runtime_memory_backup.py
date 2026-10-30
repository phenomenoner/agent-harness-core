"""Operator-only joint backup using explicitly bound native provider ports."""

import hashlib
import json
from pathlib import Path

from ahc.adapters.memory_native_process import NativeMemoryProcess
from ahc.domain.profile_registry import AgentProfileRegistry
from ahc.domain.sessions import ContractError
from ahc.infrastructure.runtime_memory_backup import (
    backup_runtime_memory,
    inspect_runtime_memory_backup,
    require_reconciled_memory_effects,
)
from ahc.infrastructure.runtime_memory_restore import prepare_runtime_memory_restore
from ahc.infrastructure.runtime_repository import RuntimeRepository
from ahc.infrastructure.runtime_restore_guard import offline_restore_source

from .foreground import read_foreground_config
from .foreground_config_native import parse_trusted_foreground_config
from .foreground_native_memory import read_native_binding


def register(commands):
    for name in (
        "runtime-memory-backup",
        "runtime-memory-backup-inspect",
        "runtime-memory-restore-prepare",
        "runtime-memory-restore-binding",
        "runtime-memory-integrity-check",
    ):
        command = commands.add_parser(name)
        command.add_argument("--foreground-config", type=Path, required=True)
        command.add_argument("--profile-registry", type=Path, required=True)
        if name == "runtime-memory-integrity-check":
            continue
        if name.endswith("restore-binding"):
            command.add_argument("--candidate-root", type=Path, required=True)
            command.add_argument("--expected-restore-digest", required=True)
            command.add_argument("--destination", type=Path, required=True)
        elif name.endswith("-inspect") or name.endswith("restore-prepare"):
            command.add_argument("--backup-root", type=Path, required=True)
            command.add_argument("--expected-manifest-digest", required=True)
            if name.endswith("restore-prepare"):
                command.add_argument("--destination", type=Path, required=True)
                command.add_argument("--supervisor-lock", type=Path, required=True)
                command.add_argument("--expected-source-digest", required=True)
        else:
            command.add_argument("--destination", type=Path, required=True)


def run(args):
    if args.profile_registry.stat().st_size > 1024 * 1024:
        raise ContractError("profile registry exceeds bound")
    registry = AgentProfileRegistry.from_dict(json.loads(args.profile_registry.read_text("utf-8")))
    config = parse_trusted_foreground_config(read_foreground_config(args.foreground_config))
    binding = read_native_binding(config, registry)
    if binding is None:
        raise ContractError("native memory binding required")
    from .mempalace_memory_binding import MemPalaceMemoryBinding
    if args.command == "runtime-memory-integrity-check":
        from ahc.adapters.memory_native_process import NativeMemoryUnavailable
        from ahc.adapters.mempalace_memory_process import (
            INTEGRITY_TIMEOUT_SECONDS,
            MemPalaceMemoryProcess,
        )

        if not isinstance(binding, MemPalaceMemoryBinding) or not binding.observe_granted:
            raise ContractError("MemPalace integrity observation not admitted")
        # Do not initialize/mutate the runtime repository for a maintenance read.
        adapter = MemPalaceMemoryProcess(binding=binding, scratch_root=config.runtime_root)
        try:
            integrity = adapter.integrity_check()
        except NativeMemoryUnavailable:
            return 1, {"command": args.command, "integrity": {"status": "unavailable"},
                       "deadline_seconds": INTEGRITY_TIMEOUT_SECONDS}
        if (type(integrity) is not dict
                or set(integrity) != {"schema_version", "status", "check", "observation_scope"}
                or integrity["schema_version"] != "ahc.memory.native.integrity.v1"
                or integrity["status"] not in ("ok", "failed")
                or integrity["check"] != "sqlite-quick-check"
                or integrity["observation_scope"] != "whole-database"):
            raise ContractError("MemPalace integrity result invalid")
        return (0 if integrity["status"] == "ok" else 1), {
            "command": args.command, "integrity": integrity,
            "deadline_seconds": INTEGRITY_TIMEOUT_SECONDS}
    store = RuntimeRepository(config.runtime_root)
    if isinstance(binding, MemPalaceMemoryBinding):
        from ahc.adapters.mempalace_memory_process import MemPalaceMemoryProcess
        adapter = MemPalaceMemoryProcess(binding=binding, scratch_root=store.root)
    else:
        adapter = NativeMemoryProcess(
            database=binding.database,
            provider_root=binding.provider_root,
            provider_digest=binding.provider_digest,
            timeout=binding.timeout,
            scratch_root=store.root,
        )
    if args.command == "runtime-memory-restore-binding":
        from .runtime_memory_binding import prepare_memory_binding

        result = prepare_memory_binding(
            args.candidate_root, args.destination, expected_digest=args.expected_restore_digest,
            binding=binding, registry=registry, provider_inspect=adapter.inspect_backup)
        return 0, {"command": args.command, "binding": result}
    if args.command == "runtime-memory-restore-prepare":
        with offline_restore_source(
            store, args.supervisor_lock, args.expected_source_digest
        ) as snapshots:
            with store._read() as connection:
                require_reconciled_memory_effects(connection)
            receipt = prepare_runtime_memory_restore(
                args.backup_root,
                args.destination,
                expected_digest=args.expected_manifest_digest,
                expected_binding_digest=binding.digest,
                source_snapshots=snapshots,
                source_database_digest=args.expected_source_digest,
                provider_inspect=adapter.inspect_backup,
            )
        digest = hashlib.sha256((args.destination / "restore.json").read_bytes()).hexdigest()
        return 0, {
            "command": args.command,
            "restore_digest": digest,
            "restore": {
                key: receipt[key]
                for key in (
                    "schema",
                    "state",
                    "activation_allowed",
                    "binding_reconfiguration_required",
                )
            },
        }
    if args.command.endswith("-inspect"):
        receipt = inspect_runtime_memory_backup(
            args.backup_root,
            expected_digest=args.expected_manifest_digest,
            expected_binding_digest=binding.digest,
            provider_inspect=adapter.inspect_backup,
        )
        digest = args.expected_manifest_digest
    else:
        receipt = backup_runtime_memory(
            store,
            args.destination,
            binding_digest=binding.digest,
            provider_backup=adapter.backup,
            provider_inspect=adapter.inspect_backup,
        )
        digest = hashlib.sha256((args.destination / "manifest.json").read_bytes()).hexdigest()
    return 0, {"command": args.command, "backup": receipt, "manifest_digest": digest}
