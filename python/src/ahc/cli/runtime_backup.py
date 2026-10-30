"""Explicit operator backup artifacts and read-only validation."""

from pathlib import Path

from ahc.infrastructure.runtime_backup import backup_runtime_database, inspect_runtime_backup
from ahc.infrastructure.runtime_repository import RuntimeRepository
from ahc.infrastructure.runtime_restore import inspect_runtime_restore, prepare_runtime_restore
from ahc.infrastructure.runtime_restore_assessment import assess_runtime_restore
from ahc.infrastructure.runtime_restore_inspection import inspect_transformed_restore
from ahc.infrastructure.runtime_restore_pending import inspect_pending_restore
from ahc.infrastructure.runtime_sidecar_capture import capture_runtime_sidecars
from ahc.infrastructure.runtime_sidecar_inspection import inspect_sidecar_capture

from . import runtime_restore_commands as restore_commands

COMMANDS = frozenset({"runtime-backup", "runtime-backup-inspect", "runtime-restore-assess",
                      "runtime-restore-prepare", "runtime-restore-inspect",
                      "runtime-recovery-inspect", "runtime-restore-pending"})
COMMANDS |= restore_commands.COMMANDS
COMMANDS |= {"runtime-sidecar-capture", "runtime-sidecar-inspect"}
COMMANDS |= {"runtime-recovery-config", "runtime-memory-backup", "runtime-memory-backup-inspect",
             "runtime-memory-restore-prepare", "runtime-memory-restore-binding",
             "runtime-memory-integrity-check"}


def register_backup_commands(commands):
    from .runtime_memory_backup import register

    register(commands)
    restore_commands.register(commands)
    config = commands.add_parser("runtime-recovery-config")
    config.add_argument("--runtime-root", type=Path, required=True)
    config.add_argument("--original-config", type=Path, required=True)
    config.add_argument("--expected-config-digest", required=True)
    config.add_argument("--destination", type=Path, required=True)
    config.add_argument("--checkpoint-digest")
    config.add_argument("--enable-backend", action="store_true")
    config.add_argument("--native-memory-binding-file", type=Path)
    config.add_argument("--native-memory-binding-digest")
    config.add_argument("--profile-registry", type=Path)
    capture = commands.add_parser("runtime-sidecar-capture")
    capture.add_argument("--runtime-root", type=Path, required=True)
    capture.add_argument("--supervisor-lock", type=Path, required=True)
    capture.add_argument("--destination", type=Path, required=True)
    sidecar = commands.add_parser("runtime-sidecar-inspect")
    sidecar.add_argument("--capture-root", type=Path, required=True)
    sidecar.add_argument("--expected-capture-digest", required=True)
    sidecar.add_argument("--expected-normalized-digest", required=True)
    create = commands.add_parser("runtime-backup")
    create.add_argument("--runtime-root", type=Path, required=True)
    create.add_argument("--destination", type=Path, required=True)
    inspect = commands.add_parser("runtime-backup-inspect")
    inspect.add_argument("--backup-root", type=Path, required=True)
    assess = commands.add_parser("runtime-restore-assess")
    assess.add_argument("--backup-root", type=Path, required=True)
    prepare = commands.add_parser("runtime-restore-prepare")
    prepare.add_argument("--backup-root", type=Path, required=True)
    prepare.add_argument("--destination", type=Path, required=True)
    verify = commands.add_parser("runtime-restore-inspect")
    verify.add_argument("--candidate-root", type=Path, required=True)
    verify.add_argument("--expected-backup-digest", required=True)
    recovery = commands.add_parser("runtime-recovery-inspect")
    recovery.add_argument("--candidate-root", type=Path, required=True)
    recovery.add_argument("--expected-digest", required=True)
    recovery.add_argument("--expected-backup-digest", required=True)
    recovery.add_argument("--expected-source-digest", required=True)
    pending = commands.add_parser("runtime-restore-pending")
    pending.add_argument("--runtime-root", type=Path, required=True)
    pending.add_argument("--expected-attempt", required=True)


def run_backup_command(args):
    if args.command in {"runtime-memory-backup", "runtime-memory-backup-inspect",
                        "runtime-memory-restore-prepare", "runtime-memory-restore-binding",
                        "runtime-memory-integrity-check"}:
        from .runtime_memory_backup import run

        return run(args)
    if args.command == "runtime-recovery-config":
        from .runtime_memory_binding import recovery_memory_options
        from .runtime_recovery_config import prepare_recovered_config

        return 0, {"command": args.command, "configuration": prepare_recovered_config(
            RuntimeRepository(args.runtime_root), args.original_config, args.expected_config_digest,
            args.destination, checkpoint_digest=args.checkpoint_digest,
            enable_backend=args.enable_backend, **recovery_memory_options(args))}
    if args.command == "runtime-sidecar-capture":
        return 0, {"command": args.command, "capture": capture_runtime_sidecars(
            RuntimeRepository(args.runtime_root), args.supervisor_lock, args.destination)}
    if args.command == "runtime-sidecar-inspect":
        return 0, {"command": args.command, "capture": inspect_sidecar_capture(
            args.capture_root, args.expected_capture_digest, args.expected_normalized_digest)}
    if args.command in restore_commands.COMMANDS:
        return restore_commands.run(args)
    if args.command == "runtime-restore-pending":
        return 0, {"command": args.command, "pending": inspect_pending_restore(
            RuntimeRepository(args.runtime_root), args.expected_attempt)}
    if args.command == "runtime-recovery-inspect":
        return 0, {"command": args.command, "recovery": inspect_transformed_restore(
            args.candidate_root, args.expected_digest, args.expected_backup_digest,
            args.expected_source_digest)}
    if args.command == "runtime-restore-inspect":
        return 0, {"command": args.command, "restore": inspect_runtime_restore(
            args.candidate_root, args.expected_backup_digest)}
    if args.command == "runtime-restore-prepare":
        return 0, {"command": args.command, "restore": prepare_runtime_restore(
            args.backup_root, args.destination)}
    if args.command == "runtime-restore-assess":
        return 0, {"command": args.command, "assessment": assess_runtime_restore(args.backup_root)}
    if args.command == "runtime-backup":
        receipt = backup_runtime_database(RuntimeRepository(args.runtime_root), args.destination)
    else:
        receipt = inspect_runtime_backup(args.backup_root)
    return 0, {"command": args.command, "backup": receipt}
