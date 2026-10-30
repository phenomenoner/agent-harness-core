"""Explicit, pinned offline recovery commands; no implicit stop, replay or cleanup."""

from pathlib import Path

from ahc.infrastructure.runtime_repository import RuntimeRepository
from ahc.infrastructure.runtime_restore_activation import (
    RestoreIndeterminate,
    activate_runtime_restore,
)
from ahc.infrastructure.runtime_restore_guard import offline_restore_source
from ahc.infrastructure.runtime_restore_pending import reconcile_pending_restore
from ahc.infrastructure.runtime_restore_transform import transform_runtime_restore
from ahc.infrastructure.runtime_sidecar_transition import (
    normalize_runtime_source,
    resume_sidecar_transition,
)
from ahc.infrastructure.working_repository import WorkingWriterBusy

COMMANDS = {"runtime-restore-transform", "runtime-restore-activate", "runtime-restore-reconcile"}
COMMANDS |= {"runtime-sidecar-normalize", "runtime-sidecar-resume"}
COMMANDS.add("runtime-channel-rebind")
COMMANDS.add("runtime-sidecar-status")


def register(commands):
    for name in sorted(COMMANDS):
        parser = commands.add_parser(name)
        parser.add_argument("--runtime-root", type=Path, required=True)
        parser.add_argument("--supervisor-lock", type=Path, required=True)
        if name == "runtime-sidecar-status":
            parser.add_argument("--expected-attempt")
        elif name == "runtime-channel-rebind":
            from .channel_recovery import register as register_channel

            register_channel(parser)
        elif name == "runtime-sidecar-normalize":
            parser.add_argument("--capture-root", type=Path, required=True)
            parser.add_argument("--expected-capture-digest", required=True)
            parser.add_argument("--expected-normalized-digest", required=True)
        elif name == "runtime-sidecar-resume":
            parser.add_argument("--expected-attempt", required=True)
        elif name == "runtime-restore-reconcile":
            parser.add_argument("--expected-attempt", required=True)
            parser.add_argument("--expected-digest", required=True)
            parser.add_argument("--decision", choices=("accept-candidate", "retain-source"),
                                required=True)
        else:
            parser.add_argument("--candidate-root", type=Path, required=True)
            parser.add_argument("--expected-backup-digest", required=True)
            parser.add_argument("--expected-source-digest", required=True)
            if name == "runtime-restore-transform":
                parser.add_argument("--destination", type=Path, required=True)
            else:
                parser.add_argument("--expected-digest", required=True)


def run(args):
    pending_sidecar = (args.command == "runtime-sidecar-resume"
        or (args.command == "runtime-sidecar-status"
            and (args.runtime_root / "sidecar-pending.json").exists()))
    store = RuntimeRepository(args.runtime_root, allow_pending_sidecar=pending_sidecar)
    try:
        if args.command == "runtime-sidecar-status":
            from ahc.infrastructure.runtime_sidecar_status import inspect_sidecar_transition

            result = inspect_sidecar_transition(store, args.supervisor_lock,
                                               expected_attempt=args.expected_attempt)
        elif args.command == "runtime-channel-rebind":
            from .channel_recovery import run as run_channel

            result = run_channel(store, args)
        elif args.command == "runtime-sidecar-normalize":
            result = normalize_runtime_source(store, args.supervisor_lock, args.capture_root,
                args.expected_capture_digest, args.expected_normalized_digest)
        elif args.command == "runtime-sidecar-resume":
            result = resume_sidecar_transition(store, args.supervisor_lock, args.expected_attempt)
        elif args.command == "runtime-restore-transform":
            with offline_restore_source(store, args.supervisor_lock,
                                        args.expected_source_digest) as snapshots:
                result = transform_runtime_restore(args.candidate_root,
                    args.expected_backup_digest, snapshots, args.expected_source_digest,
                    args.destination)
            # The private receipt includes source snapshots; the CLI does not emit them.
            result = {key: result[key] for key in (
                "schema", "database", "sha256", "backup_digest", "source_database_digest",
                "state", "activation_allowed")}
        elif args.command == "runtime-restore-activate":
            result = activate_runtime_restore(store, args.supervisor_lock, args.candidate_root,
                args.expected_digest, args.expected_backup_digest, args.expected_source_digest)
        else:
            result = reconcile_pending_restore(store, args.supervisor_lock, args.expected_attempt,
                                               args.expected_digest, args.decision)
        return 0, {"command": args.command, "restore": result}
    except RestoreIndeterminate as exc:
        return 3, {"command": args.command, "restore": {"state": "indeterminate",
                   "attempt": exc.attempt, "next_command": "runtime-restore-pending"}}
    except WorkingWriterBusy:
        return 4, {"command": args.command, "restore": {"state": "busy"}}
