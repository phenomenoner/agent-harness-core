"""Deterministic JSON command-line entry point for the standalone AHC application."""

from __future__ import annotations

import argparse
import math
from collections.abc import Sequence
from pathlib import Path
from typing import TYPE_CHECKING, NoReturn

from ahc.application.fake_turn import execute_fake_turn
from ahc.domain import ContractError, FakeTurnRequest

from .artifacts import COMMANDS as ARTIFACT_COMMANDS
from .artifacts import register_artifact_commands, run_artifact_command
from .json_output import (
    emit as _emit,
)
from .json_output import (
    emit_error as _emit_error,
)
from .json_output import (
    emit_lifecycle_result as _emit_lifecycle_result,
)
from .json_output import (
    emit_raw as _emit_raw,
)
from .judgment import is_judgment_command, register_judgment_commands, run_judgment_command
from .main_fake_turn import build_fake_turn_request
from .runtime import COMMANDS as RUNTIME_COMMANDS
from .runtime import register_runtime_commands, run_runtime_command
from .runtime_approval import COMMANDS as APPROVAL_COMMANDS
from .runtime_approval import register_approval_commands, run_approval_command
from .runtime_batch import register_batch_command, run_batch_command
from .schedules import COMMANDS as SCHEDULE_COMMANDS
from .schedules import register_schedule_commands, run_schedule_command
from .skills import is_skill_command, register_skill_commands, run_skill_command

if TYPE_CHECKING:
    from ahc.application.capability_composition import OptionalCapabilityComposition
    from ahc.domain import AgentProfileRegistry

_MAX_CLI_SECONDS = 86_400.0


class CliUsageError(ValueError):
    """The command line is not a valid invocation."""


def _fake_turn_request(args: argparse.Namespace) -> FakeTurnRequest:
    """Retain the established CLI seam for fake-turn request construction."""
    return build_fake_turn_request(args, CliUsageError)


class _JsonArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> NoReturn:
        raise CliUsageError(message)


def build_parser() -> argparse.ArgumentParser:
    """Build the parser without performing configuration or lifecycle effects."""
    parser = _JsonArgumentParser(prog="ahc")
    parser.add_argument("--config", type=Path, help="path to a strict TOML config")
    commands = parser.add_subparsers(
        dest="command", required=True, parser_class=_JsonArgumentParser
    )

    doctor = commands.add_parser("doctor")
    doctor.add_argument("--config", type=Path, default=argparse.SUPPRESS)
    doctor.add_argument("--runtime-config", type=Path)
    doctor.add_argument("--inspect-runtime", action="store_true")
    doctor.add_argument("--agent")
    preview = commands.add_parser("runtime-config-preview")
    preview.add_argument("--runtime-config", type=Path, required=True)

    run = commands.add_parser("run")
    run.add_argument("--config", type=Path, default=argparse.SUPPRESS)
    run.add_argument("--agent")
    run.add_argument(
        "--runtime-config",
        type=Path,
        help="explicit foreground workspace/backend JSON; activation must be explicit",
    )
    run.add_argument(
        "--max-runtime",
        type=_seconds_argument,
        default=None,
        help=argparse.SUPPRESS,
    )

    status = commands.add_parser("status")
    status.add_argument("--config", type=Path, default=argparse.SUPPRESS)
    status.add_argument("--agent")

    stop = commands.add_parser("stop")
    stop.add_argument("--config", type=Path, default=argparse.SUPPRESS)
    stop.add_argument("--agent")
    stop.add_argument("--wait-seconds", type=_seconds_argument, default=10.0)

    fake = commands.add_parser("fake-turn")
    fake.add_argument(
        "input_text_positional",
        nargs="?",
        help="user input text; --input-text is an equivalent named form",
    )
    fake.add_argument("--request-json", "--request", dest="request_json")
    fake.add_argument("--virtual-session-id")
    fake.add_argument("--generation", type=int)
    fake.add_argument("--predecessor-generation", type=int)
    fake.add_argument("--predecessor-id")
    fake.add_argument("--predecessor-json")
    fake.add_argument("--agent-id")
    fake.add_argument("--route-id")
    fake.add_argument("--turn-id")
    fake.add_argument("--input-text")
    register_skill_commands(commands)
    register_judgment_commands(commands)
    register_runtime_commands(commands)
    register_approval_commands(commands)
    register_artifact_commands(commands)
    register_batch_command(commands)
    register_schedule_commands(commands)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Execute one command and emit exactly one JSON result object."""
    parser = build_parser()
    args: argparse.Namespace | None = None
    try:
        args = parser.parse_args(argv)
        config_path = getattr(args, "config", None)
        if args.command in SCHEDULE_COMMANDS:
            if config_path is not None:
                raise CliUsageError("schedule commands use an explicit runtime root")
            return _emit_raw(run_schedule_command(args), 0)
        if args.command in ("runtime-read-batch", "runtime-batch-receipts", "runtime-mcp-receipts"):
            if config_path is not None:
                raise CliUsageError("batch commands use an explicit runtime root")
            return _emit_raw(run_batch_command(args), 0)
        if args.command in ARTIFACT_COMMANDS:
            if config_path is not None:
                raise CliUsageError("artifact commands use an explicit runtime root")
            code, payload = run_artifact_command(args)
            return _emit_raw(payload, code)
        if args.command == "runtime-config-preview":
            if config_path is not None:
                raise CliUsageError("runtime preview uses --runtime-config")
            from .foreground import read_foreground_config
            from .foreground_config_native import parse_trusted_foreground_config

            config = parse_trusted_foreground_config(read_foreground_config(args.runtime_config))
            return _emit_raw({"command": args.command, "preview": config.preview()}, 0)
        if args.command in RUNTIME_COMMANDS | APPROVAL_COMMANDS:
            if config_path is not None:
                raise CliUsageError("runtime commands use an explicit runtime root")
            handler = (
                run_approval_command if args.command in APPROVAL_COMMANDS else run_runtime_command
            )
            code, payload = handler(args)
            return _emit_raw(payload, code)
        if args.command == "fake-turn":
            if config_path is not None:
                raise CliUsageError("--config is not accepted for fake-turn")
            return _dispatch(args, None)
        if is_skill_command(args.command) or is_judgment_command(args.command):
            if config_path is not None:
                raise CliUsageError("--config is not accepted for ASI commands")
            return _dispatch(args, None)
        if config_path is None:
            raise CliUsageError("--config is required")
        return _dispatch(args, config_path)
    except ContractError:
        return _emit_error(getattr(args, "command", None), 2, "invalid contract", "contract")
    except CliUsageError:
        return _emit_error(getattr(args, "command", None), 2, "invalid invocation", "usage")
    except KeyboardInterrupt:
        return _emit_error(getattr(args, "command", None), 130, "interrupted", "interrupt")
    except Exception as exc:  # Keep the operator surface machine-readable on integration errors.
        from ahc.infrastructure.config import ConfigError

        command = getattr(args, "command", None)
        if isinstance(exc, ConfigError):
            return _emit_error(command, 2, "invalid configuration", "config")
        return _emit_error(command, 1, "internal failure", "internal")


def _dispatch(args: argparse.Namespace, config_path: Path | None) -> int:
    if args.command == "fake-turn":
        request = _fake_turn_request(args)
        result = execute_fake_turn(request)
        return _emit_raw(result.to_dict(), 0)
    if is_skill_command(args.command) or is_judgment_command(args.command):
        runner = run_skill_command if is_skill_command(args.command) else run_judgment_command
        code, payload = runner(args)
        return _emit_raw(payload, code)

    if config_path is None:
        raise CliUsageError("--config is required")
    from ahc.infrastructure.config import load_config

    if args.command == "doctor":
        from ahc.application.capability_doctor import inspect_capability_authority
        from ahc.application.profile_doctor import inspect_profile_authority
        from ahc.infrastructure.config.loader import _load_config_document_for_doctor

        document = _load_config_document_for_doctor(config_path)
        capability_authority = _compose_optional_capability_authority(document.profile_registry)
        from .runtime_doctor import inspect_selected_runtime

        runtime_report = inspect_selected_runtime(args, document)
        return _emit(
            {
                "command": "doctor",
                **runtime_report,
                "config": document.summary(),
                "profiles": inspect_profile_authority(document.profile_registry),
                "capabilities": inspect_capability_authority(
                    None if capability_authority is None else capability_authority.snapshot
                ),
                "exit_code": 0,
                "ok": True,
            }
        )

    from .profile_lifecycle import control_config, selected_supervisor

    config = control_config(args, load_config(config_path))
    if args.command == "run":
        # Retain the immutable authority for the complete foreground supervisor lifetime.
        _capability_authority = _compose_optional_capability_authority(config.profile_registry)
        runtime, supervisor = selected_supervisor(args, config)
        exit_code = int(supervisor.run(max_runtime_seconds=args.max_runtime))
        return _emit(
            {
                "command": "run",
                "exit_code": exit_code,
                "ok": exit_code == 0,
                **(
                    {
                        "runtime": {
                            "backend": runtime.backend_name,
                            "state": runtime.state,
                            "memory": runtime.memory_status(),
                            "workspace": runtime.workspace_status(),
                            "schedule": runtime.schedule_status(),
                            "children": runtime.child_status(),
                            "channels": runtime.channel_status(),
                        }
                    }
                    if runtime is not None
                    else {}
                ),
            }
        )

    if args.command == "status":
        from ahc.application.lifecycle import inspect_status

        exit_code, report = inspect_status(config)
        return _emit_lifecycle_result("status", exit_code, report)

    if args.command == "stop":
        from ahc.application.lifecycle import request_stop

        exit_code, report = request_stop(config, args.wait_seconds)
        return _emit_lifecycle_result("stop", exit_code, report)

    raise CliUsageError(f"unsupported command: {args.command}")


def _compose_optional_capability_authority(
    profile_registry: AgentProfileRegistry | None,
) -> OptionalCapabilityComposition | None:
    if profile_registry is None:
        return None
    from ahc.application import capability_composition

    return capability_composition.compose_optional_capability_views(
        profile_registry=profile_registry,
        definitions=None,
        grants=None,
    )


def _seconds_argument(value: str) -> float:
    try:
        parsed = float(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be a number") from exc
    if not math.isfinite(parsed) or not 0.0 <= parsed <= _MAX_CLI_SECONDS:
        raise argparse.ArgumentTypeError(f"must be a finite number in [0, {_MAX_CLI_SECONDS:g}]")
    return parsed
