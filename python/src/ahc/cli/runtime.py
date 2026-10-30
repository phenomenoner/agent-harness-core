"""Local runtime composition; only explicit inert or unavailable backends are shipped here."""

import argparse
import json
import time
from hashlib import sha256
from pathlib import Path

from ahc.application.runtime_service import RuntimeService
from ahc.application.working_continuity import WorkingContinuityService
from ahc.domain.agent_backend import AgentTurnObservation, AgentTurnRequest
from ahc.domain.judgment_goal import GoalContract
from ahc.domain.sessions import ContractError, VirtualSessionIdentity
from ahc.domain.working_sets import WorkingSetSnapshot
from ahc.infrastructure.runtime_repository import RuntimeRepository

from .runtime_backup import COMMANDS as BACKUP_COMMANDS
from .runtime_backup import register_backup_commands, run_backup_command
from .runtime_channels import COMMANDS as CHANNEL_COMMANDS
from .runtime_channels import register_channel_commands, run_channel_command

COMMANDS = frozenset(
    {
        "runtime-open",
        "runtime-submit",
        "runtime-query",
        "runtime-turn",
        "runtime-status",
        "runtime-reconcile",
        "runtime-cancel",
        "runtime-complete",
        "runtime-continue",
    }
) | BACKUP_COMMANDS | CHANNEL_COMMANDS


class UnavailableBackend:
    def invoke(self, request):
        return AgentTurnObservation(request, "unsupported", "backend-unavailable", 0, 0)


def register_runtime_commands(commands):
    register_backup_commands(commands)
    register_channel_commands(commands)
    for name in sorted(COMMANDS - BACKUP_COMMANDS - CHANNEL_COMMANDS):
        parser = commands.add_parser(name)
        parser.add_argument("--runtime-root", type=Path, required=True)
        parser.add_argument("--session-json", required=True)
        if name == "runtime-open":
            parser.add_argument("--goal-json", required=True)
        if name == "runtime-continue":
            parser.add_argument("--successor-session-json", required=True)
            parser.add_argument(
                "--reason", choices=("compaction", "backend-rollover"), required=True
            )
        if name in {"runtime-submit", "runtime-query"}:
            parser.add_argument("--operation-id", required=True)
        if name == "runtime-submit":
            parser.add_argument("--input-text", required=True)
            parser.add_argument("--goal-digest", required=True)
        if name == "runtime-turn":
            parser.add_argument("--operation-id", required=True)
            parser.add_argument("--input-text", required=True)
            parser.add_argument(
                "--backend", choices=("inert", "unavailable"), default="unavailable"
            )
            parser.add_argument("--peer-json", type=Path)
            parser.add_argument("--skill-root", type=Path)
            parser.add_argument("--role", action="append", default=[])
            parser.add_argument("--available-tool", action="append", default=[])


def _json(text):
    if type(text) is not str or len(text) > 65536:
        raise ContractError("runtime JSON input exceeds bound")

    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ContractError("duplicate runtime JSON field")
            result[key] = value
        return result

    try:
        return json.loads(text, object_pairs_hook=pairs)
    except (ValueError, RecursionError):
        raise ContractError("invalid runtime JSON input") from None


def _backend(args):
    if args.backend == "unavailable":
        if args.peer_json is not None:
            raise ContractError("peer fixture requires inert backend")
        return UnavailableBackend(), sha256(b"unavailable:no-live-effects").hexdigest()
    if args.peer_json is None:
        raise ContractError("inert backend requires an explicit fixture")
    from ahc.adapters.inert_agent_backend import InertAgentBackend, InertPeer

    with args.peer_json.open("rb") as stream:
        raw = stream.read(65537)
    frames = _json(raw.decode("utf-8"))
    if type(frames) is not list:
        raise ContractError("inert fixture must be an array of frames")
    peer = InertPeer(tuple(json.dumps(frame, allow_nan=False).encode() for frame in frames))
    return InertAgentBackend(peer), sha256(raw).hexdigest()


def run_runtime_command(args: argparse.Namespace):
    if args.command in BACKUP_COMMANDS:
        return run_backup_command(args)
    if args.command in CHANNEL_COMMANDS:
        return run_channel_command(args)
    session = VirtualSessionIdentity.from_dict(_json(args.session_json))
    if args.command == "runtime-open":
        goal = GoalContract.from_dict(_json(args.goal_json))
        snapshot = WorkingSetSnapshot.initial(session, goal)
        repo = RuntimeRepository.initialize(args.runtime_root)
        current = repo.create(snapshot)
        return 0, {"command": args.command, "working_set": current.to_dict()}
    repo = RuntimeRepository(args.runtime_root)
    current = repo.read_current(session.ref.virtual_session_id)
    if current.session != session:
        raise ContractError("runtime session identity differs from current owner")
    if args.command in {"runtime-submit", "runtime-query"}:
        from ahc.infrastructure.runtime_inbox import RuntimeInbox

        inbox = RuntimeInbox(repo)
        if args.command == "runtime-submit":
            receipt = inbox.submit(session, args.operation_id, args.goal_digest, args.input_text)
            return 0, {"command": args.command, "submission": receipt}
        result = inbox.query(session, args.operation_id)
        return (0 if result is not None else 2), {"command": args.command, "submission": result}
    if args.command == "runtime-status":
        from ahc.infrastructure.runtime_health import inspect_runtime_health

        return 0, {
            "command": args.command,
            "working_set": current.to_dict(),
            "intents": list(repo.intents(session)),
            "durable_health": inspect_runtime_health(repo, session),
            # This command reads durable state only. No foreground configuration
            # or optional adapter is inspected, so absence cannot be inferred.
            "capabilities": {
                "live_backend": "not-inspected",
                "workspace": "not-inspected",
                "memory": "not-inspected",
                "channel_delivery": "not-inspected",
            },
        }
    if args.command in {"runtime-complete", "runtime-continue"}:
        with repo.execution_lock():
            if current.active_turn_id is not None:
                raise ContractError("reconcile pending turn before changing session lifecycle")
            continuity = WorkingContinuityService(repo)
            if args.command == "runtime-complete":
                successor = continuity.complete(current)
            else:
                successor_session = VirtualSessionIdentity.from_dict(
                    _json(args.successor_session_json)
                )
                successor = continuity.continue_generation(current, successor_session, args.reason)
        return 0, {"command": args.command, "working_set": successor.to_dict()}
    if args.command == "runtime-cancel":
        successor = WorkingContinuityService(repo).cancel(current)
        return 0, {"command": args.command, "working_set": successor.to_dict()}
    if args.command == "runtime-reconcile":
        recovered = repo.reconcile(session)
        return 0, {"command": args.command, "reconciled": [turn.to_dict() for turn in recovered]}
    backend, configuration_digest = _backend(args)
    selector = None
    if args.skill_root is not None:
        from ahc.application.skill_service import SkillTurnService
        from ahc.infrastructure.skill_repository import SkillRepository

        selector = SkillTurnService(SkillRepository(args.skill_root))
    request = AgentTurnRequest(
        session,
        args.operation_id,
        current.goal.digest,
        args.input_text,
        (),
        sha256(f"ahc:{args.backend}:fixture-v1".encode()).hexdigest(),
        configuration_digest,
        "local-runtime",
        time.monotonic() + 30,
    )
    service = RuntimeService(repo, backend, selector=selector)
    turn = service.execute_with_skills(
        current, request, roles=tuple(args.role or ["procedure"]), tools=tuple(args.available_tool)
    )
    accepted = turn.result is not None and turn.result.get("accepted") is True
    return (0 if accepted else 2), {
        "command": args.command,
        "turn": turn.to_dict(),
        "skills": "selected" if selector else "unavailable",
    }
