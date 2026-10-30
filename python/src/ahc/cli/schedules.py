"""Operator-owned interval schedule intents; execution is not implied."""

from pathlib import Path

from ahc.domain.schedule_dispatch import parse_schedule
from ahc.domain.sessions import VirtualSessionIdentity
from ahc.infrastructure.runtime_repository import RuntimeRepository
from ahc.infrastructure.schedule_repository import ScheduleRepository

from .runtime import _json

COMMANDS = frozenset({"schedule-create", "schedule-read", "schedule-state", "schedule-enqueue",
                      "schedule-tick", "schedule-status", "schedule-list"})


def register_schedule_commands(commands):
    for name in sorted(COMMANDS):
        parser = commands.add_parser(name)
        parser.add_argument("--runtime-root", type=Path, required=True)
        if name == "schedule-create":
            parser.add_argument("--schedule-json", required=True)
        elif name == "schedule-list":
            parser.add_argument("--session-json", required=True)
            parser.add_argument("--after-id")
            parser.add_argument("--after-ms", type=int, required=True)
            parser.add_argument("--limit", type=int, default=50)
        elif name == "schedule-status":
            parser.add_argument("--session-json", required=True)
            parser.add_argument("--schedule-id", required=True)
            parser.add_argument("--now-ms", type=int, required=True)
        elif name == "schedule-tick":
            parser.add_argument("--expected-json", required=True)
            parser.add_argument("--now-ms", type=int, required=True)
            parser.add_argument("--limit", type=int, default=16)
        elif name == "schedule-enqueue":
            parser.add_argument("--expected-json", required=True)
            parser.add_argument("--due-ms", type=int, required=True)
            parser.add_argument("--now-ms", type=int, required=True)
        elif name == "schedule-state":
            parser.add_argument("--expected-json", required=True)
            parser.add_argument("--state", choices=("active", "paused", "removed"), required=True)
        else:
            parser.add_argument("--session-json", required=True)
            parser.add_argument("--schedule-id", required=True)
            parser.add_argument("--after-ms", type=int, required=True)


def run_schedule_command(args):
    repo = ScheduleRepository(RuntimeRepository(args.runtime_root))
    if args.command == "schedule-list":
        from ahc.domain.schedules import MAX_SAFE_INTEGER, _bounded_integer

        _bounded_integer(args.after_ms, "after_ms", 0, MAX_SAFE_INTEGER)
        values, cursor = repo.list_page(
            VirtualSessionIdentity.from_dict(_json(args.session_json)),
            after=args.after_id, limit=args.limit)
        return {"command": args.command, "schedules": [
            {"schedule_id": value.schedule_id, "schema_version": value.schema_version,
             "revision": value.revision, "state": value.state,
             "next_due_ms": value.next_after(args.after_ms)} for value in values],
             "next_after_id": cursor}
    if args.command == "schedule-status":
        from ahc.infrastructure.schedule_status import inspect_schedule

        return {"command": args.command, "progress": inspect_schedule(
            repo.store, VirtualSessionIdentity.from_dict(_json(args.session_json)),
            args.schedule_id, args.now_ms)}
    if args.command == "schedule-tick":
        from ahc.infrastructure.schedule_tick import ScheduleTick

        result = ScheduleTick(repo.store).tick(
            parse_schedule(_json(args.expected_json)), args.now_ms, limit=args.limit)
        return {"command": args.command, "tick": result}
    if args.command == "schedule-enqueue":
        from ahc.infrastructure.schedule_occurrences import ScheduleOccurrences

        receipt = ScheduleOccurrences(repo.store).enqueue(
            parse_schedule(_json(args.expected_json)), args.due_ms, args.now_ms)
        return {"command": args.command, "occurrence": receipt}
    if args.command == "schedule-create":
        value = repo.create(parse_schedule(_json(args.schedule_json)))
    elif args.command == "schedule-state":
        value = repo.set_state(parse_schedule(_json(args.expected_json)), args.state)
    else:
        session = VirtualSessionIdentity.from_dict(_json(args.session_json))
        value = repo.read(session, args.schedule_id)
        return {"command": args.command, "schedule": None if value is None else value.to_dict(),
                "next_due_ms": None if value is None else value.next_after(args.after_ms)}
    return {"command": args.command, "schedule": value.to_dict()}
