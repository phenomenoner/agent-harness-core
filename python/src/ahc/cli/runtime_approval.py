"""Local owner approval commands; no backend execution or execution-lock acquisition."""

from pathlib import Path

from ahc.domain.sessions import ContractError, VirtualSessionIdentity
from ahc.domain.turns import _sha256_hex
from ahc.infrastructure.runtime_approval_store import RuntimeApprovalStore
from ahc.infrastructure.runtime_repository import RuntimeRepository

COMMANDS = frozenset({"runtime-approval-query", "runtime-approval-respond"})
COMMANDS |= {"runtime-child-approval-query", "runtime-child-approval-respond"}
COMMANDS |= {"runtime-memory-approval-query", "runtime-memory-approval-respond"}


def register_approval_commands(commands):
    for name in sorted(COMMANDS):
        parser = commands.add_parser(name)
        parser.add_argument("--runtime-root", type=Path, required=True)
        parser.add_argument("--session-json", required=True)
        parser.add_argument(
            "--request-digest", required=name.endswith("-respond") or "-memory-" in name
        )
        if name == "runtime-memory-approval-respond":
            parser.add_argument("--valid-for", type=int, default=300)
        if name.endswith("-respond"):
            parser.add_argument("--decision", choices=("approve-once", "deny"), required=True)


def run_approval_command(args):
    from .runtime import _json

    if "-memory-" in args.command:
        from .runtime_memory_approval import run_memory_approval_command

        return run_memory_approval_command(args)

    session = VirtualSessionIdentity.from_dict(_json(args.session_json))
    repo = RuntimeRepository(args.runtime_root)
    if repo.read_current(session.ref.virtual_session_id).session != session:
        raise ContractError("approval command session is not current")
    store = RuntimeApprovalStore(repo)
    if args.command in ("runtime-child-approval-query", "runtime-child-approval-respond"):
        from ahc.infrastructure.child_approval_queries import ChildApprovalQueries

        store = ChildApprovalQueries(repo)
    if args.request_digest is not None:
        _sha256_hex(args.request_digest)
    if args.command in ("runtime-approval-query", "runtime-child-approval-query"):
        approvals = (
            store.pending(session)
            if args.request_digest is None
            else (store.query(session, args.request_digest),)
        )
        return 0, {
            "command": args.command,
            "approvals": [row for row in approvals if row is not None],
        }
    decision = store.respond(session, args.request_digest, args.decision)
    return 0, {
        "command": args.command,
        "request_digest": decision.request_digest,
        "outcome": decision.outcome,
    }
