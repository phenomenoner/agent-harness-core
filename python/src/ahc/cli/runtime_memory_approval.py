"""Local exact-payload memory review; never constructs or calls a provider."""

import time

from ahc.domain.sessions import ContractError, VirtualSessionIdentity
from ahc.domain.turns import _sha256_hex
from ahc.infrastructure.memory_store_approvals import MemoryStoreApprovals
from ahc.infrastructure.runtime_repository import RuntimeRepository


def run_memory_approval_command(args):
    from .runtime import _json

    session = VirtualSessionIdentity.from_dict(_json(args.session_json))
    _sha256_hex(args.request_digest)
    repo = RuntimeRepository(args.runtime_root)
    if repo.read_current(session.ref.virtual_session_id).session != session:
        raise ContractError("memory review session not current")
    owner = MemoryStoreApprovals(repo)
    proposal = owner.query(session, args.request_digest)
    if args.command == "runtime-memory-approval-query":
        return 0, {
            "command": args.command,
            "review": proposal,
            "content_authority": "untrusted-data",
            "provider_called": False,
        }
    if args.decision == "deny":
        owner.reject(session, args.request_digest)
    else:
        if not 1 <= args.valid_for <= 3600:
            raise ContractError("memory approval validity out of range")
        expiry = min(time.time() + args.valid_for, proposal["proposal"]["expires_at"])
        owner.approve(session, args.request_digest, approval_expires=expiry)
    return 0, {
        "command": args.command,
        "request_digest": args.request_digest,
        "state": owner.query(session, args.request_digest)["state"],
        "provider_called": False,
    }
