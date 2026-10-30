"""Operator-requested local read batches; not an agent profile grant."""

import json
import time
from pathlib import Path

from ahc.application.capability_view import _issue_agent_capability_view
from ahc.application.tool_batch import BatchCall, ReadOnlyBatch
from ahc.domain.capability_values import ApprovalMode, CapabilityDefinition, EffectClass
from ahc.domain.profile_values import ProfileResourceKind
from ahc.domain.sessions import ContractError, VirtualSessionIdentity
from ahc.infrastructure.runtime_query_port import CAPABILITIES, RuntimeQueryPort
from ahc.infrastructure.runtime_repository import RuntimeRepository

from .runtime import _json


def register_batch_command(commands):
    for name in ("runtime-batch-receipts", "runtime-mcp-receipts"):
        receipts = commands.add_parser(name)
        receipts.add_argument("--runtime-root", type=Path, required=True)
        receipts.add_argument("--session-json", required=True)
        receipts.add_argument("--operation-id", required=True)
    parser = commands.add_parser("runtime-read-batch")
    parser.add_argument("--runtime-root", type=Path, required=True)
    parser.add_argument("--session-json", required=True)
    parser.add_argument("--calls-json", required=True)
    parser.add_argument("--include-values", action="store_true")
    parser.add_argument("--concurrency", type=int, choices=(1, 2, 3, 4), default=1)


def run_batch_command(args):
    session = VirtualSessionIdentity.from_dict(_json(args.session_json))
    if args.command == "runtime-mcp-receipts":
        from ahc.infrastructure.mcp_receipts import read_mcp_receipts

        return {"command": args.command, "receipts": read_mcp_receipts(
            RuntimeRepository(args.runtime_root), session, args.operation_id)}
    if args.command == "runtime-batch-receipts":
        from ahc.infrastructure.batch_receipt_repository import BatchReceiptRepository

        rows = BatchReceiptRepository(RuntimeRepository(args.runtime_root)).read(
            session, args.operation_id)
        return {"command": args.command, "receipts": rows}
    if len(args.calls_json.encode()) > 262144:
        raise ContractError("batch input too large")
    rows = _json(args.calls_json)
    if type(rows) is not list or not 1 <= len(rows) <= 16:
        raise ContractError("batch calls invalid")
    calls = []
    for row in rows:
        if type(row) is not dict or set(row) != {"call_id", "capability_id", "arguments"}:
            raise ContractError("batch call fields invalid")
        calls.append(BatchCall(row["call_id"], row["capability_id"], "runtime",
                               json.dumps(row["arguments"], allow_nan=False)))
    # This finite allowlist expresses the local operator command's read scope.
    # Never reuse this composition as the grant authority for generated code.
    view = _issue_agent_capability_view(
        tuple(CapabilityDefinition(name, ProfileResourceKind.WORKSPACE,
                                   EffectClass.READ_ONLY, ApprovalMode.NONE)
              for name in CAPABILITIES),
        tuple((name, "runtime") for name in CAPABILITIES),
    )
    store = RuntimeRepository(args.runtime_root)
    batch = ReadOnlyBatch(session, view, RuntimeQueryPort(store, session))
    result = batch.run(session, tuple(calls), deadline=time.monotonic() + 30,
                       concurrency=args.concurrency)
    return {
        "command": args.command,
        **result.summary(),
        **({"values": result.values} if args.include_values else {}),
    }
