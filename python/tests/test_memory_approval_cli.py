import json
import time

import pytest
from test_runtime_cli import call
from test_runtime_core import setup

from ahc.domain.sessions import ContractError
from ahc.infrastructure.memory_store_approvals import MemoryStoreApprovals


def prepared(tmp_path):
    repo, snapshot, _ = setup(tmp_path)
    owner = MemoryStoreApprovals(repo)
    digest = owner.stage(
        snapshot.session,
        operation_id="one",
        resource_id="agent.memory",
        provider_digest="a" * 64,
        scope="agent",
        observation={"kind": "note", "summary": "orchid", "detail": {}},
        origin="user",
        expires_at=time.time() + 600,
    )
    args = (
        "--runtime-root",
        str(repo.root),
        "--session-json",
        json.dumps(snapshot.session.to_dict()),
        "--request-digest",
        digest,
    )
    return owner, snapshot.session, digest, args


def test_cli_exact_review_approve_and_one_consumption(tmp_path, capsys):
    owner, session, digest, args = prepared(tmp_path)
    code, result = call(capsys, "runtime-memory-approval-query", *args)
    assert code == 0 and result["review"]["proposal"]["observation"]["summary"] == "orchid"
    assert result["provider_called"] is False and result["content_authority"] == "untrusted-data"
    code, result = call(
        capsys,
        "runtime-memory-approval-respond",
        *args,
        "--decision",
        "approve-once",
        "--valid-for",
        "60",
    )
    assert code == 0 and result["state"] == "approved" and result["provider_called"] is False
    owner.consume(session, digest)
    code, _ = call(capsys, "runtime-memory-approval-respond", *args, "--decision", "approve-once")
    assert code == 2


def test_cli_deny_prevents_consumption(tmp_path, capsys):
    owner, session, digest, args = prepared(tmp_path)
    code, result = call(capsys, "runtime-memory-approval-respond", *args, "--decision", "deny")
    assert code == 0 and result["state"] == "rejected"
    with pytest.raises(ContractError):
        owner.consume(session, digest)


def test_invalid_validity_leaves_pending(tmp_path, capsys):
    owner, session, digest, args = prepared(tmp_path)
    code, _ = call(
        capsys,
        "runtime-memory-approval-respond",
        *args,
        "--decision",
        "approve-once",
        "--valid-for",
        "3601",
    )
    assert code == 2 and owner.query(session, digest)["state"] == "pending"
