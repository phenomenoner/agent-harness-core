"""Actual child protocol and operator CLI over controlled transport/unit fixtures."""

import json
import time
from dataclasses import replace

import pytest
from test_backend_custody import Runner
from test_codex_approval import approval_frame
from test_codex_effective_policy import Peer
from test_foreground_config_v2 import valid_config
from test_runtime_core import setup
from test_runtime_restore_cli import cli

from ahc.application.child_dispatch import ChildDispatchService
from ahc.application.pending_approval import PendingApprovalPort
from ahc.cli import trusted_codex_child
from ahc.cli.foreground_config_v2 import parse_foreground_config_v2
from ahc.cli.trusted_codex_backend import TrustedCodexBackendFactory
from ahc.cli.trusted_codex_child import TrustedCodexChildFactory
from ahc.domain.backend_identity import LinuxUnitIdentity
from ahc.domain.child_work import ChildWorkEnvelope
from ahc.infrastructure.backend_custody import BackendCustody
from ahc.infrastructure.child_approval_queries import ChildApprovalQueries
from ahc.infrastructure.child_dispatch_repository import ChildDispatchRepository


@pytest.mark.parametrize("case", ["approve-once", "deny", "wrong-platform", "lost-terminal"])
def test_child_protocol_with_durable_operator_and_custody(tmp_path, monkeypatch, case):
    repo, snapshot, original = setup(tmp_path)
    workspace = repo.reserve_workspace_generation(snapshot, original.workspace_scope)
    raw = valid_config()
    raw["backend"]["enabled"] = True
    config = parse_foreground_config_v2(raw)
    owner = TrustedCodexBackendFactory(repo, config)
    artifact, configuration = owner.binding(None)
    parent = replace(
        original,
        deadline=time.monotonic() + 60,
        artifact_digest=artifact,
        configuration_digest=configuration,
        effective_policy=config.effective_policy,
    )
    repo.begin(repo.read_current("session"), parent)
    children = ChildDispatchRepository(repo, parent)
    envelope = ChildWorkEnvelope(
        "child",
        children.parent,
        "task",
        parent.workspace_scope,
        time.time_ns() // 1_000_000 + 45000,
    )
    factory = TrustedCodexChildFactory(owner, children, envelope, "token", workspace)
    peer = Peer(config.effective_policy)
    peer.messages[0]["result"].update(platformOs="linux", platformFamily="unix")
    peer.messages[1]["result"].update(cwd=config.workspace["cwd"], model=config.backend["model"])
    if case == "wrong-platform":
        peer.messages[0]["result"]["platformOs"] = "windows"
    elif case == "lost-terminal":
        peer.messages.pop()
    else:
        peer.messages.insert(3, approval_frame())
    unit = LinuxUnitIdentity(
        "AHC-Codex-Ubuntu-24.04",
        "12345678-1234-1234-1234-123456789abc",
        "ahc-backend-" + "c" * 32 + ".service",
        "b" * 32,
    )
    effects, approved = [], []

    class Process(Runner):
        def __init__(self, *args, **kwargs):
            super().__init__(unit)

        def create(self, command, *, lifetime_seconds):
            effects.append("create")
            return unit

        def attach(self, selected, custody, lease):
            assert selected == unit and custody._held(lease)[0][2] == unit
            return peer

        def close(self):
            effects.append("runner-close")

    def approval_port(store, admitted, *, notifier=None):
        assert notifier is None
        def answer(_delay):
            pending = ChildApprovalQueries(repo).pending(admitted.session)
            assert len(pending) == 1 and pending[0]["current_claim"]
            digest = pending[0]["request_digest"]
            reply = cli(
                "runtime-child-approval-respond",
                "--runtime-root",
                repo.root,
                "--session-json",
                json.dumps(admitted.session.to_dict()),
                "--request-digest",
                digest,
                "--decision",
                case,
            )
            assert reply["outcome"] == case
            approved.append(digest)

        return PendingApprovalPort(store, admitted, wait=answer)

    monkeypatch.setattr(owner, "new_runner", Process)
    monkeypatch.setattr(trusted_codex_child, "PendingApprovalPort", approval_port)
    custody = BackendCustody(repo)
    lease = custody.acquire(parent.session, parent.workspace_scope)
    service = ChildDispatchService(children, lambda _: factory.port())
    try:
        with repo.execution_lock(), repo.workspace_turn_scope(parent):
            result = service.dispatch(envelope, "token")
            assert service.dispatch(envelope, "different") == result
        assert result["state"] == ("terminal" if case in ("approve-once", "deny") else "unknown")
        if approved:
            assert len(approved) == 1
            assert (
                ChildApprovalQueries(repo).query(parent.session, approved[0])["state"] == "consumed"
            )
            assert {
                "id": "approval",
                "result": {"decision": "accept" if case == "approve-once" else "decline"},
            } in peer.sent
            assert result["result"] == {"output_text": "ok"}
        else:
            assert case in ("wrong-platform", "lost-terminal")
        methods = [row.get("method") for row in peer.sent]
        assert methods.count("turn/start") == (0 if case == "wrong-platform" else 1)
        for row in peer.sent:
            assert "dynamicTools" not in row.get("params", {})
        assert lease.active and peer.closed and effects == ["create", "runner-close"]
        assert repo.read_turn(parent.session, "child") is None
        with repo._read() as connection:
            assert (
                connection.execute("SELECT state FROM backend_custody").fetchone()[0] == "cleaned"
            )
    finally:
        service.close()
        lease.close()
