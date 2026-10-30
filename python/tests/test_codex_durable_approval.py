"""Vendor peer -> durable owner CLI -> one wire response -> terminal commit."""

import json
from dataclasses import replace

from skill_test_support import goal
from test_codex_approval import approval_frame
from test_codex_effective_policy import Peer, policy, request

from ahc.adapters.codex_client import CodexProtocolClient
from ahc.application.pending_approval import PendingApprovalPort
from ahc.cli.main import main
from ahc.domain.runtime_turn import accept_observation
from ahc.domain.working_sets import WorkingSetSnapshot
from ahc.infrastructure.runtime_approval_store import RuntimeApprovalStore
from ahc.infrastructure.runtime_repository import RuntimeRepository


def test_cli_decision_composes_with_vendor_and_durable_terminal_without_replay(tmp_path, capsys):
    effective = policy()
    repo = RuntimeRepository.initialize(tmp_path / "runtime")
    decisions = RuntimeApprovalStore(repo)
    peer = Peer(effective)
    peer.messages.insert(3, approval_frame())
    shown = []

    def operator_reply(_delay):
        pending = decisions.pending(admitted.session)
        assert len(pending) == 1
        digest = pending[0]["request_digest"]
        shown.append(digest)
        assert (
            main(
                [
                    "runtime-approval-respond",
                    "--runtime-root",
                    str(repo.root),
                    "--session-json",
                    json.dumps(admitted.session.to_dict()),
                    "--request-digest",
                    digest,
                    "--decision",
                    "approve-once",
                ]
            )
            == 0
        )
        assert json.loads(capsys.readouterr().out)["outcome"] == "approve-once"

    class Port:
        def decide(self, pending, *, cancelled):
            return PendingApprovalPort(decisions, admitted, wait=operator_reply).decide(
                pending, cancelled=cancelled
            )

    client = CodexProtocolClient(
        peer,
        cwd="workspace",
        model="fixture",
        clock=lambda: 0,
        effective_policy=effective,
        approval_port=Port(),
    )
    original = request(client, effective)
    snapshot = repo.create(WorkingSetSnapshot.initial(original.session, goal()))
    admitted = replace(original, goal_digest=snapshot.goal.digest)
    with repo.execution_lock():
        repo.begin(snapshot, admitted)
        observed = client.run_admitted(admitted)
        assert observed.disposition == "completed"
        result = accept_observation(admitted, observed)
        repo.seal_observation(admitted, result)
        terminal = repo.finish(admitted, result)
    assert len(shown) == 1
    assert [x for x in peer.sent if x.get("id") == "approval"] == [
        {"id": "approval", "result": {"decision": "accept"}}
    ]
    assert decisions.query(admitted.session, shown[0])["state"] == "consumed"
    reopened = RuntimeRepository(repo.root)
    replay, is_new = reopened.begin(snapshot, admitted)
    assert not is_new and replay == terminal
    assert len(reopened.intents(admitted.session)) == 1
