"""Skill body wire delivery and durable content identity without provider calls."""

import json
import time
from dataclasses import replace

import pytest
from test_codex_client import setup as client_setup
from test_codex_transport import _FakeOwned
from test_runtime_core import Backend, setup

from ahc.adapters.codex_transport import CodexStdioTransport
from ahc.application.runtime_service import RuntimeService
from ahc.domain.runtime_turn import request_fingerprint
from ahc.domain.sessions import ContractError
from ahc.infrastructure.runtime_repository import RuntimeRepository


def test_client_delivers_skill_body_without_changing_user_input_or_permissions():
    peer, client, request = client_setup()
    request = replace(
        request, revision_digests=("c" * 64,), skill_bodies=(("c" * 64, "Inspect evidence."),)
    )
    assert client.run_admitted(request).disposition == "completed"
    turn = next(m for m in peer.sent if m.get("method") == "turn/start")
    assert turn["params"]["input"][0] == {"type": "text", "text": "question"}
    assert turn["params"]["input"][1]["text"].endswith("\n\nInspect evidence.")
    assert "c" * 64 in turn["params"]["input"][1]["text"]
    assert turn["params"]["sandboxPolicy"] == {"type": "readOnly", "networkAccess": False}
    assert turn["params"]["approvalPolicy"] == "never"


def test_content_drift_rejects_after_reopen_without_replaying_backend(tmp_path):
    repo, expected, request = setup(tmp_path)
    bound = replace(request, revision_digests=("c" * 64,), skill_bodies=(("c" * 64, "body"),))
    backend = Backend()
    result = RuntimeService(repo, backend).execute(expected, bound)
    reopened = RuntimeRepository(repo.root)
    current = reopened.read_current("session")
    assert RuntimeService(reopened, backend).execute(current, bound) == result
    changed = replace(bound, skill_bodies=(("c" * 64, "different body"),))
    assert request_fingerprint(bound) != request_fingerprint(changed)
    with pytest.raises(ContractError):
        RuntimeService(reopened, backend).execute(current, changed)
    assert backend.calls == 1


@pytest.mark.parametrize(
    "bodies",
    [
        [],
        (("d" * 64, "body"),),
        (("c" * 64, ""),),
        (("c" * 64, "\ud800"),),
        (("c" * 64, "x" * 98305),),
    ],
)
def test_invalid_disclosure_rejected_before_backend(tmp_path, bodies):
    _, _, request = setup(tmp_path)
    with pytest.raises(ContractError):
        replace(request, revision_digests=("c" * 64,), skill_bodies=bodies)


def test_outbound_supports_full_disclosure_but_remains_bounded():
    owned = _FakeOwned()
    transport = CodexStdioTransport(owned)
    try:
        message = {"text": "\x01" * 98304}
        transport.send(message, deadline=time.monotonic() + 5)
        raw = owned.process.stdin.writes[0]
        assert len(raw) > 65536 and json.loads(raw) == message
        with pytest.raises(ContractError):
            transport.send({"text": "x" * 1048576}, deadline=time.monotonic() + 5)
        assert len(owned.process.stdin.writes) == 1
        assert transport.MAX_FRAME == 4 * 1048576
    finally:
        transport.close()
