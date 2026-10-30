"""Named policy identity checks; fixture evidence is not OS enforcement."""

from dataclasses import replace

import pytest
from test_codex_client import Peer, setup

from ahc.adapters.codex_client import CodexProtocolClient
from ahc.domain.sessions import ContractError


def client(peer, **kwargs):
    return CodexProtocolClient(
        peer,
        cwd="workspace",
        model="fixture",
        clock=lambda: 0,
        permissions="ahc_offline",
        policy_configuration_digest="a" * 64,
        **kwargs,
    )


@pytest.mark.parametrize("resume", [False, True])
def test_named_policy_is_bound_on_thread_and_turn(resume):
    peer = Peer()
    bound = client(peer)
    peer.messages[1]["result"]["activePermissionProfile"] = {"id": "ahc_offline", "extends": None}
    _, legacy, request = setup()
    assert bound.configuration_digest != legacy.configuration_digest
    request = replace(
        request,
        configuration_digest=bound.configuration_digest,
        backend_thread="thread" if resume else None,
    )
    result = bound.run_admitted(request)
    assert result.disposition == "completed"
    assert peer.sent[0]["params"]["capabilities"]["experimentalApi"] is True
    for method in ("thread/resume" if resume else "thread/start", "turn/start"):
        params = next(m["params"] for m in peer.sent if m.get("method") == method)
        assert params["permissions"] == "ahc_offline"
        assert "sandbox" not in params and "sandboxPolicy" not in params
    assert peer.closed


@pytest.mark.parametrize(
    "readback",
    [
        None,
        {},
        {"id": "other"},
        {"id": "ahc_offline", "extends": "parent"},
        {"id": "ahc_offline", "extra": True},
    ],
)
def test_profile_mismatch_never_submits_turn(readback):
    peer = Peer()
    bound = client(peer)
    peer.messages[1]["result"]["activePermissionProfile"] = readback
    _, _, request = setup()
    result = bound.run_admitted(replace(request, configuration_digest=bound.configuration_digest))
    assert result.disposition == "not-submitted"
    assert not any(m.get("method") == "turn/start" for m in peer.sent)
    assert peer.closed


def test_configuration_digest_change_rejects_before_initialize():
    peer = Peer()
    bound = client(peer)
    other = CodexProtocolClient(
        Peer(),
        cwd="workspace",
        model="fixture",
        permissions="ahc_offline",
        policy_configuration_digest="b" * 64,
    )
    _, _, request = setup()
    result = bound.run_admitted(replace(request, configuration_digest=other.configuration_digest))
    assert result.disposition == "unsupported" and not peer.sent and peer.closed


@pytest.mark.parametrize(
    "kwargs",
    [
        {"permissions": "ahc_offline"},
        {"policy_configuration_digest": "a" * 64},
        {"permissions": "ahc_offline", "policy_configuration_digest": "not-a-digest"},
    ],
)
def test_named_profile_requires_configuration_digest(kwargs):
    with pytest.raises(ContractError):
        CodexProtocolClient(Peer(), cwd="workspace", model="fixture", **kwargs)
