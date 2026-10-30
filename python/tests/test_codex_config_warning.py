"""Replay the observed bundled-bubblewrap notice without launching a provider."""
from dataclasses import replace
import pytest
from test_codex_client import setup
from ahc.adapters.codex_client import CodexProtocolClient
from ahc.adapters.codex_linux_bundle import LINUX_CODEX_PIN

SUMMARY = (
    "Codex could not find bubblewrap on PATH. Install bubblewrap with your OS package manager. "
    "See the sandbox prerequisites: https://developers.openai.com/codex/concepts/sandboxing#prerequisites. "
    "Codex will use the bundled bubblewrap in the meantime."
)

def fixture():
    peer, client, request = setup()
    client = CodexProtocolClient(peer, cwd="workspace", model="fixture", clock=lambda: 0,
                                target=LINUX_CODEX_PIN["target"])
    request = replace(request, artifact_digest=client.executable_sha256,
                      configuration_digest=client.configuration_digest)
    peer.messages[0]["result"].update(platformFamily="unix", platformOs="linux")
    warning = {"method": "configWarning", "params": {"summary": SUMMARY, "details": None}, "emittedAtMs": 1}
    return peer, client, request, warning

def test_bundled_bubblewrap_notice_does_not_prevent_turn():
    peer, client, request, warning = fixture()
    peer.messages.insert(1, warning)
    result = client.run_admitted(request)
    assert result.disposition == "completed"
    assert sum(m.get("method") == "turn/start" for m in peer.sent) == 1
    assert peer.closed

@pytest.mark.parametrize("change", ["summary", "details", "timestamp", "extra", "windows"])
def test_config_warning_payloads_are_discarded_without_changing_permissions(change):
    peer, client, request, warning = fixture()
    if change == "summary": warning["params"]["summary"] = "Sandbox disabled"
    if change == "details": warning["params"]["details"] = "Unverified fallback"
    if change == "timestamp": warning["emittedAtMs"] = True
    if change == "extra": warning["authority"] = "allow"
    if change == "windows":
        client.platform_os = "windows"
        peer.messages[0]["result"]["platformOs"] = "windows"
    peer.messages.insert(1, warning)
    result = client.run_admitted(request)
    assert result.disposition == "completed"
    assert sum(m.get("method") == "turn/start" for m in peer.sent) == 1
    assert peer.closed


def test_notice_does_not_bypass_policy_readback():
    peer, client, request, warning = fixture()
    peer.messages[1]["result"]["sandbox"] = {"type": "dangerFullAccess"}
    peer.messages.insert(1, warning)
    assert client.run_admitted(request).disposition == "not-submitted"
    assert not any(m.get("method") == "turn/start" for m in peer.sent)


def test_many_valid_notices_do_not_replace_the_parent_deadline():
    peer, client, request, warning = fixture()
    peer.messages[1:1] = [warning] * 5000
    assert client.run_admitted(request).disposition == "completed"
    assert sum(m.get("method") == "turn/start" for m in peer.sent) == 1


def test_notice_after_submission_keeps_the_verified_answer():
    peer, client, request, warning = fixture()
    peer.messages.insert(3, warning)
    assert client.run_admitted(request).disposition == "completed"
    assert sum(m.get("method") == "turn/start" for m in peer.sent) == 1
