"""Pinned policy mapping checks without a Codex process or model request."""

from dataclasses import replace

import pytest

from ahc.adapters.codex_client import CodexProtocolClient
from ahc.adapters.codex_linux_bundle import LINUX_CODEX_PIN
from ahc.adapters.codex_protocol import CODEX_EXECUTABLE_SHA256
from ahc.domain.agent_backend import AgentTurnRequest
from ahc.domain.runtime_policy import EffectivePolicy
from ahc.domain.sessions import VirtualSessionIdentity


class Peer:
    def __init__(self, policy):
        self.sent, self.closed = [], False
        sandbox = {
            "read-only": {"type": "readOnly", "networkAccess": False},
            "workspace-write": {
                "type": "workspaceWrite",
                "writableRoots": [],
                "networkAccess": False,
                "excludeTmpdirEnvVar": False,
                "excludeSlashTmp": False,
            },
            "danger-full-access": {"type": "dangerFullAccess"},
        }[policy.sandbox_mode]
        self.messages = [
            {
                "id": 1,
                "result": {
                    "codexHome": "PRIVATE",
                    "platformFamily": "windows",
                    "platformOs": "windows",
                    "userAgent": "PRIVATE",
                },
            },
            {
                "id": 2,
                "result": {
                    "cwd": "workspace",
                    "model": "fixture",
                    "modelProvider": "openai",
                    "approvalPolicy": policy.codex_approval_policy,
                    "sandbox": sandbox,
                    "thread": {"id": "thread"},
                },
            },
            {"id": 3, "result": {"turn": {"id": "turn", "status": "inProgress"}}},
            {
                "method": "turn/completed",
                "params": {
                    "threadId": "thread",
                    "turn": {
                        "id": "turn",
                        "status": "completed",
                        "error": None,
                        "items": [
                            {
                                "type": "agentMessage",
                                "id": "item",
                                "phase": "final_answer",
                                "text": "ok",
                            }
                        ],
                    },
                },
            },
        ]

    def send(self, message, *, deadline):
        self.sent.append(message)

    def receive(self, *, deadline):
        return self.messages.pop(0)

    def close(self):
        self.closed = True


def policy(**changes):
    return replace(EffectivePolicy(1, "manual", "workspace-write", True, "a" * 64, 30), **changes)


def request(client, effective):
    return AgentTurnRequest(
        VirtualSessionIdentity.initial("session", "agent", "route"),
        "operation",
        "b" * 64,
        "question",
        (),
        CODEX_EXECUTABLE_SHA256,
        client.configuration_digest,
        "workspace",
        100,
        effective_policy=effective,
    )


def test_effective_policy_is_digest_bound_and_wired_on_start_and_turn():
    effective = policy()
    peer = Peer(effective)
    client = CodexProtocolClient(
        peer, cwd="workspace", model="fixture", clock=lambda: 0, effective_policy=effective
    )
    result = client.run_admitted(request(client, effective))
    assert result.disposition == "completed" and peer.closed
    start = next(item["params"] for item in peer.sent if item.get("method") == "thread/start")
    turn = next(item["params"] for item in peer.sent if item.get("method") == "turn/start")
    assert start["approvalPolicy"] == turn["approvalPolicy"] == "on-request"
    assert start["sandbox"] == "workspace-write"
    assert turn["sandboxPolicy"]["type"] == "workspaceWrite"


def test_policy_drift_rejects_before_wire_and_readback_drift_before_turn():
    effective = policy()
    peer = Peer(effective)
    client = CodexProtocolClient(
        peer, cwd="workspace", model="fixture", clock=lambda: 0, effective_policy=effective
    )
    result = client.run_admitted(
        replace(request(client, effective), effective_policy=policy(revision=2))
    )
    assert result.disposition == "unsupported" and peer.sent == [] and peer.closed

    peer = Peer(effective)
    peer.messages[1]["result"]["sandbox"] = {"type": "readOnly", "networkAccess": False}
    client = CodexProtocolClient(
        peer, cwd="workspace", model="fixture", clock=lambda: 0, effective_policy=effective
    )
    result = client.run_admitted(request(client, effective))
    assert result.disposition == "not-submitted"
    assert not any(item.get("method") == "turn/start" for item in peer.sent)


def test_yolo_changes_only_vendor_approval_policy():
    manual, yolo = policy(), policy(approval_mode="yolo")
    left = CodexProtocolClient(
        Peer(manual), cwd="workspace", model="fixture", effective_policy=manual
    )
    right = CodexProtocolClient(Peer(yolo), cwd="workspace", model="fixture", effective_policy=yolo)
    assert left.configuration_digest != right.configuration_digest
    assert manual.sandbox_mode == yolo.sandbox_mode == "workspace-write"


def linux_workspace_readback():
    # Captured from the pinned Linux backend before any model turn was sent.
    return {
        "type": "workspaceWrite",
        "writableRoots": ["/tmp"],
        "networkAccess": False,
        "excludeTmpdirEnvVar": True,
        "excludeSlashTmp": False,
    }


def run_readback(sandbox, *, linux=True, response_changes=None):
    effective = policy(approval_mode="yolo")
    peer = Peer(effective)
    if linux:
        peer.messages[0]["result"].update(platformFamily="unix", platformOs="linux")
    peer.messages[1]["result"].update(sandbox=sandbox, **(response_changes or {}))
    client = CodexProtocolClient(
        peer, cwd="workspace", model="fixture", reasoning_effort="max",
        clock=lambda: 0, effective_policy=effective,
        **({"target": LINUX_CODEX_PIN["target"]} if linux else {}),
    )
    admitted = replace(request(client, effective), artifact_digest=client.executable_sha256)
    return client.run_admitted(admitted), peer


def test_linux_workspace_write_canonical_temp_readback_completes():
    result, peer = run_readback(linux_workspace_readback())
    assert result.disposition == "completed" and peer.closed
    turn = next(item["params"] for item in peer.sent if item.get("method") == "turn/start")
    assert turn["approvalPolicy"] == "never" and turn["effort"] == "max"
    assert turn["sandboxPolicy"]["writableRoots"] == []
    assert turn["sandboxPolicy"]["networkAccess"] is False


@pytest.mark.parametrize("changes", [
    {"writableRoots": ["/home"]},
    {"writableRoots": ["/tmp", "/home"]},
    {"writableRoots": ["/tmp", "/tmp"]},
    {"networkAccess": True},
    {"networkAccess": 0},
    {"excludeTmpdirEnvVar": False},
    {"excludeTmpdirEnvVar": 1},
    {"excludeSlashTmp": True},
    {"excludeSlashTmp": 0},
    {"unknownPermission": False},
    {"type": "dangerFullAccess"},
])
def test_linux_temp_compatibility_rejects_permission_drift_before_turn(changes):
    result, peer = run_readback(dict(linux_workspace_readback(), **changes))
    assert result.disposition == "not-submitted" and peer.closed
    assert not any(item.get("method") == "turn/start" for item in peer.sent)


@pytest.mark.parametrize("changes", [
    {"cwd": "other-workspace"},
    {"model": "other-model"},
    {"modelProvider": "other-provider"},
    {"approvalPolicy": "on-request"},
])
def test_linux_temp_compatibility_preserves_thread_identity_guards(changes):
    result, peer = run_readback(linux_workspace_readback(), response_changes=changes)
    assert result.disposition == "not-submitted" and peer.closed
    assert not any(item.get("method") == "turn/start" for item in peer.sent)


def test_linux_temp_readback_does_not_change_windows_policy():
    result, peer = run_readback(linux_workspace_readback(), linux=False)
    assert result.disposition == "not-submitted" and peer.closed
    assert not any(item.get("method") == "turn/start" for item in peer.sent)


def test_legacy_boolean_integer_alias_is_rejected_before_turn():
    result, peer = run_readback({
        "type": "workspaceWrite", "writableRoots": [], "networkAccess": 0,
        "excludeTmpdirEnvVar": False, "excludeSlashTmp": False,
    })
    assert result.disposition == "not-submitted" and peer.closed
    assert not any(item.get("method") == "turn/start" for item in peer.sent)
