"""Real trusted factory with controlled transport, no WSL or provider effects."""

from dataclasses import replace

import pytest
from test_backend_custody import Runner
from test_codex_effective_policy import Peer
from test_foreground_config_v2 import valid_config
from test_foreground_runtime import setup

from ahc.cli import trusted_codex_backend as composition
from ahc.cli.foreground_config_v2 import parse_foreground_config_v2
from ahc.domain.sessions import ContractError
from ahc.infrastructure.backend_custody import LinuxUnitIdentity


def factory_for(runtime, mode="manual"):
    value = valid_config()
    value["policy"]["approval_mode"] = mode
    value["backend"]["model"] = "fixture"
    value["backend"]["enabled"] = True
    return composition.TrustedCodexBackendFactory(
        runtime.store, parse_foreground_config_v2(value)
    )


def test_binding_and_policy_drift_reject_before_runner_creation(tmp_path, monkeypatch):
    runtime, _, _, request = setup(tmp_path)
    runtime.start()
    try:
        factory = factory_for(runtime)
        artifact, configuration = factory.binding(runtime.tools)
        request = replace(
            request, artifact_digest=artifact, configuration_digest=configuration,
            effective_policy=factory.config.effective_policy,
        )
        monkeypatch.setattr(
            composition, "WslStreamRunner", lambda *a, **k: pytest.fail("unexpected launch")
        )
        for changed in (
            replace(request, artifact_digest="0" * 64),
            replace(request, configuration_digest="0" * 64),
            replace(request, effective_policy=replace(request.effective_policy, revision=2)),
        ):
            with pytest.raises(ContractError, match="launch binding"):
                factory(changed, runtime.tools)
    finally:
        runtime.close()


@pytest.mark.parametrize("failures", [0, 1, 2])
def test_managed_protocol_close_has_one_owner_and_two_automatic_attempts(
    tmp_path, monkeypatch, failures
):
    runtime, requests, _, request = setup(tmp_path)
    factory = factory_for(runtime, "yolo")
    policy = factory.config.effective_policy
    peer = Peer(policy)
    peer.messages[0]["result"].update(platformFamily="unix", platformOs="linux")
    peer.messages[1]["result"]["cwd"] = factory.config.workspace["cwd"]
    unit = LinuxUnitIdentity(
        factory.distribution, "12345678-1234-1234-1234-123456789abc",
        "ahc-backend-" + "a" * 32 + ".service", "b" * 32,
    )
    closes = []
    original_close = peer.close

    def close():
        assert runtime.store.read_answer_seal(request.session, request.operation_id)[
            "output_text"] == "ok"
        closes.append(True)
        if len(closes) <= failures:
            raise RuntimeError("PRIVATE transport cleanup failure")
        original_close()

    peer.close = close

    class Attached(Runner):
        def create(self, command, **kwargs):
            turn = runtime.store.read_turn(request.session, request.operation_id)
            assert turn.status == "submitted"
            return unit

        def attach(self, identity, custody, lease):
            with custody.attachment(lease, identity):
                return peer

    runner = Attached(unit)
    monkeypatch.setattr(composition, "WslStreamRunner", lambda *a, **kw: runner)
    runtime.backend_factory = factory
    runtime.start()
    try:
        artifact, configuration = factory.binding(runtime.tools)
        request = replace(request, artifact_digest=artifact, configuration_digest=configuration,
                          effective_policy=policy)
        requests.append(request)
        terminal = runtime.step()
        assert terminal.result["accepted"] is True
        assert terminal.result["output_text"] == "ok"
        assert "cleanup_pending" not in terminal.result  # Cleanup follows terminal delivery.
        assert len(closes) == 1
        assert runtime.cleanup_status()["state"] == ("pending" if failures else "clear")
        assert runtime.step() is None
        assert len(closes) == (2 if failures else 1)
        assert runtime.cleanup_status()["state"] == ("exhausted" if failures == 2 else "clear")
        assert runtime.step() is None
        assert len(closes) == (2 if failures else 1)
        assert sum(m.get("method") == "turn/start" for m in peer.sent) == 1
    finally:
        runtime.close()  # Explicit lifecycle cleanup may attempt an exhausted resource.
    assert len(closes) == failures + 1
    assert runner.stopped and peer.closed


@pytest.mark.parametrize("mode", ["manual", "yolo"])
@pytest.mark.parametrize("client_failure", [False, True])
def test_trusted_factory_admission_policy_and_attached_cleanup(
    tmp_path, monkeypatch, mode, client_failure
):
    runtime, requests, _, request = setup(tmp_path)
    factory = factory_for(runtime, mode)
    policy = factory.config.effective_policy
    peer = Peer(policy)
    peer.messages[0]["result"].update(platformFamily="unix", platformOs="linux")
    peer.messages[1]["result"]["cwd"] = factory.config.workspace["cwd"]
    unit = LinuxUnitIdentity(
        factory.distribution, "12345678-1234-1234-1234-123456789abc",
        "ahc-backend-" + "a" * 32 + ".service", "b" * 32,
    )

    class Attached(Runner):
        def create(self, command, **kwargs):
            admitted = runtime.store.read_turn(request.session, request.operation_id)
            assert admitted.status == "submitted"
            self.calls.append("create")
            return unit

        def attach(self, identity, custody, lease):
            with custody.attachment(lease, identity):
                self.calls.append("attach")
                return peer

        def close(self):
            self.calls.append("close-runner")

    runner = Attached(unit)
    monkeypatch.setattr(composition, "WslStreamRunner", lambda *a, **kw: runner)
    original = factory._client

    def client(transport, tools, cancelled, approval_port=None, answer_seal=None):
        if transport is not None:
            assert approval_port is not None
            if client_failure:
                raise RuntimeError("PRIVATE constructor failure")
        return original(transport, tools, cancelled, approval_port, answer_seal)

    monkeypatch.setattr(factory, "_client", client)
    runtime.backend_factory = factory
    runtime.start()
    try:
        artifact, configuration = factory.binding(runtime.tools)
        requests.append(replace(
            request, artifact_digest=artifact, configuration_digest=configuration,
            effective_policy=policy,
        ))
        result = runtime.step()
        assert result.result["accepted"] is (not client_failure)
        assert peer.closed and runner.stopped
        assert runner.calls == ["create", "attach", "close-runner", "observe", "stop", "observe"]
        if client_failure:
            assert not peer.sent
            assert "PRIVATE" not in str(result.result)
        else:
            assert result.result["output_text"] == "ok"
            start = next(m["params"] for m in peer.sent if m.get("method") == "thread/start")
            turn = next(m["params"] for m in peer.sent if m.get("method") == "turn/start")
            assert start["approvalPolicy"] == turn["approvalPolicy"] == policy.codex_approval_policy
            assert start["sandbox"] == "workspace-write"
            assert turn["sandboxPolicy"]["type"] == "workspaceWrite"
            assert "AHC runtime environment contract v1" in start["developerInstructions"]
            assert "runtime_environment" in turn["input"][0]["text"]
            assert "no_bridge_declared_by_this_adapter" in turn["input"][0]["text"]
    finally:
        runtime.close()
