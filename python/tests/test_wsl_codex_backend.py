from dataclasses import replace

import pytest
from test_backend_custody import Runner
from test_codex_client import Peer
from test_foreground_runtime import setup

from ahc.cli import wsl_codex_backend as composition
from ahc.domain.sessions import ContractError
from ahc.infrastructure.backend_custody import LinuxUnitIdentity

SPEC = dict(
    bundle="/bundle",
    home="/home/ahc-private",
    workspace="/work",
    policy_source="/policy.toml",
    policy_sha256="a" * 64,
    profile="ahc_offline",
    home_size_mib=128,
)


def factory_for(runtime):
    return composition.WslCodexBackendFactory(
        runtime.store, "C:/Windows/System32/wsl.exe", "AHC", launch_spec=SPEC, model="fixture"
    )


def test_binding_drift_rejects_before_runner_or_custody_creation(tmp_path, monkeypatch):
    runtime, _, _, request = setup(tmp_path)
    factory = factory_for(runtime)
    runtime.start()
    try:
        artifact, configuration = factory.binding(runtime.tools)
        request = replace(request, artifact_digest=artifact, configuration_digest=configuration)
        monkeypatch.setattr(
            composition, "WslCodexStreamRunner", lambda *a, **kw: pytest.fail("spawn")
        )
        with pytest.raises(ContractError, match="binding"):
            factory(replace(request, artifact_digest="0" * 64), runtime.tools)
        with pytest.raises(ContractError, match="binding"):
            factory(replace(request, configuration_digest="0" * 64), runtime.tools)
    finally:
        runtime.close()


def test_binding_includes_distribution_home_and_lifetime(tmp_path):
    runtime, _, _, _ = setup(tmp_path)
    runtime.start()
    try:
        baseline = factory_for(runtime).binding(runtime.tools)
        for distro, spec, lifetime in (
            ("Other", SPEC, 180),
            ("AHC", {**SPEC, "home": "/home/other"}, 180),
            ("AHC", SPEC, 181),
        ):
            factory = composition.WslCodexBackendFactory(
                runtime.store,
                "C:/Windows/System32/wsl.exe",
                distro,
                launch_spec=spec,
                model="fixture",
                lifetime_seconds=lifetime,
            )
            binding = factory.binding(runtime.tools)
            assert binding[0] == baseline[0] and binding[1] != baseline[1]
    finally:
        runtime.close()


@pytest.mark.parametrize("client_failure", [False, True])
def test_foreground_uses_protocol_client_and_closes_known_unit(
    tmp_path, monkeypatch, client_failure
):
    runtime, requests, _, request = setup(tmp_path)
    factory = factory_for(runtime)
    peer = Peer()
    peer.messages[0]["result"].update(platformFamily="unix", platformOs="linux")
    peer.messages[1]["result"].update(cwd="/work", activePermissionProfile={"id": "ahc_offline"})
    unit = LinuxUnitIdentity(
        "AHC",
        "12345678-1234-1234-1234-123456789abc",
        "ahc-backend-" + "a" * 32 + ".service",
        "b" * 32,
    )

    class Attached(Runner):
        def create(self, command, **kwargs):
            assert command[6] == "/bundle/bin/codex"
            self.calls.append("create")
            return unit

        def attach(self, identity, custody, lease):
            with custody.attachment(lease, identity):
                self.calls.append("attach")
                return peer

        def close(self):
            self.calls.append("close-runner")

    runner = Attached(unit)
    monkeypatch.setattr(composition, "WslCodexStreamRunner", lambda *a, **kw: runner)
    original = factory._client

    def client(transport, tools, cancelled, answer_seal=None):
        if client_failure and transport is not None:
            raise RuntimeError("constructor failed after attach")
        return original(transport, tools, cancelled, answer_seal)

    monkeypatch.setattr(factory, "_client", client)
    runtime.backend_factory = factory
    runtime.start()
    try:
        artifact, configuration = factory.binding(runtime.tools)
        requests.append(
            replace(request, artifact_digest=artifact, configuration_digest=configuration)
        )
        result = runtime.step()
        assert result.result["accepted"] is (not client_failure)
        assert peer.closed and runner.stopped
        assert runner.calls == ["create", "attach", "close-runner", "observe", "stop", "observe"]
        if not client_failure:
            assert result.result["output_text"] == "answer"
            thread = next(m for m in peer.sent if m.get("method") == "thread/start")
            assert thread["params"]["permissions"] == "ahc_offline"
            assert thread["params"]["dynamicTools"][0]["name"] == "workspace"
        else:
            assert not peer.sent
    finally:
        runtime.close()
