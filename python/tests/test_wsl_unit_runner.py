from dataclasses import replace

import pytest

from ahc.adapters import wsl_unit_runner as module
from ahc.domain.sessions import ContractError
from ahc.infrastructure.backend_custody import BackendCustodyUnavailable, LinuxUnitIdentity

IDENTITY = LinuxUnitIdentity(
    "AHC-Test",
    "12345678-1234-1234-1234-123456789abc",
    "ahc-backend-" + "a" * 32 + ".service",
    "b" * 32,
)


@pytest.mark.parametrize(
    "change",
    [
        {"distribution": "Other"},
        {"cgroup_empty": 1},
        {"boot_id": "invalid"},
        {"invocation_id": "0" * 32},
        {"state": "unknown"},
        {"unit": "unowned.service"},
    ],
)
def test_untrusted_response_rejected(monkeypatch, change):
    from dataclasses import asdict

    value = {**asdict(IDENTITY), "state": "running", "cgroup_empty": False, **change}

    class Transport:
        def __init__(self, owned):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def send(self, request, **kwargs):
            pass

        def receive(self, **kwargs):
            return {"ok": True, "result": value}

    monkeypatch.setattr(module, "spawn_managed", lambda *args, **kwargs: object())
    monkeypatch.setattr(module, "CodexStdioTransport", Transport)
    runner = module.WslUnitRunner("C:/Windows/System32/wsl.exe", "AHC-Test")
    with pytest.raises(BackendCustodyUnavailable):
        runner.observe(IDENTITY)


def test_cross_distribution_rejected_before_launch(monkeypatch):
    runner = module.WslUnitRunner("C:/Windows/System32/wsl.exe", "AHC-Test")
    monkeypatch.setattr(module, "spawn_managed", lambda *a, **kw: pytest.fail("launched"))
    with pytest.raises(ContractError):
        runner.stop(replace(IDENTITY, distribution="Other"))


@pytest.mark.parametrize(
    "command,lifetime",
    [
        ("/usr/bin/sleep", 1),
        ([], 1),
        (["relative"], 1),
        (["/bin/sleep", "1\n"], 1),
        (["/bin/sleep", "1"], True),
        (["/bin/sleep", "1"], 3601),
    ],
)
def test_invalid_creation_never_launches(monkeypatch, command, lifetime):
    runner = module.WslUnitRunner("C:/Windows/System32/wsl.exe", "AHC-Test")
    monkeypatch.setattr(runner, "_call", lambda *a: pytest.fail("launched"))
    with pytest.raises(ContractError):
        runner.create(command, lifetime_seconds=lifetime)


@pytest.mark.parametrize(
    "error", [BackendCustodyUnavailable("lost response"), OSError("client cleanup failed")]
)
def test_creation_loss_retains_attempt_without_replay_or_cleanup(monkeypatch, error):
    runner = module.WslUnitRunner("C:/Windows/System32/wsl.exe", "AHC-Test")
    calls = []

    def call(request):
        calls.append(request)
        raise error

    monkeypatch.setattr(runner, "_call", call)
    with pytest.raises(module.WslCreationIndeterminate) as caught:
        runner.create(["/usr/bin/sleep", "1"], lifetime_seconds=1)
    assert len(calls) == 1 and calls[0]["action"] == "create"
    assert caught.value.unit == calls[0]["unit"]
    assert caught.value.distribution == "AHC-Test"
    assert not isinstance(caught.value, LinuxUnitIdentity)
    with pytest.raises(ContractError):
        runner.stop(caught.value)
    assert len(calls) == 1


@pytest.mark.parametrize("phase", ["open", "send", "receive", "close", "decode"])
def test_create_retains_sanitized_control_phase_without_retry(monkeypatch, phase):
    runner = module.WslUnitRunner("C:/Windows/System32/wsl.exe", "AHC-Test")
    calls = []
    class Transport:
        def __enter__(self):
            return self
        def __exit__(self, *args):
            if phase == "close":
                raise OSError("PRIVATE diagnostic")
        def send(self, *args, **kw):
            calls.append("send")
            if phase == "send":
                raise module.CodexTransportError("PRIVATE diagnostic")
        def receive(self, **kw):
            if phase == "receive":
                raise module.CodexTransportError("PRIVATE diagnostic")
            return {"malformed": True}
    def open_transport(source):
        if phase == "open":
            raise OSError("PRIVATE diagnostic")
        return Transport()
    monkeypatch.setattr(runner, "_open_transport", open_transport)
    with pytest.raises(module.WslCreationIndeterminate) as raised:
        runner.create(["/usr/bin/true"], lifetime_seconds=10)
    assert raised.value.control_phase == phase
    assert "PRIVATE" not in str(raised.value)
    assert len(calls) <= 1
