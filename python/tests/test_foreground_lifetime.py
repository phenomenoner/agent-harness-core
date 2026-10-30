import io
import json
from types import SimpleNamespace

import pytest
from test_foreground_config_v2 import valid_config

from ahc.adapters import linux_unit_helper, linux_unit_stream
from ahc.adapters.workspace_launch import kernel_command
from ahc.adapters.wsl_stream_runner import WslStreamRunner
from ahc.cli.foreground_config_v2 import parse_foreground_config_v2
from ahc.domain.sessions import ContractError


@pytest.mark.parametrize("engine", ["python", "ipython"])
def test_startup_guard_is_bounded_and_keeps_unit_command_within_protocol(engine):
    original = kernel_command("/home/ahc/work", engine)
    guarded = kernel_command("/home/ahc/work", engine, startup_timeout=30)
    assert guarded.endswith(original) and len(guarded) <= 4096
    assert "sys.exit(124)" in guarded
    with pytest.raises(ContractError):
        kernel_command("/home/ahc/work", engine, startup_timeout=0)


@pytest.mark.parametrize("mode", ["bounded", "foreground"])
def test_lifetime_mode_is_explicit_and_bound_in_configuration(mode):
    value = valid_config()
    baseline = parse_foreground_config_v2(value)
    assert "lifetime_mode" not in baseline.workspace
    value["workspace"]["lifetime_mode"] = mode
    selected = parse_foreground_config_v2(value)
    assert selected.workspace["lifetime_mode"] == mode
    assert selected.configuration_digest != baseline.configuration_digest


@pytest.mark.parametrize("mode", [None, True, 1, {}, [], "infinite"])
def test_invalid_lifetime_mode_rejected(mode):
    value = valid_config()
    value["workspace"]["lifetime_mode"] = mode
    with pytest.raises(ContractError):
        parse_foreground_config_v2(value)


def test_backend_cannot_select_owner_lifetime():
    with pytest.raises(ContractError):
        WslStreamRunner("C:/Windows/System32/wsl.exe", "AHC-Codex-Ubuntu-24.04",
                        owner_lifetime=True)
    value = valid_config()
    value["backend"]["lifetime_mode"] = "foreground"
    with pytest.raises(ContractError):
        parse_foreground_config_v2(value)


@pytest.mark.parametrize("owner,attach", [(False, True), (True, True), (True, False)])
def test_lifetime_launch_and_disconnect_cleanup(monkeypatch, owner, attach):
    identity = {"distribution": "AHC", "boot_id": "boot", "unit": "unit",
                "invocation_id": "invocation", "state": "running", "cgroup_empty": False}
    calls, spawned = [], []
    child = SimpleNamespace(stdin=io.BytesIO(), stdout=io.BytesIO(),
                            poll=lambda: None, terminate=lambda: calls.append("terminate"))

    def control(request, *, launch=None):
        calls.append(request)
        if launch:
            return launch(["/usr/bin/systemd-run", "--property=RuntimeMaxSec=10", "--",
                           "/usr/bin/python3", "--property=RuntimeMaxSec=999"], "unit")
        return identity

    def popen(args, **kwargs):
        spawned.append(args)
        return child

    helper = SimpleNamespace(control=control, _observe=lambda unit: identity,
                             _object=linux_unit_helper._object,
                             ControlError=linux_unit_helper.ControlError)
    raw = json.dumps({"action": "create"}) + "\n"
    if attach:
        raw += json.dumps({"action": "attach", **{k: identity[k] for k in
                          ("distribution", "boot_id", "unit", "invocation_id")}}) + "\n"
    output = io.BytesIO()
    monkeypatch.setattr(linux_unit_stream.sys, "stdin", io.TextIOWrapper(io.BytesIO(raw.encode())))
    monkeypatch.setattr(linux_unit_stream.sys, "stdout", io.TextIOWrapper(output))
    monkeypatch.setattr(linux_unit_stream.subprocess, "Popen", popen)
    monkeypatch.setattr(linux_unit_stream.select, "select", lambda *args: ([1], [], []))
    # Run the relay deterministically, including EOF while output is empty.
    monkeypatch.setattr(linux_unit_stream.threading, "Thread", lambda target, args, **kw:
                        SimpleNamespace(start=lambda: target(*args)))
    linux_unit_stream.serve(helper, persistent=True, owner_lifetime=owner)
    assert ("--property=RuntimeMaxSec=infinity" in spawned[0]) is owner
    assert spawned[0][-1] == "--property=RuntimeMaxSec=999"
    stops = [c for c in calls if isinstance(c, dict) and c["action"] == "stop"]
    assert bool(stops) is owner
    assert all(c == {"action": "stop", "unit": "unit", "boot_id": "boot",
                     "invocation_id": "invocation"} for c in stops)
    frames = [json.loads(line) for line in output.getvalue().splitlines()]
    assert ({"attached": True} in frames) is attach


def test_owner_lifetime_never_claims_unknown_identity_cleanup(monkeypatch):
    helper = SimpleNamespace(ControlError=linux_unit_helper.ControlError)
    monkeypatch.setattr(linux_unit_stream.select, "select", lambda *args: ([], [], []))
    output = io.BytesIO()
    monkeypatch.setattr(linux_unit_stream.sys, "stdout", io.TextIOWrapper(output))
    linux_unit_stream.serve(helper, persistent=True, owner_lifetime=True)
    assert json.loads(output.getvalue()) == {"ok": False, "error": "unit stream unverified"}
