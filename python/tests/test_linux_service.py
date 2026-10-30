import pytest

from ahc.cli.linux_service import render_user_service
from ahc.domain.sessions import ContractError


def options():
    return dict(interpreter="/home/ahc/releases/pinned/bin/python",
                config="/home/ahc/config/app.toml", agent="agent", workspace="/home/ahc/work",
                credential_file="/home/ahc/config/auth.env")


def test_service_preserves_qualified_owner_and_bounded_restart():
    unit = render_user_service(**options())
    assert " -I -m ahc run --config /home/ahc/config/app.toml --agent agent\n" in unit
    assert all(value + "\n" in unit for value in (
        "Delegate=yes", "KillMode=control-group", "SendSIGKILL=yes", "StartLimitBurst=3",
        "StartLimitIntervalSec=60", "Restart=on-failure", "RestartSec=3", "TimeoutStopSec=30"))
    assert "EnvironmentFile=/home/ahc/config/auth.env\n" in unit
    assert "User=" not in unit and "ExecStop=" not in unit
    assert "DelegateSubgroup=" not in unit and "ExecStartPre=" not in unit
    assert "Slice=" not in unit


def test_resource_partition_is_explicit_and_uses_the_same_pinned_interpreter():
    unit = render_user_service(**options(), resource_partition=True)
    assert "DelegateSubgroup=ahc-controller\n" in unit
    assert "Slice=-.slice\n" in unit
    assert ("ExecStartPre=/home/ahc/releases/pinned/bin/python"
            " -I -m ahc.cli.linux_resource_owner\n") in unit
    assert "Delegate=yes\n" in unit and "KillMode=control-group\n" in unit
    with pytest.raises(ContractError):
        render_user_service(**options(), resource_partition="yes")


@pytest.mark.parametrize("field,value", [("agent", "a\nExecStart=/evil"), ("config", "/tmp/%h"),
    ("interpreter", "C:/python.exe"), ("workspace", "/home/a/../b"),
    ("credential_file", "/home/a $VAR"), ("config", "/home//a"), ("workspace", "/")])
def test_service_rejects_expansion_and_argument_ambiguity(field, value):
    values = options()
    values[field] = value
    with pytest.raises(ContractError):
        render_user_service(**values)


@pytest.mark.parametrize("field", ["interpreter", "config", "workspace", "credential_file"])
def test_service_rejects_windows_mounted_core_paths(field):
    values = options()
    values[field] = "/mnt/c/ahc/core"
    with pytest.raises(ContractError):
        render_user_service(**values)
