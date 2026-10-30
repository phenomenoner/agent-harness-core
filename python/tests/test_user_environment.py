"""Optional Windows discovery cannot become a Linux workspace admission gate."""

from types import SimpleNamespace
import stat

import pytest

from ahc.cli import foreground_user_environment as environment
from test_user_full_access import full_access_config


def test_full_access_has_user_and_declared_windows_tools_without_channel_secrets(monkeypatch):
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "do-not-copy")
    monkeypatch.setenv("WSL_INTEROP", "/operator-session/foreign.sock")
    monkeypatch.setattr(environment, "_user_environment", lambda: {
        "HOME": "/home/agent", "USER": "agent", "LOGNAME": "agent", "PATH": "/usr/local/bin:/usr/bin:/bin"})
    monkeypatch.setattr(environment, "_interop_socket", lambda: "/run/WSL/2_interop")
    config = full_access_config()
    config["host_paths"] = [{"host_root": "C:/", "backend_root": "/mnt/c"}]
    actual = environment.native_workspace_environment(config)
    assert actual == {"HOME": "/home/agent", "USER": "agent", "LOGNAME": "agent",
        "PATH": "/usr/local/bin:/usr/bin:/bin:/mnt/c/Windows/System32:/mnt/c/Windows/System32/WindowsPowerShell/v1.0",
        "WSL_INTEROP": "/run/WSL/2_interop"}


def test_missing_windows_server_still_constructs_the_linux_environment(monkeypatch):
    monkeypatch.setattr(environment, "_user_environment", lambda: {"HOME": "/home/agent", "PATH": "/usr/bin:/bin"})
    monkeypatch.setattr(environment, "_interop_socket", lambda: None)
    assert environment.native_workspace_environment(full_access_config()) == {
        "HOME": "/home/agent", "PATH": "/usr/bin:/bin"}


def test_protected_and_omitted_profiles_do_not_discover_windows(monkeypatch):
    def unwanted():
        pytest.fail("protected environment must stay empty")
    monkeypatch.setattr(environment, "_user_environment", unwanted)
    monkeypatch.setattr(environment, "_interop_socket", unwanted)
    assert environment.native_workspace_environment({}) == {}


@pytest.mark.parametrize("failure", ["missing", "denied", "shell-only", "replaced", "user-init", "init"])
def test_interop_discovery_is_optional_and_does_not_select_shell_sockets(monkeypatch, failure):
    class Path:
        def __init__(self, value): self.value = value
        def __truediv__(self, value): return Path(self.value + "/" + value)
        def glob(self, _pattern):
            if failure in {"missing", "denied"}: raise OSError()
            return [Path("/run/WSL/123_interop")]
        @property
        def name(self): return self.value.rsplit("/", 1)[1]
        def lstat(self): return SimpleNamespace(st_mode=stat.S_IFSOCK, st_uid=0)
        def read_text(self):
            if failure == "replaced": raise FileNotFoundError()
            name = "init-systemd(AH" if failure in {"init", "user-init"} else "bash"
            uid = "1000" if failure == "user-init" else "0"
            return f"Name:\t{name}\nUid:\t{uid}\t{uid}\t{uid}\t{uid}\n"
        def __str__(self): return self.value
    monkeypatch.setattr(environment, "Path", Path)
    assert environment._interop_socket() == ("/run/WSL/123_interop" if failure == "init" else None)
