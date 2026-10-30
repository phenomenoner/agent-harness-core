"""Explicit WSL invocation values, without launch or Linux custody claims."""

import re
from dataclasses import dataclass
from pathlib import PurePosixPath, PureWindowsPath

from ahc.domain.sessions import ContractError


def _linux_path(value: str) -> str:
    if (
        type(value) is not str
        or not value.startswith("/")
        or value.startswith("//")
        or "\\" in value
        or any(ord(char) < 32 for char in value)
        or ".." in value.split("/")
        or PurePosixPath(value).as_posix() != value
    ):
        raise ContractError("WSL requires a canonical absolute Linux path")
    return value


@dataclass(frozen=True, slots=True)
class WslCommand:
    """A command plan; Windows process exit cannot certify Linux cleanup.

    The caller must verify the launcher artifact, distribution identity and Linux
    custody before using this command for backend admission. User task content must
    travel through protocol stdin, not be supplied as launcher arguments.
    """

    host_executable: str
    distribution: str
    user: str
    directory: str
    launcher: str

    def __post_init__(self):
        host = self.host_executable
        if (
            type(host) is not str
            or not PureWindowsPath(host).is_absolute()
            or PureWindowsPath(host).drive.startswith("\\")
            or PureWindowsPath(host).name.lower() != "wsl.exe"
            or ".." in PureWindowsPath(host).parts
            or any(ord(char) < 32 for char in host)
        ):
            raise ContractError("WSL requires an explicit local Windows executable")
        if type(self.distribution) is not str or not re.fullmatch(
            r"[A-Za-z0-9][A-Za-z0-9_.-]{0,63}", self.distribution
        ):
            raise ContractError("WSL distribution must be explicit")
        if (
            type(self.user) is not str
            or not re.fullmatch(r"[a-z_][a-z0-9_-]{0,31}", self.user)
            or self.user == "root"
        ):
            raise ContractError("WSL requires an explicit non-root user")
        _linux_path(self.directory)
        _linux_path(self.launcher)
        if self.launcher == "/":
            raise ContractError("WSL launcher must name an executable")

    def argv(self) -> tuple[str, ...]:
        return (
            self.host_executable,
            "--distribution", self.distribution,
            "--user", self.user,
            "--cd", self.directory,
            "--exec", self.launcher,
        )
