"""Explicit Docker container custody for the isolated workspace backend.

Only a successful create response containing one full immutable ID grants cleanup
custody. Names, labels, failed output and prefixes never become removal targets.
"""

import json
import os
import re
import subprocess
from dataclasses import dataclass
from uuid import uuid4

from ahc.domain.sessions import ContractError

IMAGE = "sha256:540c7d91f98ff6880174c40e99067bf5941eb54d818a7a5e094d188b196a934d"
_CREATION_TOKEN = object()
TMPFS = "rw,nosuid,nodev,noexec,size=16777216,mode=1777"


class WorkspaceUnavailable(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class DockerReply:
    code: int
    stdout: str


class DockerRunner:
    def __init__(self, executable, *, client_config):
        from pathlib import Path

        path = Path(executable)
        if not path.is_absolute() or not path.is_file():
            raise ContractError("an explicit Docker executable is required")
        self.executable = str(path)
        config = Path(client_config)
        if not config.is_absolute() or not config.is_dir():
            raise ContractError("an explicit empty Docker client directory is required")
        if (config / "config.json").exists():
            raise ContractError("workspace Docker client must have no inherited configuration")
        self.prefix = [
            self.executable,
            "--host",
            "npipe:////./pipe/dockerDesktopLinuxEngine",
            "--config",
            str(config),
        ]
        self.environment = {
            key: os.environ[key] for key in ("SystemRoot", "WINDIR", "COMSPEC") if key in os.environ
        }
        self.environment.update(TEMP=str(config), TMP=str(config))

    def __call__(self, args, *, timeout=15):
        try:
            result = subprocess.run(
                [*self.prefix, *args],
                env=self.environment,
                capture_output=True,
                timeout=timeout,
                creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
            )
        except (OSError, subprocess.TimeoutExpired):
            raise WorkspaceUnavailable("Docker operation is indeterminate") from None
        if len(result.stdout) > 262144:
            raise WorkspaceUnavailable("Docker response exceeds bound")
        try:
            return DockerReply(result.returncode, result.stdout.decode("utf-8"))
        except UnicodeError:
            raise WorkspaceUnavailable("Docker response encoding invalid") from None

    def attach(self, container_id):
        if re.fullmatch(r"[0-9a-f]{64}", container_id) is None:
            raise ContractError("Docker attach requires a full container ID")
        return subprocess.Popen(
            [*self.prefix, "start", "--attach", "--interactive", container_id],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            env=self.environment,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )


class OwnedContainer:
    """Local custody object; callers must not instantiate it from discovered IDs."""

    def __init__(self, runner, container_id, nonce, command, *, token=None):
        if token is not _CREATION_TOKEN:
            raise ContractError("container custody requires successful creation")
        self._runner = runner
        self._container_id = container_id
        self.nonce = nonce
        self.command = command
        self.removed = False

    @property
    def container_id(self):
        return self._container_id

    @classmethod
    def create(cls, runner, command, *, on_created=None):
        if type(command) is not tuple or not command or any(type(x) is not str for x in command):
            raise ContractError("workspace command must be an immutable string tuple")
        nonce = uuid4().hex
        args = [
            "create",
            "--interactive",
            "--pull",
            "never",
            "--network",
            "none",
            "--read-only",
            "--tmpfs",
            f"/workspace:{TMPFS}",
            "--tmpfs",
            f"/tmp:{TMPFS}",
            "--pids-limit",
            "32",
            "--memory",
            "128m",
            "--cpus",
            "0.5",
            "--cap-drop",
            "ALL",
            "--security-opt",
            "no-new-privileges",
            "--user",
            "65534:65534",
            "--workdir",
            "/workspace",
            "--label",
            f"ahc.workspace={nonce}",
            IMAGE,
            *command,
        ]
        reply = runner(args)
        if reply.code != 0 or type(reply.stdout) is not str:
            raise WorkspaceUnavailable("container creation has no proven identity")
        identifier = reply.stdout.strip()
        if re.fullmatch(r"[0-9a-f]{64}", identifier) is None:
            raise WorkspaceUnavailable("container creation identity is indeterminate")
        owned = cls(runner, identifier, nonce, command, token=_CREATION_TOKEN)
        try:
            if on_created is not None:
                on_created(owned)
            owned.verify()
        except Exception:
            owned.remove()
            raise
        return owned

    def verify(self):
        reply = self._runner(["inspect", self.container_id])

        def require(condition):
            if not condition:
                raise WorkspaceUnavailable("container isolation configuration is unverified")

        try:
            rows = json.loads(reply.stdout)
            require(reply.code == 0 and type(rows) is list and len(rows) == 1)
            row = rows[0]
            config, host = row["Config"], row["HostConfig"]
            require(row["Id"] == self.container_id and row["Image"] == IMAGE)
            require(config["Labels"]["ahc.workspace"] == self.nonce)
            require(config["Cmd"] == list(self.command) and not config.get("Entrypoint"))
            require(config["User"] == "65534:65534" and config["WorkingDir"] == "/workspace")
            require(host["NetworkMode"] == "none" and host["ReadonlyRootfs"] is True)
            require(host["PidsLimit"] == 32 and host["Memory"] == 134217728)
            require(host["NanoCpus"] == 500000000 and host["CapDrop"] == ["ALL"])
            require("no-new-privileges" in host["SecurityOpt"])
            require(host["Privileged"] is False and not host.get("Binds") and row["Mounts"] == [])
            require(host["Tmpfs"] == {"/workspace": TMPFS, "/tmp": TMPFS})
        except (AssertionError, KeyError, TypeError, ValueError):
            raise WorkspaceUnavailable("container isolation configuration is unverified") from None

    def remove(self):
        if self.removed:
            return
        reply = self._runner(["rm", "--force", self.container_id])
        if reply.code != 0:
            raise WorkspaceUnavailable("owned container removal is unverified")
        readback = self._runner(
            [
                "container",
                "ls",
                "--all",
                "--no-trunc",
                "--filter",
                f"id={self.container_id}",
                "--format",
                "{{.ID}}",
            ]
        )
        if readback.code != 0 or readback.stdout.strip():
            raise WorkspaceUnavailable("owned container absence is unverified")
        self.removed = True


from .docker_workspace_stateful import StatefulDockerWorkspace

# Preserve the established import/introspection path after extracting its lifecycle.
StatefulDockerWorkspace.__module__ = __name__
