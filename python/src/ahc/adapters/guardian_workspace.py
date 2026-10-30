"""WorkspacePort proxy for a per-workspace custody child; no execution fallback."""

import queue
import subprocess
import sys
import threading
from dataclasses import replace
from pathlib import Path

from ahc.domain.sessions import ContractError
from ahc.domain.workspace import WorkspaceIdentity

from .docker_workspace import DockerRunner, WorkspaceUnavailable
from .guardian_topology import require_independent_guardian_host
from .workspace_guardian import FRAME_LIMIT, checkpoint_from, decode_frame, write_frame


class GuardianWorkspace:
    def __init__(self, executable, client_config, identity, *, timeout=5, checkpoint=None):
        require_independent_guardian_host()
        if type(identity) is not WorkspaceIdentity or identity.revision != 0:
            raise ContractError("guardian workspace requires initial identity")
        if type(timeout) not in (int, float) or not 0 < timeout <= 30:
            raise ContractError("guardian timeout invalid")
        runner = DockerRunner(executable, client_config=client_config)  # Validate explicit paths.
        self._identity = identity
        self._timeout = timeout + 35  # Includes bounded inner execution and exact cleanup.
        self._sequence = 0
        self._closed = False
        self._fault = False
        self._lock = threading.RLock()
        bootstrap = {
            "identity": identity.to_dict(),
            "checkpoint": None
            if checkpoint is None
            else {"origin": checkpoint.origin.to_dict(), "values_json": checkpoint.values_json},
            "docker_executable": runner.executable,
            "client_config": str(client_config),
            "timeout": timeout,
        }
        package_root = str(Path(__file__).resolve().parents[2])
        code = (
            "import sys;sys.path.insert(0,sys.argv[1]);"
            "from ahc.adapters.workspace_guardian import main;raise SystemExit(main())"
        )
        self._process = subprocess.Popen(
            [sys._base_executable, "-I", "-B", "-c", code, package_root],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            env=runner.environment,
            cwd=str(client_config),
            close_fds=True,
            bufsize=0,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )
        try:
            reply = self._exchange(bootstrap, 0)
            if set(reply) != {"id", "ok", "identity"} or reply["identity"] != identity.to_dict():
                raise ContractError("guardian startup identity mismatch")
        except BaseException:
            self._disconnect()
            raise

    @property
    def identity(self):
        return self._identity

    def _require(self, expected):
        if self._closed or self._fault or expected != self.identity:
            raise ContractError("guardian workspace reference stale or unavailable")

    def _exchange(self, request, sequence):
        replies = queue.Queue(maxsize=1)

        def exchange():
            try:
                write_frame(self._process.stdin, request)
                replies.put(decode_frame(self._process.stdout.readline(FRAME_LIMIT + 1)))
            except Exception:
                replies.put(None)

        thread = threading.Thread(target=exchange, daemon=True)
        thread.start()
        try:
            reply = replies.get(timeout=self._timeout)
            if (
                type(reply) is not dict
                or type(reply.get("id")) is not int
                or reply["id"] != sequence
                or reply.get("ok") is not True
            ):
                raise ValueError()
            return reply
        except (ValueError, queue.Empty):
            self._fault = True
            self._disconnect()
            raise WorkspaceUnavailable("guardian operation is indeterminate") from None

    def _call(self, operation, expected, value):
        self._sequence += 1
        reply = self._exchange(
            {
                "id": self._sequence,
                "operation": operation,
                "expected": expected.to_dict(),
                "value": value,
            },
            self._sequence,
        )
        if set(reply) != {"id", "ok", "result"}:
            self._fault = True
            self._disconnect()
            raise WorkspaceUnavailable("guardian response invalid")
        return reply["result"]

    def execute(self, expected, code):
        if type(code) is not str or len(code.encode("utf-8")) > 16384:
            raise ContractError("guardian code invalid")
        with self._lock:
            self._require(expected)
            self._identity = replace(expected, revision=expected.revision + 1)
            result = self._call("execute", expected, code)
            if (
                type(result) is not dict
                or set(result) != {"identity", "ok", "output", "output_truncated", "error"}
                or result.get("identity") != self.identity.to_dict()
                or type(result.get("ok")) is not bool
                or type(result.get("output")) is not str
                or len(result["output"]) > 4096
                or any(0xD800 <= ord(c) <= 0xDFFF for c in result["output"])
                or type(result.get("output_truncated")) is not bool
                or result.get("error") != (None if result["ok"] else "execution-error")
            ):
                self._fault = True
                self._disconnect()
                raise WorkspaceUnavailable("guardian execution result invalid")
            return result

    def inspect(self, expected, names):
        from ahc.domain.workspace_inspection import inspection_names, inspection_result

        inspection_names(names)
        with self._lock:
            self._require(expected)
            raw = self._call("inspect", expected, list(names))
            try:
                if (
                    type(raw) is not dict
                    or set(raw) != {"identity", "variables"}
                    or raw["identity"] != expected.to_dict()
                ):
                    raise ContractError("inspection identity mismatch")
                return inspection_result(expected, names, raw["variables"])
            except ContractError:
                self._fault = True
                self._disconnect()
                raise WorkspaceUnavailable("guardian inspection invalid") from None

    def checkpoint(self, expected, names):
        if type(names) is not tuple:
            raise ContractError("checkpoint names must be immutable")
        with self._lock:
            self._require(expected)
            raw = self._call("checkpoint", expected, list(names))
            try:
                result = checkpoint_from(raw)
            except ContractError:
                self._fault = True
                self._disconnect()
                raise WorkspaceUnavailable("guardian checkpoint response invalid") from None
            if result.origin != expected:
                self._fault = True
                self._disconnect()
                raise WorkspaceUnavailable("guardian checkpoint identity mismatch")
            return result

    def restore(self, expected, checkpoint):
        value = {"origin": checkpoint.origin.to_dict(), "values_json": checkpoint.values_json}
        with self._lock:
            self._require(expected)
            successor = replace(expected, generation=expected.generation + 1, revision=0)
            self._identity = successor
            result = self._call("restore", expected, value)
            if result != successor.to_dict():
                self._fault = True
                self._disconnect()
                raise WorkspaceUnavailable("guardian restore identity mismatch")
            return successor

    def _disconnect(self):
        if self._closed:
            return
        self._process.stdin.close()
        try:
            code = self._process.wait(timeout=45)
        except subprocess.TimeoutExpired:
            raise WorkspaceUnavailable("guardian cleanup remains unverified") from None
        self._process.stdout.close()
        if code != 0:
            self._fault = True
            raise WorkspaceUnavailable("guardian cleanup did not confirm success")
        self._closed = True

    def close(self):
        with self._lock:
            if self._closed:
                return
            if not self._fault:
                self._sequence += 1
                reply = self._exchange({"id": self._sequence, "operation": "close"}, self._sequence)
                if reply != {"id": self._sequence, "ok": True, "closed": True}:
                    self._fault = True
                    self._disconnect()
                    raise WorkspaceUnavailable("guardian close acknowledgment invalid")
            self._disconnect()

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        self.close()
