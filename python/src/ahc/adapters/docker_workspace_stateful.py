"""Bounded stateful interpreter lifecycle for the Docker workspace adapter."""

import json
import subprocess
from dataclasses import replace

from ahc.domain.sessions import ContractError


class StatefulDockerWorkspace:
    """One process-owned, revision-checked namespace behind the verified container."""

    def __init__(self, runner, identity, *, timeout=5, checkpoint=None, on_created=None):
        import threading

        from ahc.domain.workspace import WorkspaceCheckpoint, WorkspaceIdentity

        if type(identity) is not WorkspaceIdentity or identity.revision != 0:
            raise ContractError("workspace open requires an initial revision")
        if checkpoint is not None and (
            type(checkpoint) is not WorkspaceCheckpoint
            or checkpoint.origin.session != identity.session
            or checkpoint.origin.workspace_id != identity.workspace_id
            or checkpoint.origin.generation >= identity.generation
        ):
            raise ContractError("workspace reconstruction requires an older exact-owner checkpoint")
        if type(timeout) not in (int, float) or not 0 < timeout <= 30:
            raise ContractError("workspace timeout invalid")
        self._runner = runner
        self._on_created = on_created
        self._identity = identity
        self._timeout = timeout
        self._lock = threading.RLock()
        self._sequence = 0
        self._process = None
        self._owned = None
        self._closed = True
        self._open()
        if checkpoint is not None:
            try:
                reply = self._exchange(
                    {"operation": "restore", "values": json.loads(checkpoint.values_json)}
                )
                if reply["ok"] is not True:
                    raise self._unavailable("workspace reconstruction is unverified")
            except BaseException:
                self._close_locked()
                raise

    @staticmethod
    def _unavailable(message):
        from .docker_workspace import WorkspaceUnavailable

        return WorkspaceUnavailable(message)

    @property
    def identity(self):
        return self._identity

    def _open(self):
        from .docker_workspace import OwnedContainer
        from .workspace_kernel import KERNEL

        self._owned = OwnedContainer.create(
            self._runner,
            ("python", "-u", "-c", KERNEL),
            on_created=None
            if self._on_created is None
            else lambda owned: self._on_created(self._identity, owned),
        )
        try:
            self._process = self._runner.attach(self._owned.container_id)
            self._closed = False
            reply = self._exchange({"operation": "restore", "values": {}})
            if reply.get("ok") is not True:
                raise self._unavailable("workspace interpreter is unavailable")
        except Exception:
            self._close_locked()
            raise

    def _require(self, expected):
        if self._closed or expected != self._identity:
            raise ContractError("workspace owner generation or revision is stale")

    def _exchange(self, request):
        import queue
        import threading

        self._sequence += 1
        request = {**request, "id": self._sequence}
        encoded = json.dumps(request, allow_nan=False).encode("utf-8") + b"\n"
        if len(encoded) > 65536:
            raise ContractError("workspace request exceeds bound")
        received = queue.Queue(maxsize=1)
        process = self._process

        def exchange_one():
            try:
                process.stdin.write(encoded)
                process.stdin.flush()
                received.put(process.stdout.readline(65537))
            except (OSError, ValueError):
                received.put(b"")

        reader = threading.Thread(target=exchange_one, daemon=True)
        reader.start()
        try:
            raw = received.get(timeout=self._timeout)
            if not raw or len(raw) > 65536:
                raise ValueError()
            reply = json.loads(raw)
            if type(reply) is not dict or type(reply.get("id")) is not int:
                raise ValueError()
            if reply["id"] != self._sequence or type(reply.get("ok")) is not bool:
                raise ValueError()
            return reply
        except (OSError, ValueError, RecursionError, queue.Empty):
            self._close_locked()
            raise self._unavailable("workspace execution is indeterminate") from None
        finally:
            reader.join(timeout=1)

    def execute(self, expected, code):
        if type(code) is not str:
            raise ContractError("workspace code must be text")
        try:
            if len(code.encode("utf-8")) > 16384:
                raise ValueError()
        except (ValueError, UnicodeError):
            raise ContractError("workspace code exceeds encoding or size bound") from None
        with self._lock:
            self._require(expected)
            self._identity = replace(self._identity, revision=self._identity.revision + 1)
            reply = self._exchange({"operation": "execute", "code": code})
            output = reply.get("output")
            if (
                type(output) is not str
                or len(output) > 4096
                or any(0xD800 <= ord(c) <= 0xDFFF for c in output)
                or type(reply.get("output_truncated")) is not bool
            ):
                self._close_locked()
                raise self._unavailable("workspace output is invalid")
            return {
                "identity": self._identity.to_dict(),
                "ok": reply["ok"],
                "output": output,
                "output_truncated": reply["output_truncated"],
                "error": None if reply["ok"] else "execution-error",
            }

    def inspect(self, expected, names):
        from ahc.domain.workspace_inspection import inspection_names, inspection_result

        inspection_names(names)
        with self._lock:
            self._require(expected)
            reply = self._exchange({"operation": "inspect", "names": list(names)})
            try:
                if reply["ok"] is not True:
                    raise ContractError("inspection failed")
                return inspection_result(expected, names, reply.get("variables"))
            except ContractError:
                self._close_locked()
                raise self._unavailable("workspace inspection is invalid") from None

    def checkpoint(self, expected, names):
        from ahc.domain.workspace import WorkspaceCheckpoint

        if type(names) is not tuple or len(names) > 32:
            raise ContractError("checkpoint names invalid")
        if any(
            type(name) is not str
            or not name.isascii()
            or not name.isidentifier()
            or name.startswith("_")
            for name in names
        ):
            raise ContractError("checkpoint names invalid")
        if len(set(names)) != len(names):
            raise ContractError("checkpoint names invalid")
        with self._lock:
            self._require(expected)
            reply = self._exchange({"operation": "checkpoint", "names": list(names)})
            if reply["ok"] is not True:
                raise ContractError("namespace value is not checkpointable")
            values = reply.get("values")
            if type(values) is not dict or set(values) != set(names):
                raise ContractError("checkpoint values do not bind requested names")
            return WorkspaceCheckpoint(self._identity, json.dumps(values, allow_nan=False))

    def restore(self, expected, checkpoint):
        from ahc.domain.workspace import WorkspaceCheckpoint

        if type(checkpoint) is not WorkspaceCheckpoint:
            raise ContractError("workspace restore requires a portable checkpoint")
        with self._lock:
            self._require(expected)
            origin = checkpoint.origin
            if origin.session != expected.session or origin.workspace_id != expected.workspace_id:
                raise ContractError("checkpoint owner mismatch")
            if origin.generation > expected.generation or (
                origin.generation == expected.generation and origin.revision > expected.revision
            ):
                raise ContractError("checkpoint is newer than current workspace")
            # Fence old references before retiring the old interpreter, including failure paths.
            self._identity = replace(expected, generation=expected.generation + 1, revision=0)
            self._close_locked()
            self._open()
            reply = self._exchange(
                {"operation": "restore", "values": json.loads(checkpoint.values_json)}
            )
            if reply["ok"] is not True:
                self._close_locked()
                raise self._unavailable("workspace restore is unverified")
            return self._identity

    def _close_locked(self):
        self._closed = True
        if self._owned is not None:
            self._owned.remove()
        if self._process is not None:
            try:
                self._process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self._process.kill()
                self._process.wait(timeout=5)
            self._process.stdin.close()
            self._process.stdout.close()
        self._process = None
        self._owned = None

    def close(self):
        with self._lock:
            self._close_locked()

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        self.close()
