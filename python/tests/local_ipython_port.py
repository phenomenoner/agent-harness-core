"""Bounded real local IPython transport for product composition tests."""

import base64
import json
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from pathlib import Path

import pytest

from ahc.adapters.ipython_kernel import IPYTHON_KERNEL
from ahc.adapters.workspace_batch_callback import receive_reply
from ahc.domain.workspace import WorkspaceCheckpoint

IPython = pytest.importorskip("IPython")


class LocalIPythonPort:
    def __init__(self, identity, checkpoint, *, batch=None):
        self.identity, self.calls, self.closed = identity, 0, False
        self._sequence = 0
        self._batch, self._transport, self._cancelled = batch, self, lambda: False
        packages = str(Path(IPython.__file__).resolve().parent.parent)
        command = f"import sys;sys.path.insert(0,{packages!r});exec({IPYTHON_KERNEL!r})"
        self.process = subprocess.Popen(
            [sys._base_executable, "-I", "-u", "-c", command],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
            text=True, encoding="utf-8")
        self.reader = ThreadPoolExecutor(max_workers=1)
        if checkpoint is not None:
            try:
                self._request(operation="restore", values=json.loads(checkpoint.values_json))
            except BaseException:
                self.close()
                raise

    def _request(self, **fields):
        self._sequence += 1
        identifier = self._sequence
        request = {"id": identifier, **fields}
        deadline = time.monotonic() + 15
        try:
            self.send(request, deadline=deadline)
            result = receive_reply(self, request, deadline)
            assert result["id"] == identifier and result["ok"]
            return result
        except BaseException:
            self.close()
            raise

    def send(self, payload, *, deadline):
        assert time.monotonic() < deadline
        self.process.stdin.write(json.dumps(payload) + "\n")
        self.process.stdin.flush()

    def _receive(self, deadline):
        line = self.reader.submit(self.process.stdout.readline, 65537).result(
            timeout=max(0.01, deadline - time.monotonic()))
        return json.loads(line)

    def execute(self, expected, code):
        assert expected == self.identity
        result = self._request(
            operation="execute", code=code, batch_enabled=self._batch is not None)
        self.calls += 1
        self.identity = replace(expected, revision=expected.revision + 1)
        return {**result, "identity": self.identity.to_dict()}

    def export_artifact(self, expected, name):
        assert expected == self.identity
        return base64.b64decode(self._request(operation="export_artifact", name=name)["data"])

    def checkpoint(self, expected, names):
        assert expected == self.identity
        values = self._request(operation="checkpoint", names=list(names))["values"]
        return WorkspaceCheckpoint(expected, json.dumps(values))

    def close(self):
        if self.closed:
            return
        self.closed = True
        self.process.stdin.close()
        try:
            self.process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            self.process.kill()
            self.process.wait(timeout=5)
        finally:
            self.reader.shutdown(wait=True)
            self.process.stdout.close()
        assert self.process.poll() is not None
