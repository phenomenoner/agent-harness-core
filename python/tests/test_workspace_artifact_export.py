"""Kernel artifact export uses exact primitive values and bounded canonical base64."""

import base64
from hashlib import sha256
import json
import subprocess
import sys
import threading

import pytest

from ahc.adapters.docker_workspace import WorkspaceUnavailable
from ahc.adapters.workspace_artifact_export import export_artifact
from ahc.adapters.workspace_kernel import KERNEL
from ahc.adapters.wsl_python_workspace import WslPythonWorkspace
from ahc.domain.sessions import ContractError, VirtualSessionIdentity
from ahc.domain.workspace import WorkspaceIdentity

IDENTITY = WorkspaceIdentity(VirtualSessionIdentity.initial("session", "agent", "route"), "work")


def test_kernel_exports_retained_exact_text_and_bytes_without_object_hooks():
    code = '''
called = False
class Evil:
    def __str__(self):
        global called; called = True; return "bad"
    def __repr__(self):
        global called; called = True; return "bad"
    def __bytes__(self):
        global called; called = True; return b"bad"
text = "hé"
raw = b"\\x00bytes"
evil = Evil()
big = "x" * 16385
'''
    frames = [
        {"id": 1, "operation": "execute", "code": code},
        {"id": 2, "operation": "export_artifact", "name": "text"},
        {"id": 3, "operation": "export_artifact", "name": "raw"},
        {"id": 4, "operation": "export_artifact", "name": "evil"},
        {"id": 5, "operation": "export_artifact", "name": "big"},
        {"id": 6, "operation": "export_artifact", "name": "missing"},
        {"id": 7, "operation": "execute", "code": "text += '!'; assert called is False"},
        {"id": 8, "operation": "export_artifact", "name": "text"},
    ]
    result = subprocess.run(
        [sys._base_executable, "-I", "-B", "-c", KERNEL],
        input=b"".join(json.dumps(frame).encode() + b"\n" for frame in frames),
        capture_output=True,
        timeout=5,
        check=True,
    )
    replies = [json.loads(line) for line in result.stdout.splitlines()]
    assert base64.b64decode(replies[1]["data"]) == "hé".encode()
    assert base64.b64decode(replies[2]["data"]) == b"\x00bytes"
    assert replies[3]["error"] == replies[5]["error"] == "unsupported-value"
    assert base64.b64decode(replies[4]["data"]) == b"x" * 16385
    assert base64.b64decode(replies[7]["data"]) == "hé!".encode()


class FixtureWorkspace:
    def __init__(self, reply):
        self._lock = threading.RLock()
        if reply.get("ok") is True:
            content = base64.b64decode(reply.get("data", ""))
            reply = {"export_id": reply["id"], "offset": 0, "byte_count": len(content),
                     "content_digest": sha256(content).hexdigest(), **reply}
        self.reply, self.terminated = reply, False

    def _require(self, expected):
        assert expected == IDENTITY

    def _reply(self, request):
        assert request == {"operation": "export_artifact", "name": "value"}
        return self.reply

    def _terminate_indeterminate(self):
        self.terminated = True


def test_export_helper_validates_controlled_reply_and_keeps_unsupported_healthy():
    valid = FixtureWorkspace({"id": 1, "ok": True, "data": base64.b64encode(b"bytes").decode()})
    assert export_artifact(valid, IDENTITY, "value") == b"bytes"
    unsupported = FixtureWorkspace({"id": 1, "ok": False, "error": "unsupported-value"})
    with pytest.raises(ContractError):
        export_artifact(unsupported, IDENTITY, "value")
    assert unsupported.terminated is False
    malformed = FixtureWorkspace({"id": 1, "ok": True, "data": "%%%"})
    with pytest.raises(WorkspaceUnavailable, match="invalid"):
        export_artifact(malformed, IDENTITY, "value")
    assert malformed.terminated is True
    oversized = FixtureWorkspace(
        {"id": 1, "ok": True, "data": base64.b64encode(b"x" * 32_769).decode()}
    )
    with pytest.raises(WorkspaceUnavailable, match="invalid"):
        export_artifact(oversized, IDENTITY, "value")
    assert oversized.terminated is True


def test_wsl_workspace_method_delegates_to_export_helper_and_preserves_identity():
    workspace = WslPythonWorkspace.__new__(WslPythonWorkspace)
    workspace._lock, workspace._identity, workspace._closed = threading.RLock(), IDENTITY, False
    workspace._reply = lambda request: {"id": 1, "ok": True, "data": "", "export_id": 1,
        "offset": 0, "byte_count": 0, "content_digest": sha256(b"").hexdigest()}
    workspace._terminate_indeterminate = lambda: pytest.fail("valid reply must not terminate")
    assert workspace.export_artifact(IDENTITY, "value") == b""
    assert workspace.identity == IDENTITY
