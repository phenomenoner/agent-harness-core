"""Real interpreter frames discriminate former artifact caps and snapshot corruption."""

import base64
from copy import deepcopy
from hashlib import sha256
import json
import subprocess
import sys
import threading

import pytest

from ahc.adapters.docker_workspace import WorkspaceUnavailable
from ahc.adapters.ipython_kernel import IPYTHON_KERNEL
from ahc.adapters.workspace_artifact_export import CHUNK_SIZE, export_artifact
from ahc.adapters.workspace_kernel import KERNEL
from test_workspace_artifact_export import IDENTITY


def run_kernel(frames, kernel=KERNEL):
    result = subprocess.run([sys.executable, "-I", "-B", "-c", kernel],
        input=b"".join(json.dumps(frame).encode() + b"\n" for frame in frames),
        capture_output=True, timeout=15, check=True)
    lines = result.stdout.splitlines(keepends=True)
    assert all(len(line) <= 65536 for line in lines)
    return [json.loads(line) for line in lines]


def export_frames(size, code=None):
    frames = [{"id": 1, "operation": "execute", "code": code or f"raw = b'x' * {size}"},
              {"id": 2, "operation": "export_artifact", "name": "raw"}]
    for offset in range(CHUNK_SIZE, size, CHUNK_SIZE):
        frames.append({"id": len(frames) + 1, "operation": "export_artifact",
                       "export_id": 2, "offset": offset})
    return frames


class RepliesWorkspace:
    def __init__(self, frames, replies):
        self._lock = threading.RLock()
        self.frames, self.replies = list(frames), list(replies)
        self.terminated = False

    def _require(self, expected):
        assert expected == IDENTITY

    def _reply(self, request):
        frame = self.frames.pop(0)
        assert request == {key: value for key, value in frame.items() if key != "id"}
        return self.replies.pop(0)

    def _terminate_indeterminate(self):
        self.terminated = True


@pytest.mark.parametrize("size", [0, 16383, 16385, 28773, 32767, 32768, 32769, 65536, 1048577])
def test_actual_python_snapshot_roundtrips_across_former_caps_and_frame_boundaries(size):
    frames = export_frames(size)
    replies = run_kernel(frames)
    assert replies[0]["ok"]
    workspace = RepliesWorkspace(frames[1:], replies[1:])
    content = export_artifact(workspace, IDENTITY, "raw")
    assert content == b"x" * size and sha256(content).hexdigest() == replies[1]["content_digest"]
    assert not workspace.terminated and not workspace.replies


@pytest.mark.parametrize("size", [28773, 1048577])
def test_actual_ipython_uses_the_same_chunk_contract(size):
    pytest.importorskip("IPython")
    frames = export_frames(size)
    replies = run_kernel(frames, IPYTHON_KERNEL)
    workspace = RepliesWorkspace(frames[1:], replies[1:])
    assert export_artifact(workspace, IDENTITY, "raw") == b"x" * size
    assert not workspace.terminated


def test_snapshot_is_immutable_when_namespace_rebinds_and_user_shadows_helper_name():
    frames = [
        {"id": 1, "operation": "execute", "code": "raw=b'a'*32769; export_artifact_chunk=None"},
        {"id": 2, "operation": "export_artifact", "name": "raw"},
        {"id": 3, "operation": "execute", "code": "raw=b'new value'"},
        {"id": 4, "operation": "export_artifact", "export_id": 2, "offset": CHUNK_SIZE},
        {"id": 5, "operation": "export_artifact", "name": "raw"},
    ]
    replies = run_kernel(frames)
    assert base64.b64decode(replies[1]["data"]) + base64.b64decode(replies[3]["data"]) == b"a"*32769
    assert replies[1]["content_digest"] == replies[3]["content_digest"] == sha256(b"a"*32769).hexdigest()
    assert base64.b64decode(replies[4]["data"]) == b"new value"


def test_unicode_uses_encoded_byte_count_and_rejects_exact_nonprimitive_types_without_hooks():
    code = """
called = False
class BytesSubclass(bytes):
    def __bytes__(self):
        global called; called = True; return b'bad'
raw = '璃' * 11000
subclass = BytesSubclass(b'bad')
mutable = bytearray(b'bad')
view = memoryview(b'bad')
"""
    frames = export_frames(33000, code)
    frames.extend({"id": len(frames)+i+1, "operation": "export_artifact", "name": name}
                  for i, name in enumerate(("subclass", "mutable", "view")))
    frames.append({"id": len(frames)+1, "operation": "execute", "code": "assert called is False"})
    replies = run_kernel(frames)
    workspace = RepliesWorkspace(frames[1:3], replies[1:3])
    assert export_artifact(workspace, IDENTITY, "raw") == ("璃"*11000).encode()
    assert all(row["error"] == "unsupported-value" for row in replies[3:-1])
    assert replies[-1]["ok"]


@pytest.mark.parametrize("fault", ["offset", "export_id", "total", "digest", "short", "extra", "base64"])
def test_corrupt_chunk_never_becomes_a_published_value(fault):
    frames = export_frames(32769)
    replies = run_kernel(frames)[1:]
    damaged = deepcopy(replies)
    if fault == "offset":
        damaged[1]["offset"] = 0
    elif fault == "export_id":
        damaged[1]["export_id"] = 999
    elif fault == "total":
        damaged[1]["byte_count"] = 40000
    elif fault == "digest":
        for row in damaged:
            row["content_digest"] = "0"*64
    elif fault == "short":
        damaged[0]["data"] = base64.b64encode(b"x"*32767).decode()
    elif fault == "extra":
        damaged[0]["data"] = base64.b64encode(b"x"*32769).decode()
    else:
        damaged[0]["data"] = "%notbase64%"
    workspace = RepliesWorkspace(frames[1:], damaged)
    with pytest.raises(WorkspaceUnavailable):
        export_artifact(workspace, IDENTITY, "raw")
    assert workspace.terminated
