"""A hostile interpreter cannot make a host pipe write bypass the deadline."""

import io
import threading

import pytest

from ahc.adapters.docker_workspace import StatefulDockerWorkspace, WorkspaceUnavailable


def make_workspace(process):
    workspace = object.__new__(StatefulDockerWorkspace)
    workspace._sequence = 0
    workspace._timeout = 0.02
    workspace._process = process
    return workspace


def test_blocked_write_is_included_in_execution_deadline():
    released = threading.Event()
    writing = threading.Event()

    class Input:
        def write(self, _data):
            writing.set()
            assert released.wait(2)

        def flush(self):
            pass

    class Process:
        stdin = Input()
        stdout = io.BytesIO(b"")

    workspace = make_workspace(Process())
    workspace._close_locked = released.set
    with pytest.raises(WorkspaceUnavailable):
        workspace._exchange({"operation": "restore", "values": {}})
    assert writing.is_set() and released.is_set()


@pytest.mark.parametrize(
    "raw",
    [
        b'{"id":true,"ok":true}\n',
        b'{"id":2,"ok":true}\n',
        b"[" * 2000 + b"0" + b"]" * 2000,
        b"x" * 65537,
    ],
    ids=["boolean-id", "wrong-id", "deep-json", "oversized-frame"],
)
def test_invalid_frame_closes_custody(raw):
    class Process:
        stdin = io.BytesIO()
        stdout = io.BytesIO(raw)

    closed = []
    workspace = make_workspace(Process())
    workspace._close_locked = lambda: closed.append(True)
    with pytest.raises(WorkspaceUnavailable):
        workspace._exchange({"operation": "restore", "values": {}})
    assert closed == [True]
