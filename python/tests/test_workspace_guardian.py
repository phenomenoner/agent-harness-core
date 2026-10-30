"""Guardian custody and framing without Docker or provider execution."""

import io
import json
import os
import queue
import threading

import pytest

from ahc.adapters.workspace_guardian import decode_frame, dispatch, serve
from ahc.domain.sessions import ContractError, VirtualSessionIdentity
from ahc.domain.workspace import WorkspaceIdentity


class Port:
    def __init__(self):
        self.identity = WorkspaceIdentity(
            VirtualSessionIdentity.initial("session", "agent", "route"), "work"
        )
        self.calls = []
        self.closed = 0

    def execute(self, expected, value):
        self.calls.append(value)
        return {"output": "42"}

    def close(self):
        self.closed += 1


@pytest.mark.parametrize(
    "raw",
    [b'{"id":1,"id":2}\n', b"{}", b"[]\n", b"x" * 65537],
    ids=["duplicate", "unterminated", "array", "oversize"],
)
def test_bad_frame_rejected(raw):
    with pytest.raises(ContractError):
        decode_frame(raw)


def test_foreign_identity_and_cleanup_target_are_not_dispatched():
    port = Port()
    request = {
        "id": 1,
        "operation": "execute",
        "expected": port.identity.to_dict(),
        "value": "code",
    }
    request["container_id"] = "a" * 64
    with pytest.raises(ContractError):
        dispatch(port, request, 1)
    del request["container_id"]
    request["expected"]["generation"] = 2
    with pytest.raises(ContractError):
        dispatch(port, request, 1)
    assert port.calls == []


def test_eof_closes_only_owned_port():
    port = Port()
    assert serve(port, io.BytesIO(), io.BytesIO()) == 0
    assert port.closed == 1


def test_parent_eof_during_operation_retires_after_bounded_return():
    entered = threading.Event()
    release = threading.Event()
    output = queue.Queue()

    class RunningPort(Port):
        def execute(self, expected, value):
            entered.set()
            assert release.wait(2)
            return super().execute(expected, value)

    class Destination:
        def write(self, raw):
            output.put(json.loads(bytes(raw)))
            return len(raw)

        def flush(self):
            pass

    port = RunningPort()
    read_fd, write_fd = os.pipe()
    source = os.fdopen(read_fd, "rb", buffering=0)
    writer = os.fdopen(write_fd, "wb", buffering=0)
    results = []
    thread = threading.Thread(target=lambda: results.append(serve(port, source, Destination())))
    thread.start()
    try:
        assert output.get(timeout=2)["id"] == 0
        writer.write(
            json.dumps(
                {
                    "id": 1,
                    "operation": "execute",
                    "expected": port.identity.to_dict(),
                    "value": "code",
                }
            ).encode()
            + b"\n"
        )
        assert entered.wait(2)
        writer.close()
        release.set()
        thread.join(2)
        assert not thread.is_alive() and results == [0]
        assert port.closed == 1 and port.calls == ["code"]
    finally:
        release.set()
        writer.close()
        thread.join(2)
        source.close()


def test_cleanup_failure_is_not_success():
    class BrokenPort(Port):
        def close(self):
            raise RuntimeError("private")

    with pytest.raises(RuntimeError):
        serve(BrokenPort(), io.BytesIO(), io.BytesIO())


def test_orderly_child_exit_with_parent_writer_still_open(tmp_path):
    """A blocked daemon BufferedReader used to abort Python after close ack."""
    import subprocess
    import sys
    from pathlib import Path

    import ahc

    code = """
import sys
sys.path.insert(0, sys.argv[1])
from ahc.adapters import docker_workspace
from ahc.adapters.workspace_guardian import main
class Port:
    def __init__(self, runner, identity, **kwargs): self.identity = identity
    def close(self): pass
docker_workspace.DockerRunner = lambda *args, **kwargs: None
docker_workspace.StatefulDockerWorkspace = Port
raise SystemExit(main())
"""
    child = subprocess.Popen(
        [sys._base_executable, "-I", "-B", "-c", code, str(Path(ahc.__file__).parents[1])],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        bufsize=0,
    )
    replies = queue.Queue()
    reader = threading.Thread(
        target=lambda: [replies.put(line) for line in child.stdout], daemon=True
    )
    reader.start()
    try:
        boot = {
            "identity": Port().identity.to_dict(),
            "checkpoint": None,
            "docker_executable": "fixture",
            "client_config": "fixture",
            "timeout": 1,
        }
        child.stdin.write(json.dumps(boot).encode() + b"\n")
        assert json.loads(replies.get(timeout=5))["id"] == 0
        child.stdin.write(b'{"id":1,"operation":"close"}\n')
        assert json.loads(replies.get(timeout=5))["closed"] is True
        assert child.wait(timeout=5) == 0
    finally:
        child.stdin.close()
        if child.poll() is None:
            child.kill()
            child.wait(timeout=5)
        reader.join(2)
        child.stdout.close()
        child.stderr.close()
