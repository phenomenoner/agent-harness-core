"""Payload-free transport classification at the WSL control boundary."""

import io
import json
import time
from dataclasses import asdict
from types import SimpleNamespace

import pytest

from ahc.adapters.codex_transport import CodexStdioTransport, CodexTransportError
from ahc.adapters.wsl_unit_runner import (
    WslControlTransportUnavailable,
    WslCreationIndeterminate,
    WslUnitRunner,
)
from ahc.infrastructure.backend_custody import LinuxUnitIdentity

IDENTITY = LinuxUnitIdentity(
    "AHC-Test",
    "12345678-1234-1234-1234-123456789abc",
    "ahc-backend-" + "a" * 32 + ".service",
    "b" * 32,
)


class _Stdin:
    def __init__(self):
        self.writes = []
        self.closed = False

    def write(self, value):
        self.writes.append(bytes(value))
        return len(value)

    def flush(self):
        return None

    def close(self):
        self.closed = True


class _Process:
    def __init__(self, payload):
        self.stdin = _Stdin()
        self.stdout = io.BytesIO(payload)

    def wait(self, timeout=None):
        return 0


class _Owned:
    def __init__(self, payload):
        self.process = _Process(payload)
        self.cleanup_calls = []

    def cleanup(self, wait_seconds):
        self.cleanup_calls.append(wait_seconds)
        return SimpleNamespace(complete=True, remaining_count=0)


def _runner_with(payload):
    runner = WslUnitRunner("C:/Windows/System32/wsl.exe", "AHC-Test")
    owned = _Owned(payload)
    runner._open_transport = lambda _source: CodexStdioTransport(owned)
    return runner, owned


@pytest.mark.parametrize(
    ("payload", "error_type"),
    [
        pytest.param(b"\xff\xfeW\x00S\x00L\x00\n", "invalid-encoding", id="utf16-launcher-line"),
        pytest.param(b'{"reply":}\n', "malformed-json", id="malformed-json"),
    ],
)
def test_control_receive_distinguishes_sanitized_decode_categories(payload, error_type):
    runner, _ = _runner_with(payload)
    with pytest.raises(WslControlTransportUnavailable) as raised:
        runner._call({"action": "observe", "unit": IDENTITY.unit})
    assert raised.value.control_phase == "receive"
    assert raised.value.control_error_type == error_type
    assert "reply" not in repr(vars(raised.value))


def test_control_accepts_ordinary_valid_response():
    payload = {"ok": True,
               "result": {**asdict(IDENTITY), "state": "running", "cgroup_empty": False}}
    runner, owned = _runner_with((json.dumps(payload) + "\n").encode())
    observed = runner.observe(IDENTITY)
    assert observed.invocation_id == IDENTITY.invocation_id
    assert len(owned.process.stdin.writes) == 1
    assert owned.cleanup_calls == [5]


def test_eof_after_create_send_preserves_unknown_without_replay_and_cleans_up():
    runner, owned = _runner_with(b"")
    with pytest.raises(WslCreationIndeterminate) as raised:
        runner.create(["/usr/bin/true"], lifetime_seconds=10)
    assert raised.value.control_phase == "receive"
    assert raised.value.control_error_type == "eof"
    assert len(owned.process.stdin.writes) == 1
    assert owned.cleanup_calls == [5]


def test_deadline_has_a_fixed_transport_category_without_exception_text():
    class _SilentStdout:
        def readline(self, _limit):
            time.sleep(0.05)
            return b""

        def close(self):
            return None

    owned = _Owned(b"")
    owned.process.stdout = _SilentStdout()
    transport = CodexStdioTransport(owned)
    with pytest.raises(CodexTransportError) as raised:
        transport.receive(deadline=time.monotonic() - 1)
    assert raised.value.transport_error_type == "deadline"
    assert "PRIVATE" not in repr(vars(raised.value))
