"""Focused Codex stdio transport regressions using only owned fake streams."""

import io
import json
import queue
import threading
import time
from types import SimpleNamespace

import pytest

from ahc.adapters.codex_transport import CodexStdioTransport, CodexTransportError
from ahc.domain.sessions import ContractError

_RESPONSE_ERROR = "Codex response is unavailable or invalid"


class _FakeStdin:
    def __init__(self, *, block_write=False):
        self.block_write = block_write
        self.write_started = threading.Event()
        self.release = threading.Event()
        self.writes = []
        self.flushes = 0
        self.closed = False

    def write(self, data):
        self.write_started.set()
        if self.block_write and not self.release.wait(timeout=2):
            raise OSError("fake write remained blocked")
        if self.closed:
            raise OSError("fake stdin is closed")
        self.writes.append(bytes(data))
        return len(data)

    def flush(self):
        if self.closed:
            raise OSError("fake stdin is closed")
        self.flushes += 1

    def close(self):
        self.closed = True
        self.release.set()


class _FakeStdout:
    def __init__(self, payload):
        self._stream = io.BytesIO(payload)
        self.closed = False

    def readline(self, limit=-1):
        if self.closed:
            return b""
        return self._stream.readline(limit)

    def close(self):
        self.closed = True


class _FakeProcess:
    def __init__(self, payload, *, block_write=False):
        self.stdin = _FakeStdin(block_write=block_write)
        self.stdout = _FakeStdout(payload)
        self.wait_calls = []

    def wait(self, timeout=None):
        self.wait_calls.append(timeout)
        return 0


class _FakeOwned:
    def __init__(self, payload=b"", *, complete=True, remaining_count=0, block_write=False):
        self.process = _FakeProcess(payload, block_write=block_write)
        self._cleanup_result = SimpleNamespace(
            complete=complete,
            remaining_count=remaining_count,
        )
        self.cleanup_calls = []

    def cleanup(self, wait_seconds):
        self.cleanup_calls.append(wait_seconds)
        return self._cleanup_result


def _transport(payload=b"", **owned_options):
    owned = _FakeOwned(payload, **owned_options)
    return CodexStdioTransport(owned), owned


def _deadline(seconds=1):
    return time.monotonic() + seconds


def _deep_response():
    depth = 4096
    return b"[" * depth + b"0" + b"]" * depth + b"\n"


def test_success_and_close_cleanup_contracts():
    transport, owned = _transport(b'{"id":1,"result":{"ok":true}}\n')
    assert transport.MAX_FRAME == 16 * 1048576
    assert transport.MAX_OUTBOUND_FRAME == 1048576
    assert transport.receive(deadline=_deadline()) == {"id": 1, "result": {"ok": True}}

    transport.close()
    transport.close()
    assert owned.cleanup_calls == [5]
    assert owned.process.wait_calls == [5]
    assert owned.process.stdin.closed
    assert owned.process.stdout.closed

    incomplete, incomplete_owned = _transport(b"", complete=False, remaining_count=1)
    with pytest.raises(
        CodexTransportError,
        match="Codex process custody cleanup is unverified",
    ):
        incomplete.close()
    assert incomplete_owned.cleanup_calls == [5]
    assert incomplete_owned.process.wait_calls == []
    assert not incomplete_owned.process.stdin.closed
    assert not incomplete_owned.process.stdout.closed
    assert not incomplete._closed


def _large_command_event(size):
    prefix = b'{"method":"item/commandExecution/completed","params":{"output":"'
    suffix = b'"}}\n'
    assert size > len(prefix) + len(suffix)
    return prefix + b"x" * (size - len(prefix) - len(suffix)) + suffix


def test_large_command_event_and_subsequent_final_frame_are_preserved():
    event = _large_command_event(2_079_192)
    final = b'{"id":1,"result":{"status":"completed"}}\n'
    transport, _ = _transport(event + final)
    transport._reader.join(timeout=2)

    with transport:
        observed_event = transport.receive(deadline=_deadline())
        observed_final = transport.receive(deadline=_deadline())

    assert len(event) == 2_079_192
    assert observed_event == json.loads(event)
    assert observed_final == json.loads(final)


def test_large_event_discriminates_previous_64k_frame_bound(monkeypatch):
    event = _large_command_event(2_079_192)
    final = b'{"id":1,"result":{"status":"completed"}}\n'

    with monkeypatch.context() as legacy:
        legacy.setattr(CodexStdioTransport, "MAX_FRAME", 65536)
        old_transport, _ = _transport(event + final)
        old_transport._reader.join(timeout=2)
        with pytest.raises(CodexTransportError):
            old_transport.receive(deadline=_deadline())
        assert old_transport.receive_failure == "frame-size-limit"

    transport, _ = _transport(event + final)
    transport._reader.join(timeout=2)
    with transport:
        assert transport.receive(deadline=_deadline()) == json.loads(event)
        assert transport.receive(deadline=_deadline()) == json.loads(final)


def test_outbound_frame_limit_remains_one_mib():
    transport, owned = _transport()
    try:
        assert transport.MAX_OUTBOUND_FRAME == 1048576
        with pytest.raises(ContractError):
            transport.send({"text": "x" * transport.MAX_OUTBOUND_FRAME}, deadline=_deadline())
        assert owned.process.stdin.writes == []
    finally:
        transport.close()


@pytest.mark.parametrize(
    ("_case", "payload"),
    [
        pytest.param(
            "duplicate",
            b'{"secret":"PRIVATE-SENTINEL","id":1,"id":2}\n',
            id="duplicate-field",
        ),
        pytest.param("nonobject", b"[]\n", id="nonobject-response"),
        pytest.param("deep", _deep_response(), id="deep-response"),
        pytest.param(
            "oversized",
            b"{" + b"x" * (CodexStdioTransport.MAX_FRAME + 1) + b"}\n",
            id="oversized-frame",
        ),
    ],
)
def test_invalid_responses_fail_closed_with_fixed_public_error(_case, payload):
    transport, owned = _transport(payload)
    transport._reader.join(timeout=1)

    with pytest.raises(CodexTransportError) as raised:
        transport.receive(deadline=_deadline())
    assert str(raised.value) == _RESPONSE_ERROR
    assert "PRIVATE-SENTINEL" not in str(raised.value)
    assert owned.cleanup_calls == [5]
    assert owned.process.stdin.closed
    assert owned.process.stdout.closed


def test_send_deadline_covers_blocked_write_and_expiry_writes_zero_bytes():
    blocked, blocked_owned = _transport(b"", block_write=True)
    result = {}
    done = threading.Event()

    def send_blocked():
        try:
            blocked.send({"request": "PRIVATE-SENTINEL"}, deadline=_deadline(0.2))
        except Exception as exc:  # The assertion below checks the public exception.
            result["error"] = exc
        finally:
            done.set()

    sender = threading.Thread(target=send_blocked)
    sender.start()
    try:
        assert blocked_owned.process.stdin.write_started.wait(timeout=1)
        assert done.wait(timeout=2)
    finally:
        blocked_owned.process.stdin.release.set()
        assert done.wait(timeout=2)
        sender.join(timeout=2)

    assert isinstance(result.get("error"), CodexTransportError)
    assert str(result["error"]) == "Codex write is indeterminate"
    assert "PRIVATE-SENTINEL" not in str(result["error"])
    assert blocked_owned.process.stdin.writes == []
    assert blocked_owned.cleanup_calls == [5]

    expired, expired_owned = _transport(b"")
    with pytest.raises(CodexTransportError) as raised:
        expired.send({"request": "PRIVATE-SENTINEL"}, deadline=time.monotonic() - 1)
    assert str(raised.value) == "Codex transport deadline expired or invalid"
    assert "PRIVATE-SENTINEL" not in str(raised.value)
    assert expired_owned.process.stdin.writes == []
    assert expired_owned.process.stdin.write_started.is_set() is False
    assert expired_owned.cleanup_calls == [5]


class _PacedStdout:
    def __init__(self):
        self.frames = queue.Queue()

    def readline(self, limit):
        return self.frames.get()

    def close(self):
        self.frames.put(b"")


def test_more_than_the_old_frame_quota_is_accepted():
    owned = _FakeOwned()
    stream = _PacedStdout()
    owned.process.stdout = stream
    with CodexStdioTransport(owned) as transport:
        for _ in range(5000):
            stream.frames.put(b"{}\n")
            assert transport.receive(deadline=_deadline()) == {}
        assert transport.receive_failure is None


def test_more_than_the_old_cumulative_byte_quota_is_accepted_with_backpressure():
    owned = _FakeOwned()
    stream = _PacedStdout()
    owned.process.stdout = stream
    prefix, suffix = b'{"text":"', b'"}\n'
    frame_size = 2 * 1048576 + 1
    raw = prefix + b"x" * (frame_size - len(prefix) - len(suffix)) + suffix
    assert len(raw) == frame_size
    with CodexStdioTransport(owned) as transport:
        for _ in range(17):
            stream.frames.put(raw)
            assert len(transport.receive(deadline=_deadline())["text"]) == (
                len(raw) - len(prefix) - len(suffix)
            )
        assert transport.receive_failure is None


def test_exact_queue_capacity_with_eof_preserves_valid_frames():
    transport, _ = _transport(b"{}\n" * 8)
    transport._reader.join(timeout=1)
    with transport:
        for _ in range(8):
            assert transport.receive(deadline=_deadline()) == {}


def test_cleanup_exception_does_not_expose_private_diagnostics():
    transport, owned = _transport()

    def fail(_seconds):
        raise RuntimeError("PRIVATE-SENTINEL")

    owned.cleanup = fail
    with pytest.raises(CodexTransportError) as raised:
        transport.close()
    assert str(raised.value) == "Codex process custody cleanup is unverified"


def test_valid_burst_waits_for_consumer_without_losing_frames():
    owned = _FakeOwned()
    full_queue_read = threading.Event()
    capacity = min(8, CodexStdioTransport.MAX_QUEUED_BYTES // CodexStdioTransport.MAX_FRAME)

    class Burst(_FakeStdout):
        def readline(self, limit=-1):
            raw = super().readline(limit)
            if raw == f'{{"id":{capacity}}}\n'.encode():
                full_queue_read.set()
            return raw

    owned.process.stdout = Burst(b"".join(
        f'{{"id":{index}}}\n'.encode() for index in range(20)
    ))
    with CodexStdioTransport(owned) as transport:
        assert full_queue_read.wait(timeout=1)
        assert transport._frames.qsize() == transport._frames.maxsize
        for index in range(20):
            assert transport.receive(deadline=_deadline()) == {"id": index}
        assert not transport._fault.is_set()


def test_close_releases_reader_waiting_on_full_queue():
    transport, owned = _transport(b"{}\n" * 20)
    transport.close()
    assert not transport._reader.is_alive()
    assert owned.cleanup_calls == [5]


@pytest.mark.parametrize(('payload', 'reason'), [
    (b'', 'eof'),
    (b'PRIVATE-SENTINEL\n', 'invalid-json'),
    (b'[]\n', 'nonobject-frame'),
    (b'{"id":1,"id":2}\n', 'invalid-json'),
    (b'{}', 'unterminated-frame'),
    (b'x' * (CodexStdioTransport.MAX_FRAME + 1), 'frame-size-limit'),
], ids=['eof', 'invalid-json', 'nonobject', 'duplicate', 'unterminated', 'oversized'])
def test_receive_diagnostics_are_fixed_categories(payload, reason):
    transport, _ = _transport(payload)
    transport._reader.join(timeout=1)
    with pytest.raises(CodexTransportError, match=_RESPONSE_ERROR):
        transport.receive(deadline=_deadline())
    assert transport.receive_failure == reason
    assert 'PRIVATE-SENTINEL' not in repr(vars(transport))


@pytest.mark.parametrize('expired', [False, True])
def test_receive_distinguishes_timeout_from_invalid_deadline(expired):
    owned = _FakeOwned()
    owned.process.stdout = _PacedStdout()
    transport = CodexStdioTransport(owned)
    with pytest.raises(CodexTransportError, match=_RESPONSE_ERROR):
        transport.receive(deadline=_deadline(-1 if expired else 0.02))
    assert transport.receive_failure == ('deadline' if expired else 'wait-timeout')
    assert not transport._reader.is_alive()


def test_reader_io_failure_does_not_retain_error_text():
    class Broken(_FakeStdout):
        def readline(self, limit=-1):
            raise OSError('PRIVATE-SENTINEL')

    owned = _FakeOwned()
    owned.process.stdout = Broken(b'')
    transport = CodexStdioTransport(owned)
    transport._reader.join(timeout=1)
    with pytest.raises(CodexTransportError, match=_RESPONSE_ERROR):
        transport.receive(deadline=_deadline())
    assert transport.receive_failure == 'reader-io'
    assert 'PRIVATE-SENTINEL' not in repr(vars(transport))


def test_payload_free_telemetry_distinguishes_read_receive_and_write():
    payload = b'{"id":1,"result":{"secret":"PRIVATE-SENTINEL"}}\n'
    transport, owned = _transport(payload)
    transport._reader.join(timeout=1)
    try:
        before = transport.telemetry_snapshot()
        assert before["stdout_reads"] == 1 and before["stdout_bytes"] == len(payload)
        assert before["receive_returns"] == 0 and before["stdout_eof"] is True
        assert before["queue_entries"] >= 1
        assert before["last_stdout_monotonic"] is not None
        assert transport.receive(deadline=_deadline())["result"]["secret"] == "PRIVATE-SENTINEL"
        transport.send({"id": 2, "method": "PRIVATE-SENTINEL"}, deadline=_deadline())
        after = transport.telemetry_snapshot()
        assert after["receive_returns"] == after["write_attempts"] == after["write_returns"] == 1
        assert after["stdin_bytes"] == sum(map(len, owned.process.stdin.writes))
        assert after["last_receive_monotonic"] >= before["last_stdout_monotonic"]
        assert after["last_write_monotonic"] is not None
        assert "PRIVATE" not in json.dumps(after)
        after["stdout_bytes"] = -1
        assert transport.telemetry_snapshot()["stdout_bytes"] == len(payload)
    finally:
        transport.close()
    final = transport.telemetry_snapshot()
    assert final["closed"] is True and final["reader_alive"] is False


def test_payload_free_telemetry_identifies_silent_receive_deadline():
    owned = _FakeOwned()
    owned.process.stdout = _PacedStdout()
    transport = CodexStdioTransport(owned)
    initial = transport.telemetry_snapshot()
    assert initial["reader_alive"] is True and initial["queue_entries"] == 0
    with pytest.raises(CodexTransportError):
        transport.receive(deadline=_deadline(0.02))
    final = transport.telemetry_snapshot()
    assert final["stdout_bytes"] == final["stdout_reads"] == final["receive_returns"] == 0
    assert final["receive_failure"] == "wait-timeout"
    assert final["reader_failure"] is None and final["closed"] is True
