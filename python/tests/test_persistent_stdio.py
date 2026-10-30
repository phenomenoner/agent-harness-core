"""Persistent transports retain per-frame bounds without the finite turn cap."""

import io
import json
import queue
import time

import pytest
from test_codex_transport import _FakeOwned

from ahc.adapters.codex_transport import CodexStdioTransport, CodexTransportError
from ahc.adapters.linux_unit_stream import _relay_input


class Feed:
    def __init__(self):
        self.queue = queue.Queue()

    def readline(self, _limit):
        return self.queue.get(timeout=3)

    def close(self):
        self.queue.put(b"")


@pytest.mark.parametrize("streaming", [False, True])
def test_transport_modes_survive_retired_cumulative_limits(streaming):
    owned = _FakeOwned()
    feed = owned.process.stdout = Feed()
    transport = CodexStdioTransport(owned, streaming=streaming)
    try:
        # Cross the retired 4,098-frame and 32 MiB aggregate caps in both modes.
        # The per-frame bound and bounded reader queue still apply.
        for number in range(4100):
            payload = {"id": number, "result": "x" * 8192}
            feed.queue.put(json.dumps(payload).encode() + b"\n")
            assert transport.receive(deadline=time.monotonic() + 2) == payload
        final = {"id": 4100, "result": "complete"}
        feed.queue.put(json.dumps(final).encode() + b"\n")
        assert transport.receive(deadline=time.monotonic() + 2) == final
    finally:
        transport.close()


def test_explicit_long_deadline_does_not_change_default_deadline_contract():
    first = CodexStdioTransport(_FakeOwned())
    second = CodexStdioTransport(_FakeOwned(), max_wait=3600)
    try:
        with pytest.raises(CodexTransportError):
            first._remaining(time.monotonic() + 400)
        assert 399 < second._remaining(time.monotonic() + 400) <= 400
    finally:
        first.close()
        second.close()


@pytest.mark.parametrize("persistent", [False, True])
def test_linux_relay_modes_preserve_frames_after_retired_count_limit(persistent):
    class Capture(io.BytesIO):
        def close(self):
            self.did_close = True

    target = Capture()
    supplied = 4097  # Cross the retired 4,096-frame relay cap in both modes.
    expected = supplied
    _relay_input(io.BytesIO(b"{}\n" * supplied), target, persistent)
    assert target.getvalue() == b"{}\n" * expected and target.did_close
