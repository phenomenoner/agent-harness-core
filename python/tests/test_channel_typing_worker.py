"""Typing lifetime and failure bounds without real network effects."""

import threading
from types import SimpleNamespace

import pytest

from ahc.adapters.channel_typing_worker import ChannelTypingWorker
from ahc.domain.sessions import ContractError


def test_two_failures_pause_and_success_resets_counter():
    fail = [True]

    def send():
        if fail[0]:
            raise TimeoutError("private-fixture")

    worker = ChannelTypingWorker(SimpleNamespace(send=send), lambda: True)
    assert worker._attempt() is True
    fail[0] = False
    assert worker._attempt() is True and worker.status()["consecutive_failures"] == 0
    fail[0] = True
    assert worker._attempt() is True
    assert worker._attempt() is False
    assert worker.status()["state"] == "paused"
    assert "private" not in str(worker.status())
    worker.close()


def test_close_waits_for_inflight_and_never_claims_early_shutdown():
    entered, release = threading.Event(), threading.Event()
    calls = []

    def send():
        calls.append(threading.get_ident())
        entered.set()
        assert release.wait(3)

    worker = ChannelTypingWorker(SimpleNamespace(send=send), lambda: True, join_timeout=0.02)
    try:
        worker.start()
        assert entered.wait(2)
        with pytest.raises(ContractError, match="shutdown unverified"):
            worker.close()
        assert worker._thread.is_alive() and worker.status()["state"] != "closed"
    finally:
        release.set()
        worker.join_timeout = 2
        worker.close()
    assert worker.status()["state"] == "closed" and not worker._thread.is_alive()
    assert len(calls) == 1 and calls[0] != threading.get_ident()
    worker.close()
    with pytest.raises(ContractError):
        worker.start()


def test_revoked_or_stopped_owner_never_sends():
    calls = []
    worker = ChannelTypingWorker(SimpleNamespace(send=lambda: calls.append(True)), lambda: False)
    assert worker._attempt() is False and calls == []
    worker.close()
    worker.can_send = lambda: True
    assert worker._attempt() is False and calls == []
