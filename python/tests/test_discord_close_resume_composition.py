"""Peer close through the real worker and durable resume CAS, with controlled sockets."""

import pytest
import threading
from test_discord_gateway_state import HELLO, READY
from test_runtime_core import setup

from ahc.adapters.discord_gateway_driver import DiscordGatewayDriver
from ahc.adapters.discord_gateway_state import DiscordGatewayState
from ahc.adapters.discord_gateway_worker import DiscordGatewayWorker
from ahc.adapters.discord_websocket import DiscordGatewayClosed
from ahc.domain.channel_ingress import ChannelRoute
from ahc.infrastructure.discord_inbox import DiscordInbox
from ahc.infrastructure.discord_resume import DiscordResume, DiscordResumeSession
from ahc.infrastructure.runtime_inbox import RuntimeInbox
from ahc.infrastructure.threaded_runtime_repository import ThreadedRuntimeRepository


@pytest.mark.parametrize("replacement", [False, True])
def test_worker_close_invalidates_only_its_own_checkpoint(tmp_path, replacement):
    base, snapshot, _ = setup(tmp_path)
    store = ThreadedRuntimeRepository(base.root)
    route = ChannelRoute("discord", "123", "456", "789", snapshot.session)
    repository = DiscordResume(DiscordInbox(store, [route]))
    repository.save_ready("old-session", "wss://gateway.discord.gg", 1, expected=None)
    session = DiscordResumeSession(repository)
    sent, closed = [], []
    inbox = RuntimeInbox(store)
    inbox.submit(snapshot.session, "local-pending", snapshot.goal.digest, "keep local work")

    class Socket:
        def __init__(self, url):
            self.frames = [HELLO, {"op": 0, "s": 2, "t": "RESUMED", "d": {}}]

        def send(self, frame):
            sent.append(frame["op"])

        def receive(self, timeout):
            if self.frames:
                return self.frames.pop(0)
            if replacement:
                # A distinct owner advances the durable snapshot after this
                # driver's last read. The close must not clear that replacement.
                other = DiscordResume(DiscordInbox(ThreadedRuntimeRepository(base.root), [route]))
                other.save_ready("replacement", "wss://gateway.discord.gg", 99,
                                 expected=other.read())
            raise DiscordGatewayClosed(4009)

        def close(self):
            closed.append(True)

    driver = DiscordGatewayDriver("123", "fixture", 0, session.admit, connect=Socket,
        resume=session.current, checkpoint=session.checkpoint, connection_limit=1)
    worker = DiscordGatewayWorker(driver)
    published = threading.Event()
    publish = worker._publish

    def observe(value):
        publish(value)
        if value.get("state") in {"paused", "reconnecting"}:
            published.set()

    worker._publish = observe
    worker.start()
    try:
        assert published.wait(3)
        assert worker.status()["state"] == ("paused" if replacement else "reconnecting")
        assert worker.status()["close_code"] == 4009
    finally:
        worker.close()
    assert sent == [6] and closed == [True]
    fresh = ThreadedRuntimeRepository(base.root)
    recovered = DiscordResume(DiscordInbox(fresh, [route])).read()
    if replacement:
        assert recovered["session_id"] == "replacement" and recovered["sequence"] == 99
        assert driver.protocol.session_id == "old-session"
    else:
        assert recovered is None and session.current is None
        gateway = DiscordGatewayState("123", "fixture", 0, resume=recovered)
        assert gateway.receive(HELLO, lambda _: None)["op"] == 2
    assert RuntimeInbox(fresh).next_pending(snapshot.session)["input_text"] == "keep local work"


@pytest.mark.parametrize("event", ["READY", "RESUMED", "MESSAGE_CREATE", "INVALID_SESSION"])
def test_worker_durable_callback_conflict_pauses_without_borrowing_replacement(tmp_path, event):
    base, snapshot, _ = setup(tmp_path)
    store = ThreadedRuntimeRepository(base.root)
    route = ChannelRoute("discord", "123", "456", "789", snapshot.session)
    repository = DiscordResume(DiscordInbox(store, [route]))
    if event != "READY":
        repository.save_ready("old-session", "wss://gateway.discord.gg", 1, expected=None)
    session = DiscordResumeSession(repository)
    now, peers = [0.0], []
    inbox = RuntimeInbox(store)
    inbox.submit(snapshot.session, "local-pending", snapshot.goal.digest, "keep local work")
    resumed = {"op": 0, "s": 2, "t": "RESUMED", "d": {}}
    failed_frame = {
        "READY": READY,
        "RESUMED": resumed,
        "MESSAGE_CREATE": {"op": 0, "s": 3, "t": "MESSAGE_CREATE", "d": {
            "id": "10", "channel_id": "456", "author": {"id": "789"},
            "type": 0, "content": "must not admit"}},
        "INVALID_SESSION": {"op": 9, "d": False},
    }[event]

    class Socket:
        def __init__(self, url):
            self.frames = [HELLO]
            if event in {"MESSAGE_CREATE", "INVALID_SESSION"}:
                self.frames.append(resumed)
            self.frames.append(failed_frame)
            self.sent, self.closed = [], False
            peers.append(self)

        def send(self, frame):
            self.sent.append(frame["op"])

        def receive(self, timeout):
            if not self.frames:
                return None
            frame = self.frames.pop(0)
            if frame is failed_frame:
                other = DiscordResume(DiscordInbox(ThreadedRuntimeRepository(base.root), [route]))
                other.save_ready("replacement", "wss://gateway.discord.gg", 99, expected=other.read())
            return frame

        def close(self):
            self.closed = True

    driver = DiscordGatewayDriver("123", "fixture", 0, session.admit, connect=Socket,
        resume=session.current, checkpoint=session.checkpoint, connection_limit=1,
        clock=lambda: now[0], jitter=lambda: 0)
    worker = DiscordGatewayWorker(driver)
    published = threading.Event()
    publish = worker._publish

    def observe(value):
        publish(value)
        if value.get("state") in {"paused", "reconnecting"}:
            published.set()

    worker._publish = observe
    worker.start()
    try:
        assert published.wait(3)
        assert worker.status()["state"] == "paused"
        assert worker.status()["reason"] == (
            "dispatch-persistence-unavailable" if event == "MESSAGE_CREATE" else "session-checkpoint-unavailable")
        worker._thread.join(timeout=3)
        assert not worker._thread.is_alive()
        now[0] = 1000
        driver.step()
        assert len(peers) == 1 and peers[0].closed
    finally:
        worker.close()
    fresh = ThreadedRuntimeRepository(base.root)
    assert DiscordResume(DiscordInbox(fresh, [route])).read() == {
        "session_id": "replacement", "resume_url": "wss://gateway.discord.gg", "sequence": 99}
    assert session.current is None if event == "READY" else session.current["session_id"] == "old-session"
    assert driver.protocol.sequence == (None if event == "READY" else 1 if event == "RESUMED" else 2)
    assert RuntimeInbox(fresh).next_pending(snapshot.session)["input_text"] == "keep local work"
