import threading

import pytest

from ahc.adapters.discord_gateway_driver import DiscordGatewayDriver
from ahc.adapters.discord_gateway_worker import DiscordGatewayWorker
from ahc.domain.sessions import ContractError


def test_socket_heartbeat_owned_by_worker_and_close_joins():
    beats, owners, closed = threading.Event(), [], threading.Event()

    class Socket:
        def __init__(self, url):
            owners.append(threading.get_ident())
            self.frames = [{"op": 10, "d": {"heartbeat_interval": 100}},
                {"op": 0, "s": 1, "t": "READY", "d": {"user": {"id": "123", "bot": True},
                 "session_id": "fixture", "resume_gateway_url": "wss://gateway.discord.gg"}}]
            self.count = 0

        def send(self, frame):
            owners.append(threading.get_ident())
            if frame["op"] == 1:
                self.frames.append({"op": 11})
                self.count += 1
                if self.count == 2:
                    beats.set()

        def receive(self, timeout):
            owners.append(threading.get_ident())
            return self.frames.pop(0) if self.frames else None

        def close(self):
            owners.append(threading.get_ident())
            closed.set()

    driver = DiscordGatewayDriver("123", "fixture", 0, lambda frame: None,
                                  connect=Socket, jitter=lambda: 0)
    worker = DiscordGatewayWorker(driver)
    worker.start()
    try:
        assert beats.wait(3)
        assert worker.status()["state"] == "ready"
        with pytest.raises(ContractError):
            worker.start()
    finally:
        worker.close()
    assert closed.is_set() and not worker._thread.is_alive()
    assert len(set(owners)) == 1 and owners[0] != threading.get_ident()
    worker.close()


def test_command_setup_does_not_block_gateway_and_close_cancels_setup():
    setup_started, stepped = threading.Event(), threading.Event()

    class Driver:
        def step(self, **_):
            stepped.set()

        def status(self):
            return {"state": "ready"}

        def close(self):
            pass

    def setup(*, cancelled, wait):
        setup_started.set()
        wait(30)
        assert cancelled()
        return False

    worker = DiscordGatewayWorker(Driver(), setup=setup)
    worker.start()
    try:
        assert setup_started.wait(2)
        assert stepped.wait(2)
    finally:
        worker.close()
    assert not worker._menu_thread.is_alive()
