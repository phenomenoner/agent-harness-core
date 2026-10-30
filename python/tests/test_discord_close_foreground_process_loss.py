"""Actual foreground-process exit before/after close-driven checkpoint commit."""

import json
import subprocess
import sys
from pathlib import Path

import pytest
from test_discord_gateway_state import HELLO
from test_runtime_core import setup

import ahc
from ahc.adapters.discord_gateway_state import DiscordGatewayState
from ahc.application.foreground_runtime import ForegroundRuntime
from ahc.domain.channel_ingress import ChannelRoute
from ahc.infrastructure.discord_inbox import DiscordInbox
from ahc.infrastructure.discord_resume import DiscordResume
from ahc.infrastructure.runtime_inbox import RuntimeInbox
from ahc.infrastructure.threaded_runtime_repository import ThreadedRuntimeRepository

CHILD = '''
import json, os, sys
sys.path.insert(0, sys.argv[1])
from ahc.adapters.discord_gateway_driver import DiscordGatewayDriver
from ahc.adapters.discord_gateway_worker import DiscordGatewayWorker
from ahc.adapters.discord_websocket import DiscordGatewayClosed
from ahc.application.foreground_runtime import ForegroundRuntime
from ahc.domain.channel_ingress import ChannelRoute
from ahc.infrastructure.discord_inbox import DiscordInbox
from ahc.infrastructure.discord_resume import DiscordResume, DiscordResumeSession
from ahc.infrastructure.threaded_runtime_repository import ThreadedRuntimeRepository
store = ThreadedRuntimeRepository(sys.argv[2])
route = ChannelRoute.from_dict(json.loads(sys.argv[3]))
session = DiscordResumeSession(DiscordResume(DiscordInbox(store, [route])))
mutate = store._mutate
def crash(operation):
    def wrapped(connection):
        operation(connection)
        if sys.argv[4] == 'before':
            os._exit(23)
    result = mutate(wrapped)
    os._exit(23)
store._mutate = crash
class Socket:
    def __init__(self, url):
        self.first = True
    def receive(self, timeout):
        if self.first:
            self.first = False
            return {'op':10,'d':{'heartbeat_interval':45000}}
        raise DiscordGatewayClosed(4009)
    def send(self, frame):
        assert frame['op'] == 6
    def close(self):
        pass
driver = DiscordGatewayDriver('123','fixture',0,session.admit,connect=Socket,
    resume=session.current,checkpoint=session.checkpoint)
worker = DiscordGatewayWorker(driver)
runtime = ForegroundRuntime(store,route.session,lambda *_:None,
    lambda *_:None,channel_receiver=worker)
runtime.start()
worker._thread.join(timeout=5)
raise AssertionError('process crash seam not reached')
'''


@pytest.mark.parametrize("phase", ["before", "after"])
def test_foreground_exit_retains_atomic_checkpoint_and_releases_lease(tmp_path, phase):
    base, snapshot, _ = setup(tmp_path)
    route = ChannelRoute("discord", "123", "456", "789", snapshot.session)
    resume = DiscordResume(DiscordInbox(base, [route]))
    resume.save_ready("old", "wss://gateway.discord.gg", 7, expected=None)
    original = resume.read()
    RuntimeInbox(base).submit(snapshot.session, "pending", snapshot.goal.digest, "preserve")
    child = subprocess.run([sys.executable, "-I", "-c", CHILD,
        str(Path(ahc.__file__).parent.parent), str(base.root), json.dumps(route.to_dict()), phase],
        capture_output=True, timeout=10)
    assert child.returncode == 23, child.stderr.decode(errors="replace")
    fresh = ThreadedRuntimeRepository(base.root)
    position = DiscordResume(DiscordInbox(fresh, [route])).read()
    assert position == (original if phase == "before" else None)
    assert RuntimeInbox(fresh).next_pending(snapshot.session)["input_text"] == "preserve"
    with fresh._read() as connection:
        assert connection.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
    # Rollback before commit retains the previous checkpoint; it cannot claim
    # remote validity. A later remote rejection is still handled by the driver.
    gateway = DiscordGatewayState("123", "fixture", 0, resume=position)
    assert gateway.receive(HELLO, lambda _: None)["op"] == (6 if phase == "before" else 2)
    runtime = ForegroundRuntime(fresh, snapshot.session, lambda *_: None, lambda *_: None)
    runtime.start()  # OS released the exited foreground's lease.
    try:
        assert runtime.step() is None
    finally:
        runtime.close()
