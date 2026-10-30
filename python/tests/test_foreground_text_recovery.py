"""Foreground text delivery recovery after an effect without acknowledgement."""

import json
import subprocess
import sys
from dataclasses import replace
from pathlib import Path

import pytest
from test_channel_outbox import accepted
from test_runtime_core import setup

import ahc
from ahc.application.channel_ingress import ChannelIngress
from ahc.domain.channel_ingress import ChannelInboundEvent, ChannelRoute
from ahc.infrastructure.channel_outbox import ChannelOutbox
from ahc.infrastructure.runtime_inbox import RuntimeInbox

CHILD = '''
import json, os, sys
sys.path.insert(0, sys.argv[1])
from types import SimpleNamespace
from ahc.cli import foreground_channels
from ahc.application.foreground_runtime import ForegroundRuntime
from ahc.domain.channel_ingress import ChannelRoute
from ahc.infrastructure.runtime_repository import RuntimeRepository
from ahc.infrastructure.channel_outbox import ChannelOutbox
root, raw_route, marker, crash = json.loads(sys.argv[2])
repo = RuntimeRepository(root)
route = ChannelRoute.from_dict(raw_route)
os.environ['AHC_RECOVERY_FIXTURE'] = 'fixture'
class Transport:
    def send(self, *args):
        with open(marker, 'xb') as stream:
            stream.write(b'one-effect'); stream.flush(); os.fsync(stream.fileno())
        if crash:
            os._exit(23)
        raise AssertionError('unexpected resend')
foreground_channels.BotTextTransport = lambda *args: Transport()
config = SimpleNamespace(to_dict=lambda: {'channels': [
    {'route': raw_route, 'token_env': 'AHC_RECOVERY_FIXTURE', 'enabled': True}]})
delivery = foreground_channels.compose_channels(config, repo)
def forbidden(*a, **kw):
    raise AssertionError('backend replay')
runtime = ForegroundRuntime(repo, route.session, lambda *a: None, forbidden,
                            channel_delivery=delivery)
try:
    runtime.start()
    runtime.step()
    assert ChannelOutbox(repo).read(route)[0]['state'] == 'unknown'
finally:
    runtime.close()
'''


@pytest.mark.parametrize("platform", ["telegram", "discord"])
def test_foreground_reconciles_interrupted_text_without_resending(tmp_path, platform):
    repo, current, request = setup(tmp_path)
    route = ChannelRoute(platform, "123", "456", "789", current.session)
    event = ChannelInboundEvent(route, "event", "hello")
    ChannelIngress(RuntimeInbox(repo), route).submit(event, current.goal.digest)
    request = replace(request, operation_id=event.operation_id, input_text=event.input_text)
    accepted(repo, current, request)
    marker = tmp_path / "effect"
    for crash in (True, False):
        process = subprocess.run(
            [sys.executable, "-I", "-c", CHILD, str(Path(ahc.__file__).parent.parent),
             json.dumps([str(repo.root), route.to_dict(), str(marker), crash])],
            capture_output=True, timeout=15,
        )
        assert process.returncode == (23 if crash else 0), process.stderr.decode(errors="replace")
        assert ChannelOutbox(repo).read(route)[0]["state"] == ("claimed" if crash else "unknown")
        assert marker.read_bytes() == b"one-effect"
