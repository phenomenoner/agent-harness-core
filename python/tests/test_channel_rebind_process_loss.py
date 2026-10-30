"""Actual process exit around the consumer binding transaction commit."""

import json
import subprocess
import sys
from dataclasses import replace

import pytest
from test_channel_recovery_binding import consumer, position
from test_channel_recovery_dedup import recover
from test_runtime_core import setup
from test_telegram_inbox import update

from ahc.domain.channel_ingress import ChannelRoute
from ahc.domain.sessions import ContractError
from ahc.infrastructure.channel_recovery_binding import rebind_recovered_consumer
from ahc.infrastructure.runtime_inbox import RuntimeInbox
from ahc.infrastructure.runtime_repository import RuntimeRepository

CHILD = """
import json, os, sys
from pathlib import Path
from ahc.domain.channel_ingress import ChannelRoute
from ahc.infrastructure import channel_recovery_binding as binding
from ahc.infrastructure.runtime_repository import RuntimeRepository
root, before, after, expected, phase = sys.argv[1:]
original = binding.open_connection
class Connection:
    def __init__(self, connection):
        self.connection = connection
    def __getattr__(self, name):
        return getattr(self.connection, name)
    def commit(self):
        if phase == 'before-commit':
            os._exit(23)
        self.connection.commit()
        os._exit(23)
binding.open_connection = lambda *a, **kw: Connection(original(*a, **kw))
binding.rebind_recovered_consumer(RuntimeRepository(root), Path(root)/'supervisor.lock',
    [ChannelRoute.from_dict(json.loads(before))], [ChannelRoute.from_dict(json.loads(after))],
    expected=json.loads(expected))
raise AssertionError('crash seam not reached')
"""


@pytest.mark.parametrize("platform", ["telegram", "discord"])
@pytest.mark.parametrize("phase", ["before-commit", "after-commit"])
def test_process_loss_preserves_complete_binding_and_checkpoint(tmp_path, platform, phase):
    repo, saved, _ = setup(tmp_path)
    route = ChannelRoute(platform, "123", "456", "789", saved.session)
    old = consumer(repo, route)
    if platform == "telegram":
        old.admit([update(1)], expected_offset=None)
    else:
        old.save_ready("fixture", "wss://gateway.discord.gg", 1, expected=None)
        old.admit({"op": 0, "s": 2, "t": "MESSAGE_CREATE", "d": {"id": "10",
            "channel_id": "456", "author": {"id": "789"}, "type": 0, "content": "old"}},
            expected=old.read())
    checkpoint = position(old)
    successor = recover(repo, saved, tmp_path)
    new_route = replace(route, session=successor)
    with repo._read() as connection:
        submissions = [tuple(row) for row in connection.execute(
            "SELECT * FROM runtime_submissions")]
    child = subprocess.run([sys.executable, "-c", CHILD, str(repo.root),
        json.dumps(route.to_dict()), json.dumps(new_route.to_dict()),
        json.dumps(checkpoint), phase],
        capture_output=True, text=True, timeout=15)
    assert child.returncode == 23, child.stderr
    reopened = RuntimeRepository(repo.root)
    accepted = route if phase == "before-commit" else new_route
    rejected = new_route if phase == "before-commit" else route
    assert position(consumer(reopened, accepted)) == checkpoint
    with pytest.raises(ContractError, match="binding changed"):
        position(consumer(reopened, rejected))
    if phase == "before-commit":
        rebind_recovered_consumer(reopened, repo.root / "supervisor.lock", [route], [new_route],
                                  expected=checkpoint)
    else:
        with pytest.raises(ContractError, match="binding changed"):
            rebind_recovered_consumer(reopened, repo.root / "supervisor.lock", [route], [new_route],
                                      expected=checkpoint)
    assert position(consumer(reopened, new_route)) == checkpoint
    with reopened._read() as connection:
        assert [tuple(row) for row in connection.execute(
            "SELECT * FROM runtime_submissions")] == submissions
    assert RuntimeInbox(reopened).next_pending(successor) is None
    with reopened.foreground_lease(successor):
        pass  # The terminated process no longer holds cooperative runtime ownership.
