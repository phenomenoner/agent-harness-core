"""Abrupt child-process exit at the SQLite inbox/checkpoint commit boundary."""

import json
import subprocess
import sys
from pathlib import Path

import pytest
from test_discord_gateway_state import HELLO
from test_runtime_core import setup

import ahc
from ahc.adapters.discord_gateway_state import DiscordGatewayState
from ahc.domain.channel_ingress import ChannelRoute
from ahc.infrastructure.discord_inbox import DiscordInbox
from ahc.infrastructure.discord_resume import DiscordResume, DiscordResumeSession
from ahc.infrastructure.runtime_repository import RuntimeRepository

CHILD = '''
import json, os, sys
sys.path.insert(0, sys.argv[1])
from ahc.domain.channel_ingress import ChannelRoute
from ahc.infrastructure.runtime_repository import RuntimeRepository
from ahc.infrastructure.discord_inbox import DiscordInbox
from ahc.infrastructure.discord_resume import DiscordResume
store = RuntimeRepository(sys.argv[2])
route = ChannelRoute.from_dict(json.loads(sys.argv[3]))
resume = DiscordResume(DiscordInbox(store, [route]))
frame = json.loads(sys.argv[4])
if sys.argv[5] == 'before':
    mutate = store._mutate
    def die_in_transaction(operation):
        def wrapped(connection):
            operation(connection)
            os._exit(23)
        return mutate(wrapped)
    store._mutate = die_in_transaction
resume.admit(frame, expected=resume.read())
os._exit(23)
'''


@pytest.mark.parametrize("phase", ["before", "after"])
def test_process_loss_keeps_sequence_and_submission_atomic(tmp_path, phase):
    repo, snapshot, _ = setup(tmp_path)
    route = ChannelRoute("discord", "123", "456", "789", snapshot.session)
    resume = DiscordResume(DiscordInbox(repo, [route]))
    resume.save_ready("session", "wss://gateway.discord.gg", 1, expected=None)
    frame = {"op": 0, "s": 2, "t": "MESSAGE_CREATE", "d": {"id": "10", "channel_id": "456",
             "author": {"id": "789"}, "type": 0, "content": "durable message"}}
    result = subprocess.run([sys.executable, "-I", "-c", CHILD,
        str(Path(ahc.__file__).parent.parent), str(repo.root), json.dumps(route.to_dict()),
        json.dumps(frame), phase], capture_output=True, timeout=10)
    assert result.returncode == 23, result.stderr.decode(errors="replace")
    reopened = RuntimeRepository(repo.root)
    session = DiscordResumeSession(DiscordResume(DiscordInbox(reopened, [route])))
    assert session.current["sequence"] == (1 if phase == "before" else 2)
    with reopened._read() as connection:
        assert connection.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        exists = connection.execute("SELECT 1 FROM sqlite_master WHERE name='runtime_submissions'")
        count = (connection.execute("SELECT count(*) FROM runtime_submissions").fetchone()[0]
                 if exists.fetchone() else 0)
        assert count == (0 if phase == "before" else 1)
    gateway = DiscordGatewayState("123", "fixture", 0, resume=session.current,
                                  checkpoint=session.checkpoint)
    assert gateway.receive(HELLO, session.admit)["d"]["seq"] == session.current["sequence"]
    replay = {**frame, "s": session.current["sequence"] + 1}
    gateway.receive(replay, session.admit)
    with reopened._read() as connection:
        assert connection.execute("SELECT count(*) FROM runtime_submissions").fetchone()[0] == 1
    assert session.repository.read()["sequence"] == replay["s"]
