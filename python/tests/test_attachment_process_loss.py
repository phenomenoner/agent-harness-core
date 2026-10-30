"""Abrupt process exit across attachment claim, controlled effect and receipt."""

import json
import subprocess
import sys
from pathlib import Path

import pytest
from test_channel_attachment_outbox import pending

import ahc
from ahc.application.channel_attachment_delivery import ChannelAttachmentDelivery
from ahc.domain.sessions import ContractError
from ahc.infrastructure.channel_attachment_outbox import ChannelAttachmentOutbox
from ahc.infrastructure.runtime_repository import RuntimeRepository

CHILD = '''
import json, os, sys
sys.path.insert(0, sys.argv[1])
from ahc.domain.channel_ingress import ChannelRoute
from ahc.infrastructure.runtime_repository import RuntimeRepository
from ahc.infrastructure.channel_attachment_outbox import ChannelAttachmentOutbox
from ahc.application.channel_attachment_delivery import ChannelAttachmentDelivery
store = RuntimeRepository(sys.argv[2])
route = ChannelRoute.from_dict(json.loads(sys.argv[3]))
operation, phase, marker = sys.argv[4:7]
outbox = ChannelAttachmentOutbox(store)
if phase == 'before-claim-commit':
    mutate = store._mutate
    def interrupted(operation):
        def wrapped(connection):
            operation(connection)
            os._exit(23)
        return mutate(wrapped)
    store._mutate = interrupted
elif phase == 'after-claim':
    claim = outbox.claim
    def interrupted(*args):
        claim(*args)
        os._exit(23)
    outbox.claim = interrupted
class Transport:
    def send(self, upload):
        with open(marker, 'xb') as target:
            target.write(b'controlled-effect')
            target.flush()
            os.fsync(target.fileno())
        if phase == 'after-effect':
            os._exit(23)
        return {'message_id': '1', 'attachment_id': '2'}
ChannelAttachmentDelivery(outbox, route, Transport()).deliver(operation, 0, 'claim')
os._exit(23)
'''


@pytest.mark.parametrize(
    "phase", ["before-claim-commit", "after-claim", "after-effect", "after-ack"]
)
def test_crash_recovery_never_replays_an_ambiguous_attachment(tmp_path, phase):
    repo, request, route, workspace = pending(tmp_path)
    marker = tmp_path / "controlled-effect"
    try:
        result = subprocess.run([sys.executable, "-I", "-c", CHILD,
            str(Path(ahc.__file__).parent.parent), str(repo.root), json.dumps(route.to_dict()),
            request.operation_id, phase, str(marker)], capture_output=True, timeout=10)
        assert result.returncode == 23, result.stderr.decode(errors="replace")
        assert marker.exists() is (phase in {"after-effect", "after-ack"})
        reopened = RuntimeRepository(repo.root)
        with reopened._read() as connection:
            assert connection.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        outbox = ChannelAttachmentOutbox(reopened)
        expected = ("pending" if phase == "before-claim-commit" else
                    "sent" if phase == "after-ack" else "claimed")
        assert outbox.read(route)[0]["state"] == expected
        recovered = outbox.reconcile(route)[0]
        assert recovered["state"] == ("unknown" if expected == "claimed" else expected)
        calls = []

        class Transport:
            def send(self, upload):
                calls.append(upload)
                return {"message_id": "2", "attachment_id": "3"}

        delivery = ChannelAttachmentDelivery(outbox, route, Transport())
        if expected == "pending":
            assert delivery.deliver(request.operation_id, 0, "fresh")["state"] == "sent"
            assert len(calls) == 1
        else:
            with pytest.raises(ContractError):
                delivery.deliver(request.operation_id, 0, "fresh")
            assert calls == []
        if marker.exists():
            assert marker.read_bytes() == b"controlled-effect"
    finally:
        workspace.close()
