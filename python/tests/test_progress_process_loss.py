"""Real child exit across progress reservation, controlled effect and receipt."""

import json
import subprocess
import sys
from dataclasses import asdict
from pathlib import Path

import pytest
from test_channel_outbox import state

import ahc
from ahc.application.runtime_service import RuntimeService
from ahc.domain.sessions import ContractError
from ahc.infrastructure.channel_progress_store import ChannelProgressStore
from ahc.infrastructure.runtime_repository import RuntimeRepository

CHILD = '''
import json, os, sys
sys.path.insert(0, sys.argv[1])
from ahc.domain.agent_backend import AgentTurnRequest
from ahc.domain.channel_ingress import ChannelRoute
from ahc.domain.sessions import VirtualSessionIdentity
from ahc.infrastructure.runtime_repository import RuntimeRepository
from ahc.application.channel_progress import ChannelProgress
store = RuntimeRepository(sys.argv[2])
route = ChannelRoute.from_dict(json.loads(sys.argv[3]))
raw = json.loads(sys.argv[4])
raw['session'] = VirtualSessionIdentity.from_dict(raw['session'])
for field in ('revision_digests', 'capabilities', 'skill_bodies'):
    raw[field] = tuple(raw[field])
request = AgentTurnRequest(**raw)
phase, marker = sys.argv[5:7]
class Transport:
    def send(self, *args):
        with open(marker, 'xb') as target:
            target.write(b'progress-effect')
            target.flush()
            os.fsync(target.fileno())
        if phase == 'after-effect':
            os._exit(23)
        return 'controlled-receipt'
progress = ChannelProgress(store, request, route, Transport(),
                           clock=lambda: 0, wall_clock=lambda: 100)
if phase == 'before-commit':
    mutate = store._mutate
    def interrupted(operation):
        def wrapped(connection):
            operation(connection)
            os._exit(23)
        return mutate(wrapped)
    store._mutate = interrupted
elif phase == 'after-claim':
    reserve = progress.ledger.reserve
    def interrupted(*args):
        reserve(*args)
        os._exit(23)
    progress.ledger.reserve = interrupted
with store.execution_lock():
    with store.workspace_turn_scope(request):
        progress.send('progress-one', 'working')
os._exit(23)
'''


@pytest.mark.parametrize("phase", ["before-commit", "after-claim", "after-effect", "after-receipt"])
def test_progress_exit_preserves_indeterminate_effect_and_no_turn_replay(tmp_path, phase):
    repo, snapshot, request, route = state(tmp_path)
    repo.begin(snapshot, request)
    payload = asdict(request)
    payload["session"] = request.session.to_dict()
    marker = tmp_path / "progress-effect"
    result = subprocess.run([sys.executable, "-I", "-c", CHILD,
        str(Path(ahc.__file__).parent.parent), str(repo.root), json.dumps(route.to_dict()),
        json.dumps(payload), phase, str(marker)], capture_output=True, timeout=10)
    assert result.returncode == 23, result.stderr.decode(errors="replace")
    assert marker.exists() is (phase in {"after-effect", "after-receipt"})
    reopened = RuntimeRepository(repo.root)
    with reopened._read() as connection:
        assert connection.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
    ledger = ChannelProgressStore(reopened)
    rows = ledger.reconcile(route)
    if phase == "before-commit":
        assert rows == ()
    else:
        assert len(rows) == 1
        assert rows[0]["state"] == ("sent" if phase == "after-receipt" else "unknown")
        with pytest.raises(ContractError, match="already attempted"):
            ledger.reserve(request, route, "progress-one", "working", "new", 200000)

    class Backend:
        def invoke(self, request):
            pytest.fail("pending operation replayed a backend")

    assert RuntimeService(reopened, Backend()).execute(snapshot, request).status == "submitted"
