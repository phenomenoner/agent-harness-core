"""Reopen staged file imports after real child process loss at each commit seam."""

import json
import subprocess
import sys
from pathlib import Path

import pytest
from test_artifacts import state

import ahc
from ahc.domain.channel_files import ChannelFile
from ahc.domain.channel_ingress import ChannelInboundEvent, ChannelRoute
from ahc.infrastructure.artifact_repository import ArtifactRepository
from ahc.infrastructure.channel_file_import import ChannelFileImport
from ahc.infrastructure.channel_file_inbox import ChannelFileInbox
from ahc.infrastructure.runtime_repository import RuntimeRepository

CHILD = '''
import json, os, sys
sys.path.insert(0, sys.argv[1])
from ahc.domain.channel_ingress import ChannelInboundEvent, ChannelRoute
from ahc.domain.workspace import WorkspaceIdentity
from ahc.infrastructure.channel_file_import import ChannelFileImport
from ahc.infrastructure.channel_file_inbox import ChannelFileInbox
from ahc.infrastructure.runtime_repository import RuntimeRepository

store = RuntimeRepository(sys.argv[2])
route = ChannelRoute.from_dict(json.loads(sys.argv[3]))
event = ChannelInboundEvent.from_dict(json.loads(sys.argv[4]))
producer = WorkspaceIdentity.from_dict(json.loads(sys.argv[5]))
goal_digest, phase = sys.argv[6:8]
ChannelFileInbox(store).stage(event, goal_digest)

class Download:
    def fetch(self, reference):
        return b'report'

importer = ChannelFileImport(store)
if phase == 'before-import-commit':
    mutate = store._mutate
    def interrupted(operation, **kwargs):
        def wrapped(connection):
            operation(connection)
            os._exit(23)
        return mutate(wrapped, **kwargs)
    store._mutate = interrupted
importer.run(event, goal_digest, producer, Download())
if phase == 'after-import-commit':
    os._exit(23)
importer.submit(event, goal_digest)
if phase == 'after-submit-commit':
    os._exit(23)
os._exit(23)
'''


def _table_count(repository, table):
    with repository._read() as connection:
        exists = connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)
        ).fetchone()
        return 0 if exists is None else connection.execute(
            f"SELECT count(*) FROM {table}"
        ).fetchone()[0]


@pytest.mark.parametrize(
    "phase", ["before-import-commit", "after-import-commit", "after-submit-commit"]
)
def test_incoming_file_process_loss_reopens_at_import_and_submit_boundaries(tmp_path, phase):
    repo, snapshot, producer, _ = state(tmp_path)
    route = ChannelRoute("telegram", "123", "456", "789", snapshot.session)
    event = ChannelInboundEvent(
        route,
        "update-1",
        "read",
        (ChannelFile("telegram", "456", "2", "report.txt", 6, "text/plain"),),
    )
    ChannelFileInbox(repo).stage(event, snapshot.goal.digest)
    result = subprocess.run(
        [
            sys.executable,
            "-I",
            "-c",
            CHILD,
            str(Path(ahc.__file__).parent.parent),
            str(repo.root),
            json.dumps(route.to_dict()),
            json.dumps(event.to_dict()),
            json.dumps(producer.to_dict()),
            snapshot.goal.digest,
            phase,
        ],
        capture_output=True,
        timeout=10,
    )
    assert result.returncode == 23, result.stderr.decode(errors="replace")

    reopened = RuntimeRepository(repo.root)
    with reopened._read() as connection:
        assert connection.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
    assert _table_count(reopened, "runtime_channel_file_inbox") == 1
    assert _table_count(reopened, "runtime_submissions") == int(phase == "after-submit-commit")

    if phase == "before-import-commit":
        assert _table_count(reopened, "runtime_artifacts") == 0
        assert _table_count(reopened, "runtime_channel_file_imports") == 0
        return

    calls = []

    class Download:
        def fetch(self, reference):
            calls.append(reference.file_id)
            return b"report"

    selected = ChannelFileImport(reopened).run(
        event, snapshot.goal.digest, producer, Download()
    )
    assert len(selected) == 1 and calls == []
    manifest, content = ArtifactRepository(reopened).read(
        route.session, selected[0]["artifact_id"]
    )
    assert manifest.byte_count == 6 and content == b"report"
    assert _table_count(reopened, "runtime_artifacts") == 1
    assert _table_count(reopened, "runtime_channel_file_imports") == 1

    if phase == "after-import-commit":
        receipt = ChannelFileImport(reopened).submit(event, snapshot.goal.digest)
        assert receipt["operation_id"] == event.operation_id
        assert _table_count(reopened, "runtime_submissions") == 1
        return

    assert _table_count(reopened, "runtime_submissions") == 1
    first = ChannelFileImport(reopened).submit(event, snapshot.goal.digest)
    second = ChannelFileImport(reopened).submit(event, snapshot.goal.digest)
    assert first == second
    assert _table_count(reopened, "runtime_submissions") == 1
