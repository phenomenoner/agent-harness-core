"""Atomic import uses real artifact storage and does not create an executable turn."""

from types import SimpleNamespace

import pytest
from test_artifacts import state

from ahc.application.workspace_tools import WorkspaceTools
from ahc.domain.channel_files import ChannelFile
from ahc.domain.channel_ingress import ChannelInboundEvent, ChannelRoute
from ahc.infrastructure.artifact_repository import ArtifactRepository
from ahc.infrastructure.channel_file_import import ChannelFileImport
from ahc.infrastructure.channel_file_inbox import ChannelFileInbox
from ahc.infrastructure.runtime_execution_scope import workspace_turn
from ahc.infrastructure.runtime_inbox import RuntimeInbox
from ahc.infrastructure.runtime_repository import RuntimeRepository
from ahc.infrastructure.working_repository import WorkingRepositoryError


@pytest.mark.parametrize("rollback", [False, True])
def test_import_commit_reopen_and_no_redownload(tmp_path, rollback):
    repo, snapshot, producer, _ = state(tmp_path)
    route = ChannelRoute("telegram", "123", "456", "789", snapshot.session)
    event = ChannelInboundEvent(route, "update-1", "read", (
        ChannelFile("telegram", "456", "2", "one.txt", 4, "text/plain"),
        ChannelFile("telegram", "456", "3", "two.txt", 4, "text/plain")))
    ChannelFileInbox(repo).stage(event, snapshot.goal.digest)
    calls = []

    class Download:
        def fetch(self, reference):
            # A read is allowed while downloading; no write transaction is held.
            assert repo.read_current("session").session == snapshot.session
            calls.append(reference.file_id)
            return b"one!" if reference.file_id == "2" else b"two!"

    importer = ChannelFileImport(repo)
    if rollback:
        with pytest.raises(WorkingRepositoryError):
            importer.run(event, snapshot.goal.digest, producer, Download(), fail_before_commit=True)
        with repo._read() as connection:
            assert connection.execute("SELECT count(*) FROM runtime_artifacts").fetchone()[0] == 0
        calls.clear()
    result = importer.run(event, snapshot.goal.digest, producer, Download())
    assert calls == ["2", "3"]
    reopened = RuntimeRepository(repo.root)
    assert ChannelFileImport(reopened).run(
        event, snapshot.goal.digest, producer, Download()) == result
    assert calls == ["2", "3"]
    for selected, expected in zip(result, (b"one!", b"two!"), strict=True):
        stored = ArtifactRepository(reopened).read(route.session, selected["artifact_id"])
        assert stored[1] == expected
    assert RuntimeInbox(reopened).next_pending(route.session) is None
    submitter = ChannelFileImport(reopened)
    receipt = submitter.submit(event, snapshot.goal.digest)
    assert submitter.submit(event, snapshot.goal.digest) == receipt
    pending = RuntimeInbox(reopened).next_pending(route.session)
    assert pending["operation_id"] == event.operation_id
    assert pending["input_text"] == event.input_text
    request = SimpleNamespace(session=route.session, workspace_scope=producer.workspace_id,
                              operation_id=event.operation_id, goal_digest=snapshot.goal.digest)
    tools = WorkspaceTools(SimpleNamespace(identity=producer, store=reopened, closed=False),
                           artifacts=ArtifactRepository(reopened))
    arguments = {"expected": producer.to_dict()}
    assert tools.invoke("workspace.incoming_artifacts", arguments)["disposition"] == "rejected"
    with reopened.execution_lock(), workspace_turn(reopened, request):
        assert tools.invoke("workspace.incoming_artifacts", arguments)["result"] == result
    assert tools.invoke("workspace.incoming_artifacts", arguments)["disposition"] == "rejected"
