"""Selected artifacts and channel intents share the actual terminal transaction."""

from dataclasses import replace

import pytest
from test_channel_outbox import state
from test_workspace_tools import Port

from ahc.application.runtime_service import RuntimeService
from ahc.domain.agent_backend import AgentTurnObservation
from ahc.domain.result_artifacts import ResultArtifact
from ahc.domain.runtime_turn import accept_observation
from ahc.domain.sessions import ContractError, VirtualSessionIdentity
from ahc.domain.working_sets import WorkingSetSnapshot
from ahc.infrastructure.artifact_repository import ArtifactRepository
from ahc.infrastructure.channel_outbox import ChannelOutbox
from ahc.infrastructure.runtime_repository import RuntimeRepository
from ahc.infrastructure.working_repository import WorkingRepositoryError


def prepared(tmp_path):
    repo, snapshot, request, route = state(tmp_path)
    workspace = RuntimeService(repo, None).open_workspace(snapshot, "work", Port)
    manifest = ArtifactRepository(repo).publish(workspace.identity, b"report", "text/plain")
    selection = ResultArtifact(manifest.artifact_id, "report.txt")
    observation = AgentTurnObservation(request, "completed", "fixture", 1, 1,
                                       output_text="answer", artifacts=(selection,))
    return repo, snapshot, request, route, workspace, observation


def test_selected_artifact_intent_commits_with_final_and_reopens(tmp_path):
    repo, snapshot, request, route, workspace, observation = prepared(tmp_path)
    try:
        class Backend:
            def invoke(self, request):
                return observation

        result = RuntimeService(repo, Backend()).execute(snapshot, request)
        assert result.result["artifacts"] == [observation.artifacts[0].to_dict()]
        reopened = RuntimeRepository(repo.root)
        assert reopened.read_turn(route.session, request.operation_id) == result
        assert ChannelOutbox(reopened).read(route)[0]["payload"] == {"output_text": "answer"}
        with reopened._read() as connection:
            rows = connection.execute("SELECT * FROM runtime_channel_attachments").fetchall()
            assert len(rows) == 1 and rows[0]["state"] == "pending"
            assert rows[0]["operation_id"] == request.operation_id
            assert rows[0]["ordinal"] == 0 and rows[0]["claim_token"] is None
        assert RuntimeService(reopened, Backend()).execute(snapshot, request) == result
        with reopened._read() as connection:
            assert connection.execute(
                "SELECT count(*) FROM runtime_channel_attachments").fetchone()[0] == 1
    finally:
        workspace.close()


def test_nine_selections_reach_the_ninth_controlled_attachment_send(tmp_path):
    from ahc.adapters.codex_workspace_tools import CodexWorkspaceTools
    from ahc.application.workspace_tools import WorkspaceTools
    from ahc.infrastructure.channel_attachment_outbox import ChannelAttachmentOutbox
    from ahc.application.channel_attachment_delivery import ChannelAttachmentDelivery
    from email import policy
    from email.parser import BytesParser

    repo, snapshot, request, route, workspace, _ = prepared(tmp_path)
    artifacts = ArtifactRepository(repo)
    tools = WorkspaceTools(workspace, artifacts=artifacts)
    request = replace(request, workspace_scope="work", capabilities=tools.capabilities())
    manifests = [artifacts.publish(workspace.identity, str(i).encode(), "text/plain") for i in range(9)]
    class Backend:
        def invoke(self, bound):
            bridge = CodexWorkspaceTools(bound, tools, "thread", "turn", clock=lambda: 0)
            for index, manifest in enumerate(manifests):
                response = bridge.respond({"id": index, "method": "item/tool/call", "params": {
                    "threadId": "thread", "turnId": "turn", "namespace": "workspace", "tool": "attach",
                    "callId": "attach-"+str(index), "arguments": {"expected": workspace.identity.to_dict(),
                        "artifact_id": manifest.artifact_id, "filename": f"report-{index}.txt"}}})
                assert response["result"]["success"]
            return AgentTurnObservation(bound, "completed", "fixture", 1, 1,
                                        output_text="nine reports", artifacts=bridge.artifacts)
    try:
        turn = RuntimeService(repo, Backend()).execute(snapshot, request)
        assert turn.result["accepted"] and len(turn.result["artifacts"]) == 9
        outbox = ChannelAttachmentOutbox(repo)
        records = outbox.read(route)
        assert len(records) == 9 and records[-1]["ordinal"] == 8
        class Destination:
            def __init__(self):
                self.calls = []
            def send(self, upload):
                index = len(self.calls)
                message = BytesParser(policy=policy.default).parsebytes(
                    f"Content-Type: {upload.content_type}\r\nMIME-Version: 1.0\r\n\r\n".encode()+upload.body)
                assert list(message.iter_parts())[-1].get_payload(decode=True) == str(index).encode()
                assert upload.route == route and upload.artifact_id == manifests[index].artifact_id
                assert upload.filename == f"report-{index}.txt"
                self.calls.append(upload.artifact_id)
                return {"peer_receipt": str(index), "artifact_id": upload.artifact_id}
        destination = Destination()
        delivery = ChannelAttachmentDelivery(outbox, route, destination)
        for index in range(9):
            assert delivery.deliver(request.operation_id, index, f"token-{index}")["state"] == "sent"
        assert destination.calls == [manifest.artifact_id for manifest in manifests]
        assert all(row["state"] == "sent" for row in outbox.read(route))
        with pytest.raises(ContractError):
            delivery.deliver(request.operation_id, 8, "ninth-replay")
        assert len(destination.calls) == 9
        assert RuntimeService(repo, Backend()).execute(snapshot, request) == turn
    finally:
        workspace.close()


@pytest.mark.parametrize("fault", ["missing", "other_owner", "before_commit"])
def test_invalid_selection_or_interruption_leaves_no_partial_terminal(tmp_path, fault):
    repo, snapshot, request, route, workspace, observation = prepared(tmp_path)
    try:
        if fault == "missing":
            observation = replace(observation, artifacts=(ResultArtifact("0" * 64, "x.txt"),))
        elif fault == "other_owner":
            other = repo.create(WorkingSetSnapshot.initial(
                VirtualSessionIdentity.initial("other", "other-agent", "other-route"),
                snapshot.goal))
            other_workspace = RuntimeService(repo, None).open_workspace(other, "work", Port)
            try:
                manifest = ArtifactRepository(repo).publish(
                    other_workspace.identity, b"private", "text/plain")
                observation = replace(observation, artifacts=(
                    ResultArtifact(manifest.artifact_id, "private.txt"),))
            finally:
                other_workspace.close()
        repo.begin(snapshot, request)
        result = accept_observation(request, observation)
        repo.seal_observation(request, result)
        error = WorkingRepositoryError if fault == "before_commit" else ContractError
        with pytest.raises(error):
            repo.finish(request, result, fail_before_commit=fault == "before_commit")
        reopened = RuntimeRepository(repo.root)
        assert reopened.read_turn(route.session, request.operation_id).status == "submitted"
        assert ChannelOutbox(reopened).read(route) == ()
        with reopened._read() as connection:
            assert connection.execute(
                "SELECT count(*) FROM runtime_channel_attachments").fetchone()[0] == 0
            assert connection.execute("SELECT count(*) FROM runtime_intents").fetchone()[0] == 0
    finally:
        workspace.close()


@pytest.mark.parametrize("fault", ["duplicate", "overflow", "mutable", "unknown"])
def test_observation_rejects_invalid_artifact_selection(tmp_path, fault):
    _, _, request, _, workspace, observation = prepared(tmp_path)
    try:
        selections = observation.artifacts
        if fault == "duplicate":
            selections *= 2
        elif fault == "overflow":
            selections *= 9
        elif fault == "mutable":
            selections = list(selections)
        observation = replace(observation, artifacts=selections,
                              disposition="unknown" if fault == "unknown" else "completed")
        with pytest.raises(ContractError):
            accept_observation(request, observation)
    finally:
        workspace.close()
