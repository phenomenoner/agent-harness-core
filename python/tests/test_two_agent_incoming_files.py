"""Two agents in one database retain distinct channel files, tools and final routes."""

import base64
from dataclasses import replace
from types import SimpleNamespace

import pytest
from test_runtime_core import setup
from test_workspace_tools import Port

from ahc.application.runtime_service import RuntimeService
from ahc.application.workspace_tools import WorkspaceTools
from ahc.domain.agent_backend import AgentTurnObservation
from ahc.domain.channel_files import ChannelFile
from ahc.domain.channel_ingress import ChannelInboundEvent, ChannelRoute
from ahc.domain.sessions import ContractError, VirtualSessionIdentity
from ahc.domain.working_sets import WorkingSetSnapshot
from ahc.infrastructure.artifact_repository import ArtifactRepository
from ahc.infrastructure.channel_file_import import ChannelFileImport
from ahc.infrastructure.channel_file_inbox import ChannelFileInbox
from ahc.infrastructure.channel_outbox import ChannelOutbox
from ahc.infrastructure.runtime_inbox import RuntimeInbox
from ahc.infrastructure.runtime_repository import RuntimeRepository


@pytest.mark.parametrize("platform", ["telegram", "discord"])
@pytest.mark.parametrize("axis", ["account", "channel", "user"])
def test_two_agent_file_turn_and_route_isolation(tmp_path, platform, axis):
    repo, first, template = setup(tmp_path)
    second = repo.create(WorkingSetSnapshot.initial(
        VirtualSessionIdentity.initial("second", "second-agent", "second-route"), first.goal))
    snapshots = [first, second]
    workspaces, routes, events, selected, requests, results = [], [], [], [], [], []
    try:
        for index, snapshot in enumerate(snapshots):
            workspace = RuntimeService(repo, None).open_workspace(snapshot, f"work-{index}", Port)
            workspaces.append(workspace)
            channel = str(456 + (index if axis == "channel" else 0))
            account = str(123 + (index if axis == "account" else 0))
            user = str(789 + (index if axis == "user" else 0))
            route = ChannelRoute(platform, account, channel, user, snapshot.session)
            routes.append(route)
            url = (f"https://cdn.discordapp.com/attachments/{channel}/2/report.txt"
                   if platform == "discord" else None)
            event = ChannelInboundEvent(route, "same-event", "read report", (
                ChannelFile(platform, channel, "2", "report.txt", 4, "text/plain", url),))
            events.append(event)
            ChannelFileInbox(repo).stage(event, snapshot.goal.digest)
            importer = ChannelFileImport(repo)
            imported = importer.run(event, snapshot.goal.digest, workspace.identity,
                                    SimpleNamespace(fetch=lambda ref, i=index: f"dat{i}".encode()))
            selected.append(imported[0])
            importer.submit(event, snapshot.goal.digest)
        assert events[0].operation_id != events[1].operation_id
        assert selected[0]["artifact_id"] != selected[1]["artifact_id"]
        for index, snapshot in enumerate(snapshots):
            workspace, route, event = workspaces[index], routes[index], events[index]
            tools = WorkspaceTools(workspace, artifacts=ArtifactRepository(repo))
            request = replace(template, session=snapshot.session, operation_id=event.operation_id,
                              input_text=event.input_text,
                              workspace_scope=workspace.identity.workspace_id,
                              capabilities=tools.capabilities())
            requests.append(request)

            class Backend:
                def invoke(self, admitted, workspace=workspace, tools=tools, index=index):
                    expected = workspace.identity.to_dict()
                    listed = tools.invoke("workspace.incoming_artifacts", {"expected": expected})
                    assert listed["result"] == [selected[index]]
                    own = tools.invoke("workspace.read_artifact", {"expected": expected,
                        "artifact_id": selected[index]["artifact_id"], "offset": 0, "length": 4096})
                    assert base64.b64decode(own["result"]["data"]) == f"dat{index}".encode()
                    foreign = tools.invoke("workspace.read_artifact", {"expected": expected,
                        "artifact_id": selected[1-index]["artifact_id"],
                        "offset": 0, "length": 4096})
                    assert foreign["disposition"] == "rejected"
                    return AgentTurnObservation(admitted, "completed", "fixture", 1, 1,
                                                output_text=f"agent-{index} answer")

            result = RuntimeService(repo, Backend()).execute(snapshot, request)
            assert result.result["accepted"] is True
            results.append(result)
            rows = ChannelOutbox(repo).read(route)
            assert len(rows) == 1 and rows[0]["operation_id"] == event.operation_id
            assert rows[0]["payload"] == {"output_text": f"agent-{index} answer"}
            with pytest.raises(ContractError):
                ArtifactRepository(repo).read(snapshot.session, selected[1-index]["artifact_id"])
        reopened = RuntimeRepository(repo.root)
        for snapshot, request, result in zip(snapshots, requests, results, strict=True):
            assert RuntimeInbox(reopened).next_pending(snapshot.session) is None
            assert RuntimeService(reopened, None).execute(snapshot, request) == result
    finally:
        for workspace in reversed(workspaces):
            workspace.close()
