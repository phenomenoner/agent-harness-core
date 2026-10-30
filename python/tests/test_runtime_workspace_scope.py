"""Backend tools share only their turn's lock; recovery and foreign contexts cannot."""

import threading
from contextvars import copy_context

import pytest
from test_workspace_tools import setup

from ahc.application.runtime_service import RuntimeService
from ahc.domain.agent_backend import AgentTurnObservation, AgentTurnRequest
from ahc.infrastructure.runtime_repository import RuntimeRepository
from ahc.infrastructure.working_repository import WorkingWriterBusy


def test_tool_executes_inside_backend_but_recovery_and_competitors_stay_blocked(tmp_path):
    repo, workspace, tools = setup(tmp_path)
    snapshot = repo.read_current("session")
    request = AgentTurnRequest(
        snapshot.session,
        "turn",
        snapshot.goal.digest,
        "question",
        (),
        "a" * 64,
        "b" * 64,
        "work",
        100,
    )
    captured = []

    class Backend:
        def invoke(self, bound):
            captured.append(copy_context())
            result = tools.invoke(
                "workspace.execute", {"expected": workspace.identity.to_dict(), "code": "x=42"}
            )
            assert result["disposition"] == "observed"
            for store in (repo, RuntimeRepository(repo.root)):
                with pytest.raises(WorkingWriterBusy):
                    store.reconcile(bound.session)
                with pytest.raises(WorkingWriterBusy):
                    RuntimeService(store, self).execute(snapshot, bound)
            checkpoint = tools.invoke(
                "workspace.checkpoint", {"expected": workspace.identity.to_dict(), "names": ["x"]}
            )
            assert checkpoint["disposition"] == "observed"
            return AgentTurnObservation(bound, "completed", "fixture", 1, 1, "thread", "turn", "42")

    try:
        result = RuntimeService(repo, Backend()).execute(snapshot, request)
        assert result.result["accepted"] and workspace.port.calls == 1
        expired = captured[0].run(
            tools.invoke,
            "workspace.execute",
            {"expected": workspace.identity.to_dict(), "code": "x=99"},
        )
        assert expired["disposition"] == "rejected" and not expired["unresolved_effects"]
        assert workspace.port.calls == 1
        assert repo.reconcile(snapshot.session) == ()
    finally:
        workspace.close()


@pytest.mark.parametrize("scope", ["foreign", "work"])
def test_foreign_workspace_and_copied_thread_context_cannot_use_grant(tmp_path, scope):
    repo, workspace, tools = setup(tmp_path)
    snapshot = repo.read_current("session")
    request = AgentTurnRequest(
        snapshot.session,
        "turn",
        snapshot.goal.digest,
        "question",
        (),
        "a" * 64,
        "b" * 64,
        scope,
        100,
    )

    class Backend:
        def invoke(self, bound):
            args = {"expected": workspace.identity.to_dict(), "code": "x=42"}
            expected = "observed" if scope == "work" else "rejected"
            assert tools.invoke("workspace.execute", args)["disposition"] == expected
            args = {"expected": workspace.identity.to_dict(), "code": "x=42"}
            results = []
            context = copy_context()
            child = threading.Thread(
                target=lambda: results.append(context.run(tools.invoke, "workspace.execute", args))
            )
            child.start()
            child.join(2)
            assert not child.is_alive() and results[0]["disposition"] == "rejected"
            assert not results[0]["unresolved_effects"]
            assert workspace.port.calls == (1 if scope == "work" else 0)
            return AgentTurnObservation(
                bound, "completed", "fixture", 1, 1, "thread", "turn", "answer"
            )

    try:
        assert RuntimeService(repo, Backend()).execute(snapshot, request).result["accepted"]
    finally:
        workspace.close()
