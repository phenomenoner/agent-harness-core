from dataclasses import replace

import pytest
from test_workspace_tools import setup

from ahc.domain.agent_backend import AgentTurnRequest
from ahc.domain.artifacts import ArtifactNotPublished
from ahc.domain.child_result import ChildResult
from ahc.domain.child_work import ChildWorkEnvelope
from ahc.domain.result_artifacts import ResultArtifact
from ahc.domain.sessions import ContractError
from ahc.infrastructure.child_artifacts import ChildArtifacts
from ahc.infrastructure.child_dispatch_repository import ChildDispatchRepository
from ahc.infrastructure.child_work_repository import ChildWorkRepository


def test_parent_monotonic_expiry_rejects_publication_even_with_wall_time_remaining(tmp_path):
    store, workspace, _ = setup(tmp_path)
    current = store.read_current("session")
    request = AgentTurnRequest(current.session, "parent", current.goal.digest, "work", (),
                               "a" * 64, "b" * 64, "work", 100)
    now = [0]
    children = ChildDispatchRepository(store, request, clock=lambda: 100,
                                       monotonic_clock=lambda: now[0])
    envelope = ChildWorkEnvelope("child", children.parent, "task", "work", 200)
    try:
        with store.execution_lock():
            store.begin(current, request)
            children.admit(envelope)
            artifacts = ChildArtifacts(children)
            artifacts.allocate(envelope.parent, "child", max_count=1, max_bytes=4)
            children.claim(envelope.parent, "child", "claim")
            now[0] = request.deadline
            with pytest.raises(ArtifactNotPublished) as rejected:
                artifacts.publish(envelope.parent, "child", "claim", workspace.identity,
                                  b"data", "text/plain")
            assert "owner mismatch" in str(rejected.value.__cause__)
            with store._read() as connection:
                for table in ("runtime_artifacts", "runtime_child_artifacts"):
                    assert connection.execute(f"SELECT count(*) FROM {table}").fetchone()[0] == 0
    finally:
        workspace.close()


def test_atomic_child_publication_budget_and_terminal_ownership(tmp_path):
    store, workspace, _ = setup(tmp_path)
    current = store.read_current("session")
    request = AgentTurnRequest(current.session, "parent", current.goal.digest, "work", (),
                               "a" * 64, "b" * 64, "work", 100)
    children = ChildDispatchRepository(store, request, clock=lambda: 100,
                                       monotonic_clock=lambda: 0)
    first = ChildWorkEnvelope("first", children.parent, "task", "work", 200)
    second = replace(first, child_id="second")
    artifacts = ChildArtifacts(children)
    try:
        with store.execution_lock():
            store.begin(current, request)
            for item in (first, second):
                children.admit(item)
                artifacts.allocate(item.parent, item.child_id, max_count=1, max_bytes=4)
                children.claim(item.parent, item.child_id, "claim")
            manifest = artifacts.publish(first.parent, first.child_id, "claim",
                                          workspace.identity, b"data", "text/plain")
            assert artifacts.publish(first.parent, first.child_id, "claim",
                workspace.identity, b"data", "text/plain") == manifest
            with pytest.raises(ArtifactNotPublished) as rejected:
                artifacts.publish(first.parent, first.child_id, "claim",
                                  workspace.identity, b"extra", "text/plain")
            assert "exhausted" in str(rejected.value.__cause__)
            with store._read() as connection:
                for table in ("runtime_artifacts", "runtime_child_artifacts"):
                    assert connection.execute(f"SELECT count(*) FROM {table}").fetchone()[0] == 1
            result = ChildResult("done", (ResultArtifact(manifest.artifact_id, "report.txt"),))
            with pytest.raises(ContractError, match="not owned"):
                children.complete(second.parent, second.child_id, "claim", result)
            terminal = children.complete(first.parent, first.child_id, "claim", result)
            assert terminal["result"] == result.to_dict()
            assert children.complete(first.parent, first.child_id, "claim", result) == terminal
            with pytest.raises(ContractError):
                artifacts.publish(first.parent, first.child_id, "claim",
                                  workspace.identity, b"data", "text/plain")
        assert ChildWorkRepository(store).read(first.parent)[0]["result"] == result.to_dict()
    finally:
        workspace.close()


@pytest.mark.parametrize("budget", [20001, 9_000_000])
def test_child_text_above_old_limit_and_known_budget_rejection_preserve_useful_selection(tmp_path, budget):
    from ahc.infrastructure.child_artifact_tools import ChildArtifactTools

    store, workspace, _ = setup(tmp_path)
    current = store.read_current("session")
    request = AgentTurnRequest(current.session, "parent", current.goal.digest, "work", (),
                               "a"*64, "b"*64, "work", 100)
    children = ChildDispatchRepository(store, request, clock=lambda: 100, monotonic_clock=lambda: 0)
    envelope = ChildWorkEnvelope("child", children.parent, "task", "work", 200)
    try:
        with store.execution_lock():
            store.begin(current, request)
            children.admit(envelope)
            ChildArtifacts(children).allocate(envelope.parent, "child", max_count=9, max_bytes=budget)
            children.claim(envelope.parent, "child", "claim")
            tools = ChildArtifactTools(children, envelope, "claim", workspace.identity)
            def publish(text):
                return tools.invoke("workspace.publish_text", {"expected": workspace.identity.to_dict(),
                    "text": text, "media_type": "text/plain"})
            first = publish("x"*20000)
            assert first["disposition"] == "observed"
            second = publish("y"*20000)
            assert second["disposition"] == ("rejected" if budget == 20001 else "observed")
            assert second["unresolved_effects"] is False
            selected = tools.invoke("workspace.attach", {"expected": workspace.identity.to_dict(),
                "artifact_id": first["result"]["artifact_id"], "filename": "report.txt"})
            assert selected["disposition"] == "observed"
            missing = tools.invoke("workspace.attach", {"expected": workspace.identity.to_dict(),
                "artifact_id": "0"*64, "filename": "missing.txt"})
            assert missing["disposition"] == "rejected" and not missing["unresolved_effects"]
    finally:
        workspace.close()
