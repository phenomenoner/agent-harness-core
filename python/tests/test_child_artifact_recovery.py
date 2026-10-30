import json

import pytest
from test_foreground_trusted import config_file
from test_workspace_tools import setup

from ahc.domain.agent_backend import AgentTurnObservation, AgentTurnRequest
from ahc.domain.child_result import ChildResult
from ahc.domain.child_work import ChildWorkEnvelope
from ahc.domain.result_artifacts import ResultArtifact
from ahc.domain.runtime_turn import accept_observation
from ahc.domain.sessions import ContractError
from ahc.infrastructure.artifact_repository import ArtifactRepository
from ahc.infrastructure.child_artifact_schema import prepare_child_artifact_schema
from ahc.infrastructure.child_artifacts import ChildArtifacts
from ahc.infrastructure.child_dispatch_repository import ChildDispatchRepository
from ahc.infrastructure.child_work_repository import ChildWorkRepository
from ahc.infrastructure.runtime_backup import _digest, backup_runtime_database
from ahc.infrastructure.runtime_repository import RuntimeRepository
from ahc.infrastructure.runtime_restore import prepare_runtime_restore
from ahc.infrastructure.runtime_restore_activation import activate_runtime_restore
from ahc.infrastructure.runtime_restore_transform import transform_runtime_restore
from ahc.infrastructure.working_repository import WorkingSetConflict


@pytest.mark.parametrize("enabled", [False, True])
def test_schema_prepared_only_for_enabled_foreground(tmp_path, monkeypatch, enabled):
    from ahc.cli import foreground_trusted
    from ahc.cli.foreground import compose_foreground

    original, _, raw, path = config_file(tmp_path, enabled=enabled)
    store = original.store
    store._mutate(lambda c: (c.execute("DROP TABLE runtime_child_artifacts"),
                            c.execute("DROP TABLE runtime_child_artifact_limits")))
    raw["child_limits"] = {"max_children": 1, "max_concurrency": 1,
                           "max_artifacts": 1, "artifact_bytes": 100}
    path.write_text(json.dumps(raw), encoding="utf-8")

    class StopProbe(Exception):
        pass

    def backend(*args):
        raise StopProbe()

    monkeypatch.setattr(foreground_trusted, "TrustedCodexBackendFactory", backend)
    if enabled:
        with pytest.raises(StopProbe):
            compose_foreground(path)
    else:
        compose_foreground(path)
    with store._read() as connection:
        present = connection.execute("SELECT 1 FROM sqlite_master "
            "WHERE name='runtime_child_artifacts'").fetchone() is not None
    assert present is enabled


@pytest.mark.parametrize("observations", [False, True])
def test_old_schema_preparation_and_populated_successor_restore(tmp_path, observations):
    store, workspace, _ = setup(tmp_path)
    # Disposable older-schema fixture; existing runtime data must remain untouched.
    store._mutate(lambda c: (c.execute("DROP TABLE runtime_child_artifacts"),
                            c.execute("DROP TABLE runtime_child_artifact_limits")))
    current = store.read_current("session")
    prepare_child_artifact_schema(store)
    prepare_child_artifact_schema(store)
    assert store.read_current("session") == current
    request = AgentTurnRequest(current.session, "parent", current.goal.digest, "work", (),
                               "a" * 64, "b" * 64, "work", 100)
    children = ChildDispatchRepository(store, request, clock=lambda: 100,
                                       monotonic_clock=lambda: 0)
    envelope = ChildWorkEnvelope("child", children.parent, "task", "work", 200)
    try:
        with store.execution_lock():
            store.begin(current, request)
            if observations:
                from ahc.domain.schedules import IntervalSchedule
                from ahc.infrastructure.mcp_receipts import McpReceipts
                from ahc.infrastructure.schedule_occurrences import ScheduleOccurrences
                from ahc.infrastructure.schedule_repository import ScheduleRepository

                schedule = IntervalSchedule(
                    "retained-schedule", request.session, request.goal_digest, 1000, 100, "later")
                ScheduleRepository(store).create(schedule)
                occurrence = ScheduleOccurrences(store).enqueue(schedule, 1000, 1000)
                McpReceipts(store, request).begin(
                    request.session, "unfinished-call", "fixture", "c" * 64, "echo", {})
            children.admit(envelope)
            ChildArtifacts(children).allocate(envelope.parent, "child", max_count=1, max_bytes=4)
            children.claim(envelope.parent, "child", "claim")
            if observations:
                from dataclasses import replace

                from ahc.domain.token_observation import TokenCounts, TokenObservation
                from ahc.infrastructure.child_token_observations import record as record_tokens

                counts = TokenCounts(3, 2, 1, 0, 5)
                record_tokens(children, envelope, "claim", AgentTurnObservation(
                    replace(request, operation_id="child"), "completed", "fixture", 1, 1,
                    "thread", "turn", token_observation=TokenObservation(
                        "thread", "turn", counts, counts)))
            manifest = ChildArtifacts(children).publish(envelope.parent, "child", "claim",
                workspace.identity, b"data", "text/plain")
            result = ChildResult("done", (ResultArtifact(manifest.artifact_id, "report.txt"),))
            children.complete(envelope.parent, "child", "claim", result)
            if observations:
                # Simulate loss of the reply after the artifact result committed.
                recovered = children.reconcile_completion(envelope, "claim", result)
                assert recovered["observations"] == ["completion-reply-lost"]
                cancelled = ChildWorkEnvelope(
                    "cancelled-child", children.parent, "unused", "work", 200)
                children.admit(cancelled)
                children.cancel(cancelled.parent, cancelled.child_id)
            terminal_result = accept_observation(request,
                AgentTurnObservation(request, "completed", "fixture", 1, 1, output_text="done"))
            store.seal_observation(request, terminal_result)
            store.finish(request, terminal_result)
    finally:
        workspace.close()
    current = store.read_current("session")
    source_digest = _digest(store.db_path)
    if observations:
        with store._read() as connection:
            retained_operations = {
                table: [tuple(row) for row in connection.execute("SELECT * FROM " + table)]
                for table in ("runtime_schedules", "runtime_schedule_occurrences",
                              "runtime_submissions", "runtime_mcp_receipts")}
    backup, prepared, candidate = [tmp_path / name for name in ("backup", "prepared", "candidate")]
    receipt = backup_runtime_database(store, backup)
    prepare_runtime_restore(backup, prepared)
    transformed = transform_runtime_restore(prepared, receipt["sha256"], (current,),
                                             source_digest, candidate)
    activate_runtime_restore(store, tmp_path / "supervisor.lock", candidate,
                             transformed["sha256"], receipt["sha256"], source_digest)
    fresh = RuntimeRepository(store.root)
    successor = fresh.read_current("session").session
    assert successor == current.session.successor()
    assert ChildWorkRepository(fresh).read(envelope.parent)[0]["result"] == result.to_dict()
    history = {row["child_id"]: row for row in ChildWorkRepository(fresh).read(envelope.parent)}
    if observations:
        with fresh._read() as connection:
            assert retained_operations == {
                table: [tuple(row) for row in connection.execute("SELECT * FROM " + table)]
                for table in retained_operations}
        assert occurrence["operation_id"]
        assert ScheduleRepository(fresh).read(request.session, schedule.schedule_id) == schedule
        with pytest.raises(ContractError):
            ScheduleOccurrences(fresh).enqueue(schedule, 1000, 1000)
        with pytest.raises(ContractError):
            McpReceipts(fresh, request).begin(
                request.session, "new-call", "fixture", "c" * 64, "echo", {})
        assert history["child"]["token_observation"]["last"]["total_tokens"] == 5
        assert history["child"]["token_observation"]["complete"] is False
        assert history["child"]["observations"] == ["completion-reply-lost"]
        assert history["cancelled-child"]["observations"] == ["cancellation-observed"]
        assert history["cancelled-child"]["state"] == "cancelled"
    else:
        assert "observations" not in history["child"]
    with pytest.raises(WorkingSetConflict):
        ChildDispatchRepository(fresh, request).admit(
            ChildWorkEnvelope("late-child", children.parent, "late", "work", 200))
    retained, content = ArtifactRepository(fresh).read(successor, manifest.artifact_id)
    assert retained == manifest and content == b"data"
    with fresh._read() as connection:
        assert tuple(connection.execute("SELECT max_count,max_bytes FROM "
            "runtime_child_artifact_limits WHERE child_id='child'").fetchone()) == (1, 4)
        assert connection.execute("SELECT artifact_id FROM runtime_child_artifacts "
            "WHERE child_id='child'").fetchone()[0] == manifest.artifact_id
