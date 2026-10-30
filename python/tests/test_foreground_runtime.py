import time
from dataclasses import replace

import pytest
from skill_test_support import goal
from test_workspace_tools import Port

from ahc import AppConfig, Supervisor
from ahc.application.foreground_runtime import ForegroundRuntime
from ahc.application.runtime_service import RuntimeService
from ahc.domain.agent_backend import AgentTurnObservation, AgentTurnRequest
from ahc.domain.sessions import ContractError, VirtualSessionIdentity
from ahc.domain.working_sets import WorkingSetSnapshot
from ahc.infrastructure.backend_custody import BackendCustody, LinuxUnitIdentity, UnitObservation
from ahc.infrastructure.persistence import StateStore
from ahc.infrastructure.runtime_repository import RuntimeRepository


def setup(tmp_path, *, cleanup_fails=False):
    repo = RuntimeRepository.initialize(tmp_path / "runtime")
    session = VirtualSessionIdentity.initial("session", "agent", "route")
    current = repo.create(WorkingSetSnapshot.initial(session, goal()))
    requests = []
    invoked = []

    def open_workspace(snapshot):
        workspace = RuntimeService(repo, None).open_workspace(snapshot, "work", Port)
        if cleanup_fails:

            def fail():
                raise RuntimeError("PRIVATE")

            workspace.port.close = fail
        return workspace

    def factory(request, tools, *, cancelled):
        invoked.append(request.operation_id)

        class Backend:
            def invoke(self, request):
                result = tools.invoke(
                    "workspace.execute",
                    {"expected": tools.workspace.identity.to_dict(), "code": "x=42"},
                )
                assert result["disposition"] == "observed"
                return AgentTurnObservation(
                    request, "completed", "fixture", 1, 1, "thread", "turn", "done"
                )

        return Backend()

    runtime = ForegroundRuntime(
        repo,
        session,
        lambda *_: requests.pop(0) if requests else None,
        factory,
        open_workspace=open_workspace,
    )
    request = AgentTurnRequest(
        session,
        "op",
        current.goal.digest,
        "input",
        (),
        "a" * 64,
        "b" * 64,
        "work",
        time.monotonic() + 30,
        capabilities=("workspace.execute", "workspace.inspect", "workspace.checkpoint",
                      "workspace.environment"),
    )
    return runtime, requests, invoked, request


def test_two_turns_and_duplicate_use_one_workspace(tmp_path):
    runtime, requests, invoked, request = setup(tmp_path)
    runtime.start()
    try:
        requests.extend([request, request, replace(request, operation_id="op2")])
        first = runtime.step()
        assert runtime.step() == first
        assert runtime.step().result["accepted"]
        assert invoked == ["op", "op2"]
        assert runtime.workspace.identity.revision == 2
    finally:
        runtime.close()
    assert runtime.workspace.closed and runtime.state == "closed"
    with pytest.raises(ContractError):
        runtime.step()


def test_foreground_passes_each_admitted_deadline_and_releases_scope(tmp_path):
    from contextlib import contextmanager

    runtime, requests, invoked, request = setup(tmp_path)
    runtime.start()
    seen, active = [], []

    @contextmanager
    def turn_scope(deadline):
        active.append(deadline)
        seen.append(deadline)
        try:
            yield
        finally:
            active.pop()

    runtime.workspace.port.turn_scope = turn_scope
    execute = runtime.workspace.port.execute

    def checked_execute(expected, code):
        assert active == [seen[-1]]
        return execute(expected, code)

    runtime.workspace.port.execute = checked_execute
    try:
        following = replace(request, operation_id="following", deadline=request.deadline+60)
        requests.extend([request, following])
        assert runtime.step().result["accepted"]
        assert not active
        assert runtime.step().result["accepted"]
        assert seen == [request.deadline, following.deadline] and not active
    finally:
        runtime.close()


def test_unknown_workspace_cell_is_not_replayed_and_next_distinct_task_recovers(tmp_path):
    runtime, requests, invoked, request = setup(tmp_path)
    runtime.start()
    generations = [runtime.workspace.identity.generation]
    first_execute = runtime.workspace.port.execute

    def lost_result(expected, code):
        first_execute(expected, code)  # An effect occurred before losing its result.
        raise OSError("private payload must never be returned")

    runtime.workspace.port.execute = lost_result

    def factory(bound, tools, *, cancelled):
        invoked.append(bound.operation_id)
        class Backend:
            def invoke(self, bound):
                result=tools.invoke("workspace.execute",{"expected":tools.workspace.identity.to_dict(),"code":"x=42"})
                generations.append(tools.workspace.identity.generation)
                if result["unresolved_effects"]:
                    return AgentTurnObservation(bound,"unknown","tool",1,1,"thread","turn",unresolved_effects=True)
                return AgentTurnObservation(bound,"completed","fixture",1,1,"thread","turn","done")
        return Backend()

    runtime.backend_factory = factory
    try:
        requests.extend([request, request, replace(request,operation_id="following")])
        unknown=runtime.step()
        assert unknown.result["disposition"]=="unknown" and runtime.workspace.closed
        assert runtime.step()==unknown  # Duplicate submission is read back, never executed.
        assert runtime.step().result["accepted"]
        assert invoked==["op","following"] and generations[-1]>generations[0]
        assert runtime.store.read_turn(request.session,"op")==unknown
    finally:
        runtime.close()


def test_foreign_request_never_constructs_backend(tmp_path):
    runtime, requests, invoked, request = setup(tmp_path)
    runtime.start()
    try:
        requests.append(replace(request, workspace_scope="foreign"))
        with pytest.raises(ContractError):
            runtime.step()
        assert invoked == []
    finally:
        runtime.close()


@pytest.mark.parametrize("failed", [False, True])
def test_supervisor_closes_workspace_before_terminal_state(tmp_path, failed):
    runtime, _, _, _ = setup(tmp_path, cleanup_fails=failed)
    config = AppConfig(state_dir=tmp_path / "supervisor")
    supervisor = Supervisor(config, runtime=runtime)
    published = []
    original = supervisor._publish

    def publish(status):
        if status in ("stopped", "degraded"):
            published.append((status, runtime.state, runtime.workspace.closed))
        original(status)

    supervisor._publish = publish
    result = supervisor.run(max_runtime_seconds=0)
    assert result == (4 if failed else 0)
    assert published[-1] == (
        ("degraded", "stopping", False) if failed else ("stopped", "closed", True)
    )
    assert "PRIVATE" not in str(StateStore(config.state_dir).read())


def test_supervisor_step_failure_closes_workspace(tmp_path):
    runtime, requests, invoked, request = setup(tmp_path)
    requests.append(replace(request, workspace_scope="foreign"))
    config = AppConfig(state_dir=tmp_path / "supervisor", poll_interval_seconds=0.001)
    assert Supervisor(config, runtime=runtime).run(max_runtime_seconds=1) == 4
    assert runtime.state == "closed" and runtime.workspace.closed and not invoked


def test_runtime_start_failure_does_not_publish_private_error(tmp_path):
    runtime, _, _, _ = setup(tmp_path)

    def fail(_snapshot):
        raise RuntimeError("PRIVATE-START-SENTINEL")

    runtime.open_workspace = fail
    config = AppConfig(state_dir=tmp_path / "supervisor")
    assert Supervisor(config, runtime=runtime).run(max_runtime_seconds=0) == 4
    assert runtime.state == "closed"
    assert "PRIVATE-START-SENTINEL" not in str(StateStore(config.state_dir).read())


def test_stop_arriving_in_ingress_prevents_new_backend_admission(tmp_path):
    from ahc.application.operator import request_stop

    runtime, _, invoked, request = setup(tmp_path)
    config = AppConfig(state_dir=tmp_path / "supervisor", poll_interval_seconds=0.001)

    def ingress(*_):
        assert request_stop(config, 0)[0] == 0
        return request

    runtime.ingress = ingress
    assert Supervisor(config, runtime=runtime).run(max_runtime_seconds=1) == 0
    assert invoked == []
    assert runtime.workspace.identity.revision == 0
    assert runtime.workspace.closed


@pytest.mark.parametrize("cleanup", [True, False])
def test_cancel_during_backend_construction_requires_cleanup(tmp_path, cleanup):
    runtime, requests, invoked, request = setup(tmp_path)
    stopped = [False]
    closed = []

    def factory(_request, _tools, *, cancelled):
        assert not cancelled()
        stopped[0] = True

        class Backend:
            def invoke(self, _request):
                invoked.append("unexpected")

        backend = Backend()
        if cleanup:
            backend.close = lambda: closed.append(True)
        return backend

    runtime.backend_factory = factory
    runtime.start()
    try:
        requests.append(request)
        result = runtime.step(cancelled=lambda: stopped[0])
        assert invoked == []
        assert result.result["disposition"] == ("not-submitted" if cleanup else "unknown")
        assert result.result["unresolved_effects"] is (not cleanup)
        assert closed == ([True] if cleanup else [])
    finally:
        runtime.close()


def test_expired_request_never_constructs_backend(tmp_path):
    runtime, requests, invoked, request = setup(tmp_path)
    runtime.start()
    try:
        requests.append(replace(request, deadline=1))
        result = runtime.step()
        assert result.result["disposition"] == "not-submitted"
        assert invoked == []
    finally:
        runtime.close()


@pytest.mark.parametrize("invoke_fails", [False, True])
def test_backend_closes_after_durable_terminal(tmp_path, invoke_fails):
    runtime, requests, _, request = setup(tmp_path)
    closed = []

    class Backend:
        def invoke(self, bound):
            if invoke_fails:
                raise RuntimeError("private invoke failure")
            return AgentTurnObservation(
                bound, "completed", "fixture", 1, 1, "thread", "turn", "done"
            )

        def close(self):
            assert (
                runtime.store.read_turn(request.session, request.operation_id).status == "terminal"
            )
            closed.append(True)

    runtime.backend_factory = lambda *a, **kw: Backend()
    runtime.start()
    try:
        requests.append(request)
        result = runtime.step()
        assert closed == [True]
        assert result.result["accepted"] is (not invoke_fails)
    finally:
        runtime.close()
    assert closed == [True]


def test_failed_backend_cleanup_fences_next_turn_and_retries_before_workspace_close(tmp_path):
    runtime, requests, _, request = setup(tmp_path)
    calls = []

    class Backend:
        def invoke(self, bound):
            return AgentTurnObservation(
                bound, "completed", "fixture", 1, 1, "thread", "turn", "done"
            )

        def close(self):
            calls.append("close")
            assert not runtime.workspace.closed
            if len(calls) == 1:
                raise RuntimeError("private cleanup failure")

    runtime.backend_factory = lambda *a, **kw: Backend()
    runtime.start()
    requests.append(request)
    result = runtime.step()
    assert result.result["disposition"] == "completed" and not result.result["unresolved_effects"]
    assert result.result["accepted"] and runtime.cleanup_status()["state"] == "pending"
    seal = runtime.store.read_answer_seal(request.session, request.operation_id)
    assert seal["output_text"] == "done" and "cleanup_trace" not in seal
    assert "private cleanup failure" not in str(runtime.cleanup_status()["failure_trace"])
    assert runtime.step() is None  # Exact cleanup succeeds without another backend admission.
    runtime.close()
    assert calls == ["close", "close"] and runtime.workspace.closed


def test_custodied_backend_cleanup_reuses_the_active_workspace_turn_grant(tmp_path):
    runtime, requests, _, request = setup(tmp_path)
    custody = BackendCustody(runtime.store)
    unit = LinuxUnitIdentity(
        "AHC-Codex-Ubuntu-24.04", "12345678-1234-1234-1234-123456789abc",
        "ahc-backend-" + "c" * 32 + ".service", "d" * 32,
    )
    lease = [None]

    class Runner:
        stopped = False

        def observe(self, observed):
            assert observed == unit
            return UnitObservation(
                unit.distribution, unit.boot_id, unit.unit,
                unit.invocation_id if not self.stopped else unit.invocation_id,
                "stopped" if self.stopped else "running", self.stopped,
            )

        def stop(self, observed):
            assert observed == unit
            self.stopped = True

    runner = Runner()

    class Backend:
        def invoke(self, bound):
            lease[0] = custody.acquire(bound.session, "work")
            custody.record_created(lease[0], runtime.workspace.identity, unit)
            return AgentTurnObservation(
                bound, "completed", "fixture", 1, 1, "thread", "turn", "done"
            )

        def close(self):
            assert custody.recover(lease[0], runner) == (unit,)
            lease[0].close()
            lease[0] = None

    runtime.backend_factory = lambda *args, **kwargs: Backend()
    runtime.start()
    try:
        requests.append(request)
        assert runtime.step().result["accepted"]
        assert runner.stopped and lease[0] is None
    finally:
        if lease[0] is not None:
            lease[0].close()
        runtime.close()


def test_cleanup_recovery_is_bounded_and_fences_workspace_effects(tmp_path):
    runtime, requests, _, request = setup(tmp_path)
    events, attempts = [], []

    class Backend:
        def invoke(self, bound):
            return AgentTurnObservation(
                bound, "completed", "fixture", 1, 1, "thread", "turn", "done"
            )

        def close(self):
            events.append("close")
            attempts.append(True)
            if len(attempts) <= 2:
                raise RuntimeError("private cleanup failure")

    runtime.backend_factory = lambda *args, **kwargs: Backend()
    runtime.channel_delivery = lambda **_: events.append("delivery")
    runtime.channel_ingress = lambda **_: events.append("ingress")
    runtime.channel_files = lambda *_args, **_kwargs: events.append("files")
    runtime.schedule = lambda *_args, **_kwargs: events.append("schedule")
    runtime.start()
    try:
        requests.append(request)
        assert runtime.step().result["accepted"]
        assert runtime.cleanup_status()["state"] == "pending"
        # Scheduling only admits on idle cycles, never alongside this selected request.
        assert events == ["delivery", "ingress", "files", "delivery", "close"]
        events.clear()
        assert runtime.step() is None
        assert events == ["delivery", "close"]
        assert runtime.cleanup_status()["state"] == "exhausted"
        events.clear()
        assert runtime.step() is None
        assert events == ["delivery"]
        assert {key: value for key, value in runtime.cleanup_status().items()
                if key != "failure_trace"} == {
            "state": "exhausted", "automatic_attempts": 2,
            "automatic_attempt_limit": 2, "observation_scope": "current-runtime",
        }
    finally:
        runtime.close()
