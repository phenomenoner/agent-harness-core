"""The job route yields, reconnects and retains unknown work without replay."""

import json
import threading
import time
from contextlib import contextmanager
from dataclasses import replace
from types import SimpleNamespace

import pytest
from test_runtime_core import setup

from ahc.application.runtime_workspace import RuntimeWorkspace
from ahc.application.workspace_jobs import WorkspaceJobs
from ahc.application.workspace_tools import WorkspaceTools
from ahc.domain.sessions import ContractError
from ahc.domain.workspace_job_lease import WorkspaceJobLease


class Port:
    def __init__(self, identity, checkpoint=None):
        self.identity = identity
        self.calls = 0
        self.closed = False
        self.entered = threading.Event()
        self.release = threading.Event()
        self._turn_deadline = None
        self._runner = SimpleNamespace(owner_lifetime=True)

    def validate_execute(self, code):
        if type(code) is not str:
            raise ContractError("code invalid")

    @contextmanager
    def turn_scope(self, deadline):
        self._turn_deadline = deadline
        try:
            yield
        finally:
            self._turn_deadline = None

    def execute_job(self, expected, code, lease):
        assert expected == self.identity
        self.identity = replace(expected, revision=expected.revision+1)
        self.calls += 1
        self.entered.set()
        while not self.release.wait(0.005):
            if lease.remaining(time.monotonic()) <= 0:
                raise TimeoutError()
        if self.closed or code == "loss":
            raise OSError("transport unavailable")
        return {"identity": self.identity.to_dict(), "ok": True, "output": "once",
                "output_truncated": False}

    def execute(self, expected, code):
        raise AssertionError("competing execution must not reach the interpreter")

    def cancel_job(self, lease):
        lease.request_cancel()
        self.close()
        lease.stopped.set()

    def close(self):
        self.closed = True
        self.release.set()


@pytest.fixture
def owned(tmp_path):
    store, snapshot, _ = setup(tmp_path)
    workspace = RuntimeWorkspace.open(store, snapshot, "work", Port)
    try:
        with workspace.turn_scope(time.monotonic()+60):
            yield store, workspace, WorkspaceTools(workspace)
    finally:
        workspace.port.release.set()
        workspace.jobs.finish_turn()
        workspace.close()


def call(tools, action, job_id="work1", **fields):
    return tools.invoke("workspace.job", {
        "expected": tools.workspace.identity.to_dict(),
        "action": action, "job_id": job_id, **fields})


def start(tools, **fields):
    return call(tools, "start", code="count-once", expected_seconds=10,
                wait_seconds=0, **fields)


def test_yield_reattach_and_duplicate_never_execute_twice(owned):
    store, workspace, tools = owned
    first = start(tools)
    assert first["result"]["status"] == "running"
    assert workspace.port.entered.wait(1)
    assert start(tools)["result"]["job_id"] == "work1"
    assert workspace.port.calls == 1
    assert call(tools, "status")["result"]["status"] == "running"
    workspace.port.release.set()
    result = call(tools, "wait", wait_seconds=1)
    assert result["result"]["status"] == "complete"
    assert result["result"]["execution"]["output"] == "once"
    # Fresh host handle reads the stored result; it dispatches no worker.
    workspace.jobs = WorkspaceJobs(workspace)
    assert call(tools, "status")["result"]["status"] == "complete"
    assert workspace.port.calls == 1
    assert store.observe_workspace_step(workspace.identity) == "ready"


def test_busy_namespace_and_conflicting_job_id_reject_before_execution(owned):
    _, workspace, tools = owned
    start(tools)
    assert workspace.port.entered.wait(1)
    other = call(tools, "start", code="different", expected_seconds=10)
    assert other["disposition"] == "rejected"
    with pytest.raises(Exception):
        workspace.execute(workspace.identity, "must-not-run")
    assert workspace.port.calls == 1


@pytest.mark.parametrize("operation", ["inspect", "checkpoint"])
def test_busy_namespace_read_rejects_without_inventing_unknown_effect(owned, operation):
    store, workspace, tools = owned
    start(tools)
    assert workspace.port.entered.wait(1)
    result = tools.invoke("workspace." + operation, {
        "expected": workspace.identity.to_dict(), "names": ["progress_line"]})
    assert result["disposition"] == "rejected"
    assert result["unresolved_effects"] is False
    assert not workspace.unavailable and not workspace.closed
    assert call(tools, "status")["result"]["status"] == "running"
    workspace.port.release.set()
    assert call(tools, "wait", wait_seconds=1)["result"]["status"] == "complete"
    assert store.observe_workspace_step(workspace.identity) == "ready"
    assert workspace.port.calls == 1


def test_inspection_failure_after_port_admission_stays_unknown(owned, monkeypatch):
    _, workspace, tools = owned
    def lost_reply(*args):
        raise OSError("inspection reply lost")
    monkeypatch.setattr(workspace.port, "inspect", lost_reply, raising=False)
    result = tools.invoke("workspace.inspect", {
        "expected": workspace.identity.to_dict(), "names": ["progress_line"]})
    assert result["disposition"] == "unknown"
    assert result["unresolved_effects"] is True
    assert workspace.unavailable and workspace.closed


def test_cancel_reports_stop_and_unknown_effects_and_keeps_status(owned):
    _, workspace, tools = owned
    start(tools)
    assert workspace.port.entered.wait(1)
    result = call(tools, "cancel")["result"]
    assert result["status"] == "cancelled"
    assert result["cancellation"]["cancel_requested"]
    assert result["cancellation"]["execution_stopped"]
    assert result["cancellation"]["effects_unknown"]
    assert call(tools, "status")["result"]["status"] == "cancelled"
    assert workspace.port.closed


def test_new_generation_cancellation_never_closes_replacement(owned):
    _, workspace, tools = owned
    start(tools)
    assert workspace.port.entered.wait(1)
    original = workspace.port
    replacement = Port(replace(original.identity, generation=original.identity.generation+1))
    workspace.port = replacement
    result = call(tools, "cancel")
    assert result["disposition"] == "rejected"
    assert not original.closed and not replacement.closed
    workspace.port = original


def test_lost_host_worker_becomes_unknown_and_never_replays(owned):
    _, workspace, tools = owned
    start(tools)
    assert workspace.port.entered.wait(1)
    old_jobs = workspace.jobs
    workspace.jobs = WorkspaceJobs(workspace)
    result = call(tools, "status")
    assert result["disposition"] == "unknown"
    assert result["result"]["status"] == "unknown"
    assert workspace.port.calls == 1
    # A late receipt cannot erase the durable unknown state.
    workspace.port.release.set()
    assert old_jobs._workers["work1"]["done"].wait(1)
    assert call(tools, "status")["result"]["status"] == "unknown"
    workspace.jobs = old_jobs


def test_expiry_is_unknown_and_no_second_execution(owned):
    _, workspace, tools = owned
    result = call(tools, "start", code="count-once", expected_seconds=0.05, wait_seconds=1)
    assert result["result"]["status"] == "unknown"
    assert workspace.port.calls == 1
    assert call(tools, "status")["disposition"] == "unknown"


@pytest.mark.parametrize("seconds", [0, -1, True, float("inf"), float("nan"), 1000])
def test_invalid_estimate_has_no_effect(owned, seconds):
    _, workspace, tools = owned
    result = call(tools, "start", code="count-once", expected_seconds=seconds)
    assert result["disposition"] == "rejected"
    assert workspace.port.calls == 0


def test_wait_is_bounded(owned):
    _, workspace, tools = owned
    assert call(tools, "wait", wait_seconds=31)["disposition"] == "rejected"
    assert workspace.port.calls == 0


def test_silent_work_renews_only_within_original_estimate_and_hard_cap():
    lease = WorkspaceJobLease(0, 600, 400, slice_seconds=120)
    assert lease.remaining(119) == 1
    assert lease.remaining(120) == 120
    assert lease.to_dict(120)["renewals"] == 1
    assert lease.remaining(399) == 1
    assert lease.remaining(400) == 0
    assert lease.remaining(601) == 0


def test_budget_read_uses_task_deadline_and_does_not_enter_busy_kernel(owned):
    _, workspace, tools = owned
    start(tools)
    result = tools.invoke("workspace.budget", {"expected": workspace.identity.to_dict()})
    assert result["disposition"] == "observed"
    assert result["result"]["deadline"]["source"] == "admitted-task"
    assert result["result"]["deadline"]["direct_timeout_applies_to_admitted_cell"] is False
    assert workspace.port.calls <= 1
