"""Controlled local child dispatch over a durable parent turn; no provider use."""

import pytest
from test_runtime_core import setup as runtime_setup

from ahc.application.child_dispatch import ChildDispatchService
from ahc.domain.child_work import ChildWorkEnvelope
from ahc.domain.sessions import ContractError
from ahc.infrastructure.child_dispatch_repository import ChildDispatchRepository
from ahc.infrastructure.runtime_repository import RuntimeRepository
from ahc.infrastructure.working_repository import WorkingWriterBusy


def state(tmp_path, *, deadline=200):
    repo, snapshot, request = runtime_setup(tmp_path)
    repo.begin(snapshot, request)
    clock = [100]
    children = ChildDispatchRepository(
        repo, request, clock=lambda: clock[0], monotonic_clock=lambda: 0
    )
    envelope = ChildWorkEnvelope(
        "child", children.parent, "work", request.workspace_scope, deadline
    )
    return repo, request, clock, children, envelope


def test_dispatch_runs_once_with_cooperative_callback_and_closes(tmp_path):
    _, _, _, children, envelope = state(tmp_path)

    class Port:
        closed = 0

        def run(self, received, *, cancelled):
            assert received == envelope and cancelled() is False
            return "answer"

        def close(self):
            self.closed += 1

    port = Port()
    result = ChildDispatchService(children, lambda _: port).dispatch(envelope, "token")
    assert result["state"] == "terminal" and result["result"] == {"output_text": "answer"}
    assert port.closed == 1


def test_cancel_or_expiry_before_claim_never_creates_a_port(tmp_path):
    _, _, clock, children, envelope = state(tmp_path, deadline=101)
    calls = []
    service = ChildDispatchService(children, lambda _: calls.append(True))
    assert service.dispatch(envelope, "token", cancelled=lambda: True)["state"] == "cancelled"
    assert calls == []

    _, _, clock, children, envelope = state(tmp_path / "expired", deadline=101)
    children.admit(envelope)
    clock[0] = 101
    assert ChildDispatchService(children, lambda _: calls.append(True)).dispatch(envelope, "token")[
        "state"
    ] == "cancelled"
    assert calls == []


@pytest.mark.parametrize(
    "field,value", (("artifact_scope", "other"), ("deadline_unix_ms", 100_101))
)
def test_parent_artifact_and_deadline_bounds_reject_before_factory(tmp_path, field, value):
    _, _, _, children, envelope = state(tmp_path)
    values = {
        "artifact_scope": envelope.artifact_scope,
        "deadline_unix_ms": envelope.deadline_unix_ms,
    }
    values[field] = value
    bounded = ChildWorkEnvelope(
        envelope.child_id,
        envelope.parent,
        envelope.task_text,
        values["artifact_scope"],
        values["deadline_unix_ms"],
    )
    calls = []
    with pytest.raises(ContractError):
        ChildDispatchService(children, lambda _: calls.append(True)).dispatch(bounded, "token")
    assert calls == []


@pytest.mark.parametrize("failure", ("factory", "run", "close"))
def test_factory_run_or_close_failure_becomes_unknown_and_closes_once(tmp_path, failure):
    _, _, _, children, envelope = state(tmp_path)
    closed = []

    class Port:
        def run(self, *_ , **__):
            if failure == "run":
                raise RuntimeError("private failure")
            return "answer"

        def close(self):
            closed.append(True)
            if failure == "close":
                raise RuntimeError("private close failure")

    def factory(_):
        if failure == "factory":
            raise RuntimeError("private factory failure")
        return Port()

    result = ChildDispatchService(children, factory).dispatch(envelope, "token")
    assert result["state"] == "unknown"
    assert len(closed) == (0 if failure == "factory" else 1)


def test_cancelled_during_run_marks_unknown_and_closes_once(tmp_path):
    _, _, _, children, envelope = state(tmp_path)
    closed = []

    class Port:
        def run(self, _, *, cancelled):
            assert cancelled() is False
            trigger[0] = True
            assert cancelled() is True
            return "ignored"

        def close(self):
            closed.append(True)

    trigger = [False]
    result = ChildDispatchService(children, lambda _: Port()).dispatch(
        envelope, "token", cancelled=lambda: trigger[0]
    )
    assert result["state"] == "unknown" and closed == [True]


def test_stop_callback_failure_after_factory_closes_and_marks_unknown(tmp_path):
    _, _, _, children, envelope = state(tmp_path)
    closed, calls = [], [0]

    class Port:
        def run(self, *_ , **__):
            raise AssertionError("run must not start after stop callback failure")

        def close(self):
            closed.append(True)

    def cancelled():
        calls[0] += 1
        if calls[0] == 3:
            raise RuntimeError("private callback failure")
        return False

    result = ChildDispatchService(children, lambda _: Port()).dispatch(
        envelope, "token", cancelled=cancelled
    )
    assert result["state"] == "unknown" and closed == [True]


def test_close_failure_retains_port_and_fences_until_explicit_retry(tmp_path):
    repo, request, _, children, envelope = state(tmp_path)
    closes, factories = [], []

    class Port:
        def run(self, *_ , **__):
            return "answer"

        def close(self):
            closes.append(True)
            if len(closes) == 1:
                raise RuntimeError("private close failure")
            other = ChildDispatchRepository(
                RuntimeRepository(repo.root), request, clock=lambda: 100, monotonic_clock=lambda: 0
            )
            with pytest.raises(WorkingWriterBusy):
                other.reconcile(envelope.parent)

    service = ChildDispatchService(children, lambda _: factories.append(True) or Port())
    assert service.dispatch(envelope, "token")["state"] == "unknown"
    with pytest.raises(ContractError, match="close remains pending"):
        service.dispatch(envelope, "other")
    assert factories == [True]
    service.close()
    assert closes == [True, True]


def test_close_can_advance_deadline_before_completion(tmp_path):
    _, _, clock, children, envelope = state(tmp_path, deadline=101)

    class Port:
        def run(self, *_ , **__):
            return "answer"

        def close(self):
            clock[0] = 101

    assert ChildDispatchService(children, lambda _: Port()).dispatch(envelope, "token")[
        "state"
    ] == "unknown"


def test_keyboard_interrupt_closes_port_and_preserves_claimed_child(tmp_path):
    _, _, _, children, envelope = state(tmp_path)
    closed = []

    class Port:
        def run(self, *_ , **__):
            raise KeyboardInterrupt()

        def close(self):
            closed.append(True)

    with pytest.raises(KeyboardInterrupt):
        ChildDispatchService(children, lambda _: Port()).dispatch(envelope, "token")
    assert closed == [True] and children.read(envelope.parent)[0]["state"] == "claimed"


def test_missing_run_port_still_closes_before_unknown(tmp_path):
    _, _, _, children, envelope = state(tmp_path)
    closed = []

    class CloseOnly:
        def close(self):
            closed.append(True)

    assert ChildDispatchService(children, lambda _: CloseOnly()).dispatch(envelope, "token")[
        "state"
    ] == "unknown"
    assert closed == [True]


def test_terminal_reopen_does_not_replay_and_claimed_repeat_rejects(tmp_path):
    repo, request, _, children, envelope = state(tmp_path)
    calls = []

    class Port:
        def run(self, *_ , **__):
            calls.append(True)
            return "answer"

        def close(self):
            pass

    service = ChildDispatchService(children, lambda _: Port())
    assert service.dispatch(envelope, "token")["state"] == "terminal"
    reopened = ChildDispatchRepository(
        RuntimeRepository(repo.root), request, clock=lambda: 100, monotonic_clock=lambda: 0
    )
    assert (
        ChildDispatchService(reopened, lambda _: Port()).dispatch(envelope, "other")["state"]
        == "terminal"
    )
    assert calls == [True]

    repo2, _, _, children2, envelope2 = state(tmp_path / "claimed")
    children2.admit(envelope2)
    children2.claim(envelope2.parent, envelope2.child_id, "claim")
    with pytest.raises(ContractError, match="claimed"):
        ChildDispatchService(children2, lambda _: Port()).dispatch(envelope2, "claim")


def test_live_dispatch_scope_excludes_recovery_until_port_returns(tmp_path):
    repo, request, _, children, envelope = state(tmp_path)

    class Port:
        def run(self, *_ , **__):
            other = ChildDispatchRepository(
                RuntimeRepository(repo.root), request, clock=lambda: 100, monotonic_clock=lambda: 0
            )
            with pytest.raises(WorkingWriterBusy):
                other.reconcile(envelope.parent)
            return "answer"

        def close(self):
            pass

    assert (
        ChildDispatchService(children, lambda _: Port()).dispatch(envelope, "token")["state"]
        == "terminal"
    )
