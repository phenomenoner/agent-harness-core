from __future__ import annotations

import pytest

from ahc.application.working_continuity import WorkingContinuityService
from ahc.domain.judgment_goal import GOAL_SCHEMA, GoalContract
from ahc.domain.sessions import ContractError, VirtualSessionIdentity
from ahc.domain.working_sets import WorkingSetSnapshot, validate_working_set_transition


class MemoryPort:
    def __init__(self) -> None:
        self.current: WorkingSetSnapshot | None = None
        self.append_count = 0

    def create(self, snapshot: WorkingSetSnapshot) -> WorkingSetSnapshot:
        if self.current is not None:
            raise AssertionError("duplicate test create")
        self.current = snapshot
        return snapshot

    def read_current(self, virtual_session_id: str) -> WorkingSetSnapshot:
        if (
            self.current is None
            or self.current.session.ref.virtual_session_id != virtual_session_id
        ):
            raise AssertionError("missing test state")
        return self.current

    def append(
        self, expected: WorkingSetSnapshot, successor: WorkingSetSnapshot
    ) -> WorkingSetSnapshot:
        assert self.current == expected
        validate_working_set_transition(expected, successor)
        self.current = successor
        self.append_count += 1
        return successor


def goal() -> GoalContract:
    return GoalContract(
        GOAL_SCHEMA,
        "goal-main",
        1,
        None,
        "Preserve current work.",
        "Exercise the application state machine.",
        ("Transitions are exact.",),
        ("M2.2 only.",),
        ("No external effects.",),
        ("The service owns transition ordering.",),
        (),
        "acknowledged",
    )


def service() -> tuple[WorkingContinuityService, MemoryPort, WorkingSetSnapshot]:
    port = MemoryPort()
    api = WorkingContinuityService(port)
    current = api.create(
        VirtualSessionIdentity.initial("session-main", "agent-main", "route-main"),
        goal(),
        progress=("created",),
        evidence_refs=("evidence-1",),
    )
    return api, port, current


def test_advance_interrupt_resume_and_complete_are_ordered() -> None:
    api, port, current = service()
    advanced = api.advance(
        current,
        progress=("created", "advanced"),
        evidence_refs=("evidence-1", "evidence-2"),
        active_turn_id="turn-1",
    )
    interrupted = api.interrupt(advanced, "process-loss")
    resumed = api.resume(interrupted)
    completed = api.complete(resumed)

    assert completed.status == "completed"
    assert completed.active_turn_id is None
    assert port.append_count == 4
    assert api.current("session-main") == completed


def test_cancel_and_invalid_terminal_operations_do_not_append() -> None:
    api, port, current = service()
    interrupted = api.interrupt(current, "operator")
    cancelled = api.cancel(interrupted)
    before = port.append_count

    for operation in (
        lambda: api.advance(cancelled),
        lambda: api.resume(cancelled),
        lambda: api.cancel(cancelled),
        lambda: api.complete(cancelled),
    ):
        with pytest.raises(ContractError):
            operation()
    assert port.append_count == before


def test_compaction_and_backend_rollover_create_exact_successors() -> None:
    api, _, current = service()
    compacted = api.continue_generation(current, current.session.successor(), "compaction")
    assert compacted.continuation_reason == "compaction"
    assert compacted.progress == current.progress

    rolled = api.continue_generation(compacted, compacted.session.successor(), "backend-rollover")
    assert rolled.session.ref.generation == 3
    assert rolled.continuation_reason == "backend-rollover"


def test_wrong_successor_identity_is_rejected_before_port_append() -> None:
    api, port, current = service()
    wrong = current.session.successor(route_id="route-other")
    with pytest.raises(ContractError, match="agent and route"):
        api.continue_generation(current, wrong, "compaction")
    assert port.append_count == 0
