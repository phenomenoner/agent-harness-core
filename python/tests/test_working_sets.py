from __future__ import annotations

from dataclasses import FrozenInstanceError

import pytest

from ahc.domain.judgment_goal import GOAL_SCHEMA, GoalContract
from ahc.domain.sessions import ContractError, VirtualSessionIdentity
from ahc.domain.working_sets import WorkingSetSnapshot, validate_working_set_transition
from ahc.domain.working_values import WORKING_SET_SCHEMA_VERSION


def goal(
    revision: int = 1,
    predecessor: str | None = None,
    acknowledgement: str = "acknowledged",
) -> GoalContract:
    return GoalContract(
        GOAL_SCHEMA,
        "goal-main",
        revision,
        predecessor,
        "Keep exact task continuity.",
        "Finish the bounded M2.2 slice.",
        ("Current state survives restart.",),
        ("Working continuity only.",),
        ("No provider execution.",),
        ("CK owns scope decisions.",),
        ("Repository root is explicit.",),
        acknowledgement,
    )


def session() -> VirtualSessionIdentity:
    return VirtualSessionIdentity.initial("session-main", "agent-main", "route-main")


def test_initial_snapshot_round_trip_is_immutable_and_digest_bound() -> None:
    snapshot = WorkingSetSnapshot.initial(
        session(), goal(), progress=("Spec frozen.",), evidence_refs=("evidence-1",)
    )

    assert snapshot.schema_version == WORKING_SET_SCHEMA_VERSION
    assert WorkingSetSnapshot.from_dict(snapshot.to_dict()) == snapshot
    assert len(snapshot.snapshot_digest or "") == 64
    with pytest.raises(FrozenInstanceError):
        snapshot.revision = 2  # type: ignore[misc]

    tampered = snapshot.to_dict()
    tampered["progress"] = ["Different."]
    with pytest.raises(ContractError, match="snapshot_digest"):
        WorkingSetSnapshot.from_dict(tampered)


def test_initial_shape_and_wire_digest_are_mandatory() -> None:
    initial = WorkingSetSnapshot.initial(session(), goal())
    with pytest.raises(ContractError, match="initial working set"):
        WorkingSetSnapshot(
            WORKING_SET_SCHEMA_VERSION,
            session(),
            1,
            None,
            goal(),
            (),
            (),
            None,
            "completed",
            None,
            None,
        )

    payload = initial.to_dict()
    payload["snapshot_digest"] = None
    with pytest.raises(ContractError, match="must be supplied"):
        WorkingSetSnapshot.from_dict(payload)


def test_snapshot_accepts_exact_progress_and_evidence_bounds() -> None:
    snapshot = WorkingSetSnapshot.initial(
        session(),
        goal(),
        progress=tuple("x" * 512 for _ in range(32)),
        evidence_refs=tuple(f"evidence-{index}" for index in range(64)),
    )
    assert len(snapshot.progress) == 32
    assert len(snapshot.evidence_refs) == 64

    with pytest.raises(ContractError, match="progress"):
        WorkingSetSnapshot.initial(session(), goal(), progress=("x" * 513,))
    with pytest.raises(ContractError, match="evidence_refs"):
        WorkingSetSnapshot.initial(session(), goal(), evidence_refs=("same", "same"))
    with pytest.raises(ContractError, match="acknowledged"):
        WorkingSetSnapshot.initial(session(), goal(acknowledgement="proposed"))


@pytest.mark.parametrize(
    "mutator, pattern",
    [
        (lambda value: value.__setitem__("revision", True), "revision"),
        (lambda value: value.__setitem__("unexpected", 1), "unknown keys"),
        (lambda value: value.__setitem__("status", "paused"), "status"),
        (lambda value: value.__setitem__("interruption", "operator"), "interruption"),
        (lambda value: value.__setitem__("continuation_reason", "compaction"), "continuation"),
    ],
)
def test_snapshot_rejects_invalid_shapes(mutator, pattern: str) -> None:
    payload = WorkingSetSnapshot.initial(session(), goal()).to_dict()
    mutator(payload)
    with pytest.raises(ContractError, match=pattern):
        WorkingSetSnapshot.from_dict(payload)


def test_same_generation_and_continuation_transitions_are_exact() -> None:
    initial = WorkingSetSnapshot.initial(
        session(), goal(), progress=("one",), evidence_refs=("evidence-1",)
    )
    revised = initial.revise(progress=("one", "two"), active_turn_id="turn-1")
    validate_working_set_transition(initial, revised)

    continued = revised.continue_as(revised.session.successor(), "compaction")
    validate_working_set_transition(revised, continued)
    assert continued.revision == 1
    assert continued.session.ref.generation == 2
    assert continued.predecessor_digest == revised.snapshot_digest
    assert continued.progress == revised.progress
    assert continued.evidence_refs == revised.evidence_refs
    assert continued.active_turn_id is None
    assert continued.continuation_reason == "compaction"


def test_transition_rejects_wrong_owner_lineage_and_terminal_successor() -> None:
    initial = WorkingSetSnapshot.initial(session(), goal())
    wrong_owner = initial.continue_as(
        initial.session.successor(agent_id="agent-other"), "compaction"
    )
    with pytest.raises(ContractError, match="agent and route"):
        validate_working_set_transition(initial, wrong_owner)

    completed = initial.revise(status="completed")
    validate_working_set_transition(initial, completed)
    with pytest.raises(ContractError, match="terminal"):
        validate_working_set_transition(completed, completed.revise())
