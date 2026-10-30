"""Transport-neutral M2.2 working-continuity application service."""

from __future__ import annotations

from typing import Protocol

from ahc.domain.judgment_goal import GoalContract
from ahc.domain.sessions import ContractError, VirtualSessionIdentity
from ahc.domain.working_sets import WorkingSetSnapshot


class WorkingSetPort(Protocol):
    def create(self, snapshot: WorkingSetSnapshot) -> WorkingSetSnapshot: ...

    def read_current(self, virtual_session_id: str) -> WorkingSetSnapshot: ...

    def append(
        self, expected: WorkingSetSnapshot, successor: WorkingSetSnapshot
    ) -> WorkingSetSnapshot: ...


class WorkingContinuityService:
    """Own legal working-set use-case transitions over one repository port."""

    def __init__(self, repository: WorkingSetPort) -> None:
        self.repository = repository

    def create(
        self,
        session: VirtualSessionIdentity,
        goal: GoalContract,
        *,
        progress: tuple[str, ...] = (),
        evidence_refs: tuple[str, ...] = (),
    ) -> WorkingSetSnapshot:
        return self.repository.create(
            WorkingSetSnapshot.initial(
                session, goal, progress=progress, evidence_refs=evidence_refs
            )
        )

    def current(self, virtual_session_id: str) -> WorkingSetSnapshot:
        return self.repository.read_current(virtual_session_id)

    def advance(
        self,
        expected: WorkingSetSnapshot,
        *,
        goal: GoalContract | None = None,
        progress: tuple[str, ...] | None = None,
        evidence_refs: tuple[str, ...] | None = None,
        active_turn_id: str | None = None,
    ) -> WorkingSetSnapshot:
        if expected.status != "active":
            raise ContractError("advance requires active working set")
        successor = expected.revise(
            goal=goal,
            progress=progress,
            evidence_refs=evidence_refs,
            active_turn_id=active_turn_id,
        )
        return self.repository.append(expected, successor)

    def interrupt(self, expected: WorkingSetSnapshot, disposition: str) -> WorkingSetSnapshot:
        if expected.status != "active":
            raise ContractError("interrupt requires active working set")
        successor = expected.revise(
            active_turn_id=expected.active_turn_id,
            status="interrupted",
            interruption=disposition,
        )
        return self.repository.append(expected, successor)

    def resume(self, expected: WorkingSetSnapshot) -> WorkingSetSnapshot:
        if expected.status != "interrupted":
            raise ContractError("resume requires interrupted working set")
        successor = expected.revise(active_turn_id=expected.active_turn_id)
        return self.repository.append(expected, successor)

    def cancel(self, expected: WorkingSetSnapshot) -> WorkingSetSnapshot:
        if expected.status not in {"active", "interrupted"}:
            raise ContractError("cancel requires active or interrupted working set")
        successor = expected.revise(active_turn_id=None, status="cancelled")
        return self.repository.append(expected, successor)

    def complete(self, expected: WorkingSetSnapshot) -> WorkingSetSnapshot:
        if expected.status != "active":
            raise ContractError("complete requires active working set")
        successor = expected.revise(active_turn_id=None, status="completed")
        return self.repository.append(expected, successor)

    def continue_generation(
        self,
        expected: WorkingSetSnapshot,
        successor_session: VirtualSessionIdentity,
        reason: str,
    ) -> WorkingSetSnapshot:
        if expected.status not in {"active", "interrupted"}:
            raise ContractError("continuation requires active or interrupted working set")
        successor = expected.continue_as(successor_session, reason)
        return self.repository.append(expected, successor)


__all__ = ["WorkingContinuityService", "WorkingSetPort"]
