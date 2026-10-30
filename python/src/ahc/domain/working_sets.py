"""Immutable working-set snapshots and transition invariants."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import cast

from .judgment_goal import GoalContract
from .sessions import ContractError, VirtualSessionIdentity, _exact_mapping, _schema_version
from .working_values import (
    CONTINUATION_REASONS,
    INTERRUPTION_DISPOSITIONS,
    WORKING_SET_SCHEMA_VERSION,
    WORKING_SET_STATUSES,
    evidence_refs,
    optional_choice,
    optional_identifier,
    positive_revision,
    progress_items,
    snapshot_digest,
)


@dataclass(frozen=True, slots=True)
class WorkingSetSnapshot:
    schema_version: str
    session: VirtualSessionIdentity
    revision: int
    predecessor_digest: str | None
    goal: GoalContract
    progress: tuple[str, ...]
    evidence_refs: tuple[str, ...]
    active_turn_id: str | None
    status: str
    interruption: str | None
    continuation_reason: str | None
    snapshot_digest: str | None = None

    def __post_init__(self) -> None:
        _schema_version(self.schema_version, WORKING_SET_SCHEMA_VERSION, "working set")
        if type(self.session) is not VirtualSessionIdentity:
            raise ContractError("working set session must be a VirtualSessionIdentity")
        revision = positive_revision(self.revision)
        if self.predecessor_digest is not None:
            from .skill_values import digest

            predecessor = digest(self.predecessor_digest, "predecessor_digest")
        else:
            predecessor = None
        if type(self.goal) is not GoalContract:
            raise ContractError("working set goal must be a GoalContract")
        if self.goal.acknowledgement != "acknowledged":
            raise ContractError("working set goal must be acknowledged")
        progress = progress_items(self.progress)
        refs = evidence_refs(self.evidence_refs)
        turn = optional_identifier(self.active_turn_id, "active_turn_id")
        if type(self.status) is not str or self.status not in WORKING_SET_STATUSES:
            raise ContractError("working-set status has an unsupported value")
        interruption = optional_choice(self.interruption, INTERRUPTION_DISPOSITIONS, "interruption")
        continuation = optional_choice(
            self.continuation_reason, CONTINUATION_REASONS, "continuation_reason"
        )
        self._validate_shape(revision, predecessor, interruption, continuation, turn)
        payload = self._payload(
            predecessor=predecessor,
            progress=progress,
            evidence=refs,
            turn=turn,
            interruption=interruption,
            continuation=continuation,
        )
        actual = snapshot_digest(payload, self.snapshot_digest)
        for name, value in (
            ("predecessor_digest", predecessor),
            ("progress", progress),
            ("evidence_refs", refs),
            ("active_turn_id", turn),
            ("interruption", interruption),
            ("continuation_reason", continuation),
            ("snapshot_digest", actual),
        ):
            object.__setattr__(self, name, value)

    def _validate_shape(
        self,
        revision: int,
        predecessor: str | None,
        interruption: str | None,
        continuation: str | None,
        turn: str | None,
    ) -> None:
        generation = self.session.ref.generation
        initial = generation == 1 and revision == 1
        if initial != (predecessor is None):
            raise ContractError("predecessor_digest is null exactly for the initial snapshot")
        if initial and (
            self.status != "active"
            or interruption is not None
            or continuation is not None
            or turn is not None
        ):
            raise ContractError(
                "initial working set must start active with no interruption/continuation/turn"
            )
        if revision == 1 and generation > 1:
            if continuation is None:
                raise ContractError("successor generation requires continuation_reason")
            if self.status != "active" or interruption is not None or turn is not None:
                raise ContractError(
                    "successor generation must start active with no interruption/turn"
                )
        elif continuation is not None:
            raise ContractError("continuation_reason is allowed only on successor revision 1")
        if (self.status == "interrupted") != (interruption is not None):
            raise ContractError("interruption is required exactly for interrupted status")
        if self.status in {"cancelled", "completed"} and turn is not None:
            raise ContractError("terminal working set must not retain active_turn_id")

    def _payload(
        self,
        *,
        predecessor: str | None,
        progress: tuple[str, ...],
        evidence: tuple[str, ...],
        turn: str | None,
        interruption: str | None,
        continuation: str | None,
    ) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "session": self.session.to_dict(),
            "revision": self.revision,
            "predecessor_digest": predecessor,
            "goal": self.goal.to_dict(),
            "progress": list(progress),
            "evidence_refs": list(evidence),
            "active_turn_id": turn,
            "status": self.status,
            "interruption": interruption,
            "continuation_reason": continuation,
        }

    @classmethod
    def initial(
        cls,
        session: VirtualSessionIdentity,
        goal: GoalContract,
        *,
        progress: tuple[str, ...] = (),
        evidence_refs: tuple[str, ...] = (),
    ) -> WorkingSetSnapshot:
        return cls(
            WORKING_SET_SCHEMA_VERSION,
            session,
            1,
            None,
            goal,
            progress,
            evidence_refs,
            None,
            "active",
            None,
            None,
        )

    def revise(
        self,
        *,
        goal: GoalContract | None = None,
        progress: tuple[str, ...] | None = None,
        evidence_refs: tuple[str, ...] | None = None,
        active_turn_id: str | None = None,
        status: str = "active",
        interruption: str | None = None,
    ) -> WorkingSetSnapshot:
        return type(self)(
            WORKING_SET_SCHEMA_VERSION,
            self.session,
            self.revision + 1,
            self.snapshot_digest,
            self.goal if goal is None else goal,
            self.progress if progress is None else progress,
            self.evidence_refs if evidence_refs is None else evidence_refs,
            active_turn_id,
            status,
            interruption,
            None,
        )

    def continue_as(self, successor: VirtualSessionIdentity, reason: str) -> WorkingSetSnapshot:
        return type(self)(
            WORKING_SET_SCHEMA_VERSION,
            successor,
            1,
            self.snapshot_digest,
            self.goal,
            self.progress,
            self.evidence_refs,
            None,
            "active",
            None,
            reason,
        )

    def to_dict(self) -> dict[str, object]:
        payload = self._payload(
            predecessor=self.predecessor_digest,
            progress=self.progress,
            evidence=self.evidence_refs,
            turn=self.active_turn_id,
            interruption=self.interruption,
            continuation=self.continuation_reason,
        )
        payload["snapshot_digest"] = self.snapshot_digest
        return payload

    @classmethod
    def from_dict(cls, value: Mapping[str, object]) -> WorkingSetSnapshot:
        keys = frozenset(
            {
                "schema_version",
                "session",
                "revision",
                "predecessor_digest",
                "goal",
                "progress",
                "evidence_refs",
                "active_turn_id",
                "status",
                "interruption",
                "continuation_reason",
                "snapshot_digest",
            }
        )
        raw = _exact_mapping(value, keys, "working set")
        try:
            session = VirtualSessionIdentity.from_dict(cast(Mapping[str, object], raw["session"]))
            goal = GoalContract.from_dict(raw["goal"])
        except ContractError as exc:
            raise ContractError(f"working set nested value: {exc}") from exc
        if type(raw["snapshot_digest"]) is not str:
            raise ContractError("snapshot_digest must be supplied on the JSON boundary")
        return cls(
            cast(str, raw["schema_version"]),
            session,
            cast(int, raw["revision"]),
            cast(str | None, raw["predecessor_digest"]),
            goal,
            cast(tuple[str, ...], raw["progress"]),
            cast(tuple[str, ...], raw["evidence_refs"]),
            cast(str | None, raw["active_turn_id"]),
            cast(str, raw["status"]),
            cast(str | None, raw["interruption"]),
            cast(str | None, raw["continuation_reason"]),
            cast(str, raw["snapshot_digest"]),
        )


def validate_working_set_transition(
    current: WorkingSetSnapshot, successor: WorkingSetSnapshot
) -> None:
    if successor.continuation_reason == "restore":
        raise ContractError("restore requires explicit recovery transition")
    if current.status in {"cancelled", "completed"}:
        raise ContractError("terminal working set cannot transition")
    if successor.session.ref.virtual_session_id != current.session.ref.virtual_session_id:
        raise ContractError("successor must bind the same virtual_session_id")
    if (
        successor.session.agent_id != current.session.agent_id
        or successor.session.route_id != current.session.route_id
    ):
        raise ContractError("successor must retain agent and route")
    if successor.predecessor_digest != current.snapshot_digest:
        raise ContractError("successor must bind the current snapshot digest")
    if successor.session.ref.generation == current.session.ref.generation:
        if successor.session != current.session or successor.revision != current.revision + 1:
            raise ContractError("same-generation successor must increment current revision")
        allowed = {
            "active": {"active", "interrupted", "cancelled", "completed"},
            "interrupted": {"active", "cancelled"},
        }
        if successor.status not in allowed[current.status]:
            raise ContractError("working-set status transition is not allowed")
        return
    if successor.session != current.session.successor() or successor.revision != 1:
        raise ContractError("continued working set must use the exact next session generation")
    if current.status not in {"active", "interrupted"}:
        raise ContractError("current working set cannot continue")
    if (successor.goal, successor.progress, successor.evidence_refs) != (
        current.goal,
        current.progress,
        current.evidence_refs,
    ):
        raise ContractError("continued working set must preserve bounded content")


__all__ = ["WorkingSetSnapshot", "validate_working_set_transition"]
