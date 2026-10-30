"""Application skill-turn, pinning, disclosure, and material-use commands."""

from __future__ import annotations

from hashlib import sha256

from ahc.domain.judgment_models import DirectionSignal, GoalContract, IntentFrame
from ahc.domain.judgment_records import DirectionDecisionReceipt, JudgmentAssessment
from ahc.domain.judgment_routing import (
    DirectionGateError,
    gate_how_selection,
    needs_direction_assessment,
    validate_assessment,
    validate_decision,
)
from ahc.domain.sessions import ContractError
from ahc.domain.skill_models import SelectionRequest
from ahc.domain.skill_records import SkillEpisode
from ahc.domain.skill_selection import SelectionResult, disclose_bodies, select_revisions
from ahc.domain.skill_values import SKILL_EPISODE_SCHEMA_VERSION, digest_json
from ahc.infrastructure.skill_repository import SkillRepository


class SkillTurnService:
    """Owns selection/pinning while delegating durability to SkillRepository."""

    def __init__(self, repository: SkillRepository) -> None:
        self.repository = repository
        self.scoped_learning = False
        self.scoped_route = None

    def select_and_pin(
        self,
        request: SelectionRequest,
        *,
        episode_id: str,
        intent: IntentFrame | None = None,
        goal: GoalContract | None = None,
        signals: tuple[DirectionSignal, ...] = (),
        assessment: JudgmentAssessment | None = None,
        decision: DirectionDecisionReceipt | None = None,
    ) -> dict[str, object]:
        if goal is not None:
            if request.goal_digest != goal.digest:
                raise ContractError("selection request goal digest does not match current goal")
            if (
                intent is not None
                and needs_direction_assessment(intent, goal, signals)
                and assessment is None
            ):
                raise DirectionGateError("intent uncertainty requires direction assessment")
            if assessment is not None:
                if intent is None:
                    raise DirectionGateError("assessment requires the bound intent frame")
                validate_assessment(intent, goal, signals, assessment)
            if decision is not None:
                if assessment is None:
                    raise DirectionGateError("decision requires the bound assessment")
                validate_decision(assessment, goal, decision)
            gate = gate_how_selection(goal, signals, assessment, decision)
            if not gate.allowed:
                raise DirectionGateError(gate.reason)
        context_digest = digest_json({
            "session": request.session.to_dict(), "turn_id": request.turn_id,
            "task_text": request.task_text, "roles": list(request.roles),
            "available_tools": list(request.available_tools), "goal_digest": request.goal_digest,
        })
        active_revisions, availability_pinned = self.repository.pin_revision_availability(
            request.session,
        )
        if self.scoped_learning:
            from ahc.infrastructure.scoped_skill_trials import pin_operation_catalog
            route = (self.scoped_route(request.session, request.turn_id)
                     if callable(self.scoped_route) else None)
            active_revisions = pin_operation_catalog(self.repository, request.session, request.turn_id, route=route)
        pinned = self.repository.get_manifest(
            request.session, turn_id=request.turn_id, request_digest=context_digest,
        )
        if pinned is None:
            materialized_revisions = self.repository.list_revisions()
            result = select_revisions(
                request,
                active_revisions,
                self._belief_map(),
                materialized_revisions=materialized_revisions,
            )
            manifest = self.repository.save_manifest(
                result.manifest, request_digest=context_digest,
            )
            selected = result.selected
            abstained = result.abstained_roles
        else:
            manifest = pinned
            selected = tuple(
                self.repository.get_revision(entry.revision_digest) for entry in manifest.entries
            )
            abstained = tuple(
                role
                for role in request.roles
                if not any(entry.role == role for entry in manifest.entries)
            )
            result = SelectionResult(manifest, selected, abstained)
        episode = SkillEpisode(
            SKILL_EPISODE_SCHEMA_VERSION,
            episode_id,
            request.session,
            request.turn_id,
            sha256(request.task_text.encode("utf-8")).hexdigest(),
            manifest.manifest_digest or "0" * 64,
            tuple(dict.fromkeys(entry.revision_digest for entry in manifest.entries)),
            tuple(dict.fromkeys(entry.revision_digest for entry in manifest.entries)),
            (),
        )
        existing = self.repository.get_episode(episode_id)
        if existing is None:
            episode = self.repository.save_episode(episode)
        else:
            if (
                existing.session != episode.session or existing.turn_id != episode.turn_id
                or existing.task_digest != episode.task_digest
                or existing.manifest_digest != episode.manifest_digest
            ):
                raise ContractError("episode identity already has different evidence")
            episode = existing
        for entry in manifest.entries:
            self.repository.append_event(
                event_key=f"{episode_id}:surfaced:{entry.revision_digest}:{entry.role}",
                episode_id=episode_id,
                event_kind="surfaced",
                revision_digest=entry.revision_digest,
                manifest_digest=manifest.manifest_digest,
                payload={"role": entry.role, "revision_digest": entry.revision_digest},
            )
            self.repository.append_event(
                event_key=f"{episode_id}:disclosed:{entry.revision_digest}:{entry.role}",
                episode_id=episode_id,
                event_kind="disclosed",
                revision_digest=entry.revision_digest,
                manifest_digest=manifest.manifest_digest,
                payload={"role": entry.role, "revision_digest": entry.revision_digest},
            )
        revisions = {item.revision_digest or "": item for item in selected}
        return {
            "manifest": manifest,
            "episode": episode,
            "disclosure": disclose_bodies(manifest, revisions),
            "abstained_roles": abstained,
            "pinned": availability_pinned,
        }

    def mark_used(self, episode_id: str, used_revisions: tuple[str, ...]) -> SkillEpisode:
        episode = self.repository.get_episode(episode_id)
        if episode is None:
            raise ContractError("episode is not recorded")
        used = tuple(used_revisions)
        if len(set(used)) != len(used) or not set(used).issubset(episode.disclosed_revisions):
            raise ContractError("materially used revisions must be disclosed and unique")
        updated = SkillEpisode(
            episode.schema_version,
            episode.episode_id,
            episode.session,
            episode.turn_id,
            episode.task_digest,
            episode.manifest_digest,
            episode.surfaced_revisions,
            episode.disclosed_revisions,
            used,
            episode.outcome,
            episode.evidence_refs,
            episode.lesson,
            episode.attribution_digest,
        )
        updated = self.repository.update_episode(updated)
        for revision_digest in used:
            self.repository.append_event(
                event_key=f"{episode_id}:used:{revision_digest}",
                episode_id=episode_id,
                event_kind="used",
                revision_digest=revision_digest,
                manifest_digest=episode.manifest_digest,
                payload={"revision_digest": revision_digest},
            )
        return updated

    def _belief_map(self) -> dict[tuple[str, str, str], dict[str, object]]:
        return {
            (row["revision_digest"], row["agent_id"], row["role"]): row
            for row in self.repository.beliefs()
        }


__all__ = ["SkillTurnService"]
