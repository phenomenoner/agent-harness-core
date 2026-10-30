"""Strict identity and pinned-evidence checks for skill episodes."""

from __future__ import annotations

from ahc.domain.sessions import ContractError
from ahc.domain.skill_records import SkillEpisode, SkillManifest


def validate_episode_manifest_binding(episode: SkillEpisode, manifest: SkillManifest) -> None:
    if manifest.session != episode.session or manifest.turn_id != episode.turn_id:
        raise ContractError("episode identity does not match its pinned manifest")


def validate_episode_update(previous: SkillEpisode, episode: SkillEpisode) -> None:
    if (
        previous.session != episode.session
        or previous.turn_id != episode.turn_id
        or previous.task_digest != episode.task_digest
        or previous.manifest_digest != episode.manifest_digest
        or previous.surfaced_revisions != episode.surfaced_revisions
        or previous.disclosed_revisions != episode.disclosed_revisions
    ):
        raise ContractError("episode identity and pinned evidence are immutable")
    if previous.outcome != "unknown" and previous.to_dict() != episode.to_dict():
        raise ContractError("terminal episode outcome is immutable")


__all__ = ["validate_episode_manifest_binding", "validate_episode_update"]
