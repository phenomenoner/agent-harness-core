"""Manifest, episode, event, belief, and judgment persistence methods."""
# ruff: noqa: E501

from __future__ import annotations

import json
import sqlite3

from ahc.domain.judgment_models import GoalContract, IntentFrame
from ahc.domain.judgment_records import (
    DirectionDecisionReceipt,
    JudgmentAssessment,
    OutcomeAttribution,
)
from ahc.domain.sessions import ContractError
from ahc.domain.skill_records import SkillEpisode, SkillManifest
from ahc.domain.skill_values import canonical_json

from .skill_episode_validation import (
    validate_episode_manifest_binding,
    validate_episode_update,
)
from .skill_event_validation import validate_event_binding


class SkillRecordStoreMixin:
    """Mutation methods mixed into the one SkillRepository writer authority."""

    def save_episode(self, episode: SkillEpisode) -> SkillEpisode:
        encoded = canonical_json(episode.to_dict()).decode("utf-8")

        def operation(connection: sqlite3.Connection) -> SkillEpisode:
            manifest_row = connection.execute(
                "SELECT manifest_json FROM manifests WHERE manifest_digest = ?",
                (episode.manifest_digest,),
            ).fetchone()
            if manifest_row is None:
                raise ContractError("episode references an unknown manifest")
            manifest = SkillManifest.from_dict(json.loads(manifest_row[0]))
            validate_episode_manifest_binding(episode, manifest)
            row = connection.execute(
                "SELECT episode_json FROM episodes WHERE episode_id = ?", (episode.episode_id,)
            ).fetchone()
            if row is not None:
                if row[0] != encoded:
                    raise ContractError("episode identity already has different evidence")
                return SkillEpisode.from_dict(json.loads(row[0]))
            connection.execute(
                "INSERT INTO episodes(episode_id, manifest_digest, episode_json, outcome, attribution_digest, created_seq) VALUES(?,?,?,?,?,?)",
                (
                    episode.episode_id,
                    episode.manifest_digest,
                    encoded,
                    episode.outcome,
                    episode.attribution_digest,
                    self._seq(connection),
                ),
            )
            return episode

        return self._mutate(operation)

    def update_episode(self, episode: SkillEpisode) -> SkillEpisode:
        encoded = canonical_json(episode.to_dict()).decode("utf-8")

        def operation(connection: sqlite3.Connection) -> SkillEpisode:
            row = connection.execute(
                "SELECT episode_json FROM episodes WHERE episode_id = ?", (episode.episode_id,)
            ).fetchone()
            if row is None:
                raise ContractError("episode is not recorded")
            previous = SkillEpisode.from_dict(json.loads(row[0]))
            validate_episode_update(previous, episode)
            connection.execute(
                "UPDATE episodes SET episode_json = ?, outcome = ?, attribution_digest = ? WHERE episode_id = ?",
                (encoded, episode.outcome, episode.attribution_digest, episode.episode_id),
            )
            return episode

        return self._mutate(operation)

    def get_episode(self, episode_id: str) -> SkillEpisode | None:
        with self._read() as connection:
            row = connection.execute(
                "SELECT episode_json FROM episodes WHERE episode_id = ?", (episode_id,)
            ).fetchone()
        return None if row is None else SkillEpisode.from_dict(json.loads(row[0]))

    def append_event(
        self,
        *,
        event_key: str,
        episode_id: str,
        event_kind: str,
        revision_digest: str | None,
        manifest_digest: str | None,
        payload: dict[str, object],
    ) -> dict[str, object]:
        encoded = canonical_json(payload).decode("utf-8")

        def operation(connection: sqlite3.Connection) -> dict[str, object]:
            validate_event_binding(
                connection,
                event_key=event_key,
                episode_id=episode_id,
                event_kind=event_kind,
                revision_digest=revision_digest,
                manifest_digest=manifest_digest,
                payload=payload,
            )
            row = connection.execute(
                "SELECT payload_json, event_kind, episode_id, revision_digest, manifest_digest FROM events WHERE event_key = ?",
                (event_key,),
            ).fetchone()
            if row is not None:
                if tuple(row) != (
                    encoded,
                    event_kind,
                    episode_id,
                    revision_digest,
                    manifest_digest,
                ):
                    raise ContractError("event key is already bound to different evidence")
                return payload
            connection.execute(
                "INSERT INTO events(event_key, episode_id, event_kind, revision_digest, manifest_digest, payload_json, created_seq) VALUES(?,?,?,?,?,?,?)",
                (
                    event_key,
                    episode_id,
                    event_kind,
                    revision_digest,
                    manifest_digest,
                    encoded,
                    self._seq(connection),
                ),
            )
            return payload

        return self._mutate(operation)

    def apply_belief_update(self, episode: SkillEpisode, *, outcome: str) -> int:
        if outcome == "unknown":
            return 0
        if outcome not in ("verified-success", "verified-failure", "corrected"):
            raise ContractError("belief update requires a terminal verified outcome")

        def operation(connection: sqlite3.Connection) -> int:
            stored_row = connection.execute(
                "SELECT episode_json FROM episodes WHERE episode_id = ?", (episode.episode_id,)
            ).fetchone()
            if stored_row is None:
                raise ContractError("belief update references an unknown episode")
            stored = SkillEpisode.from_dict(json.loads(stored_row[0]))
            if stored.to_dict() != episode.to_dict() or stored.outcome != outcome:
                raise ContractError("belief update does not match the exact stored episode")
            row = connection.execute(
                "SELECT manifest_json FROM manifests WHERE manifest_digest = ?",
                (stored.manifest_digest,),
            ).fetchone()
            if row is None:
                raise ContractError("belief update references an unknown manifest")
            manifest = SkillManifest.from_dict(json.loads(row[0]))
            changed = 0
            for entry in manifest.entries:
                if entry.revision_digest not in stored.used_revisions:
                    continue
                inserted = connection.execute(
                    "INSERT OR IGNORE INTO contributions(episode_id, outcome, revision_digest, role) VALUES(?,?,?,?)",
                    (stored.episode_id, outcome, entry.revision_digest, entry.role),
                ).rowcount
                if not inserted:
                    continue
                connection.execute(
                    "INSERT OR IGNORE INTO beliefs(revision_digest, agent_id, role) VALUES(?,?,?)",
                    (entry.revision_digest, stored.session.agent_id, entry.role),
                )
                column = "successes" if outcome == "verified-success" else "failures"
                connection.execute(
                    f"UPDATE beliefs SET {column} = {column} + 1 WHERE revision_digest = ? AND agent_id = ? AND role = ?",
                    (entry.revision_digest, stored.session.agent_id, entry.role),
                )
                changed += 1
            return changed

        return self._mutate(operation)

    def beliefs(self) -> tuple[dict[str, object], ...]:
        with self._read() as connection:
            rows = connection.execute(
                "SELECT revision_digest, agent_id, role, successes, failures, prior_alpha, prior_beta, expected_cost FROM beliefs ORDER BY revision_digest, agent_id, role"
            ).fetchall()
        return tuple(dict(row) for row in rows)

    def save_judgment_record(
        self,
        record: IntentFrame
        | GoalContract
        | DirectionDecisionReceipt
        | JudgmentAssessment
        | OutcomeAttribution,
    ) -> str:
        encoded = canonical_json(record.to_dict()).decode("utf-8")
        record_digest = record.digest
        kind = type(record).__name__

        def operation(connection: sqlite3.Connection) -> str:
            row = connection.execute(
                "SELECT record_json FROM judgment_records WHERE record_digest = ?", (record_digest,)
            ).fetchone()
            if row is not None:
                if row[0] != encoded:
                    raise ContractError("judgment digest is already bound to different bytes")
                return record_digest
            connection.execute(
                "INSERT INTO judgment_records(record_digest, record_kind, record_json, created_seq) VALUES(?,?,?,?)",
                (record_digest, kind, encoded, self._seq(connection)),
            )
            return record_digest

        return self._mutate(operation)

    def save_proposal(
        self,
        proposal_digest: str,
        episode_id: str,
        candidate_digest: str,
        attribution_digest: str,
        payload: dict[str, object],
    ) -> str:
        encoded = canonical_json(payload).decode("utf-8")

        def operation(connection: sqlite3.Connection) -> str:
            if (
                connection.execute(
                    "SELECT 1 FROM episodes WHERE episode_id = ?", (episode_id,)
                ).fetchone()
                is None
            ):
                raise ContractError("proposal references an unknown episode")
            row = connection.execute(
                "SELECT proposal_json FROM proposals WHERE proposal_digest = ?", (proposal_digest,)
            ).fetchone()
            if row is not None:
                if row[0] != encoded:
                    raise ContractError("proposal digest is already bound to different bytes")
                return proposal_digest
            connection.execute(
                "INSERT INTO proposals(proposal_digest, episode_id, candidate_digest, attribution_digest, proposal_json, created_seq) VALUES(?,?,?,?,?,?)",
                (
                    proposal_digest,
                    episode_id,
                    candidate_digest,
                    attribution_digest,
                    encoded,
                    self._seq(connection),
                ),
            )
            return proposal_digest

        return self._mutate(operation)


__all__ = ["SkillRecordStoreMixin"]
