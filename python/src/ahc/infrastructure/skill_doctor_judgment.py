"""Read-only judgment-record, goal, decision, and attribution lineage audit."""

from __future__ import annotations

import json
from typing import Any

from ahc.domain.judgment_models import DirectionSignal, GoalContract, IntentFrame
from ahc.domain.judgment_records import (
    DirectionDecisionReceipt,
    JudgmentAssessment,
    OutcomeAttribution,
)
from ahc.domain.judgment_routing import allowed_learning_layer, validate_decision

_RECORD_TYPES: dict[str, Any] = {
    "IntentFrame": IntentFrame,
    "GoalContract": GoalContract,
    "DirectionSignal": DirectionSignal,
    "JudgmentAssessment": JudgmentAssessment,
    "DirectionDecisionReceipt": DirectionDecisionReceipt,
    "OutcomeAttribution": OutcomeAttribution,
}


def _append_once(issues: list[str], value: str) -> None:
    if value not in issues:
        issues.append(value)


def judgment_issues(repository: Any) -> tuple[str, ...]:
    """Return stable issue identifiers without mutating repository state."""
    issues: list[str] = []
    with repository._read() as connection:
        rows = connection.execute(
            "SELECT record_digest, record_kind, record_json FROM judgment_records "
            "ORDER BY created_seq, record_digest"
        ).fetchall()
        episodes = {
            row["episode_id"]: row
            for row in connection.execute(
                "SELECT episode_id, attribution_digest FROM episodes"
            ).fetchall()
        }
        proposals = connection.execute(
            "SELECT episode_id, candidate_digest, attribution_digest FROM proposals"
        ).fetchall()
        revisions = {
            row["revision_digest"]: row["cognitive_layer"]
            for row in connection.execute(
                "SELECT revision_digest, cognitive_layer FROM revisions"
            ).fetchall()
        }

    parsed: dict[str, object] = {}
    for row in rows:
        record_type = _RECORD_TYPES.get(row["record_kind"])
        if record_type is None:
            _append_once(issues, f"judgment-kind:{row['record_kind']}")
            continue
        try:
            record = record_type.from_dict(json.loads(row["record_json"]))
            if record.digest != row["record_digest"]:
                raise ValueError("digest mismatch")
            parsed[row["record_digest"]] = record
        except (TypeError, ValueError, KeyError, json.JSONDecodeError):
            _append_once(issues, f"judgment-invalid:{row['record_digest']}")

    intents = {key: value for key, value in parsed.items() if isinstance(value, IntentFrame)}
    goals = {key: value for key, value in parsed.items() if isinstance(value, GoalContract)}
    assessments = {
        key: value for key, value in parsed.items() if isinstance(value, JudgmentAssessment)
    }
    decisions = {
        key: value for key, value in parsed.items() if isinstance(value, DirectionDecisionReceipt)
    }
    attributions = {
        key: value for key, value in parsed.items() if isinstance(value, OutcomeAttribution)
    }

    for key, assessment in assessments.items():
        if assessment.intent_digest not in intents:
            _append_once(issues, f"assessment-intent:{key}")
        if assessment.goal_digest not in goals:
            _append_once(issues, f"assessment-goal:{key}")

    for key, decision in decisions.items():
        assessment = assessments.get(decision.assessment_digest)
        prior = goals.get(decision.prior_goal_digest)
        successor = goals.get(decision.resulting_goal_digest or "")
        if assessment is None or prior is None:
            _append_once(issues, f"decision-lineage:{key}")
            continue
        material = decision.decision in ("accept-alternative", "revise-goal", "narrow-scope")
        try:
            validate_decision(assessment, prior, decision, successor if material else None)
        except (TypeError, ValueError):
            _append_once(issues, f"decision-lineage:{key}")

    resulting = {
        decision.resulting_goal_digest
        for decision in decisions.values()
        if decision.resulting_goal_digest != decision.prior_goal_digest
    }
    for key, current in goals.items():
        if current.predecessor_digest is not None:
            if current.predecessor_digest not in goals:
                _append_once(issues, f"goal-predecessor:{key}")
            if key not in resulting:
                _append_once(issues, f"goal-missing-decision:{key}")

    for key, attribution in attributions.items():
        episode = episodes.get(attribution.episode_id)
        if episode is None or episode["attribution_digest"] != key:
            _append_once(issues, f"attribution-episode:{key}")

    for proposal in proposals:
        attribution = attributions.get(proposal["attribution_digest"])
        layer = revisions.get(proposal["candidate_digest"])
        if (
            attribution is None
            or layer is None
            or not allowed_learning_layer(attribution, layer)
            or proposal["episode_id"] != attribution.episode_id
        ):
            _append_once(issues, f"proposal-attribution:{proposal['candidate_digest']}")

    return tuple(issues)


__all__ = ["judgment_issues"]
