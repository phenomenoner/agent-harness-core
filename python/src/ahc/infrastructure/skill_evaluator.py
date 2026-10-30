"""Independent deterministic evaluator for one candidate revision."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

from ahc.domain.sessions import ContractError
from ahc.domain.skill_assets import MARKDOWN_SKILL_SCHEMA
from ahc.domain.skill_behavior import HeldOutTaskEvidence, assess_behavioral_improvement
from ahc.domain.skill_graph import hypothetical_active_revisions, validate_active_graph
from ahc.domain.skill_guard import GuardViolation, guard_payload, guard_raw
from ahc.domain.skill_models import SkillRevision
from ahc.domain.skill_selection import trigger_evidence
from ahc.domain.skill_values import digest, digest_json


@dataclass(frozen=True, slots=True)
class EvaluationCheck:
    check_id: str
    passed: bool
    detail: str

    def to_dict(self) -> dict[str, object]:
        return {"check_id": self.check_id, "passed": self.passed, "detail": self.detail}


@dataclass(frozen=True, slots=True)
class EvaluationReport:
    evaluator_id: str
    candidate_digest: str
    input_digest: str
    graph_input_digest: str
    checks: tuple[EvaluationCheck, ...]
    verdict: str
    evaluation_digest: str | None = None

    def __post_init__(self) -> None:
        digest(self.candidate_digest, "candidate_digest")
        digest(self.input_digest, "input_digest")
        digest(self.graph_input_digest, "graph_input_digest")
        if self.verdict not in ("accept", "block"):
            raise ContractError("evaluation verdict must be accept or block")
        if not self.checks or any(type(check) is not EvaluationCheck for check in self.checks):
            raise ContractError("evaluation must contain checks")
        payload = {
            "evaluator_id": self.evaluator_id,
            "candidate_digest": self.candidate_digest,
            "input_digest": self.input_digest,
            "graph_input_digest": self.graph_input_digest,
            "checks": [check.to_dict() for check in self.checks],
            "verdict": self.verdict,
        }
        actual = digest_json(payload)
        if self.evaluation_digest is not None and self.evaluation_digest != actual:
            raise ContractError("evaluation_digest does not match report")
        object.__setattr__(self, "evaluation_digest", actual)

    @property
    def structural_verdict(self) -> str:
        """Metadata and guard validity, independent of learned performance evidence."""
        structural_checks = (
            check for check in self.checks if check.check_id != "behavioral-improvement"
        )
        return "accept" if all(check.passed for check in structural_checks) else "block"

    def to_dict(self) -> dict[str, object]:
        return {
            "evaluator_id": self.evaluator_id,
            "candidate_digest": self.candidate_digest,
            "input_digest": self.input_digest,
            "graph_input_digest": self.graph_input_digest,
            "checks": [check.to_dict() for check in self.checks],
            "verdict": self.verdict,
            "evaluation_digest": self.evaluation_digest,
        }


def _semantic_version(value: str) -> tuple[int, int, int]:
    return tuple(int(part) for part in value.split("."))  # type: ignore[return-value]


def _check(check_id: str, passed: bool, detail: str) -> EvaluationCheck:
    return EvaluationCheck(check_id, passed, detail)


def _graph_report_checks(snapshot) -> tuple[EvaluationCheck, ...]:
    codes = {issue.code for issue in snapshot.issues}
    return (
        _check(
            "relationship-targets",
            "missing-target" not in codes,
            "relationship targets are exact materialized revisions",
        ),
        _check(
            "relationship-direction",
            "later-layer-dependency" not in codes,
            "dependencies do not point to a later layer",
        ),
        _check(
            "relationship-activation",
            "inactive-dependency" not in codes,
            "dependency targets are active in the hypothetical graph",
        ),
        _check(
            "relationship-cycles",
            "dependency-cycle" not in codes,
            "active dependency relationships are acyclic",
        ),
        _check(
            "conflict-safety",
            "active-conflict" not in codes,
            "active conflict relationships are symmetric-safe",
        ),
        _check(
            "relationship-graph",
            snapshot.valid,
            "canonical hypothetical active graph is valid",
        ),
    )


def evaluate_revision(
    candidate: SkillRevision | Mapping[str, object],
    *,
    active_revisions: tuple[SkillRevision, ...] | list[SkillRevision] = (),
    materialized_revisions: tuple[SkillRevision, ...] | list[SkillRevision] = (),
    behavior_evidence: tuple[HeldOutTaskEvidence, ...] | list[HeldOutTaskEvidence] | None = None,
    evaluator_id: str = "deterministic-evaluator-v1",
) -> EvaluationReport:
    raw = candidate.to_dict() if isinstance(candidate, SkillRevision) else dict(candidate)
    input_digest = digest_json(raw)
    active = tuple(active_revisions)
    materialized = tuple(materialized_revisions)
    checks: list[EvaluationCheck] = []
    parsed: SkillRevision | None = None
    try:
        reparsed = SkillRevision.from_dict(raw)
        parsed = SkillRevision.from_dict(reparsed.to_dict())
        checks.append(_check("schema-and-digest", True, "independently reparsed exact revision"))
    except (ContractError, TypeError, ValueError) as exc:
        checks.append(_check("schema-and-digest", False, type(exc).__name__))
    if parsed is None:
        graph = validate_active_graph(active, materialized)
        raw_digest = raw.get("revision_digest", "0" * 64)
        candidate_digest = raw_digest if isinstance(raw_digest, str) else "0" * 64
        if len(candidate_digest) != 64:
            candidate_digest = "0" * 64
        return EvaluationReport(
            evaluator_id,
            candidate_digest,
            input_digest,
            graph.graph_digest,
            tuple(checks),
            "block",
        )
    try:
        if parsed.schema_version != MARKDOWN_SKILL_SCHEMA:
            guard_payload(parsed.to_dict())
            guard_raw(parsed.body.encode("utf-8"))
        checks.append(_check("content-guard", True,
            "v2 instruction data grants no authority; effect gates remain external"
            if parsed.schema_version == MARKDOWN_SKILL_SCHEMA else "no protected content family"))
    except GuardViolation as exc:
        checks.append(_check("content-guard", False, exc.family))
    positive_ok = all(
        trigger_evidence(trigger, example)
        for example in parsed.positive_examples
        for trigger in parsed.triggers
    )
    negative_ok = all(
        not trigger_evidence(trigger, example)
        for example in parsed.negative_examples
        for trigger in parsed.triggers
    )
    checks.append(
        _check(
            "positive-activation",
            positive_ok or parsed.schema_version == MARKDOWN_SKILL_SCHEMA,
            "v2 examples advise model selection" if parsed.schema_version == MARKDOWN_SKILL_SCHEMA
            else "positive examples have deterministic trigger evidence",
        )
    )
    checks.append(
        _check(
            "negative-non-activation",
            negative_ok or parsed.schema_version == MARKDOWN_SKILL_SCHEMA,
            "v2 non-trigger examples advise model selection"
            if parsed.schema_version == MARKDOWN_SKILL_SCHEMA
            else "negative examples remain outside trigger evidence",
        )
    )
    active_current = next((item for item in active if item.skill_name == parsed.skill_name), None)
    lineage_ok = (active_current is None and parsed.predecessor_digest is None) or (
        active_current is not None
        and parsed.predecessor_digest == active_current.revision_digest
        and _semantic_version(parsed.version) > _semantic_version(active_current.version)
    )
    checks.append(
        _check(
            "predecessor-version",
            lineage_ok,
            "candidate predecessor and version bind the active row",
        )
    )
    graph = validate_active_graph(
        hypothetical_active_revisions(parsed, active),
        materialized,
    )
    checks.extend(_graph_report_checks(graph))
    bounds_ok = len(parsed.body.encode("utf-8")) <= (
        262144 if parsed.schema_version == MARKDOWN_SKILL_SCHEMA else 49152)
    checks.append(
        _check("disclosure-bounds", bounds_ok, "body and disclosure bounds are within contract")
    )
    if behavior_evidence is not None:
        assessment = assess_behavioral_improvement(
            parsed.revision_digest or "0" * 64,
            behavior_evidence,
        )
        checks.append(
            _check("behavioral-improvement", assessment.accepted, assessment.detail())
        )
    verdict = "accept" if all(check.passed for check in checks) else "block"
    return EvaluationReport(
        evaluator_id,
        parsed.revision_digest or "0" * 64,
        input_digest,
        graph.graph_digest,
        tuple(checks),
        verdict,
    )


__all__ = ["EvaluationCheck", "EvaluationReport", "evaluate_revision"]
