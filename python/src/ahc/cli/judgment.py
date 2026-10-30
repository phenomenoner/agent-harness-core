"""CLI adapter for explicit direction assessment and decision receipts."""

from __future__ import annotations

import json
from argparse import Namespace
from typing import Any

from ahc.application.judgment_service import JudgmentService
from ahc.domain import (
    DirectionDecisionReceipt,
    DirectionSignal,
    GoalContract,
    IntentFrame,
    JudgmentAssessment,
)
from ahc.infrastructure.skill_repository import SkillRepository

JUDGMENT_COMMANDS = frozenset({"judgment-assess", "judgment-decide"})


def register_judgment_commands(commands: Any) -> None:
    assess = commands.add_parser("judgment-assess")
    assess.add_argument("--skill-root", required=True)
    assess.add_argument("--intent-json", required=True)
    assess.add_argument("--goal-json", required=True)
    assess.add_argument("--signals-json", default="[]")
    assess.add_argument("--assessment-json", required=True)
    decide = commands.add_parser("judgment-decide")
    decide.add_argument("--skill-root", required=True)
    decide.add_argument("--assessment-json", required=True)
    decide.add_argument("--prior-goal-json", required=True)
    decide.add_argument("--decision-json", required=True)
    decide.add_argument("--successor-goal-json")


def is_judgment_command(command: str | None) -> bool:
    return command in JUDGMENT_COMMANDS


def _json(value: str, label: str) -> object:
    try:
        return json.loads(value)
    except json.JSONDecodeError as exc:
        raise ValueError(f"{label} must contain valid JSON") from exc


def run_judgment_command(args: Namespace) -> tuple[int, dict[str, object]]:
    repository = SkillRepository.create(args.skill_root)
    service = JudgmentService(repository)
    if args.command == "judgment-assess":
        intent = IntentFrame.from_dict(_json(args.intent_json, "intent-json"))
        goal = GoalContract.from_dict(_json(args.goal_json, "goal-json"))
        signals = tuple(
            DirectionSignal.from_dict(item) for item in _json(args.signals_json, "signals-json")
        )
        assessment = JudgmentAssessment.from_dict(_json(args.assessment_json, "assessment-json"))
        return 0, {
            "command": args.command,
            "assessment": service.assess(intent, goal, signals, assessment).to_dict(),
        }
    if args.command == "judgment-decide":
        assessment = JudgmentAssessment.from_dict(_json(args.assessment_json, "assessment-json"))
        prior = GoalContract.from_dict(_json(args.prior_goal_json, "prior-goal-json"))
        receipt = DirectionDecisionReceipt.from_dict(_json(args.decision_json, "decision-json"))
        successor = (
            None
            if args.successor_goal_json is None
            else GoalContract.from_dict(_json(args.successor_goal_json, "successor-goal-json"))
        )
        return 0, {
            "command": args.command,
            "decision": service.decide(assessment, prior, receipt, successor).to_dict(),
        }
    raise ValueError(f"unsupported judgment command {args.command}")


__all__ = [
    "JUDGMENT_COMMANDS",
    "is_judgment_command",
    "register_judgment_commands",
    "run_judgment_command",
]
