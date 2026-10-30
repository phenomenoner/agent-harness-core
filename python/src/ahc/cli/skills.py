"""CLI adapter for the local active skill loop."""

from __future__ import annotations

import json
from argparse import Namespace
from typing import Any

from ahc.application.skill_learning import learn_episode
from ahc.application.skill_service import SkillTurnService
from ahc.domain import (
    VIRTUAL_SESSION_REF_SCHEMA_VERSION,
    VIRTUAL_SESSION_SCHEMA_VERSION,
    DirectionDecisionReceipt,
    DirectionSignal,
    FakeTurnRequest,
    GoalContract,
    IntentFrame,
    JudgmentAssessment,
    OutcomeAttribution,
    SelectionRequest,
    VirtualSessionIdentity,
    VirtualSessionRef,
)
from ahc.domain.sessions import ContractError
from ahc.domain.skill_behavior import HeldOutTaskEvidence
from ahc.infrastructure.skill_doctor import doctor
from ahc.infrastructure.skill_repository import SkillRepository

SKILL_COMMANDS = frozenset(
    {"skill-turn", "skill-learn", "skill-status", "skill-doctor", "skill-rollback",
     "learning-control"}
)
_TURN_REQUEST_BUILD_FIELDS = (
    "virtual_session_id",
    "predecessor_json",
    "agent_id",
    "route_id",
    "turn_id",
    "input_text",
    "roles",
    "available_tools",
    "task_text",
)


def register_skill_commands(commands: Any) -> None:
    control = commands.add_parser("learning-control")
    control.add_argument("--skill-root", required=True)
    control.add_argument("--agent-id", required=True)
    choice = control.add_mutually_exclusive_group()
    choice.add_argument("--enable", action="store_true")
    choice.add_argument("--disable", action="store_true")

    turn = commands.add_parser("skill-turn")
    turn.add_argument("--skill-root", required=True)
    turn.add_argument("--request-json")
    turn.add_argument("--virtual-session-id")
    turn.add_argument("--generation", type=int, default=1)
    turn.add_argument("--predecessor-json")
    turn.add_argument("--agent-id")
    turn.add_argument("--route-id")
    turn.add_argument("--turn-id")
    turn.add_argument("--input-text")
    turn.add_argument("--roles", nargs="+")
    turn.add_argument("--tool", dest="available_tools", action="append", default=[])
    turn.add_argument("--task-text")
    turn.add_argument("--goal-json", required=True)
    turn.add_argument("--intent-json")
    turn.add_argument("--signals-json")
    turn.add_argument("--assessment-json")
    turn.add_argument("--decision-json")
    turn.add_argument("--episode-id")

    learn = commands.add_parser("skill-learn")
    learn.add_argument("--skill-root", required=True)
    learn.add_argument("--episode-id", required=True)
    learn.add_argument(
        "--outcome",
        choices=("unknown", "verified-success", "verified-failure", "corrected"),
        required=True,
    )
    learn.add_argument("--evidence-ref", action="append", default=[])
    learn.add_argument("--used-revision", action="append", default=[])
    learn.add_argument("--attribution-json")
    learn.add_argument("--lesson-json")
    learn.add_argument("--reflect", action="store_true")
    learn.add_argument("--evaluate", action="store_true")
    learn.add_argument("--held-out-evidence-json", default="[]")
    learn.add_argument("--activate", action="store_true")
    learn.add_argument("--operation-key")

    for name in ("skill-status", "skill-doctor"):
        command = commands.add_parser(name)
        command.add_argument("--skill-root", required=True)
    rollback = commands.add_parser("skill-rollback")
    rollback.add_argument("--skill-root", required=True)
    rollback.add_argument("--skill-name", required=True)
    rollback.add_argument("--expected-active-digest", required=True)
    rollback.add_argument("--operation-key", required=True)


def is_skill_command(command: str | None) -> bool:
    return command in SKILL_COMMANDS


def _json(value: str, label: str) -> Any:
    try:
        return json.loads(value)
    except (TypeError, json.JSONDecodeError) as exc:
        raise ValueError(f"{label} must contain valid JSON") from exc


def _identity(args: Namespace) -> VirtualSessionIdentity:
    if args.request_json:
        request = FakeTurnRequest.from_dict(_json(args.request_json, "request-json"))
        return request.session
    session_id = args.virtual_session_id or "offline-session"
    generation = args.generation
    predecessor = None
    if args.predecessor_json:
        predecessor = VirtualSessionRef.from_dict(_json(args.predecessor_json, "predecessor-json"))
    elif generation > 1:
        predecessor = VirtualSessionRef(
            VIRTUAL_SESSION_REF_SCHEMA_VERSION, session_id, generation - 1
        )
    return VirtualSessionIdentity(
        VIRTUAL_SESSION_SCHEMA_VERSION,
        VirtualSessionRef(VIRTUAL_SESSION_REF_SCHEMA_VERSION, session_id, generation),
        args.agent_id or "offline-agent",
        args.route_id or "offline-route",
        predecessor,
    )


def _request(args: Namespace) -> SelectionRequest:
    if args.request_json:
        if args.generation != 1 or any(
            getattr(args, name, None) not in (None, []) for name in _TURN_REQUEST_BUILD_FIELDS
        ):
            raise ContractError("--request-json cannot be combined with request-building options")
        request = FakeTurnRequest.from_dict(_json(args.request_json, "request-json"))
        task_text = request.input_text
        turn_id = request.turn_id
        agent_id = request.session.agent_id
    else:
        task_text = args.task_text or args.input_text
        turn_id = args.turn_id or "turn-1"
        agent_id = args.agent_id or "offline-agent"
        if task_text is None:
            raise ValueError("skill-turn requires task text")
    return SelectionRequest(
        _identity(args),
        turn_id,
        tuple(args.roles or ("procedure",)),
        agent_id,
        tuple(sorted(set(args.available_tools))),
        task_text,
        None,
    )


def _optional(value: str | None, parser: Any) -> Any:
    return None if value is None else parser.from_dict(_json(value, parser.__name__))


def run_skill_command(args: Namespace) -> tuple[int, dict[str, object]]:
    command = args.command
    if command == "learning-control":
        from ahc.application.skill_experiments import SkillExperiments

        repository = SkillRepository(args.skill_root)
        if args.enable or args.disable:
            experiments = SkillExperiments(repository, owner=args.agent_id, granted_tools=(),
                protected_names=(), protection_check=lambda: False)
        with repository._read() as db:
            exists = db.execute(
                "SELECT 1 FROM sqlite_master WHERE name='learning_control'").fetchone()
            row = (
                db.execute("SELECT enabled,daily_limit FROM learning_control WHERE id=1").fetchone()
                if exists else None
            )
        if args.enable or args.disable:
            experiments.configure(enabled=args.enable, daily_limit=row[1])
        return 0, {"command": command, "automatic_enabled": args.enable if (
            args.enable or args.disable) else bool(row and row[0]),
            "daily_limit": row[1] if row else None,
            "protection_still_required_for_activation": True}
    if command == "skill-doctor":
        report = doctor(args.skill_root)
        return (0 if report.get("ok") else 2), {"command": command, "report": report}
    if command == "skill-status":
        repository = SkillRepository(args.skill_root)
        return 0, {
            "command": command,
            "status": repository.status(),
            "active": [item.to_dict() for item in repository.active_revisions()],
            "beliefs": list(repository.beliefs()),
        }
    repository = SkillRepository.create(args.skill_root)
    if command == "skill-turn":
        goal = GoalContract.from_dict(_json(args.goal_json, "goal-json"))
        request = _request(args)
        request = SelectionRequest(
            request.session,
            request.turn_id,
            request.roles,
            request.agent_id,
            request.available_tools,
            request.task_text,
            goal.digest,
        )
        intent = _optional(args.intent_json, IntentFrame)
        signals = (
            ()
            if args.signals_json is None
            else tuple(
                DirectionSignal.from_dict(item) for item in _json(args.signals_json, "signals-json")
            )
        )
        assessment = _optional(args.assessment_json, JudgmentAssessment)
        decision = _optional(args.decision_json, DirectionDecisionReceipt)
        result = SkillTurnService(repository).select_and_pin(
            request,
            episode_id=args.episode_id or request.turn_id,
            intent=intent,
            goal=goal,
            signals=signals,
            assessment=assessment,
            decision=decision,
        )
        return 0, {
            "command": command,
            "manifest": result["manifest"].to_dict(),
            "episode": result["episode"].to_dict(),
            "disclosure": result["disclosure"],
            "abstained_roles": list(result["abstained_roles"]),
            "pinned": result["pinned"],
        }
    if command == "skill-learn":
        if args.used_revision:
            SkillTurnService(repository).mark_used(
                args.episode_id,
                tuple(args.used_revision),
            )
        attribution = (
            None
            if args.attribution_json is None
            else OutcomeAttribution.from_dict(_json(args.attribution_json, "attribution-json"))
        )
        lesson = None if args.lesson_json is None else _json(args.lesson_json, "lesson-json")
        evidence = _json(args.held_out_evidence_json, "held-out-evidence-json")
        if type(evidence) is not list or len(evidence) > 32:
            raise ContractError("invalid held-out evidence array")
        result = learn_episode(
            repository,
            episode_id=args.episode_id,
            outcome=args.outcome,
            evidence_refs=tuple(sorted(set(args.evidence_ref))),
            attribution=attribution,
            lesson=lesson,
            reflect=args.reflect,
            evaluate=args.evaluate,
            activate=args.activate,
            held_out_evidence=tuple(HeldOutTaskEvidence.from_dict(item) for item in evidence),
            operation_key=args.operation_key,
        )
        payload = {
            "command": command,
            "episode": result["episode"].to_dict(),
            "belief_updates": result["belief_updates"],
        }
        for name in ("candidate", "evaluation"):
            if name in result:
                payload[name] = result[name].to_dict()
        for name in ("proposal_digest", "activation"):
            if name in result:
                payload[name] = result[name]
        return 0, payload
    if command == "skill-rollback":
        return 0, {
            "command": command,
            "rollback": repository.rollback(
                skill_name=args.skill_name,
                expected_active_digest=args.expected_active_digest,
                operation_key=args.operation_key,
            ),
        }
    raise ValueError(f"unsupported skill command {command}")


__all__ = ["SKILL_COMMANDS", "is_skill_command", "register_skill_commands", "run_skill_command"]
