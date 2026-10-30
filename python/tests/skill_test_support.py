"""Shared bounded builders for skill/judgment discriminating tests."""

from __future__ import annotations

from pathlib import Path

import pytest

from ahc.domain import (
    VIRTUAL_SESSION_REF_SCHEMA_VERSION,
    VIRTUAL_SESSION_SCHEMA_VERSION,
    GoalContract,
    SelectionRequest,
    VirtualSessionIdentity,
    VirtualSessionRef,
)
from ahc.domain.judgment_models import GOAL_SCHEMA
from ahc.domain.skill_models import SkillRelationship, SkillRevision
from ahc.domain.skill_values import SKILL_REVISION_SCHEMA_VERSION
from ahc.infrastructure import skill_root as skill_root_module
from ahc.infrastructure.skill_repository import SkillRepository


@pytest.fixture
def skill_root(tmp_path: Path, monkeypatch) -> Path:
    monkeypatch.setattr(skill_root_module, "WINDOWS_WORKSPACE_BASE", tmp_path)
    monkeypatch.setattr(skill_root_module, "LINUX_WORKSPACE_BASE", tmp_path)
    return tmp_path / "skills"


@pytest.fixture
def repository(skill_root: Path) -> SkillRepository:
    return SkillRepository.create(skill_root)


def session(generation: int, session_id: str = "session-test") -> VirtualSessionIdentity:
    predecessor = None
    if generation > 1:
        predecessor = VirtualSessionRef(
            VIRTUAL_SESSION_REF_SCHEMA_VERSION, session_id, generation - 1
        )
    return VirtualSessionIdentity(
        VIRTUAL_SESSION_SCHEMA_VERSION,
        VirtualSessionRef(VIRTUAL_SESSION_REF_SCHEMA_VERSION, session_id, generation),
        "agent-test",
        "route-test",
        predecessor,
    )


def goal(*, acknowledgement: str = "acknowledged") -> GoalContract:
    return GoalContract(
        GOAL_SCHEMA,
        "goal-test",
        1,
        None,
        "Improve bounded offline work",
        "Select and learn a local skill",
        ("Complete the local loop",),
        ("offline skill loop",),
        ("no external effects",),
        ("local authority only",),
        (),
        acknowledgement,
    )


def request(generation: int, goal_digest: str, task: str = "analyze task") -> SelectionRequest:
    return SelectionRequest(
        session(generation),
        f"turn-{generation}",
        ("procedure",),
        "agent-test",
        (),
        task,
        goal_digest,
    )


def revision(
    *,
    version: str = "1.0.0",
    predecessor: str | None = None,
    name: str = "analyze-skill",
    trigger: str = "analyze",
    layer: str = "how-procedure",
    roles: tuple[str, ...] = ("procedure",),
    relationships: tuple[SkillRelationship, ...] = (),
    required_tools: tuple[str, ...] = (),
) -> SkillRevision:
    body = (
        f"# {name}\n\n"
        "## Purpose\nBounded analysis.\n\n"
        f"## Activation\nUse for {trigger} tasks.\n\n"
        "## Procedure\nInspect the provided evidence.\n\n"
        "## Verification\nCheck the bounded result.\n\n"
        "## Boundaries\nRemain within the accepted goal.\n"
    )
    return SkillRevision(
        SKILL_REVISION_SCHEMA_VERSION,
        name,
        version,
        "Bounded analysis procedure",
        layer,
        roles,
        (),
        required_tools,
        (trigger,),
        ("bounded report",),
        (f"{trigger} task",),
        ("summarize task",),
        relationships,
        predecessor,
        body,
    )
