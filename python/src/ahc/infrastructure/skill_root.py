"""Skill-root authority and safe path normalization."""

from __future__ import annotations

import os
from pathlib import Path

from ahc.domain.sessions import ContractError

WINDOWS_WORKSPACE_BASE = Path(r"D:\AHC_workspace")
LINUX_WORKSPACE_BASE: Path | None = None


class SkillRootError(ContractError):
    """A requested root is outside the one permitted workspace base."""


def workspace_base() -> Path:
    if os.name == "nt":
        return WINDOWS_WORKSPACE_BASE
    return LINUX_WORKSPACE_BASE if LINUX_WORKSPACE_BASE is not None else Path.home() / "AHC_workspace"


def _inside(path: Path, base: Path) -> bool:
    return path != base and base in path.parents


def validate_skill_root(value: str | os.PathLike[str], *, create: bool = False) -> Path:
    if not isinstance(value, (str, os.PathLike)):
        raise SkillRootError("skill-root must be an absolute path")
    candidate = Path(value)
    if not candidate.is_absolute():
        raise SkillRootError("skill-root must be an absolute path")
    base = workspace_base().resolve(strict=False)
    resolved = candidate.resolve(strict=False)
    if not _inside(resolved, base):
        raise SkillRootError("skill-root must be a proper descendant of the workspace base")
    nearest = candidate
    while not nearest.exists() and nearest != nearest.parent:
        nearest = nearest.parent
    if not nearest.exists() or not nearest.is_dir():
        raise SkillRootError("skill-root has no existing directory ancestor")
    ancestor = nearest.resolve(strict=True)
    if ancestor != base and base not in ancestor.parents:
        raise SkillRootError("skill-root ancestor resolves outside the workspace base")
    if candidate.exists() and not candidate.is_dir():
        raise SkillRootError("skill-root must be a directory")
    if create:
        candidate.mkdir(parents=True, exist_ok=True)
        if candidate.resolve(strict=True) != resolved:
            raise SkillRootError("skill-root changed while being created")
    return resolved


def ensure_skill_layout(value: str | os.PathLike[str]) -> Path:
    root = validate_skill_root(value, create=True)
    (root / "revisions").mkdir(exist_ok=True)
    return root
