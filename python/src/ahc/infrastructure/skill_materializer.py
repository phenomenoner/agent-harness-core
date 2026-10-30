"""Immutable revision-directory materialization and verification."""

from __future__ import annotations

import json
from pathlib import Path

from ahc.domain.sessions import ContractError
from ahc.domain.skill_models import SkillRevision
from ahc.domain.skill_values import canonical_json, digest_bytes

from .skill_asset_store import read_asset


class MaterializationError(ContractError):
    """Revision files are missing, tampered, or cannot be materialized."""


def revision_directory(root: Path, revision: SkillRevision) -> Path:
    return root / "revisions" / revision.skill_name / (revision.revision_digest or "")


def _manifest_bytes(revision: SkillRevision) -> bytes:
    return canonical_json(revision.to_dict()) + b"\n"


def materialize_revision(root: Path, revision: SkillRevision) -> Path:
    if revision.revision_digest is None:
        raise MaterializationError("revision has no content digest")
    directory = revision_directory(root, revision)
    parent = directory.parent
    parent.mkdir(parents=True, exist_ok=True)
    if directory.exists() or directory.is_symlink():
        if directory.is_symlink() or not directory.is_dir():
            raise MaterializationError("revision directory is not a real directory")
        verify_materialized(directory, revision)
        return directory
    directory.mkdir()
    manifest = directory / "manifest.json"
    body = directory / "SKILL.md"
    try:
        manifest.write_bytes(_manifest_bytes(revision))
        body.write_bytes(revision.body.encode("utf-8"))
        verify_materialized(directory, revision)
    except Exception:
        raise
    return directory


def verify_materialized(directory: Path, expected: SkillRevision | None = None) -> SkillRevision:
    if directory.is_symlink() or not directory.is_dir():
        raise MaterializationError("revision directory is not a real directory")
    manifest = directory / "manifest.json"
    body = directory / "SKILL.md"
    if not manifest.is_file() or not body.is_file() or manifest.is_symlink() or body.is_symlink():
        raise MaterializationError("revision directory is incomplete")
    try:
        parsed = json.loads(manifest.read_bytes().decode("utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise MaterializationError("manifest.json is not valid JSON") from exc
    revision = SkillRevision.from_dict(parsed)
    body_bytes = body.read_bytes()
    if (
        body_bytes != revision.body.encode("utf-8")
        or digest_bytes(body_bytes) != revision.body_digest
    ):
        raise MaterializationError("SKILL.md does not match its manifest")
    if expected is not None and revision != expected:
        raise MaterializationError("materialized revision differs from expected immutable value")
    for asset in revision.assets:
        read_asset(directory.parent.parent.parent, asset)
    return revision


def read_materialized(root: Path, skill_name: str, revision_digest: str) -> SkillRevision:
    directory = root / "revisions" / skill_name / revision_digest
    return verify_materialized(directory)


def materialized_directories(root: Path) -> tuple[Path, ...]:
    revisions = root / "revisions"
    if not revisions.exists():
        return ()
    result: list[Path] = []
    for skill_dir in sorted(revisions.iterdir()):
        if not skill_dir.is_dir() or skill_dir.is_symlink():
            continue
        for directory in sorted(skill_dir.iterdir()):
            if directory.is_dir() or directory.is_symlink():
                result.append(directory)
    return tuple(result)
