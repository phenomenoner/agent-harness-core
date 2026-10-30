"""Read-only integrity and orphan diagnostics for skill state."""
# ruff: noqa: E501

from __future__ import annotations

import json
from pathlib import Path

from ahc.domain.skill_graph import validate_active_graph
from ahc.domain.skill_records import SkillManifest

from .skill_doctor_judgment import judgment_issues
from .skill_materializer import materialized_directories, verify_materialized
from .skill_repository import NotInitializedError, RepositoryError, SkillRepository
from .skill_repository_graph import relationship_projection_matches
from .skill_root import SkillRootError, validate_skill_root


def inventory(root: Path) -> tuple[str, ...]:
    if not root.exists():
        return ()
    return tuple(
        sorted(path.relative_to(root).as_posix() for path in root.rglob("*") if path.is_file())
    )


def doctor(root_value: str | Path) -> dict[str, object]:
    try:
        root = validate_skill_root(root_value, create=False)
    except SkillRootError as exc:
        return {"ok": False, "initialized": False, "error": str(exc), "issues": ["invalid-root"]}
    before = inventory(root)
    try:
        repository = SkillRepository(root)
    except NotInitializedError as exc:
        return {
            "ok": False,
            "initialized": False,
            "error": str(exc),
            "issues": ["not-initialized"],
            "inventory": list(before),
        }
    issues: list[str] = []
    try:
        status = repository.status()
        if status["quick_check"] != "ok":
            issues.append("database-quick-check")
        with repository._read() as connection:
            rows = connection.execute(
                "SELECT skill_name, revision_digest, path, body_digest FROM revisions ORDER BY skill_name, revision_digest"
            ).fetchall()
            active = connection.execute(
                "SELECT skill_name, revision_digest FROM active_revisions ORDER BY skill_name"
            ).fetchall()
            manifests = connection.execute("SELECT manifest_json FROM manifests").fetchall()
            relationships = connection.execute(
                "SELECT revision_digest, kind, target_digest FROM relationships"
            ).fetchall()
        registered = {}
        registered_directories = set()
        for row in rows:
            try:
                relative = "/".join(("revisions", row["skill_name"], row["revision_digest"]))
                if row["path"] not in (relative, relative.replace("/", "\\")):
                    raise RepositoryError("revision path differs from identity")
                directory = root.joinpath(*relative.split("/"))
                revision = verify_materialized(directory)
                registered[revision.revision_digest] = revision
                registered_directories.add(directory.resolve())
                if (
                    revision.revision_digest != row["revision_digest"]
                    or revision.body_digest != row["body_digest"]
                    or revision.skill_name != row["skill_name"]
                ):
                    issues.append("revision-row-digest")
            except (RepositoryError, OSError, ValueError):
                issues.append(f"revision:{row['revision_digest']}")
        row_digests = {row["revision_digest"] for row in rows}
        active_revisions = []
        for row in active:
            revision = registered.get(row["revision_digest"])
            if revision is None:
                issues.append(f"active-missing:{row['skill_name']}")
            elif revision.skill_name != row["skill_name"]:
                issues.append(f"active-name:{row['skill_name']}")
            else:
                active_revisions.append(revision)
        if not relationship_projection_matches(registered.values(), relationships):
            issues.append("relationship-projection")
        if any(row["target_digest"] not in row_digests for row in relationships):
            issues.append("relationship-target")
        for row in manifests:
            try:
                manifest = SkillManifest.from_dict(json.loads(row["manifest_json"]))
                for entry in manifest.entries:
                    if entry.revision_digest not in row_digests:
                        issues.append(f"manifest-missing:{entry.revision_digest}")
            except (ValueError, TypeError, KeyError):
                issues.append("manifest-invalid")
        graph = validate_active_graph(tuple(active_revisions), tuple(registered.values()))
        issues.extend(f"graph:{issue.code}" for issue in graph.issues)
        for directory in materialized_directories(root):
            if directory.resolve() not in registered_directories:
                issues.append(f"orphan:{directory.relative_to(root).as_posix()}")
        issues.extend(judgment_issues(repository))
        after = inventory(root)
        if before != after:
            issues.append("doctor-mutated-inventory")
        return {
            "ok": not issues,
            "initialized": True,
            "issues": issues,
            "status": status,
            "inventory_before": list(before),
            "inventory_after": list(after),
        }
    except Exception as exc:
        return {"ok": False, "initialized": True, "issues": [f"doctor-error:{type(exc).__name__}"]}


__all__ = ["doctor", "inventory"]
