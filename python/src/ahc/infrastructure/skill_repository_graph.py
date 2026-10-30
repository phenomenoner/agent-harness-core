"""Immutable relationship catalog and SQLite projection agreement."""

from __future__ import annotations

import sqlite3
from collections.abc import Iterable
from pathlib import Path

from ahc.domain.sessions import ContractError
from ahc.domain.skill_models import SkillRevision

from .skill_materializer import read_materialized
from .skill_repository_errors import RelationshipProjectionError, RepositoryError


def relationship_projection_matches(
    revisions: Iterable[SkillRevision], rows: Iterable[sqlite3.Row]
) -> bool:
    expected = {
        (revision.revision_digest, relation.kind, relation.target)
        for revision in revisions
        for relation in revision.relationships
    }
    actual = {(row["revision_digest"], row["kind"], row["target_digest"]) for row in rows}
    return actual == expected


def load_verified_revision_catalog(
    root: Path, connection: sqlite3.Connection
) -> tuple[SkillRevision, ...]:
    rows = connection.execute(
        "SELECT skill_name, revision_digest FROM revisions ORDER BY skill_name, created_seq"
    ).fetchall()
    try:
        revisions = tuple(
            read_materialized(root, row["skill_name"], row["revision_digest"]) for row in rows
        )
    except (ContractError, OSError, UnicodeError, KeyError) as exc:
        raise RepositoryError("registered immutable revision cannot be verified") from exc
    if any(
        revision.skill_name != row["skill_name"]
        or revision.revision_digest != row["revision_digest"]
        for row, revision in zip(rows, revisions, strict=True)
    ):
        raise RepositoryError("revision projection disagrees with immutable metadata")
    relationship_rows = connection.execute(
        "SELECT revision_digest, kind, target_digest FROM relationships"
    ).fetchall()
    if not relationship_projection_matches(revisions, relationship_rows):
        raise RelationshipProjectionError(
            "relationship projection disagrees with immutable revision metadata"
        )
    return revisions


def active_revisions_from_catalog(
    connection: sqlite3.Connection,
    catalog: tuple[SkillRevision, ...],
) -> tuple[SkillRevision, ...]:
    by_digest = {revision.revision_digest: revision for revision in catalog}
    rows = connection.execute(
        "SELECT skill_name, revision_digest FROM active_revisions ORDER BY skill_name"
    ).fetchall()
    active: list[SkillRevision] = []
    for row in rows:
        revision = by_digest.get(row["revision_digest"])
        if revision is None or revision.skill_name != row["skill_name"]:
            raise RepositoryError("active revision projection is invalid")
        active.append(revision)
    return tuple(active)


__all__ = [
    "active_revisions_from_catalog",
    "load_verified_revision_catalog",
    "relationship_projection_matches",
]
