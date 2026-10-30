"""Single-writer SQLite/CAS authority for immutable skill revisions."""
# ruff: noqa: E501

from __future__ import annotations

import sqlite3
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import portalocker

from ahc.domain.skill_assets import SkillAsset
from ahc.domain.skill_models import SkillRevision
from ahc.domain.skill_values import canonical_json

from .skill_asset_store import read_asset, store_asset
from .skill_db import initialize_connection, open_connection
from .skill_materializer import materialize_revision, read_materialized
from .skill_repository_activation import SkillActivationStoreMixin
from .skill_repository_errors import (
    CompareAndSetError,
    NotInitializedError,
    OperationConflictError,
    RepositoryError,
    WriterBusyError,
)
from .skill_repository_graph import (
    active_revisions_from_catalog,
    load_verified_revision_catalog,
)
from .skill_repository_manifests import SkillManifestStoreMixin
from .skill_repository_records import SkillRecordStoreMixin
from .skill_repository_status import SkillStatusStoreMixin
from .skill_root import ensure_skill_layout, validate_skill_root


class SkillRepository(
    SkillManifestStoreMixin,
    SkillRecordStoreMixin,
    SkillActivationStoreMixin,
    SkillStatusStoreMixin,
):
    """One SQLite database and one nonblocking writer authority."""

    def __init__(self, root: str | Path, *, create: bool = False) -> None:
        self.root = ensure_skill_layout(root) if create else validate_skill_root(root, create=False)
        self.db_path = self.root / "learning.db"
        self.lock_path = self.root / "writer.lock"
        if not create and not self.db_path.is_file():
            raise NotInitializedError("skill state is not initialized")

    @classmethod
    def create(cls, root: str | Path) -> SkillRepository:
        repository = cls(root, create=True)
        repository.initialize()
        return repository

    def initialize(self) -> None:
        def operation(connection: sqlite3.Connection) -> None:
            initialize_connection(connection)

        self._mutate(operation)

    @contextmanager
    def writer_lock(self) -> Iterator[None]:
        try:
            with portalocker.Lock(
                str(self.lock_path),
                flags=portalocker.LOCK_EX | portalocker.LOCK_NB,
                timeout=0,
            ):
                yield
        except portalocker.exceptions.LockException as exc:
            raise WriterBusyError("skill writer lock is busy") from exc

    @contextmanager
    def _read(self) -> Iterator[sqlite3.Connection]:
        if not self.db_path.is_file():
            raise NotInitializedError("skill state is not initialized")
        connection = open_connection(self.db_path, readonly=True)
        try:
            yield connection
        finally:
            connection.close()

    def _mutate(
        self, operation: Callable[[sqlite3.Connection], Any], *, fail_before_commit: bool = False
    ) -> Any:
        with self.writer_lock():
            existed = self.db_path.is_file()
            connection = open_connection(self.db_path, readonly=False)
            try:
                if not existed:
                    initialize_connection(connection)
                connection.execute("BEGIN IMMEDIATE")
                result = operation(connection)
                if fail_before_commit:
                    raise RepositoryError("injected pre-commit failure")
                connection.commit()
                return result
            except Exception:
                connection.rollback()
                raise
            finally:
                connection.close()

    def _revision_mutation(
        self,
        revision: SkillRevision,
        operation: Callable[[sqlite3.Connection], Any],
        *,
        fail_before_commit: bool = False,
    ) -> Any:
        with self.writer_lock():
            existed = self.db_path.is_file()
            connection = open_connection(self.db_path, readonly=False)
            try:
                if not existed:
                    initialize_connection(connection)
                materialize_revision(self.root, revision)
                connection.execute("BEGIN IMMEDIATE")
                result = operation(connection)
                if fail_before_commit:
                    raise RepositoryError("injected pre-commit failure")
                connection.commit()
                return result
            except Exception:
                connection.rollback()
                raise
            finally:
                connection.close()

    @staticmethod
    def _seq(connection: sqlite3.Connection) -> int:
        return int(
            connection.execute(
                "SELECT COALESCE(MAX(created_seq), 0) + 1 FROM revisions"
            ).fetchone()[0]
        )

    def register_revision(
        self, revision: SkillRevision, *, fail_before_commit: bool = False
    ) -> SkillRevision:
        if type(revision) is not SkillRevision:
            raise RepositoryError("register_revision requires SkillRevision")

        def operation(connection: sqlite3.Connection) -> SkillRevision:
            existing = connection.execute(
                "SELECT metadata_json FROM revisions WHERE revision_digest = ?",
                (revision.revision_digest,),
            ).fetchone()
            encoded = canonical_json(revision.to_dict()).decode("utf-8")
            if existing is not None:
                if existing[0] != encoded:
                    raise RepositoryError("revision digest is already bound to different metadata")
                return revision
            if revision.predecessor_digest is not None:
                predecessor = connection.execute(
                    "SELECT 1 FROM revisions WHERE revision_digest = ?",
                    (revision.predecessor_digest,),
                ).fetchone()
                if predecessor is None:
                    raise RepositoryError("revision predecessor is not materialized")
            sequence = self._seq(connection)
            path = (self.root / "revisions" / revision.skill_name / (
                revision.revision_digest or "")).relative_to(self.root).as_posix()
            connection.execute(
                "INSERT INTO revisions(revision_digest, skill_name, version, cognitive_layer, body_digest, metadata_json, path, predecessor_digest, created_seq) VALUES(?,?,?,?,?,?,?,?,?)",
                (
                    revision.revision_digest,
                    revision.skill_name,
                    revision.version,
                    revision.cognitive_layer,
                    revision.body_digest,
                    encoded,
                    path,
                    revision.predecessor_digest,
                    sequence,
                ),
            )
            for relation in revision.relationships:
                target = connection.execute(
                    "SELECT revision_digest FROM revisions WHERE revision_digest = ?",
                    (relation.target,),
                ).fetchone()
                if target is None:
                    raise RepositoryError("relationship target is not materialized")
                connection.execute(
                    "INSERT INTO relationships(revision_digest, kind, target_digest) VALUES(?,?,?)",
                    (revision.revision_digest, relation.kind, target[0]),
                )
            return revision

        return self._revision_mutation(revision, operation, fail_before_commit=fail_before_commit)

    def stage_assets(self, assets: tuple[SkillAsset, ...], values: dict[str, bytes]) -> None:
        """Operator/importer staging; no activation, paths or execution from model input."""
        if set(values) != {asset.path for asset in assets}:
            raise RepositoryError("resource staging must cover the exact declared assets")
        with self.writer_lock():
            for asset in assets:
                store_asset(self.root, asset, values[asset.path])

    def read_asset(self, revision_digest: str, path: str) -> bytes:
        revision = self.get_revision(revision_digest)
        asset = next((item for item in revision.assets if item.path == path), None)
        if asset is None:
            raise RepositoryError("resource is not declared by the revision")
        return read_asset(self.root, asset)

    def get_revision(self, revision_digest: str) -> SkillRevision:
        with self._read() as connection:
            row = connection.execute(
                "SELECT skill_name, revision_digest FROM revisions WHERE revision_digest = ?",
                (revision_digest,),
            ).fetchone()
        if row is None:
            raise RepositoryError("revision is not registered")
        return read_materialized(self.root, row["skill_name"], row["revision_digest"])

    def list_revisions(self, skill_name: str | None = None) -> tuple[SkillRevision, ...]:
        with self._read() as connection:
            catalog = load_verified_revision_catalog(self.root, connection)
        if skill_name is None:
            return catalog
        return tuple(revision for revision in catalog if revision.skill_name == skill_name)

    def graph_revisions(self) -> tuple[tuple[SkillRevision, ...], tuple[SkillRevision, ...]]:
        """Read one projection-verified active and materialized graph snapshot."""
        with self._read() as connection:
            catalog = load_verified_revision_catalog(self.root, connection)
            active = active_revisions_from_catalog(connection, catalog)
        return active, catalog

    def _graph_revisions(
        self,
        connection: sqlite3.Connection,
        extra_revisions: tuple[SkillRevision, ...] = (),
    ) -> tuple[tuple[SkillRevision, ...], tuple[SkillRevision, ...]]:
        catalog = load_verified_revision_catalog(self.root, connection)
        by_digest = {revision.revision_digest: revision for revision in catalog}
        for revision in extra_revisions:
            stored = by_digest.get(revision.revision_digest)
            if stored is None or stored != revision:
                raise RepositoryError("graph revision disagrees with registered immutable metadata")
        return active_revisions_from_catalog(connection, catalog), catalog

    def get_active(self, skill_name: str) -> SkillRevision | None:
        with self._read() as connection:
            row = connection.execute(
                "SELECT revision_digest FROM active_revisions WHERE skill_name = ?", (skill_name,)
            ).fetchone()
        return None if row is None else self.get_revision(row["revision_digest"])

    def active_revisions(self) -> tuple[SkillRevision, ...]:
        with self._read() as connection:
            catalog = load_verified_revision_catalog(self.root, connection)
            return active_revisions_from_catalog(connection, catalog)


__all__ = [
    "CompareAndSetError",
    "NotInitializedError",
    "OperationConflictError",
    "RepositoryError",
    "SkillRepository",
    "WriterBusyError",
]
