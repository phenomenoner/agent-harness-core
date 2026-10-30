"""Pinned manifest persistence under strict virtual-session identity."""

from __future__ import annotations

import json
import sqlite3

from ahc.domain.sessions import ContractError, VirtualSessionIdentity
from ahc.domain.skill_records import SkillManifest
from ahc.domain.skill_values import canonical_json, digest, digest_json

from .skill_repository_graph import active_revisions_from_catalog, load_verified_revision_catalog


class SkillManifestStoreMixin:
    def pin_revision_availability(self, session: VirtualSessionIdentity, *,
                                  expected_revisions=None) -> tuple[tuple, bool]:
        """Snapshot the active set atomically; only a new generation can adopt new revisions."""
        if expected_revisions is not None:
            if type(expected_revisions) is not tuple or len(expected_revisions) > 4096:
                raise ContractError("expected skill revisions must be a bounded unique tuple")
            for value in expected_revisions:
                digest(value, "expected skill revision")
            if len(set(expected_revisions)) != len(expected_revisions):
                raise ContractError("expected skill revisions must be a bounded unique tuple")
            expected_revisions = tuple(sorted(expected_revisions))

        def operation(connection: sqlite3.Connection) -> tuple[tuple[str, ...], bool]:
            row = connection.execute(
                "SELECT pin_json, pin_digest FROM revision_availability "
                "WHERE session_id = ? AND generation = ?",
                (session.ref.virtual_session_id, session.ref.generation),
            ).fetchone()
            if row is not None:
                pin = json.loads(row[0])
                if digest_json(pin) != row[1] or pin["session"] != session.to_dict():
                    raise ContractError("revision availability identity or digest mismatch")
                if (expected_revisions is not None
                        and tuple(sorted(pin["revisions"])) != expected_revisions):
                    raise ContractError("successor skill pin differs from expected revisions")
                return tuple(pin["revisions"]), True
            catalog = load_verified_revision_catalog(self.root, connection)
            active = active_revisions_from_catalog(connection, catalog)
            digests = tuple(item.revision_digest for item in active)
            if expected_revisions is not None and tuple(sorted(digests)) != expected_revisions:
                raise ContractError("active skill catalog differs from expected revisions")
            pin = {"session": session.to_dict(), "revisions": list(digests)}
            connection.execute(
                "INSERT INTO revision_availability VALUES(?,?,?,?)",
                (session.ref.virtual_session_id, session.ref.generation,
                 canonical_json(pin).decode("utf-8"), digest_json(pin)),
            )
            return digests, False

        digests, existed = self._mutate(operation)
        return tuple(self.get_revision(value) for value in digests), existed

    def save_manifest(
        self, manifest: SkillManifest, *, request_digest: str | None = None,
    ) -> SkillManifest:
        encoded = canonical_json(manifest.to_dict()).decode("utf-8")

        def operation(connection: sqlite3.Connection) -> SkillManifest:
            row = connection.execute(
                "SELECT manifest_json, request_digest FROM manifests "
                "WHERE session_id = ? AND generation = ? AND turn_id = ?",
                (manifest.session.ref.virtual_session_id, manifest.session.ref.generation,
                 manifest.turn_id),
            ).fetchone()
            if row is not None:
                if row[0] != encoded or row[1] != request_digest:
                    raise ContractError(
                        "virtual-session turn already has a different manifest"
                    )
                return SkillManifest.from_dict(json.loads(row[0]))
            connection.execute(
                "INSERT INTO manifests(manifest_digest, session_id, generation, turn_id, "
                "goal_digest, manifest_json, request_digest, created_seq) VALUES(?,?,?,?,?,?,?,?)",
                (
                    manifest.manifest_digest,
                    manifest.session.ref.virtual_session_id,
                    manifest.session.ref.generation,
                    manifest.turn_id,
                    manifest.goal_digest,
                    encoded,
                    request_digest,
                    self._seq(connection),
                ),
            )
            return manifest

        return self._mutate(operation)

    def get_manifest(
        self, session: VirtualSessionIdentity, *, turn_id: str | None = None,
        request_digest: str | None = None,
    ) -> SkillManifest | None:
        with self._read() as connection:
            query = "SELECT * FROM manifests WHERE session_id = ? AND generation = ?"
            args = [session.ref.virtual_session_id, session.ref.generation]
            if turn_id is not None:
                query += " AND turn_id = ?"
                args.append(turn_id)
            rows = connection.execute(query, args).fetchall()
        if not rows:
            return None
        if len(rows) != 1:
            raise ContractError("turn_id is required for multiple selected manifests")
        row = rows[0]
        if request_digest is not None and row["request_digest"] != request_digest:
            raise ContractError("turn request context differs from pinned manifest")
        manifest = SkillManifest.from_dict(json.loads(row["manifest_json"]))
        if manifest.session != session:
            raise ContractError("virtual-session identity collision for pinned manifest")
        return manifest


__all__ = ["SkillManifestStoreMixin"]
