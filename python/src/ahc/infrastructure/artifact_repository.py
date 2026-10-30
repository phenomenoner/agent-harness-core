"""Content-addressed artifact bytes in the existing runtime database."""

import json

from ahc.domain.artifacts import ArtifactManifest, ArtifactNotPublished, content_digest
from ahc.domain.sessions import ContractError, VirtualSessionIdentity
from ahc.domain.skill_values import canonical_json
from ahc.domain.turns import _sha256_hex
from ahc.domain.workspace import WorkspaceIdentity

from .runtime_recovery_access import historical_session_allows
from .working_repository import WorkingSetCorrupt


class ArtifactRepository:
    def __init__(self, store):
        self.store = store

    def incoming(self, identity, *, require_provenance=False):
        from .channel_file_import import ChannelFileImport
        from .channel_file_inbox import ChannelFileInbox
        from .runtime_execution_scope import workspace_request

        request = workspace_request(self.store, identity.session, identity.workspace_id)
        with self.store._read() as connection:
            self.store._require_workspace(connection, identity, "ready")
            if connection.execute("SELECT 1 FROM sqlite_master WHERE type='table' "
                                  "AND name='runtime_channel_file_inbox'").fetchone() is None:
                return []
            row = connection.execute("SELECT * FROM runtime_channel_file_inbox "
                                     "WHERE operation_id=?",
                                     (request.operation_id,)).fetchone()
            if row is None:
                return []
            event = ChannelFileInbox._decode(row)
            if (event.route.session != identity.session
                    or row["goal_digest"] != request.goal_digest):
                raise ContractError("incoming artifact turn binding mismatch")
            result = ChannelFileImport(self.store)._existing(
                connection, event, require_provenance=require_provenance)
            if result is None:
                raise ContractError("incoming artifact import incomplete")
            return result

    @staticmethod
    def _exists(connection):
        return connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='runtime_artifacts'"
        ).fetchone() is not None

    @staticmethod
    def _decode(row):
        try:
            content = row["content"]
            manifest = ArtifactManifest.from_dict(json.loads(row["manifest_json"]))
            if (
                type(content) is not bytes
                or row["artifact_id"] != manifest.artifact_id
                or row["manifest_json"] != canonical_json(manifest.to_dict()).decode()
                or manifest.content_digest != content_digest(content)
                or manifest.byte_count != len(content)
            ):
                raise ValueError()
            return manifest, content
        except (KeyError, TypeError, ValueError, json.JSONDecodeError, ContractError):
            raise WorkingSetCorrupt("artifact record corrupted") from None

    def publish(self, producer, content, media_type, *, lineage=(), fail_before_commit=False,
                _transaction=None):
        try:
            if type(producer) is not WorkspaceIdentity or type(lineage) is not tuple:
                raise ContractError("artifact publish input invalid")
            manifest = ArtifactManifest(
                producer, content_digest(content), media_type, len(content), lineage
            )
            encoded = canonical_json(manifest.to_dict()).decode()
        except Exception as error:
            raise ArtifactNotPublished("artifact-publication-rejected") from error
        mutating = False

        def write(connection):
            nonlocal mutating
            if not self._exists(connection):
                raise WorkingSetCorrupt("artifact storage unavailable")
            self.store._require_workspace(connection, producer, "ready")
            for artifact_id in lineage:
                row = connection.execute(
                    "SELECT * FROM runtime_artifacts WHERE artifact_id=?", (artifact_id,)
                ).fetchone()
                if row is None:
                    raise ContractError("artifact lineage unavailable")
                ancestor, _ = self._decode(row)
                if (
                    ancestor.producer.session != producer.session
                    or ancestor.producer.workspace_id != producer.workspace_id
                ):
                    raise ContractError("artifact lineage producer mismatch")
            row = connection.execute(
                "SELECT * FROM runtime_artifacts WHERE artifact_id=?", (manifest.artifact_id,)
            ).fetchone()
            if row is not None:
                existing, stored = self._decode(row)
                if existing != manifest or stored != content:
                    raise WorkingSetCorrupt("artifact identifier binding corrupted")
                return existing
            mutating = True
            connection.execute(
                "INSERT INTO runtime_artifacts VALUES(?,?,?)",
                (manifest.artifact_id, encoded, content),
            )
            return manifest

        try:
            if _transaction is not None:
                if fail_before_commit:
                    raise ContractError("nested artifact fault injection unsupported")
                return write(_transaction)
            return self.store._mutate(write, fail_before_commit=fail_before_commit)
        except Exception as error:
            if not mutating:
                raise ArtifactNotPublished("artifact-publication-rejected") from error
            if _transaction is not None:
                raise  # The outer transaction owns commit/rollback, not this call.
            # Reconcile this immutable local record once; never repeat the write.
            try:
                with self.store._read() as connection:
                    row = connection.execute(
                        "SELECT * FROM runtime_artifacts WHERE artifact_id=?",
                        (manifest.artifact_id,),
                    ).fetchone()
                    if row is None:
                        raise ArtifactNotPublished("artifact-publication-not-committed") from error
                    existing, stored = self._decode(row)
                    if existing == manifest and stored == content:
                        return existing
            except ArtifactNotPublished:
                raise
            except Exception:
                pass  # An unreadable result remains unknown to the caller.
            raise

    def read(self, session, artifact_id):
        if type(session) is not VirtualSessionIdentity:
            raise ContractError("artifact read session invalid")
        _sha256_hex(artifact_id)
        with self.store._read() as connection:
            if not self._exists(connection):
                return None
            row = connection.execute(
                "SELECT * FROM runtime_artifacts WHERE artifact_id=?", (artifact_id,)
            ).fetchone()
            if row is None:
                return None
            manifest, content = self._decode(row)
            if (manifest.producer.session != session
                    and not historical_session_allows(self.store, connection, session,
                                            manifest.producer.session)):
                raise ContractError("artifact read owner mismatch")
            return manifest, content
