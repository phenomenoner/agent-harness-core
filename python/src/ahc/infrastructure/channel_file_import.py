"""Download outside transactions, then atomically bind imported artifacts to staged events."""

import json
from dataclasses import replace

from ahc.domain.channel_ingress import ChannelInboundEvent
from ahc.domain.sessions import ContractError
from ahc.domain.skill_values import canonical_json, digest_json
from ahc.domain.workspace import WorkspaceIdentity
from ahc.infrastructure.artifact_repository import ArtifactRepository
from ahc.infrastructure.channel_file_inbox import ChannelFileInbox
from ahc.infrastructure.working_repository import WorkingSetCorrupt

_TABLE = """CREATE TABLE IF NOT EXISTS runtime_channel_file_imports (
operation_id TEXT PRIMARY KEY, event_digest TEXT NOT NULL, selections_json TEXT NOT NULL,
selection_digest TEXT)"""


class LegacyFileImport(ContractError):
    """A readable old import has no verifiable selection provenance."""


def _selection_digest(event, selected):
    return digest_json({"operation_id": event.operation_id,
                        "event_digest": digest_json(event.to_dict()), "selections": selected})


class FileImportCancelled(ContractError):
    pass


def _check_cancelled(cancelled):
    if cancelled():
        raise FileImportCancelled("file import cancelled")


def require_imported_submission(store, connection, operation_id, goal_digest, event):
    if connection.execute("SELECT 1 FROM sqlite_master WHERE type='table' "
                          "AND name='runtime_channel_file_inbox'").fetchone() is None:
        return
    row = connection.execute("SELECT * FROM runtime_channel_file_inbox WHERE operation_id=?",
                             (operation_id,)).fetchone()
    if row is None:
        return
    original = ChannelFileInbox._decode(row)
    if (event != replace(original, files=()) or goal_digest != row["goal_digest"]
            or ChannelFileImport(store)._existing(connection, original) is None):
        raise ContractError("staged file event requires matching complete import")


class ChannelFileImport:
    def __init__(self, store):
        self.store = store

    def submit(self, event, goal_digest):
        """Admit original text only after its immutable import record is verified."""
        from .runtime_inbox import RuntimeInbox

        if type(event) is not ChannelInboundEvent or not event.files:
            raise ContractError("file submission event invalid")

        def write(connection):
            self._staged(connection, event, goal_digest)
            if self._existing(connection, event) is None:
                raise ContractError("file import incomplete")
            return RuntimeInbox(self.store).submit(
                event.route.session, event.operation_id, goal_digest, event.input_text,
                channel_event=replace(event, files=()), _transaction=connection)

        return self.store._mutate(write)

    def _staged(self, connection, event, goal_digest):
        row = connection.execute("SELECT * FROM runtime_channel_file_inbox WHERE operation_id=?",
                                 (event.operation_id,)).fetchone()
        if (row is None or ChannelFileInbox._decode(row) != event
                or row["goal_digest"] != goal_digest):
            raise ContractError("file import staged event mismatch")
        current = self.store._load_current(connection, event.route.session.ref.virtual_session_id)
        if (current.session != event.route.session or current.status != "active"
                or current.goal.digest != goal_digest):
            raise ContractError("file import owner changed")
        from .runtime_channel_store import _exists

        if (_exists(connection, "channel_held") and connection.execute(
                "SELECT 1 FROM channel_held WHERE operation_id=?",
                (event.operation_id,)).fetchone()):
            raise FileImportCancelled("file import held by conversation reset")

    def _existing(self, connection, event, *, require_provenance=False):
        if connection.execute("SELECT 1 FROM sqlite_master WHERE type='table' "
                              "AND name='runtime_channel_file_imports'").fetchone() is None:
            return None
        row = connection.execute("SELECT * FROM runtime_channel_file_imports WHERE operation_id=?",
                                 (event.operation_id,)).fetchone()
        if row is None:
            return None
        try:
            selected = json.loads(row["selections_json"])
            if (row["event_digest"] != digest_json(event.to_dict())
                    or canonical_json(selected).decode() != row["selections_json"]
                    or type(selected) is not list or len(selected) != len(event.files)):
                raise ValueError()
            for item, reference in zip(selected, event.files, strict=True):
                if (set(item) != {"artifact_id", "filename"}
                        or item["filename"] != reference.filename):
                    raise ValueError()
                artifact = connection.execute("SELECT * FROM runtime_artifacts WHERE artifact_id=?",
                                              (item["artifact_id"],)).fetchone()
                if artifact is None:
                    raise ValueError()
                manifest, _ = ArtifactRepository._decode(artifact)
                if (manifest.producer.session != event.route.session
                        or manifest.byte_count != reference.byte_count
                        or manifest.media_type != reference.media_type):
                    raise ValueError()
            seal = row["selection_digest"] if "selection_digest" in row.keys() else None
            if seal is not None and seal != _selection_digest(event, selected):
                raise ValueError()
            if require_provenance and seal is None:
                raise LegacyFileImport("legacy file import requires fresh upload for native media")
            return selected
        except LegacyFileImport:
            raise
        except (ValueError, TypeError, KeyError, ContractError):
            raise WorkingSetCorrupt("file import binding corrupted") from None

    def run(self, event, goal_digest, producer, downloader, *, fail_before_commit=False,
            cancelled=lambda: False):
        if (type(event) is not ChannelInboundEvent or not event.files
                or type(producer) is not WorkspaceIdentity
                or producer.session != event.route.session):
            raise ContractError("file import workspace binding invalid")
        if not callable(cancelled):
            raise ContractError("file import cancellation invalid")
        _check_cancelled(cancelled)
        with self.store.execution_lock(session=producer.session, workspace_id=producer.workspace_id):
            with self.store._read() as connection:
                self._staged(connection, event, goal_digest)
                self.store._require_workspace(connection, producer, "ready")
                existing = self._existing(connection, event)
                if existing is not None:
                    return existing
            contents = []
            for reference in event.files:
                _check_cancelled(cancelled)
                contents.append(downloader.fetch(reference))
                _check_cancelled(cancelled)
            if any(type(data) is not bytes or len(data) != reference.byte_count
                   for data, reference in zip(contents, event.files, strict=True)):
                raise ContractError("file import content size mismatch")

            def write(connection):
                _check_cancelled(cancelled)
                self._staged(connection, event, goal_digest)
                self.store._require_workspace(connection, producer, "ready")
                connection.execute(_TABLE)
                columns = {row[1] for row in connection.execute(
                    "PRAGMA table_info(runtime_channel_file_imports)")}
                if "selection_digest" not in columns:
                    connection.execute("ALTER TABLE runtime_channel_file_imports "
                                       "ADD COLUMN selection_digest TEXT")
                selected = []
                for reference, data in zip(event.files, contents, strict=True):
                    manifest = ArtifactRepository(self.store).publish(
                        producer, data, reference.media_type, _transaction=connection)
                    selected.append({"artifact_id": manifest.artifact_id,
                                     "filename": reference.filename})
                connection.execute("INSERT INTO runtime_channel_file_imports "
                                   "(operation_id,event_digest,selections_json,selection_digest) "
                                   "VALUES(?,?,?,?)",
                                   (event.operation_id, digest_json(event.to_dict()),
                                    canonical_json(selected).decode(),
                                    _selection_digest(event, selected)))
                _check_cancelled(cancelled)
                return selected

            return self.store._mutate(write, fail_before_commit=fail_before_commit)
