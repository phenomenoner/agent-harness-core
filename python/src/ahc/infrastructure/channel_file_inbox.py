"""Durable file-event staging before download/import; never an executable submission."""

import json

from ahc.domain.channel_ingress import ChannelInboundEvent, ChannelRoute
from ahc.domain.sessions import ContractError
from ahc.domain.skill_values import canonical_json, digest_json
from ahc.domain.turns import _input_text, _sha256_hex
from ahc.infrastructure.working_repository import WorkingSetCorrupt

_TABLE = """CREATE TABLE IF NOT EXISTS runtime_channel_file_inbox (
operation_id TEXT PRIMARY KEY, route_json TEXT NOT NULL, goal_digest TEXT NOT NULL,
event_json TEXT NOT NULL, event_digest TEXT NOT NULL)"""


class ChannelFileInbox:
    def __init__(self, store):
        self.store = store

    @staticmethod
    def _unsubmitted(connection):
        filters = ""
        for table in ("channel_held", "runtime_channel_file_failures"):
            if connection.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
                                  (table,)).fetchone() is not None:
                filters += (f" AND NOT EXISTS (SELECT 1 FROM {table} h "
                            "WHERE h.operation_id=runtime_channel_file_inbox.operation_id)")
        if connection.execute("SELECT 1 FROM sqlite_master WHERE type='table' "
                              "AND name='runtime_submissions'").fetchone() is None:
            return filters
        return filters + (" AND NOT EXISTS (SELECT 1 FROM runtime_submissions s "
                "WHERE s.operation_id=runtime_channel_file_inbox.operation_id)")

    @staticmethod
    def _decode(row):
        try:
            raw = json.loads(row["event_json"])
            event = ChannelInboundEvent.from_dict(raw)
            if (not event.files or event.operation_id != row["operation_id"]
                    or canonical_json(event.to_dict()).decode() != row["event_json"]
                    or digest_json(raw) != row["event_digest"]
                    or canonical_json(event.route.to_dict()).decode() != row["route_json"]):
                raise ValueError()
            _sha256_hex(row["goal_digest"])
            _input_text(event.input_text)
            return event
        except (ValueError, TypeError, KeyError, ContractError):
            raise WorkingSetCorrupt("channel file inbox binding corrupted") from None

    def stage(self, event, goal_digest, *, _transaction=None):
        if type(event) is not ChannelInboundEvent or not event.files:
            raise ContractError("file staging requires a file-bearing event")
        _input_text(event.input_text)
        _sha256_hex(goal_digest)
        encoded = canonical_json(event.to_dict()).decode()
        fingerprint = digest_json(event.to_dict())
        route_json = canonical_json(event.route.to_dict()).decode()

        def write(connection):
            connection.execute(_TABLE)
            from .channel_context_store import bind_context

            bind_context(connection, event)
            current = self.store._load_current(
                connection, event.route.session.ref.virtual_session_id)
            if (current.session != event.route.session or current.status != "active"
                    or current.goal.digest != goal_digest):
                raise ContractError("file staging owner is not current and active")
            row = connection.execute("SELECT * FROM runtime_channel_file_inbox "
                                     "WHERE operation_id=?", (event.operation_id,)).fetchone()
            if row is not None:
                self._decode(row)
                if row["event_digest"] != fingerprint or row["goal_digest"] != goal_digest:
                    raise ContractError("file event operation already bound")
            else:
                for table in ("runtime_submissions", "runtime_turns"):
                    if (connection.execute("SELECT 1 FROM sqlite_master WHERE type='table' "
                                           "AND name=?", (table,)).fetchone() is not None
                            and connection.execute(f"SELECT 1 FROM {table} WHERE operation_id=?",
                                                   (event.operation_id,)).fetchone() is not None):
                        raise ContractError("cannot attach files to an existing operation")
                if connection.execute("SELECT count(*) FROM runtime_channel_file_inbox "
                                      "WHERE route_json=?" + self._unsubmitted(connection),
                                      (route_json,)).fetchone()[0] >= 64:
                    raise ContractError("pending file event limit reached")
                connection.execute("INSERT INTO runtime_channel_file_inbox VALUES(?,?,?,?,?)",
                                   (event.operation_id, route_json, goal_digest,
                                    encoded, fingerprint))
            return {"operation_id": event.operation_id, "event_digest": fingerprint,
                    "status": "awaiting-file-import"}

        return write(_transaction) if _transaction is not None else self.store._mutate(write)

    def pending(self, route):
        """Internal import input, including private download URLs; not CLI diagnostics."""
        if type(route) is not ChannelRoute:
            raise ContractError("file inbox route invalid")
        with self.store._read() as connection:
            current = self.store._load_current(connection, route.session.ref.virtual_session_id)
            if current.session != route.session or current.status != "active":
                raise ContractError("file inbox owner is not current and active")
            if connection.execute("SELECT 1 FROM sqlite_master WHERE type='table' "
                                  "AND name='runtime_channel_file_inbox'").fetchone() is None:
                return []
            rows = connection.execute("SELECT * FROM runtime_channel_file_inbox "
                                      "WHERE route_json=?" + self._unsubmitted(connection)
                                      + " ORDER BY rowid LIMIT 64",
                                      (canonical_json(route.to_dict()).decode(),)).fetchall()
            return [(self._decode(row), row["goal_digest"]) for row in rows]
