"""Atomic handoff from a scheduled occurrence to its independent workbench."""

from dataclasses import replace

from ahc.domain.channel_ingress import ChannelInboundEvent
from ahc.domain.sessions import ContractError, VirtualSessionIdentity
from ahc.domain.skill_values import canonical_json, digest_json
from ahc.domain.working_sets import WorkingSetSnapshot

from .runtime_channel_store import _exists, _route
from .runtime_inbox import _TABLE as SUBMISSIONS_TABLE
from .runtime_inbox import RuntimeInbox
from .working_db import insert_snapshot, snapshot_pointer
from .working_repository import WorkingSetCorrupt, WorkingSetNotFound

TABLE = """CREATE TABLE IF NOT EXISTS runtime_background_dispatch (
source_operation_id TEXT PRIMARY KEY REFERENCES runtime_submissions(operation_id),
source_session_json TEXT NOT NULL, source_digest TEXT NOT NULL,
schedule_id TEXT NOT NULL, due_ms INTEGER NOT NULL,
worker_session_json TEXT NOT NULL, workspace_id TEXT NOT NULL,
worker_operation_id TEXT NOT NULL UNIQUE REFERENCES runtime_submissions(operation_id))"""


class BackgroundScheduleDispatch:
    def __init__(self, store):
        self.store = store

        def prepare(connection):
            connection.execute(SUBMISSIONS_TABLE)
            connection.execute("CREATE TABLE IF NOT EXISTS channel_held (operation_id TEXT PRIMARY KEY)")
            connection.execute(TABLE)

        store._mutate(prepare)

    @staticmethod
    def _identity(payload, schedule_id):
        parent = VirtualSessionIdentity.from_dict(payload["session"])
        key = digest_json({"session": payload["session"], "goal": payload["goal_digest"],
                           "schedule": schedule_id})
        return (VirtualSessionIdentity.initial("schedule-" + key, parent.agent_id, parent.route_id),
                "schedule-" + key)

    @classmethod
    def _record(cls, connection, row):
        source = connection.execute("SELECT * FROM runtime_submissions WHERE operation_id=?",
                                    (row["source_operation_id"],)).fetchone()
        if source is None:
            raise WorkingSetCorrupt("background source submission is missing")
        payload = RuntimeInbox._decode(source)
        occurrence = connection.execute("SELECT schedule_id,due_ms FROM "
            "runtime_schedule_occurrences WHERE operation_id=?",
            (row["source_operation_id"],)).fetchone()
        if (occurrence is None or occurrence["schedule_id"] != row["schedule_id"]
                or occurrence["due_ms"] != row["due_ms"]
                or connection.execute("SELECT 1 FROM channel_held WHERE operation_id=?",
                    (row["source_operation_id"],)).fetchone() is None
                or connection.execute("SELECT 1 FROM runtime_turns WHERE operation_id=?",
                    (row["source_operation_id"],)).fetchone() is not None):
            raise WorkingSetCorrupt("background occurrence custody corrupted")
        session, workspace = cls._identity(payload, row["schedule_id"])
        if (row["source_digest"] != source["payload_digest"]
                or row["source_session_json"] != source["session_json"]
                or row["worker_session_json"] != canonical_json(session.to_dict()).decode()
                or row["workspace_id"] != workspace):
            raise WorkingSetCorrupt("background handoff binding corrupted")
        worker = connection.execute("SELECT * FROM runtime_submissions WHERE operation_id=?",
                                    (row["worker_operation_id"],)).fetchone()
        if (worker is None or RuntimeInbox._decode(worker) != RuntimeInbox._payload(
                session, row["worker_operation_id"], payload["goal_digest"], payload["input_text"])):
            raise WorkingSetCorrupt("background worker submission differs from source")
        return {**dict(row), "worker_session": session, "source_payload": payload}

    def claim_one(self, parent):
        def claim(connection):
            current = self.store._load_current(connection, parent.ref.virtual_session_id)
            if current.session != parent or current.status != "active":
                return None
            row = connection.execute(
                "SELECT s.*,o.schedule_id,o.due_ms FROM runtime_submissions s "
                "JOIN runtime_schedule_occurrences o ON o.operation_id=s.operation_id "
                "WHERE s.session_json=? AND s.goal_digest=? "
                "AND NOT EXISTS (SELECT 1 FROM runtime_turns t WHERE t.operation_id=s.operation_id) "
                "AND NOT EXISTS (SELECT 1 FROM channel_held h WHERE h.operation_id=s.operation_id) "
                "ORDER BY o.due_ms,s.rowid LIMIT 1",
                (canonical_json(parent.to_dict()).decode(), current.goal.digest),
            ).fetchone()
            if row is None:
                return None
            payload = RuntimeInbox._decode(row)
            session, workspace = self._identity(payload, row["schedule_id"])
            try:
                existing = self.store._load_current(connection, session.ref.virtual_session_id)
            except WorkingSetNotFound:
                initial = WorkingSetSnapshot.initial(session, current.goal)
                insert_snapshot(connection, initial)
                connection.execute("INSERT INTO current_working_sets VALUES(?,?,?,?,?,?)",
                                   snapshot_pointer(initial))
            else:
                if existing.session != session or existing.goal != current.goal:
                    raise WorkingSetCorrupt("background workbench owner changed")
            route_row = connection.execute(
                "SELECT * FROM runtime_channel_routes WHERE operation_id=?",
                (payload["operation_id"],),
            ).fetchone()
            event = None
            if route_row is not None:
                route = _route(route_row)
                if route.session != parent:
                    raise WorkingSetCorrupt("background source route owner differs")
                event_id = "background-" + digest_json({"source": payload["operation_id"],
                                                       "session": session.to_dict()})
                event = ChannelInboundEvent(replace(route, session=session), event_id,
                                            payload["input_text"])
            operation = event.operation_id if event is not None else (
                "background-" + digest_json({"source": payload["operation_id"],
                                            "session": session.to_dict()}))
            RuntimeInbox(self.store).submit(session, operation, payload["goal_digest"],
                payload["input_text"], channel_event=event, _transaction=connection,
                _skip_channel_controls=True)
            connection.execute("INSERT INTO runtime_background_dispatch VALUES(?,?,?,?,?,?,?,?)", (
                payload["operation_id"], row["session_json"], row["payload_digest"],
                row["schedule_id"], row["due_ms"], canonical_json(session.to_dict()).decode(),
                workspace, operation))
            # Older binaries already respect this hold. A downgrade cannot replay
            # a source occurrence that was handed to an independent workbench.
            connection.execute("INSERT INTO channel_held VALUES(?)", (payload["operation_id"],))
            saved = connection.execute("SELECT * FROM runtime_background_dispatch "
                                       "WHERE source_operation_id=?", (payload["operation_id"],)).fetchone()
            return self._record(connection, saved)

        return self.store._mutate(claim)

    @classmethod
    def origin_route(cls, connection, worker_session, worker_operation):
        if not _exists(connection, "runtime_background_dispatch"):
            raise ContractError("background origin is not bound")
        row = connection.execute("SELECT * FROM runtime_background_dispatch "
                                 "WHERE worker_operation_id=?", (worker_operation,)).fetchone()
        if row is None:
            raise ContractError("background origin is not bound")
        record = cls._record(connection, row)
        if record["worker_session"] != worker_session:
            raise WorkingSetCorrupt("background origin session mismatch")
        source = connection.execute("SELECT * FROM runtime_channel_routes WHERE operation_id=?",
                                    (row["source_operation_id"],)).fetchone()
        worker = connection.execute("SELECT * FROM runtime_channel_routes WHERE operation_id=?",
                                    (worker_operation,)).fetchone()
        if source is None or worker is None:
            raise WorkingSetCorrupt("background origin route missing")
        route = _route(source)
        if _route(worker) != replace(route, session=worker_session):
            raise WorkingSetCorrupt("background destination differs from origin")
        return route

    def records(self, parent):
        with self.store._read() as connection:
            rows = connection.execute("SELECT * FROM runtime_background_dispatch "
                "WHERE source_session_json=? ORDER BY due_ms,source_operation_id",
                (canonical_json(parent.to_dict()).decode(),)).fetchall()
            return tuple(self._record(connection, row) for row in rows)

    def pending(self, parent):
        with self.store._read() as connection:
            rows = connection.execute(
                "SELECT d.* FROM runtime_background_dispatch d LEFT JOIN runtime_turns t "
                "ON t.operation_id=d.worker_operation_id WHERE d.source_session_json=? AND "
                "(t.status IS NULL OR t.status='submitted' OR EXISTS "
                "(SELECT 1 FROM runtime_channel_outbox b WHERE b.operation_id=d.worker_operation_id "
                "AND b.state IN ('pending','claimed')) OR EXISTS "
                "(SELECT 1 FROM runtime_channel_attachments a "
                "WHERE a.operation_id=d.worker_operation_id AND a.state IN ('pending','claimed'))) "
                "ORDER BY d.due_ms,d.source_operation_id",
                (canonical_json(parent.to_dict()).decode(),)).fetchall()
            return tuple(self._record(connection, row) for row in rows)

    def status(self, parent, *, active_operation=None):
        counts = {"pending": 0, "running": 0, "terminal": 0, "unresolved": 0}
        with self.store._read() as connection:
            rows = connection.execute(
                "SELECT CASE WHEN d.worker_operation_id=? THEN 'running' "
                "WHEN t.status IS NULL THEN 'pending' WHEN t.status='terminal' THEN 'terminal' "
                "ELSE 'unresolved' END kind,count(*) amount FROM runtime_background_dispatch d "
                "LEFT JOIN runtime_turns t ON t.operation_id=d.worker_operation_id "
                "WHERE d.source_session_json=? GROUP BY kind",
                (active_operation, canonical_json(parent.to_dict()).decode())).fetchall()
            counts.update({row["kind"]: row["amount"] for row in rows})
            counts["delivery_pending"] = connection.execute(
                "SELECT count(*) FROM runtime_background_dispatch d WHERE "
                "d.source_session_json=? AND (EXISTS (SELECT 1 FROM runtime_channel_outbox b "
                "WHERE b.operation_id=d.worker_operation_id AND b.state IN ('pending','claimed')) "
                "OR EXISTS (SELECT 1 FROM runtime_channel_attachments a WHERE "
                "a.operation_id=d.worker_operation_id AND a.state IN ('pending','claimed')))",
                (canonical_json(parent.to_dict()).decode(),)).fetchone()[0]
        return {"observation_scope": "current-runtime-and-durable-handoffs", **counts}
