"""Immutable local submissions in the existing runtime database.

Foreground owns backend/configuration/capability binding at dispatch. A submission
contains user input and exact session/goal identity, never an execution privilege.
"""

import json

from ahc.domain.sessions import ContractError, _identifier
from ahc.domain.skill_values import canonical_json, digest_json
from ahc.domain.turns import _input_text, _sha256_hex

from .working_repository import WorkingSetConflict, WorkingSetCorrupt

_TABLE = """CREATE TABLE IF NOT EXISTS runtime_submissions (
operation_id TEXT PRIMARY KEY, session_json TEXT NOT NULL,
goal_digest TEXT NOT NULL, payload_json TEXT NOT NULL, payload_digest TEXT NOT NULL)"""


class RuntimeInbox:
    def __init__(self, store):
        self.store = store

    @staticmethod
    def _exists(connection):
        return (
            connection.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='runtime_submissions'"
            ).fetchone()
            is not None
        )

    @staticmethod
    def _payload(session, operation_id, goal_digest, input_text):
        return {
            "session": session.to_dict(),
            "operation_id": _identifier(operation_id, "submission operation"),
            "goal_digest": _sha256_hex(goal_digest),
            "input_text": _input_text(input_text),
        }

    @staticmethod
    def _decode(row):
        value = json.loads(row["payload_json"])
        if (
            digest_json(value) != row["payload_digest"]
            or value.get("operation_id") != row["operation_id"]
            or value.get("goal_digest") != row["goal_digest"]
            or canonical_json(value.get("session")).decode() != row["session_json"]
        ):
            raise WorkingSetCorrupt("submission binding corrupted")
        return value

    def submit(self, session, operation_id, goal_digest, input_text, *, channel_event=None,
               _transaction=None, _skip_channel_controls=False):
        if type(_skip_channel_controls) is not bool:
            raise ContractError("channel control bypass flag invalid")
        base_payload = self._payload(session, operation_id, goal_digest, input_text)
        if channel_event is not None:
            from ahc.domain.channel_ingress import ChannelInboundEvent

            if type(channel_event) is ChannelInboundEvent and channel_event.files:
                raise ContractError("channel files require durable import before submission")
            if (
                type(channel_event) is not ChannelInboundEvent
                or channel_event.route.session != session
                or channel_event.operation_id != operation_id
                or channel_event.input_text != input_text
            ):
                raise ContractError("channel submission binding mismatch")
        base_encoded = canonical_json(base_payload).decode()
        base_fingerprint = digest_json(base_payload)
        owner = canonical_json(session.to_dict()).decode()

        def write(connection):
            submission_payload = base_payload
            submission_encoded = base_encoded
            submission_fingerprint = base_fingerprint
            from .channel_file_import import require_imported_submission

            connection.execute(_TABLE)
            if channel_event is not None:
                from .channel_context_store import bind_context

                bind_context(connection, channel_event)
                if (channel_event.context or {}).get("notice") and getattr(
                        self.store, "channel_controls", None) is None:
                    raise ContractError("channel media notices require channel controls")
            require_imported_submission(self.store, connection, operation_id,
                                        goal_digest, channel_event)
            controls = getattr(self.store, "channel_controls", None)
            if controls is not None and channel_event is not None and not _skip_channel_controls:
                receipt = controls.submit(
                    connection,
                    channel_event,
                    base_fingerprint,
                    session=session,
                    goal_digest=goal_digest,
                )
                if receipt is not None:
                    queued_text = receipt.get("queue_input_text")
                    if queued_text is None:
                        return receipt
                    submission_payload = self._payload(
                        session, operation_id, goal_digest, queued_text
                    )
                    submission_encoded = canonical_json(submission_payload).decode()
                    submission_fingerprint = digest_json(submission_payload)
            row = connection.execute(
                "SELECT * FROM runtime_submissions WHERE operation_id=?", (operation_id,)
            ).fetchone()
            if row is not None:
                self._decode(row)
                if row["payload_digest"] != submission_fingerprint:
                    raise ContractError("submission operation already bound")
                if channel_event is not None:
                    from .runtime_channel_store import bind_submission

                    bind_submission(connection, channel_event, existing=True)
                return {"operation_id": operation_id, "submission_digest": submission_fingerprint}
            current = self.store._load_current(connection, session.ref.virtual_session_id)
            if (
                current.session != session
                or current.goal.digest != goal_digest
                or current.status != "active"
            ):
                raise WorkingSetConflict("submission owner is not current and active")
            if connection.execute(
                "SELECT 1 FROM runtime_turns WHERE operation_id=?", (operation_id,)
            ).fetchone():
                raise ContractError("operation already admitted outside local inbox")
            count = connection.execute(
                "SELECT count(*) FROM runtime_submissions s WHERE session_json=? AND NOT EXISTS "
                "(SELECT 1 FROM runtime_turns t WHERE t.operation_id=s.operation_id)"
                + (" AND NOT EXISTS (SELECT 1 FROM channel_held h "
                   "WHERE h.operation_id=s.operation_id)" if controls is not None else ""),
                (owner,),
            ).fetchone()[0]
            if count >= 64:
                raise ContractError("local pending submission limit reached")
            connection.execute(
                "INSERT INTO runtime_submissions VALUES(?,?,?,?,?)",
                (operation_id, owner, goal_digest, submission_encoded, submission_fingerprint),
            )
            if channel_event is not None:
                from .runtime_channel_store import bind_submission

                bind_submission(connection, channel_event, existing=False)
            return {"operation_id": operation_id, "submission_digest": submission_fingerprint}

        # Internal same-store composition lets occurrence and inbox commit together.
        return write(_transaction) if _transaction is not None else self.store._mutate(write)

    def next_pending(self, session, *, exclude_scheduled=False):
        if type(exclude_scheduled) is not bool:
            raise ContractError("inbox schedule selection flag invalid")
        with self.store._read() as connection:
            if not self._exists(connection):
                return None
            current = self.store._load_current(connection, session.ref.virtual_session_id)
            if current.session != session or current.status != "active":
                raise WorkingSetConflict("inbox owner is not current")
            if current.active_turn_id is not None:
                return None
            from .channel_control_store import restart_pending

            if restart_pending(connection, session):
                return None
            from .runtime_channel_store import _exists

            held_filter = ("AND NOT EXISTS (SELECT 1 FROM channel_held h "
                           "WHERE h.operation_id=s.operation_id) "
                           if _exists(connection, "channel_held") else "")
            scheduled_filter = ("AND NOT EXISTS (SELECT 1 FROM runtime_schedule_occurrences o "
                                "WHERE o.operation_id=s.operation_id) "
                                if exclude_scheduled and _exists(
                                    connection, "runtime_schedule_occurrences") else "")
            row = connection.execute(
                "SELECT s.* FROM runtime_submissions s WHERE session_json=? AND goal_digest=? "
                "AND NOT EXISTS (SELECT 1 FROM runtime_turns t "
                "WHERE t.operation_id=s.operation_id) "
                + held_filter + scheduled_filter + "ORDER BY s.rowid LIMIT 1",
                (canonical_json(session.to_dict()).decode(), current.goal.digest),
            ).fetchone()
            return None if row is None else self._decode(row)

    def query(self, session, operation_id):
        _identifier(operation_id, "submission operation")
        with self.store._read() as connection:
            if not self._exists(connection):
                return None
            row = connection.execute(
                "SELECT * FROM runtime_submissions WHERE operation_id=?", (operation_id,)
            ).fetchone()
            if row is None:
                return None
            payload = self._decode(row)
            if payload["session"] != session.to_dict():
                raise ContractError("submission query owner mismatch")
            turn = connection.execute(
                "SELECT * FROM runtime_turns WHERE operation_id=?", (operation_id,)
            ).fetchone()
            return {
                "operation_id": operation_id,
                "submission_digest": row["payload_digest"],
                "status": "pending" if turn is None else turn["status"],
                "turn": None if turn is None else self.store._turn(turn).to_dict(),
            }
