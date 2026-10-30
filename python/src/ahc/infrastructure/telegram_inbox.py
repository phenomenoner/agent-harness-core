"""Atomic Telegram batch admission and account cursor in the existing runtime store."""

from time import time_ns

from ahc.adapters.channel_events import decode_channel_event
from ahc.domain.channel_ingress import ChannelRoute
from ahc.domain.sessions import ContractError
from ahc.domain.skill_values import canonical_json
from ahc.infrastructure.runtime_inbox import RuntimeInbox

from .channel_recovery_dedup import retained_recovery_event
from .telegram_album_inbox import (
    TelegramAlbumInputInvalid, flush_albums, stage_album, validate_album_input,
)

_TABLE = """CREATE TABLE IF NOT EXISTS runtime_telegram_cursors (
account_id TEXT PRIMARY KEY, routes_json TEXT NOT NULL, next_offset INTEGER NOT NULL)"""


class TelegramInbox:
    """One complete configured route set per bot; route rebind requires explicit migration."""

    def __init__(self, store, routes, *, include_files=False):
        if type(include_files) is not bool:
            raise ContractError("Telegram file admission option invalid")
        self.include_files = include_files
        self._album_clock = time_ns
        routes = tuple(routes)
        if (not routes or any(type(r) is not ChannelRoute for r in routes)
                or any(r.platform != "telegram" for r in routes)
                or len({r.account_id for r in routes}) != 1
                or len({(r.channel_id, r.user_id) for r in routes}) != len(routes)):
            raise ContractError("Telegram consumer routes invalid")
        self.store, self.routes = store, routes
        self.account_id = routes[0].account_id
        self._routes_json = canonical_json(
            [r.to_dict() for r in sorted(routes, key=lambda r: (r.channel_id, r.user_id))]).decode()

    def _offset(self, connection):
        if connection.execute("SELECT 1 FROM sqlite_master WHERE name='runtime_telegram_cursors'"
                              " AND type='table'").fetchone() is None:
            return None
        row = connection.execute("SELECT * FROM runtime_telegram_cursors WHERE account_id=?",
                                 (self.account_id,)).fetchone()
        if row is None:
            return None
        if row["routes_json"] != self._routes_json:
            raise ContractError("Telegram consumer route binding changed")
        offset = row["next_offset"]
        if type(offset) is not int or not 0 <= offset < 2**63:
            raise ContractError("Telegram cursor invalid")
        return offset

    def offset(self):
        with self.store._read() as connection:
            return self._offset(connection)

    def admit(self, updates, *, expected_offset):
        if type(updates) is not list or not updates:
            raise ContractError("Telegram admission batch invalid")
        if expected_offset is not None and (
                type(expected_offset) is not int or not 0 <= expected_offset < 2**63):
            raise ContractError("Telegram expected cursor invalid")

        def write(connection):
            connection.execute(_TABLE)
            if self._offset(connection) != expected_offset:
                raise ContractError("Telegram cursor changed")
            goals = {}
            for route in self.routes:
                current = self.store._load_current(connection, route.session.ref.virtual_session_id)
                if current.session != route.session or current.status != "active":
                    raise ContractError("Telegram route is not current and active")
                goals[route] = current.goal.digest
            previous = -1 if expected_offset is None else expected_offset - 1
            receipts = []
            callbacks = []
            held_count = 0
            for update in updates:
                number = update.get("update_id") if type(update) is dict else None
                if type(number) is not int or not previous < number < 2**63 - 1:
                    raise ContractError("Telegram update sequence invalid")
                previous = number
                controls = getattr(self.store, "channel_controls", None)
                if ("callback_query" in update and controls is not None
                        and controls.options.get("native_ui", False)):
                    from .telegram_control_callbacks import admit_callback

                    for route in self.routes:
                        answer = admit_callback(controls, connection, route, update)
                        if answer is not None:
                            callbacks.append(answer)
                    continue
                try:
                    # Only pure remote-input decoding is quarantinable. Local
                    # route, cursor, storage and submission failures still escape.
                    events = [decode_channel_event(route, update, include_files=self.include_files)
                              for route in self.routes]
                    if self.include_files:
                        for event in events:
                            validate_album_input(event)
                except ContractError:
                    from .telegram_hold import _retain_update

                    _retain_update(connection, self, update)
                    held_count += 1
                    continue
                for route, event in zip(self.routes, events, strict=True):
                    if event is not None:
                        retained = retained_recovery_event(self.store, connection, event)
                        if retained is not None:
                            receipts.append(retained)
                            continue
                        if self.include_files:
                            try:
                                album = stage_album(connection, event, goals[route],
                                                    self._album_clock())
                            except TelegramAlbumInputInvalid:
                                from .telegram_hold import _retain_update

                                _retain_update(connection, self, update)
                                held_count += 1
                                continue
                            if album is not None:
                                receipts.append(album)
                                continue
                        if event.files:
                            from .channel_file_inbox import ChannelFileInbox

                            receipts.append(ChannelFileInbox(self.store).stage(
                                event, goals[route], _transaction=connection))
                            continue
                        receipts.append(RuntimeInbox(self.store).submit(
                            route.session, event.operation_id, goals[route], event.input_text,
                            channel_event=event, _transaction=connection))
            receipts.extend(flush_albums(self.store, connection, goals, self._album_clock()))
            connection.execute("INSERT INTO runtime_telegram_cursors VALUES(?,?,?) "
                               "ON CONFLICT(account_id) DO UPDATE "
                               "SET next_offset=excluded.next_offset",
                               (self.account_id, self._routes_json, previous + 1))
            return {"next_offset": previous + 1, "submissions": receipts,
                    **({"held_count": held_count} if held_count else {}),
                    **({"callbacks": callbacks} if callbacks else {})}

        return self.store._mutate(write)

    def flush_albums(self):
        """Resume quiet retained albums even when the next provider poll is empty."""
        def write(connection):
            goals = {}
            for route in self.routes:
                current = self.store._load_current(connection, route.session.ref.virtual_session_id)
                if current.session != route.session or current.status != "active":
                    raise ContractError("Telegram route is not current and active")
                goals[route] = current.goal.digest
            return flush_albums(self.store, connection, goals, self._album_clock())

        return self.store._mutate(write)
