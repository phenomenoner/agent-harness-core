"""Durable Telegram capture that never promotes held updates into the runtime inbox."""

from ahc.domain.sessions import ContractError
from ahc.domain.skill_values import canonical_json

from .telegram_inbox import _TABLE as _CURSOR_TABLE
from .telegram_inbox import TelegramInbox

_TABLE = """CREATE TABLE IF NOT EXISTS runtime_telegram_holds (
account_id TEXT NOT NULL, update_id INTEGER NOT NULL, routes_json TEXT NOT NULL,
update_json BLOB NOT NULL, PRIMARY KEY(account_id, update_id))"""


def _consumer(value):
    if type(value) is not TelegramInbox:
        raise ContractError("Telegram hold consumer invalid")
    return value


def _expected_offset(value):
    if value is not None and (type(value) is not int or not 0 <= value < 2**63):
        raise ContractError("Telegram expected cursor invalid")


def _retain_update(connection, consumer, update):
    """Keep raw input and its exact subscription inside the caller's transaction."""
    connection.execute(_TABLE)
    number = update["update_id"]
    encoded = canonical_json(update)
    if connection.execute(
        "SELECT 1 FROM runtime_telegram_holds WHERE account_id=? AND update_id=?",
        (consumer.account_id, number),
    ).fetchone() is not None:
        raise ContractError("Telegram update already held")
    connection.execute(
        "INSERT INTO runtime_telegram_holds VALUES(?,?,?,?)",
        (consumer.account_id, number, consumer._routes_json, encoded),
    )


def hold_updates(consumer: TelegramInbox, updates: list, expected_offset):
    """Persist one ordered batch before advancing its durable Telegram cursor."""

    consumer = _consumer(consumer)
    if type(updates) is not list or not updates:
        raise ContractError("Telegram hold batch invalid")
    _expected_offset(expected_offset)

    def write(connection):
        connection.execute(_CURSOR_TABLE)
        connection.execute(_TABLE)
        if consumer._offset(connection) != expected_offset:
            raise ContractError("Telegram cursor changed")
        for route in consumer.routes:
            current = consumer.store._load_current(
                connection, route.session.ref.virtual_session_id
            )
            if current.session != route.session or current.status != "active":
                raise ContractError("Telegram route is not current and active")
        previous = -1 if expected_offset is None else expected_offset - 1
        held_ids = []
        for update in updates:
            number = update.get("update_id") if type(update) is dict else None
            if type(number) is not int or not previous < number < 2**63 - 1:
                raise ContractError("Telegram update sequence invalid")
            _retain_update(connection, consumer, update)
            previous = number
            held_ids.append(number)
        next_offset = previous + 1
        connection.execute(
            "INSERT INTO runtime_telegram_cursors VALUES(?,?,?) "
            "ON CONFLICT(account_id) DO UPDATE "
            "SET next_offset=excluded.next_offset",
            (consumer.account_id, consumer._routes_json, next_offset),
        )
        return {
            "held_count": len(held_ids),
            "update_ids": held_ids,
            "next_offset": next_offset,
        }

    return consumer.store._mutate(write)


def inspect_held_updates(consumer: TelegramInbox):
    """Return only count and identifiers for holds bound to this exact consumer."""

    consumer = _consumer(consumer)
    with consumer.store._read() as connection:
        consumer._offset(connection)
        if connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='runtime_telegram_holds'"
        ).fetchone() is None:
            return {"held_count": 0, "update_ids": []}
        rows = connection.execute(
            "SELECT update_id FROM runtime_telegram_holds "
            "WHERE account_id=? AND routes_json=? ORDER BY update_id",
            (consumer.account_id, consumer._routes_json),
        ).fetchall()
        return {"held_count": len(rows), "update_ids": [row[0] for row in rows]}
