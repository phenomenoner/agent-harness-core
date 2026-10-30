"""Explicit offline recovery of a consumer binding, preserving its delivery position."""

from contextlib import closing

from ahc.domain.channel_ingress import ChannelRoute
from ahc.domain.sessions import ContractError
from ahc.domain.skill_values import canonical_json

from .discord_inbox import DiscordInbox
from .discord_resume import DiscordResume
from .runtime_recovery_access import recovery_allows
from .runtime_restore_guard import offline_restore_ownership
from .telegram_inbox import TelegramInbox
from .working_db import open_connection, require_schema


def rebind_recovered_consumer(store, supervisor_lock, old_routes, new_routes, *, expected):
    """No startup, token access, checkpoint reset, event migration or automatic replay."""
    old_routes, new_routes = tuple(old_routes), tuple(new_routes)
    if (not old_routes or not new_routes
            or any(type(route) is not ChannelRoute for route in (*old_routes, *new_routes))):
        raise ContractError("recovery consumer routes missing")
    platform = old_routes[0].platform
    if platform not in {"telegram", "discord"}:
        raise ContractError("recovery consumer platform invalid")
    constructor = TelegramInbox if platform == "telegram" else DiscordInbox
    old, new = constructor(store, old_routes), constructor(store, new_routes)
    def key(route):
        return route.platform, route.account_id, route.channel_id, route.user_id

    before, after = sorted(old.routes, key=key), sorted(new.routes, key=key)
    if ([key(route) for route in before] != [key(route) for route in after]
            or before == after):
        raise ContractError("recovery consumer external routes must remain identical")
    table = "runtime_telegram_cursors" if platform == "telegram" else "runtime_discord_resume"
    binding = canonical_json([route.to_dict() for route in after]).decode()
    with offline_restore_ownership(store, supervisor_lock):
        with closing(open_connection(store.db_path, readonly=False)) as connection:
            require_schema(connection)
            connection.execute("BEGIN IMMEDIATE")
            try:
                position = (old._offset(connection) if platform == "telegram"
                            else DiscordResume(old)._read(connection))
                if position is None or canonical_json(position) != canonical_json(expected):
                    raise ContractError("recovery consumer checkpoint changed or missing")
                for previous, successor in zip(before, after, strict=True):
                    current = store._load_current(
                        connection, successor.session.ref.virtual_session_id)
                    if current.session != successor.session or current.status != "active":
                        raise ContractError("recovery consumer successor is not active")
                    if (previous.session != successor.session and not recovery_allows(
                            store, connection, successor.session, previous.session)):
                        raise ContractError("recovery consumer lineage not verified")
                connection.execute(f"UPDATE {table} SET routes_json=? WHERE account_id=?",
                                   (binding, new.account_id))
                connection.commit()
            except BaseException:
                connection.rollback()
                raise
    return {"platform": platform, "account_id": new.account_id,
            "checkpoint": position, "state": "rebound", "started": False,
            "automatic_replay": False}
