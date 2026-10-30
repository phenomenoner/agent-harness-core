"""Exact-route bindings for schedules created through a configured chat."""

import json

from ahc.domain.channel_ingress import ChannelRoute
from ahc.domain.schedule_dispatch import SCHEDULE_TYPES
from ahc.domain.sessions import ContractError
from ahc.domain.skill_values import canonical_json

from .runtime_channel_store import _route_json
from .schedule_repository import ScheduleRepository

_TABLE = """CREATE TABLE IF NOT EXISTS runtime_channel_schedules (
schedule_id TEXT PRIMARY KEY REFERENCES runtime_schedules(schedule_id),
route_json TEXT NOT NULL)"""


class ChannelScheduleRoutes:
    """Keep a chat-created schedule coupled to one saved channel route."""

    @staticmethod
    def bind(connection, value, route):
        if type(value) not in SCHEDULE_TYPES or type(route) is not ChannelRoute:
            raise ContractError("channel schedule binding invalid")
        if value.session != route.session:
            raise ContractError("channel schedule route session mismatch")
        row = connection.execute(
            "SELECT * FROM runtime_schedules WHERE schedule_id=?", (value.schedule_id,)
        ).fetchone()
        if row is None or ScheduleRepository._decode(row) != value:
            raise ContractError("channel schedule is not durably created")
        connection.execute(_TABLE)
        encoded = _route_json(route)
        existing = connection.execute(
            "SELECT route_json FROM runtime_channel_schedules WHERE schedule_id=?",
            (value.schedule_id,),
        ).fetchone()
        if existing is None:
            connection.execute(
                "INSERT INTO runtime_channel_schedules VALUES(?,?)", (value.schedule_id, encoded)
            )
        elif existing["route_json"] != encoded:
            raise ContractError("schedule identifier already bound to another route")

    @staticmethod
    def route_for(connection, value):
        if type(value) not in SCHEDULE_TYPES:
            raise ContractError("channel schedule value invalid")
        exists = connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='runtime_channel_schedules'"
        ).fetchone()
        if exists is None:
            return None
        row = connection.execute(
            "SELECT route_json FROM runtime_channel_schedules WHERE schedule_id=?",
            (value.schedule_id,),
        ).fetchone()
        if row is None:
            return None
        try:
            route = ChannelRoute.from_dict(json.loads(row["route_json"]))
        except (TypeError, ValueError, json.JSONDecodeError, ContractError):
            raise ContractError("channel schedule route corrupted") from None
        canonical_route = canonical_json(route.to_dict()).decode()
        if route.session != value.session or row["route_json"] != canonical_route:
            raise ContractError("channel schedule route corrupted")
        return route
