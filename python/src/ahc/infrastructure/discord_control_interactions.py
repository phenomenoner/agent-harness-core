"""Durable control admission for authenticated Gateway interactions, without tokens at rest."""

import re

from ahc.domain.channel_commands import COMMANDS
from ahc.domain.channel_ingress import ChannelInboundEvent
from ahc.domain.skill_values import digest_json

from .runtime_channel_store import _route_json
from .runtime_inbox import RuntimeInbox


def _command_value(name, value):
    if type(value) is not str:
        return False
    if name in {"steer", "cron"}:
        return (0 < len(value) <= 4000 and not any(
            0xD800 <= ord(char) <= 0xDFFF or ord(char) == 0 for char in value))
    return re.fullmatch(r"[A-Za-z0-9_.:-]{1,128}", value) is not None


def admit_interaction(controls, connection, route, interaction):
    if type(interaction) is not dict or type(interaction.get("data")) is not dict:
        return None
    try:
        member = interaction.get("member", {})
        if type(member) is not dict:
            return None
        actor = interaction.get("user") or member.get("user")
        if (
            type(actor) is not dict
            or actor.get("id") != route.user_id
            or actor.get("bot", False) is not False
            or interaction.get("application_id") != route.account_id
            or interaction.get("channel_id") != route.channel_id
            or type(interaction.get("id")) is not str
            or not re.fullmatch(r"[1-9][0-9]{0,19}", interaction["id"])
            or route not in controls.routes
        ):
            return None
        data, kind = interaction["data"], interaction["type"]
        command = None
        rejection = "互動項目已失效；未執行，請使用 /help。"
        if type(kind) is int and kind == 2 and data.get("type") == 1:
            name = data.get("name")
            options = data.get("options", [])
            if name in {row[0] for row in COMMANDS} and type(options) is list:
                if not options:
                    command = "/" + name
                elif (
                    len(options) == 1
                    and type(options[0]) is dict
                    and options[0].get("name") == "value"
                    and options[0].get("type") == 3
                    and _command_value(name, options[0].get("value"))
                ):
                    command = "/" + name + " " + options[0]["value"]
        elif type(kind) is int and kind == 3 and data.get("component_type") == 2:
            value = data.get("custom_id")
            message = interaction.get("message", {})
            if type(message) is not dict or type(message.get("author")) is not dict:
                return None
            if (
                type(value) is str
                and re.fullmatch(r"ahc:(approve|deny):[0-9a-f]{24}", value)
                and message.get("channel_id") == route.channel_id
                and message.get("author", {}).get("id") == route.account_id
                and message.get("author", {}).get("bot") is True
                and type(message.get("id")) is str
                and re.fullmatch(r"[1-9][0-9]{0,19}", message["id"])
            ):
                action, short = value.split(":")[1:]
                receipt = f"discord:{route.channel_id}:{message['id']}"
                rows = connection.execute(
                    "SELECT * FROM channel_controls WHERE route_json=? "
                    "AND state='sent' AND receipt=?",
                    (_route_json(route), receipt),
                ).fetchall()
                matches = [
                    r
                    for r in rows
                    if r["operation_id"] == "approval-" + r["input_digest"]
                    and r["input_digest"].startswith(short)
                ]
                if len(matches) == 1:
                    command = f"/{action} {matches[0]['input_digest']}"
        if command is None:
            return {"reply": rejection}
        event = ChannelInboundEvent(route, "interaction-" + interaction["id"], command)
        # Never fingerprint/persist the ephemeral interaction response token.
        fingerprint = digest_json(
            {"interaction_id": interaction["id"], "command": command, "route": route.to_dict()}
        )
        current = controls.store._load_current(connection, route.session.ref.virtual_session_id)
        receipt = controls.submit(connection, event, fingerprint,
                                  session=route.session, goal_digest=current.goal.digest)
        if receipt is not None and receipt.get("queue_input_text") is not None:
            queued = ChannelInboundEvent(route, event.event_id, receipt["queue_input_text"])
            RuntimeInbox(controls.store).submit(
                route.session, queued.operation_id, current.goal.digest, queued.input_text,
                channel_event=queued, _transaction=connection, _skip_channel_controls=True)
        row = connection.execute(
            "SELECT * FROM channel_controls WHERE operation_id=?", (event.operation_id,)
        ).fetchone()
        if row["state"] != "pending":
            return None  # Gateway redelivery cannot repeat the callback HTTP effect.
        connection.execute(
            "UPDATE channel_controls SET state='sending' WHERE operation_id=?",
            (event.operation_id,),
        )
        return {"operation_id": event.operation_id, "reply": row["reply"]}
    except (KeyError, TypeError, ValueError):
        return None
