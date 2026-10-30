"""Explicit automatic channel delivery bindings; no platform ingress is implied."""

import os
import re
from uuid import uuid4

from ahc.adapters.channel_attachment import BotAttachmentTransport
from ahc.adapters.channel_text import BotTextTransport
from ahc.application.channel_attachment_delivery import ChannelAttachmentDelivery
from ahc.application.channel_delivery import ChannelDelivery
from ahc.domain.channel_ingress import ChannelRoute
from ahc.domain.sessions import ContractError
from ahc.domain.skill_values import digest_json
from ahc.infrastructure.channel_attachment_outbox import ChannelAttachmentOutbox
from ahc.infrastructure.channel_outbox import ChannelOutbox
from ahc.infrastructure.channel_progress_store import ChannelProgressStore
from ahc.infrastructure.working_db import sqlite_is_busy
from ahc.infrastructure.working_repository import WorkingWriterBusy


def parse_channels(raw, session):
    if "channels" not in raw:
        return {}
    rows = raw["channels"]
    if type(rows) is not list:
        raise ContractError("foreground channels invalid")
    result, seen = [], set()
    for row in rows:
        if (type(row) is not dict or not {"route", "token_env", "enabled"} <= set(row)
                or set(row) - {"route", "token_env", "enabled", "typing", "progress"}):
            raise ContractError("foreground channel fields invalid")
        route = ChannelRoute.from_dict(row["route"])
        if (type(row.get("progress", False)) is not bool
                or (row.get("progress") and not row["enabled"])):
            raise ContractError("progress requires an enabled channel")
        if type(row.get("typing", False)) is not bool or (row.get("typing") and not row["enabled"]):
            raise ContractError("typing requires an enabled channel")
        name = row["token_env"]
        if (route.session != session or route in seen or type(row["enabled"]) is not bool
                or type(name) is not str
                or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,127}", name)):
            raise ContractError("foreground channel binding invalid")
        # Validate platform destination without resolving a credential or making a request.
        BotTextTransport(route, "validation")
        seen.add(route)
        result.append({"route": route.to_dict(), "token_env": name, "enabled": row["enabled"]})
        if "typing" in row:
            result[-1]["typing"] = row["typing"]
        if "progress" in row:
            result[-1]["progress"] = row["progress"]
    return {"channels": result}


def parse_channel_options(raw, session):
    from .foreground_discord import parse_discord
    from .foreground_telegram import parse_telegram

    return {**parse_channels(raw, session), **parse_telegram(raw, session),
            **parse_discord(raw, session)}


def compose_channels(config, store):
    outbox, bindings = ChannelOutbox(store), []
    attachments, reconciled = ChannelAttachmentOutbox(store), set()
    for row in config.to_dict().get("channels", []):
        if row["enabled"]:
            token = os.environ.get(row["token_env"])
            if token is None:
                raise ContractError("foreground channel credential unavailable")
            route = ChannelRoute.from_dict(row["route"])
            bindings.append((route, ChannelDelivery(outbox, route, BotTextTransport(route, token)),
                             ChannelAttachmentDelivery(attachments, route,
                                                       BotAttachmentTransport(route, token))))
    if not bindings:
        return None

    def deliver_one(*, cancelled):
        for route, delivery, attachment_delivery in bindings:
            if cancelled():
                return
            try:
                if route not in reconciled:
                    outbox.reconcile(route)
                    attachments.reconcile(route)
                    ChannelProgressStore(store).reconcile(route)
                    reconciled.add(route)
                text_records = outbox.read(route)
            except Exception as error:
                # This phase has no transport effect. Retry only on a later
                # foreground cycle; corruption and other failures remain fatal.
                if isinstance(error, WorkingWriterBusy) or sqlite_is_busy(error):
                    return
                raise
            for record in text_records:
                if cancelled():
                    return
                if record["state"] == "pending":
                    delivery.deliver(record["operation_id"], "channel-" + uuid4().hex)
                    return
            sent = {row["operation_id"] for row in text_records if row["state"] == "sent"}
            try:
                attachment_records = attachments.read(route)
            except Exception as error:
                if isinstance(error, WorkingWriterBusy) or sqlite_is_busy(error):
                    return
                raise
            for record in attachment_records:
                if cancelled():
                    return
                if record["state"] == "pending" and record["operation_id"] in sent:
                    attachment_delivery.deliver(record["operation_id"], record["ordinal"],
                                                "attachment-" + uuid4().hex)
                    return

    return deliver_one


def preview_channels(config):
    rows = config.to_dict().get("channels")
    if rows is None:
        return {}
    backend_enabled = config.backend["enabled"]
    return {"channels": {
        "configured_routes": len(rows),
        "typing_routes": sum(row.get("typing", False) and backend_enabled for row in rows),
        "progress_routes": sum(row.get("progress", False) and backend_enabled for row in rows),
        "automatic_delivery_routes": sum(row["enabled"] and backend_enabled for row in rows),
        "credentials": "not-inspected",
        "discord_receiver": bool(backend_enabled and config.to_dict().get(
            "discord_gateway", {}).get("enabled", False)),
        "ingress": ("telegram-polling" if config.to_dict().get("telegram_polling", {}).get(
            "enabled", False) and backend_enabled else "not-configured"),
        "routes": [{"platform": row["route"]["platform"],
                    "route_digest": digest_json(row["route"]),
                    "enabled": row["enabled"],
                    "automatic_delivery": row["enabled"] and backend_enabled} for row in rows],
    }}
