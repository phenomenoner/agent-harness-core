"""Explicit Discord receiver composition; no connection occurs until foreground start."""

import os
import re

from ahc.adapters.discord_gateway_driver import DiscordGatewayDriver
from ahc.adapters.discord_gateway_state import DiscordGatewayState
from ahc.adapters.discord_gateway_worker import DiscordGatewayWorker
from ahc.adapters.discord_identify import DiscordIdentifyAdmission
from ahc.domain.channel_ingress import ChannelRoute
from ahc.domain.sessions import ContractError
from ahc.infrastructure.discord_inbox import DiscordInbox
from ahc.infrastructure.discord_resume import DiscordResume, DiscordResumeSession


def parse_discord(raw, session):
    if "discord_gateway" not in raw:
        return {}
    value = raw["discord_gateway"]
    if type(value) is not dict or set(value) - {"include_files"} != {
            "enabled", "account_id", "token_env", "intents", "connection_limit"}:
        raise ContractError("Discord Gateway configuration fields invalid")
    if (type(value.get("include_files", False)) is not bool
            or type(value["enabled"]) is not bool or type(value["connection_limit"]) is not int
            or value["connection_limit"] < 1 or type(value["token_env"]) is not str
            or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,127}", value["token_env"])):
        raise ContractError("Discord Gateway configuration invalid")
    DiscordGatewayState(value["account_id"], "validation", value["intents"])
    routes = [ChannelRoute.from_dict(row["route"]) for row in raw.get("channels", [])
              if row["route"]["platform"] == "discord"
              and row["route"]["account_id"] == value["account_id"]]
    if not routes or any(route.session != session for route in routes):
        raise ContractError("Discord Gateway requires configured routes")
    return {"discord_gateway": dict(value)}


def compose_discord(config, store):
    document = config.to_dict()
    value = document.get("discord_gateway")
    if value is None or not value["enabled"]:
        return None
    token = os.environ.get(value["token_env"])
    if token is None:
        raise ContractError("Discord Gateway credential unavailable")
    routes = [ChannelRoute.from_dict(row["route"]) for row in document["channels"]
              if row["route"]["platform"] == "discord"
              and row["route"]["account_id"] == value["account_id"]]
    inbox = DiscordInbox(store, routes, include_files=value.get("include_files", False))
    session = DiscordResumeSession(DiscordResume(inbox))
    interactions = None
    if (getattr(store, "channel_controls", None) is not None
            and document.get("channel_control", {}).get("native_ui", False)):
        from ahc.adapters.discord_interactions import DiscordInteractions

        interactions = DiscordInteractions(store.channel_controls, routes, token)

    def admit(frame):
        receipts = session.admit(frame)
        if interactions is not None:
            interactions.respond(frame, receipts)
        return receipts

    driver = DiscordGatewayDriver(inbox.account_id, token, value["intents"], admit,
                                  connection_limit=value["connection_limit"],
                                  resume=session.current, checkpoint=session.checkpoint,
                                  identify_admission=DiscordIdentifyAdmission(token))
    return DiscordGatewayWorker(driver, setup=interactions.configure if interactions else None)
