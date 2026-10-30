"""Operator-selected channel outbox inspection and one-shot delivery."""

import os
import re
from pathlib import Path
from uuid import uuid4

from ahc.adapters.channel_attachment import BotAttachmentTransport
from ahc.adapters.channel_text import BotTextTransport
from ahc.application.channel_attachment_delivery import ChannelAttachmentDelivery
from ahc.application.channel_delivery import ChannelDelivery
from ahc.application.channel_ingress import ChannelIngress
from ahc.domain.channel_ingress import ChannelInboundEvent, ChannelRoute
from ahc.domain.sessions import ContractError
from ahc.infrastructure.channel_attachment_outbox import ChannelAttachmentOutbox
from ahc.infrastructure.channel_outbox import ChannelOutbox
from ahc.infrastructure.channel_progress_store import ChannelProgressStore
from ahc.infrastructure.runtime_inbox import RuntimeInbox
from ahc.infrastructure.runtime_repository import RuntimeRepository

COMMANDS = frozenset({"runtime-channel-outbox", "runtime-channel-send",
                      "runtime-channel-submit", "runtime-channel-attachments",
                      "runtime-channel-progress", "runtime-channel-progress-reconcile",
                      "runtime-channel-attachments-reconcile", "runtime-channel-attachment-send"})


def register_channel_commands(commands):
    for name in sorted(COMMANDS):
        parser = commands.add_parser(name)
        parser.add_argument("--runtime-root", type=Path, required=True)
        parser.add_argument("--route-json", required=True)
        if name == "runtime-channel-submit":
            parser.add_argument("--event-id", required=True)
            parser.add_argument("--input-text", required=True)
            parser.add_argument("--goal-digest", required=True)
        if name in {"runtime-channel-send", "runtime-channel-attachment-send"}:
            parser.add_argument("--operation-id", required=True)
            parser.add_argument("--token-env", required=True)
        if name == "runtime-channel-attachment-send":
            parser.add_argument("--ordinal", type=int, choices=range(8), required=True)


def run_channel_command(args):
    from .runtime import _json

    route = ChannelRoute.from_dict(_json(args.route_json))
    repo = RuntimeRepository(args.runtime_root)
    if args.command in {"runtime-channel-progress", "runtime-channel-progress-reconcile"}:
        progress = ChannelProgressStore(repo)
        rows = (progress.read(route) if args.command == "runtime-channel-progress"
                else progress.reconcile(route))
        return 0, {"command": args.command, "progress": list(rows),
                   "platform_readback": "not-performed", "automatic_retry": False}
    if args.command in {"runtime-channel-attachments", "runtime-channel-attachments-reconcile"}:
        attachments = ChannelAttachmentOutbox(repo)
        rows = (attachments.read(route) if args.command == "runtime-channel-attachments"
                else attachments.reconcile(route))
        return 0, {"command": args.command, "attachments": list(rows),
                   "platform_readback": "not-performed", "automatic_retry": False}
    if args.command == "runtime-channel-submit":
        event = ChannelInboundEvent(route, args.event_id, args.input_text)
        receipt = ChannelIngress(RuntimeInbox(repo), route).submit(event, args.goal_digest)
        return 0, {"command": args.command, "submission": receipt}
    outbox = ChannelOutbox(repo)
    if args.command == "runtime-channel-outbox":
        return 0, {"command": args.command, "outbox": list(outbox.read(route))}
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,127}", args.token_env):
        raise ContractError("channel credential environment reference invalid")
    token = os.environ.get(args.token_env)
    if token is None:
        raise ContractError("channel credential unavailable")
    if args.command == "runtime-channel-attachment-send":
        delivery = ChannelAttachmentDelivery(ChannelAttachmentOutbox(repo), route,
                                             BotAttachmentTransport(route, token))
        result = delivery.deliver(args.operation_id, args.ordinal, "attachment-" + uuid4().hex)
        return (0 if result["state"] == "sent" else 2), {
            "command": args.command, "delivery": result}
    transport = BotTextTransport(route, token)
    result = ChannelDelivery(outbox, route, transport).deliver(
        args.operation_id, "channel-" + uuid4().hex)
    return (0 if result["state"] == "sent" else 2), {
        "command": args.command, "delivery": result}
