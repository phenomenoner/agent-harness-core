"""Resolve a button through its exact acknowledged message inside inbox admission."""

import re

from ahc.domain.channel_ingress import ChannelInboundEvent
from ahc.domain.skill_values import digest_json

from .runtime_channel_store import _route_json


def admit_callback(controls, connection, route, update):
    """Return a bounded toast; untrusted callback data alone never grants approval."""
    query = update.get("callback_query")
    if type(query) is not dict or route not in controls.routes:
        return None
    try:
        sender, message = query["from"], query["message"]
        if (type(sender["id"]) is not int or str(sender["id"]) != route.user_id
                or sender.get("is_bot") is not False
                or type(message["chat"]["id"]) is not int
                or str(message["chat"]["id"]) != route.channel_id
                or type(message["from"]["id"]) is not int
                or str(message["from"]["id"]) != route.account_id
                or message["from"].get("is_bot") is not True
                or type(message["message_id"]) is not int or message["message_id"] <= 0
                or type(query["id"]) is not str
                or not re.fullmatch(r"[A-Za-z0-9_-]{1,128}", query["id"])):
            return None
        data = query.get("data")
        if type(data) is not str or not re.fullmatch(r"ahc:(approve|deny):[0-9a-f]{24}", data):
            return {"id": query["id"], "text": "此按鈕已失效，請使用 /help。"}
        command, short = data.split(":")[1:]
        receipt = f"telegram:{route.channel_id}:{message['message_id']}"
        rows = connection.execute(
            "SELECT * FROM channel_controls WHERE route_json=? AND state='sent' AND receipt=?",
            (_route_json(route), receipt),
        ).fetchall()
        matches = [row for row in rows if row["operation_id"].startswith("approval-")
                   and row["operation_id"] == "approval-" + row["input_digest"]
                   and row["input_digest"].startswith(short)]
        if len(matches) != 1:
            return {"id": query["id"], "text": "找不到對應的核准項目；未執行。"}
        event = ChannelInboundEvent(route, "callback-" + query["id"],
                                    f"/{command} {matches[0]['input_digest']}")
        # Same transaction as cursor + command dedup + active-turn/expiry/approval CAS.
        controls.submit(connection, event, digest_json({"callback": query["id"],
                        "receipt": receipt, "data": data}))
        row = connection.execute("SELECT reply FROM channel_controls WHERE operation_id=?",
                                 (event.operation_id,)).fetchone()
        return {"id": query["id"], "text": row["reply"][:180]}
    except (KeyError, TypeError, ValueError):
        return None
