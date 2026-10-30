"""Small interactive approval cards; authority remains in the control store."""

import json
from datetime import datetime, timezone

from .channel_text import BotTextTransport, _post


def approval_text(request):
    expiry = datetime.fromtimestamp(request.expires_at, timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    action, separator, payload = request.action_summary.partition(": ")
    detail = ""
    if separator:
        try:
            value = json.loads(payload)
            if type(value) is dict:
                preview = value["code"] if type(value.get("code")) is str else payload
                detail = "\n```\n" + preview[:900].replace("```", "` ` `") + "\n```"
                if len(preview) > 900:
                    detail += "\n（預覽已截短；請先確認完整操作再核准。）"
        except (ValueError, TypeError):
            detail = "\n" + payload[:900]
            if len(payload) > 900:
                detail += "\n（預覽已截短。）"
    return (f"**需要核准**\n核准截止：{expiry}（逾期即失效）\n工具：`{action[:120]}`{detail}\n"
            "僅核准這一次操作；過期或工作已停止時不能核准。\n"
            f"/approve {request.digest}\n/deny {request.digest}")


class BotControlTransport:
    def __init__(self, route, token, *, post=_post):
        self.route, self.token, self.post = route, token, post
        BotTextTransport(route, token, post=post)  # Validate credentials/binding at composition.

    def send(self, route, payload, delivery_id):
        return BotTextTransport(self.route, self.token, post=self.post).send(
            route, payload, delivery_id)

    def send_approval(self, route, payload, delivery_id, digest):
        def post(platform, token, channel, body):
            if platform == "telegram":
                body = {**body, "reply_markup": {"inline_keyboard": [[
                    {"text": "✓ 核准一次", "callback_data": "ahc:approve:" + digest[:24]},
                    {"text": "✕ 拒絕", "callback_data": "ahc:deny:" + digest[:24]},
                ]]}}
            else:
                body = {**body, "components": [{"type": 1, "components": [
                    {"type": 2, "style": 3, "label": "核准一次",
                     "custom_id": "ahc:approve:" + digest[:24]},
                    {"type": 2, "style": 4, "label": "拒絕",
                     "custom_id": "ahc:deny:" + digest[:24]},
                ]}]}
            return self.post(platform, token, channel, body)
        return BotTextTransport(self.route, self.token, post=post).send(
            route, payload, delivery_id)
