import json

import pytest

from ahc.adapters import channel_activity
from ahc.adapters.channel_formatting import discord_text, telegram_text
from ahc.adapters.channel_text import BotTextTransport
from ahc.domain.channel_ingress import ChannelRoute
from ahc.domain.sessions import VirtualSessionIdentity


def _route(platform):
    return ChannelRoute(platform, "11", "22", "33",
                        VirtualSessionIdentity.initial("session", "agent", "route"))


def test_telegram_renders_nested_inline_and_block_markdown_without_raw_html():
    rendered = telegram_text(
        '# **Bold *nested***\n\n'
        '- [label](https://example.test/?a=1&b=2)\n'
        '> <b>literal</b> & text'
    )

    assert rendered["parse_mode"] == "HTML"
    assert "<b>Bold <i>nested</i></b>" in rendered["text"]
    assert "<i>italic</i>" in telegram_text("*italic* and _under_")["text"]
    assert 'href="https://example.test/?a=1&amp;b=2">label</a>' in rendered["text"]
    assert "• <a href=" in rendered["text"]
    assert "<blockquote>&lt;b&gt;literal&lt;/b&gt; &amp; text</blockquote>" in rendered["text"]


def test_telegram_fenced_code_preserves_newlines_and_escapes_code_content():
    rendered = telegram_text("```python\nif a < b:\n    print('&')\n```\nnext")

    assert rendered["text"].startswith(
        '<pre><code class="language-python">if a &lt; b:\n    print(&#x27;&amp;&#x27;)</code></pre>'
    )
    assert rendered["text"].endswith("\n\nnext")


def test_telegram_malformed_markers_remain_literal_and_rich_overflow_keeps_full_plain_text():
    malformed = telegram_text("**unclosed `code\n```python\n<b>literal</b>")
    assert "<b>unclosed" not in malformed["text"]
    assert "**unclosed" in malformed["text"]
    assert "```python" in malformed["text"]
    assert "&lt;b&gt;literal&lt;/b&gt;" in malformed["text"]

    source = "😀" * 1500 + "&" * 1000
    fallback = telegram_text(source)
    assert "parse_mode" not in fallback
    assert fallback["text"] == source


def test_telegram_utf16_limit_rejects_without_truncating():
    with pytest.raises(ValueError, match="UTF-16"):
        telegram_text("😀" * 2049)


def test_discord_uses_one_embed_for_long_text_and_suppresses_mentions():
    source = "@everyone " + "x" * 1992
    payload = discord_text(source)
    assert "content" not in payload
    assert payload["embeds"] == [{"description": source}]
    assert payload["allowed_mentions"] == {"parse": []}


def test_text_transport_posts_formatted_body_once_for_telegram_and_discord_embed():
    calls = []

    def post(platform, token, channel, body):
        calls.append((platform, body))
        if platform == "telegram":
            return {"ok": True, "result": {"message_id": 1, "chat": {"id": 22},
                                              "from": {"id": 11}}}
        return {"id": "1", "channel_id": "22", "author": {"id": "11"}}

    telegram = BotTextTransport(_route("telegram"), "fixture", post=post)
    discord = BotTextTransport(_route("discord"), "fixture", post=post)
    assert telegram.send(_route("telegram"), {"output_text": "**bold**"}, "tg")
    assert discord.send(_route("discord"), {"output_text": "x" * 2001}, "dc")
    assert len(calls) == 2
    assert calls[0][1]["parse_mode"] == "HTML"
    assert calls[0][1]["text"] == "<b>bold</b>"
    assert "content" not in calls[1][1]
    assert calls[1][1]["embeds"][0]["description"] == "x" * 2001


def test_activity_edit_uses_same_telegram_formatter_and_one_http_attempt(monkeypatch):
    calls = []

    class Response:
        status = 200

        def read(self, _limit):
            return json.dumps({"ok": True, "result": {
                "message_id": 4, "chat": {"id": 22}, "from": {"id": 11}
            }}).encode()

    class Connection:
        def request(self, method, path, body, headers):
            calls.append((method, path, json.loads(body), headers))

        def getresponse(self):
            return Response()

        def close(self):
            pass

    monkeypatch.setattr(channel_activity.http.client, "HTTPSConnection",
                        lambda *args, **kwargs: Connection())
    assert channel_activity._edit("telegram", "fixture", "22", "4", "**bold**")
    assert len(calls) == 1
    assert calls[0][2]["parse_mode"] == "HTML"
    assert calls[0][2]["text"] == "<b>bold</b>"
