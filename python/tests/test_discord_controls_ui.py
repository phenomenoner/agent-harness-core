"""Native Discord controls use the same exact-route, durable command transaction."""

import time
from dataclasses import replace
from types import SimpleNamespace

import pytest
from test_channel_controls import make

from ahc.adapters.channel_control_ui import BotControlTransport
from ahc.adapters.discord_interactions import DiscordInteractions
from ahc.application.channel_ingress import ChannelIngress
from ahc.domain.channel_ingress import ChannelInboundEvent
from ahc.domain.runtime_approval import ApprovalRequest
from ahc.infrastructure.channel_control_store import ChannelControlStore
from ahc.infrastructure.discord_inbox import DiscordInbox
from ahc.infrastructure.discord_resume import DiscordResume, DiscordResumeSession
from ahc.infrastructure.runtime_approval_store import RuntimeApprovalStore
from ahc.infrastructure.runtime_inbox import RuntimeInbox


def fixture(tmp_path):
    store, current, template, old, old_controls, _, _ = make(tmp_path)
    route = replace(old, platform="discord")
    doc = old_controls.config.to_dict()
    doc["channels"] = [{"route": route.to_dict(), "enabled": True}]
    doc["channel_control"]["native_ui"] = True
    config = SimpleNamespace(**{**vars(old_controls.config), "to_dict": lambda: doc})
    controls = ChannelControlStore(store, config)
    store.channel_controls = controls
    inbox = DiscordInbox(store, [route])
    resume = DiscordResume(inbox)
    resume.save_ready("gateway", "wss://gateway.discord.gg", 0, expected=None)
    return store, current, template, route, controls, DiscordResumeSession(resume)


def frame(route, number, data, kind=2):
    return {
        "op": 0,
        "s": number,
        "t": "INTERACTION_CREATE",
        "d": {
            "id": str(100 + number),
            "application_id": route.account_id,
            "channel_id": route.channel_id,
            "user": {"id": route.user_id},
            "type": kind,
            "data": data,
            "token": "PRIVATE_TOKEN",
        },
    }


def test_native_slash_command_answers_once_with_no_model_submission_or_token_storage(tmp_path):
    store, _, _, route, controls, session = fixture(tmp_path)
    update = frame(
        route,
        1,
        {"type": 1, "name": "model", "options": [{"name": "value", "type": 3, "value": "luna"}]},
    )
    replies = session.admit(update)
    sent = []
    adapter = DiscordInteractions(
        controls, [route], "fixture", request=lambda *args: sent.append(args) or True
    )
    adapter.respond(update, replies)
    assert len(sent) == 1 and sent[0][2]["type"] == 4
    assert sent[0][2]["data"]["flags"] == 64
    assert "已更新" in sent[0][2]["data"]["content"]
    replay = {**update, "s": 2}
    adapter.respond(replay, session.admit(replay))
    assert len(sent) == 1
    with store._read() as c:
        assert (
            c.execute("SELECT 1 FROM sqlite_master WHERE name='runtime_submissions'").fetchone()
            is None
        )
        rows = c.execute("SELECT * FROM channel_controls").fetchall()
        assert len(rows) == 1 and rows[0]["state"] == "sent"
        assert "PRIVATE_TOKEN" not in str([tuple(r) for r in rows])


@pytest.mark.parametrize(
    "variation", ["ok", "sender", "message", "application", "stopped", "duplicate"]
)
def test_component_approval_exact_ownership_and_once(tmp_path, variation):
    store, current, template, route, controls, session = fixture(tmp_path)
    event = ChannelInboundEvent(route, "work", "Run selected tool")
    ChannelIngress(RuntimeInbox(store), route).submit(event, current.goal.digest)
    request = replace(template, operation_id=event.operation_id, input_text=event.input_text)
    store.begin(current, request)
    approval = ApprovalRequest(
        route.session,
        event.operation_id,
        request.effective_policy.digest,
        "thread",
        "turn",
        "wire",
        "b" * 64,
        time.time() + 5,
        "workspace.execute: print(42)",
    )
    RuntimeApprovalStore(store).publish(request, approval)
    controls.notify_approval(approval)
    sent = []

    def post(*args):
        sent.append(args[-1])
        return {"id": "50", "channel_id": route.channel_id, "author": {"id": route.account_id}}

    controls.deliver_one({route: BotControlTransport(route, "fixture", post=post)})
    button = sent[0]["components"][0]["components"][0]
    update = frame(route, 1, {"component_type": 2, "custom_id": button["custom_id"]}, 3)
    update["d"]["message"] = {
        "id": "50",
        "channel_id": route.channel_id,
        "author": {"id": route.account_id, "bot": True},
    }
    if variation == "sender":
        update["d"]["user"]["id"] = "999"
    if variation == "application":
        update["d"]["application_id"] = "999"
    if variation == "message":
        update["d"]["message"]["id"] = "51"
    if variation == "stopped":
        store._mutate(
            lambda c: c.execute("INSERT INTO channel_cancel VALUES(?)", (event.operation_id,))
        )
    replies = session.admit(update)
    if variation == "duplicate":
        again = frame(route, 2, update["d"]["data"], 3)
        again["d"]["message"] = update["d"]["message"]
        result = session.admit(again)
        assert "沒有唯一" in result[0]["interaction_response"]["reply"]
    with store._read() as c:
        row = c.execute("SELECT state,outcome FROM runtime_approvals").fetchone()
        assert row["state"] == ("answered" if variation in ("ok", "duplicate") else "pending")
        if variation in ("ok", "duplicate"):
            assert row["outcome"] == "approve-once"
    if variation in ("sender", "application"):
        assert replies == []


def test_command_setup_preserves_other_names_and_only_registers_dm_context(tmp_path):
    _, _, _, route, controls, _ = fixture(tmp_path)
    calls = []

    def request(method, path, body, token=None):
        calls.append((method, path, body))
        if path == "/users/@me":
            return {"id": route.account_id, "bot": True}
        if path == "/oauth2/applications/@me":
            return {"id": route.account_id}
        if path.startswith("/channels/"):
            return {"id": route.channel_id, "type": 1}
        if method == "GET":
            return [{"id": "9", "name": "unrelated", "type": 1}]
        return {**body, "application_id": route.account_id}

    adapter = DiscordInteractions(controls, [route], "fixture", request=request)
    assert adapter.configure()
    writes = [(path, body) for method, path, body in calls if method == "POST"]
    assert len(writes) == 16 and all(body["contexts"] == [1] for _, body in writes)
    assert all(body["name"] != "unrelated" for _, body in writes)
    assert not any(method in ("PUT", "DELETE") for method, _, _ in calls)


def test_command_setup_waits_for_rejected_rate_limit_without_replaying_success(tmp_path):
    from ahc.adapters.discord_interactions import DiscordRateLimited

    _, _, _, route, controls, _ = fixture(tmp_path)
    now, writes, waits = [0.0], [], []

    def request(method, path, body, token=None):
        if path == "/users/@me":
            return {"id": route.account_id, "bot": True}
        if path == "/oauth2/applications/@me":
            return {"id": route.account_id}
        if path.startswith("/channels/"):
            return {"id": route.channel_id, "type": 1}
        if method == "GET":
            return []
        writes.append(body["name"])
        if len(writes) == 2:
            raise DiscordRateLimited(7)
        return {**body, "application_id": route.account_id}

    def wait(seconds):
        waits.append(seconds)
        now[0] += seconds

    adapter = DiscordInteractions(controls, [route], "fixture", request=request,
                                  clock=lambda: now[0])
    assert adapter.configure(wait=wait)
    assert waits == [7]
    assert writes[:3] == ["help", "status", "status"]
    assert len(writes) == 17


def test_registered_optional_false_omission_does_not_cause_repeated_upserts(tmp_path):
    from ahc.domain.channel_commands import discord_commands

    _, _, _, route, controls, _ = fixture(tmp_path)
    prior = discord_commands()
    for entry in prior:
        entry.update(contexts=[1], integration_types=[0])
        for option in entry.get("options", []):
            option.pop("required", None)  # Real Discord response omits the false default.

    def request(method, path, body, token=None):
        assert method == "GET"
        if path == "/users/@me":
            return {"id": route.account_id, "bot": True}
        if path == "/oauth2/applications/@me":
            return {"id": route.account_id}
        if path.startswith("/channels/"):
            return {"id": route.channel_id, "type": 1}
        return prior

    assert DiscordInteractions(controls, [route], "fixture", request=request).configure()


def test_menu_rate_wait_over_budget_is_not_attempted(tmp_path):
    from ahc.adapters.discord_interactions import DiscordRateLimited

    _, _, _, route, controls, _ = fixture(tmp_path)
    calls, waits = [], []

    def request(*args):
        calls.append(args[0])
        raise DiscordRateLimited(301)

    adapter = DiscordInteractions(controls, [route], "fixture", request=request)
    assert not adapter.configure(wait=waits.append)
    assert calls == ["GET"] and waits == []


def test_rate_limit_wire_metadata_is_bounded_and_sanitized(monkeypatch):
    import ahc.adapters.discord_interactions as module

    class Response:
        status = 429

        def read(self, limit):
            return b'{"retry_after":2.5,"message":"rate limited"}'

    class Connection:
        def __init__(self, *args, **kwargs):
            pass

        def request(self, *args, **kwargs):
            pass

        def getresponse(self):
            return Response()

        def close(self):
            pass

    monkeypatch.setattr(module.http.client, "HTTPSConnection", Connection)
    with pytest.raises(module.DiscordRateLimited) as error:
        module._http("POST", "/applications/123/commands", {}, "secret-fixture")
    assert error.value.retry_after == 2.5
    assert "secret-fixture" not in str(error.value)
