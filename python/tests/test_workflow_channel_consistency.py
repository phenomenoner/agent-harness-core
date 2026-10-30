"""Offline real channel admission/control/outbox lifecycle with controlled HTTP/model peers."""
import http.client
import threading
from types import SimpleNamespace

import pytest
from test_channel_interaction_composition import channel, receipt
from test_channel_optional_quote import reply_payload
from test_workspace_tools import Port

from ahc.adapters.channel_activity import BotActivityTransport
from ahc.adapters.channel_activity_worker import ChannelActivityWorker
from ahc.adapters.channel_text import BotTextTransport
from ahc.adapters.discord_gateway_state import DiscordGatewayState
from ahc.application.foreground_runtime import ForegroundRuntime
from ahc.application.local_runtime_ingress import LocalRuntimeIngress
from ahc.application.runtime_service import RuntimeService
from ahc.cli import foreground_channels
from ahc.domain.agent_backend import AgentTurnObservation
from ahc.infrastructure.channel_outbox import ChannelOutbox
from ahc.infrastructure.channel_progress_store import ChannelProgressStore
from ahc.infrastructure.runtime_inbox import RuntimeInbox
from ahc.infrastructure.runtime_repository import RuntimeRepository


@pytest.fixture(autouse=True)
def no_external_http(monkeypatch):
    def blocked(*_args, **_kwargs):
        pytest.fail("offline workflow attempted external HTTP")
    monkeypatch.setattr(http.client.HTTPSConnection, "request", blocked)


@pytest.mark.parametrize("platform", ["telegram", "discord"])
@pytest.mark.parametrize("presentation_fault", ["create", "status"])
def test_quote_and_presentation_downgrade_deliver_control_and_next_task_once(
        tmp_path, monkeypatch, platform, presentation_fault):
    store, current, template, route, controls, receiver, _ = channel(tmp_path, platform)
    called, posted, edited, ui_attempts, workers, entered = [], [], [], [], [], {}

    def post(observed_platform, token, destination, body):
        assert (observed_platform, token, destination) == (platform, "fixture", route.channel_id)
        if "Working" in str(body):
            ui_attempts.append(body)
            entered[pending_operation[0]].set()
            if presentation_fault == "create":
                raise OSError("PRIVATE uncertain activity response")
        posted.append(body)
        return receipt(route, len(posted))

    transport = BotTextTransport(route, "fixture", post=post)
    monkeypatch.setenv("AHC_WORKFLOW_CHANNEL_FIXTURE", "fixture")
    monkeypatch.setattr(foreground_channels, "BotTextTransport", lambda *_: transport)
    configuration = SimpleNamespace(to_dict=lambda: {"channels": [{
        "route": route.to_dict(), "enabled": True, "token_env": "AHC_WORKFLOW_CHANNEL_FIXTURE"}]})
    pending_operation = [None]

    def activity(bound, cancelled):
        pending_operation[0] = bound.operation_id
        entered[bound.operation_id] = threading.Event()
        ledger, claim = ChannelProgressStore(store), f"activity-{len(workers)}"

        class Worker(ChannelActivityWorker):
            def status(self):
                if presentation_fault == "status":
                    raise RuntimeError("PRIVATE observation")
                return super().status()

        def edit(observed_platform, token, destination, message, text):
            assert (observed_platform, token, destination) == (platform, "fixture", route.channel_id)
            edited.append((message, text))
            return receipt(route, message)

        worker = Worker(None, BotActivityTransport(route, "fixture", text_transport=transport, edit=edit),
            lambda: not cancelled(),
            lambda text: ledger.reserve(bound, route, "activity-status", text, claim, 1),
            lambda value: ledger.finish(bound, route, "activity-status", claim, value),
            bound.operation_id)
        workers.append(worker)
        return worker

    def factory(bound, _tools, **_kwargs):
        class Backend:
            def invoke(self, request):
                assert entered[request.operation_id].wait(2)
                called.append(request.operation_id)
                return AgentTurnObservation(request, "completed", "controlled-model", 1, 1,
                    "thread", "turn", "answer: " + request.input_text)
        return Backend()

    runtime = ForegroundRuntime(store, route.session,
        LocalRuntimeIngress(RuntimeInbox(store), artifact_digest="a" * 64,
            configuration_digest=lambda _: "b" * 64, workspace_scope="workspace",
            turn_timeout=10, effective_policy=template.effective_policy), factory,
        open_workspace=lambda snapshot: RuntimeService(store, None).open_workspace(
            snapshot, "workspace", Port),
        channel_delivery=foreground_channels.compose_channels(configuration, store),
        channel_typing=activity)
    controls.bind_cleanup_status(runtime.cleanup_status)
    controls.bind_admission_status(runtime.admission_status)
    gateway = None
    sequence = 1
    if platform == "discord":
        gateway = DiscordGatewayState(route.account_id, "fixture", 0,
            resume=receiver.current, checkpoint=receiver.checkpoint)
        assert gateway.receive({"op": 10, "d": {"heartbeat_interval": 10000}}, receiver.admit)["op"] == 6
        gateway.receive({"op": 0, "s": sequence, "t": "RESUMED", "d": {}}, receiver.admit)
        sequence += 1
        # Unused dispatch data still advances its exact checkpoint, without enrolling work.
        gateway.receive({"op": 0, "s": sequence, "t": "PRESENCE_UPDATE",
                         "d": ["future incidental fields"]}, receiver.admit)
        assert gateway.state == "ready" and receiver.current["sequence"] == sequence

    def receive(text, number):
        nonlocal sequence
        payload = reply_payload(platform, text=text)
        message = payload["message"] if platform == "telegram" else payload["d"]
        if platform == "telegram":
            payload["update_id"] = number
            message["message_id"] = number + 10
            message["chat"]["id"] = int(route.channel_id)
            message["from"]["id"] = int(route.user_id)
            receiver.admit([payload], expected_offset=receiver.offset())
            assert receiver.offset() == number + 1
        else:
            sequence += 1
            payload["s"] = sequence
            message["id"] = str(number + 10)
            message["channel_id"] = route.channel_id
            message["author"]["id"] = route.user_id
            gateway.receive(payload, receiver.admit)
            assert gateway.state == "ready" and receiver.current["sequence"] == sequence

    runtime.start()
    try:
        receive("first question", 1)
        first = runtime.step()
        assert first.result["accepted"] and not first.result["unresolved_effects"]
        assert runtime.cleanup_status()["state"] == "clear"
        receive("/status", 2)
        assert RuntimeInbox(store).next_pending(route.session) is None
        controls.deliver_one({route: transport})
        assert len(called) == 1
        receive("follow-up question", 3)
        second = runtime.step()
        assert second.result["accepted"] and not second.result["unresolved_effects"]
        assert runtime.cleanup_status()["state"] == "clear" and len(called) == 2
        assert len(ui_attempts) == 2  # One attempt per distinct task; no uncertain replay.
        assert "PRIVATE" not in str(runtime.channel_status())
        with store._read() as db:
            control_rows = db.execute("SELECT state FROM channel_controls").fetchall()
            assert len(control_rows) == 1 and control_rows[0]["state"] == "sent"
            states = db.execute("SELECT state FROM runtime_channel_progress").fetchall()
            assert len(states) == 2
            assert {row["state"] for row in states} == {"unknown" if presentation_fault == "create" else "sent"}
        rows = ChannelOutbox(store).read(route)
        assert len(rows) == 2 and {row["state"] for row in rows} == {"sent"}
        assert {row["payload"]["output_text"] for row in rows} == {
            "answer: first question", "answer: follow-up question"}
        assert runtime.step() is None and len(called) == 2
    finally:
        runtime.close()
    assert all(not worker._thread.is_alive() for worker in workers)
    restarted = RuntimeRepository(store.root)
    assert RuntimeInbox(restarted).next_pending(route.session) is None
    assert ChannelOutbox(restarted).read(route) == rows
    assert len(ui_attempts) == 2 and len(called) == 2
