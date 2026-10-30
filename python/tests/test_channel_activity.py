"""Discriminating activity lifecycle and exact-message transport fixtures."""

import threading
import time
from dataclasses import replace
from types import SimpleNamespace

import pytest
from test_channel_outbox import state
from test_foreground_runtime import setup

from ahc.adapters.channel_activity import BotActivityTransport
from ahc.adapters.channel_activity_worker import ChannelActivityWorker
from ahc.adapters.codex_activity import observe_activity
from ahc.cli import foreground_typing
from ahc.domain.agent_backend import AgentTurnObservation
from ahc.domain.channel_ingress import ChannelRoute
from ahc.domain.sessions import ContractError, VirtualSessionIdentity
from ahc.infrastructure.channel_progress_store import ChannelProgressStore


def test_rich_working_card_retains_recent_commentary_and_approval_phase():
    worker = ChannelActivityWorker(None, None, lambda: True, None, None, "op", clock=lambda: 65)
    worker._started = 0
    for text in ("first", "second", "third", "fourth"):
        worker.observe("模型處理中", text)
    worker.observe("等待核准")
    text = worker._text()
    assert "Working" in text and "01:05" in text and "等待核准" in text
    assert "/approve" in text and "/stop" in text
    assert "first" not in text and all(value in text for value in ("second", "third", "fourth"))
    worker.finish(True)
    final = worker._text(final=True)
    assert "回覆已產生" in final and "fourth" in final


def test_cancelled_turn_gets_only_one_owned_terminal_card_edit():
    updated = []
    worker = ChannelActivityWorker(None,
        SimpleNamespace(create=lambda *a: "receipt", update=updated.append),
        lambda: False, lambda text: True, lambda receipt: None, "op",
        can_finalize=lambda: True, stop_requested=lambda: True)
    worker._started = time.monotonic()
    worker._initial_text = "Working"
    # Existing acknowledged card, cancellation after create: no ongoing edits.
    worker.can_send = lambda: True
    def create(*args):
        worker.can_send = lambda: False
        worker._stop.set()
        return "receipt"
    worker.transport.create = create
    worker._run()
    assert len(updated) == 1 and "已提出停止" in updated[0]
    assert "回覆已產生" not in updated[0]


@pytest.mark.parametrize("platform", ["telegram", "discord"])
def test_edit_requires_own_acknowledged_message_and_exact_response(platform):
    route = ChannelRoute(platform, "11", "22", "33",
                         VirtualSessionIdentity.initial("session", "agent", "route"))
    sent, edited = [], []
    def send(bound, payload, logical):
        sent.append((bound, payload, logical))
        return f"{platform}:22:44"
    def edit(p, token, channel, message, text):
        edited.append((p, channel, message, text))
        if p == "telegram":
            return {"ok": True, "result": {"message_id": 44, "chat": {"id": 22},
                                            "from": {"id": 11}}}
        return {"id": "44", "channel_id": "22", "author": {"id": "11"}}
    transport = BotActivityTransport(route, "fixture", text_transport=SimpleNamespace(send=send),
                                     edit=edit)
    with pytest.raises(ContractError):
        transport.update("unowned")
    assert transport.create("Working", "op") == f"{platform}:22:44"
    transport.update("Working 00:05")
    assert len(sent) == 1 and edited == [(platform, "22", "44", "Working 00:05")]
    with pytest.raises(ContractError):
        transport.create("duplicate", "op")
    transport._edit = lambda *args: {"id": "99", "channel_id": "22", "author": {"id": "11"}}
    with pytest.raises(ContractError):
        transport.update("wrong receipt")


def test_timer_commentary_terminal_and_no_unknown_create_replay():
    created, updated, recorded = [], [], []
    edited = threading.Event()
    def update(text):
        updated.append(text)
        edited.set()
    worker = ChannelActivityWorker(None, SimpleNamespace(
        create=lambda *args: created.append(args) or "receipt", update=update),
        lambda: True, lambda text: True, recorded.append, "op", interval=0.01)
    worker.start()
    worker.observe("工具執行中", "Checking the result")
    assert edited.wait(2)
    worker.finish(True)
    worker.close()
    assert len(created) == 1 and recorded == ["receipt"]
    assert any("Working" in text and "Checking the result" in text for text in updated)
    assert "回覆已產生" in updated[-1] and "Checking the result" in updated[-1]
    with pytest.raises(ContractError):
        worker.start()
    def fail(*args):
        created.append("unknown")
        raise TimeoutError()
    failed = ChannelActivityWorker(None, SimpleNamespace(create=fail, update=update),
        lambda: True, lambda text: True, recorded.append, "other", interval=0.01)
    failed.start()
    failed._thread.join(2)
    failed.close()
    assert created.count("unknown") == 1 and recorded[-1] is None
    assert failed.status()["state"] == "unavailable"


def test_public_commentary_only_and_exact_backend_tuple():
    calls = []
    client = SimpleNamespace(thread="thread", turn="turn",
                             activity_event=lambda *a: calls.append(a))
    def event(kind, phase="commentary", thread="thread"):
        return {"method": "item/completed", "params": {"threadId": thread, "turnId": "turn",
            "item": {"type": kind, "phase": phase, "text": "visible"}}}
    observe_activity(client, event("reasoning"))
    observe_activity(client, event("agentMessage", "analysis"))
    observe_activity(client, event("agentMessage", thread="other"))
    assert not calls
    observe_activity(client, event("agentMessage"))
    assert calls == [("模型處理中", "visible")]


def test_protocol_forwards_public_commentary_without_changing_final_answer():
    from test_codex_client import setup as protocol_setup

    peer, client, request = protocol_setup()
    calls = []
    client.activity_event = lambda *args: calls.append(args)
    peer.messages.insert(3, {"method": "item/completed", "params": {
        "threadId": "thread", "turnId": "turn", "item": {"type": "agentMessage",
        "id": "comment", "phase": "commentary", "text": "I am checking the result."}}})
    result = client.run_admitted(request)
    assert result.disposition == "completed" and result.output_text == "answer"
    assert ("模型處理中", "I am checking the result.") in calls
    assert sum(m.get("method") == "turn/start" for m in peer.sent) == 1


@pytest.mark.parametrize("early_ui", [False, True])
def test_final_delivery_precedes_slow_cleanup_and_early_status_covers_factory(tmp_path, early_ui):
    runtime, requests, _, request = setup(tmp_path)
    events = []
    ui = SimpleNamespace(start=lambda: events.append("status-start"),
                         close=lambda: events.append("status-close"),
                         finish=lambda accepted: events.append("answer-ready"))
    runtime.channel_typing = (lambda *_: ui) if early_ui else None
    def factory(*args, **kwargs):
        assert events == (["status-start"] if early_ui else [])
        class Backend:
            def invoke(self, bound):
                return AgentTurnObservation(bound, "completed", "fixture", 1, 1,
                                            "thread", "turn", "answer")
            def close(self):
                assert "delivered" in events
                events.append("cleanup")
        return Backend()
    def deliver(**kwargs):
        turn = runtime.store.read_turn(request.session, request.operation_id)
        if turn is not None and turn.status == "terminal":
            assert turn.result["accepted"]
            events.append("delivered")
    runtime.backend_factory, runtime.channel_delivery = factory, deliver
    runtime.start()
    try:
        requests.append(request)
        assert runtime.step().result["accepted"]
        assert events == ((["status-start", "answer-ready", "status-close"] if early_ui else [])
                          + ["delivered", "cleanup"])
    finally:
        runtime.close()


def test_composition_reserves_once_and_persists_exact_receipt(tmp_path, monkeypatch):
    repo, snapshot, request, route = state(tmp_path)
    request = replace(request, deadline=time.monotonic() + 30)
    repo.begin(snapshot, request)
    sent = threading.Event()
    def create(text, operation):
        assert operation == request.operation_id
        rows = ChannelProgressStore(repo).read(route)
        assert len(rows) == 1 and rows[0]["state"] == "claimed"
        sent.set()
        return "activity-fixture-receipt"
    monkeypatch.setenv("AHC_ACTIVITY_TEST", "fixture")
    monkeypatch.setattr(foreground_typing, "BotActivityTransport", lambda *args:
                        SimpleNamespace(create=create, update=lambda text: None))
    config = SimpleNamespace(to_dict=lambda: {"channels": [{"route": route.to_dict(),
        "typing": False, "progress": True, "token_env": "AHC_ACTIVITY_TEST"}]})
    factory = foreground_typing.compose_typing(config, repo)
    worker = factory(request, lambda: False)
    worker.start()
    assert sent.wait(2)
    worker.finish(True)
    worker.close()
    rows = ChannelProgressStore(repo).read(route)
    assert len(rows) == 1 and rows[0]["state"] == "sent"
    assert rows[0]["receipt"] == "activity-fixture-receipt"
    duplicate = factory(request, lambda: False)
    duplicate.start()
    duplicate.close()
    assert duplicate.status()["state"] == "unavailable"


@pytest.mark.parametrize("revocation", ["cancel", "deadline", "replacement"])
def test_cancel_closes_own_card_but_replacement_revokes_all_edits(
        tmp_path, monkeypatch, revocation):
    from ahc.application.working_continuity import WorkingContinuityService

    repo, snapshot, request, route = state(tmp_path)
    request = replace(request, deadline=time.monotonic() + 30)
    repo.begin(snapshot, request)
    cancelled, edits = [False], []
    def create(*args):
        if revocation == "cancel":
            cancelled[0] = True
        elif revocation == "deadline":
            monkeypatch.setattr(foreground_typing.time, "monotonic", lambda: request.deadline)
        else:
            WorkingContinuityService(repo).cancel(repo.read_current("session"))
        return "receipt"
    monkeypatch.setenv("AHC_ACTIVITY_TEST", "fixture")
    monkeypatch.setattr(foreground_typing, "BotActivityTransport", lambda *args:
                        SimpleNamespace(create=create, update=edits.append))
    config = SimpleNamespace(to_dict=lambda: {"channels": [{"route": route.to_dict(),
        "typing": False, "progress": True, "token_env": "AHC_ACTIVITY_TEST"}]})
    worker = foreground_typing.compose_typing(config, repo)(request, lambda: cancelled[0])
    worker.interval = 0.01
    worker.start()
    worker._thread.join(2)
    worker.finish(True)
    worker.close()
    assert ChannelProgressStore(repo).read(route)[0]["receipt"] == "receipt"
    if revocation == "replacement":
        assert edits == []
    else:
        assert len(edits) == 1
        assert ("已提出停止" if revocation == "cancel" else "狀態更新已暫停") in edits[0]
    assert not worker._thread.is_alive()


def test_update_budget_pauses_card_without_claiming_work_finished():
    edits = []
    worker = ChannelActivityWorker(None, SimpleNamespace(
        create=lambda *args: "receipt", update=edits.append), lambda: True,
        lambda text: True, lambda receipt: None, "op", interval=0.01, update_limit=2)
    worker.start()
    worker._thread.join(2)
    worker.close()
    assert len(edits) == 2 and worker.status()["updates"] == 2
    assert "狀態更新已暫停" in edits[-1] and "處理已結束" not in edits[-1]


def test_inflight_edit_is_bounded_when_cancel_follows_dispatch_check():
    allowed, checks, edits = [True], [], []
    def can_send():
        decision = allowed[0]
        checks.append(decision)
        if len(checks) == 2:
            allowed[0] = False  # Cancellation just after this dispatch's admission check.
        return decision
    worker = ChannelActivityWorker(None, SimpleNamespace(
        create=lambda *args: "receipt", update=edits.append), can_send,
        lambda text: True, lambda receipt: None, "op", interval=0.01)
    worker.start()
    worker._thread.join(2)
    worker.close()
    assert len(edits) == 1  # Admitted in-flight edit may finish; no subsequent/final edit.
    assert checks[:2] == [True, True] and all(not value for value in checks[2:])


@pytest.mark.parametrize('failures', [1, 2, 3])
def test_transient_edit_recovers_with_two_lifetime_retries_and_one_create(failures):
    calls, created = [], []
    worker = None
    def update(text):
        calls.append(text)
        if len(calls) <= failures:
            raise TimeoutError('unverified edit')
        worker._stop.set()
    worker = ChannelActivityWorker(None, SimpleNamespace(create=lambda *a: created.append(a) or 'receipt', update=update),
        lambda: True, lambda text: True, lambda receipt: None, 'op', interval=0,
        edit_retry_delays=(0, 0), can_finalize=lambda: False)
    worker.start(); worker._thread.join(2); worker.close()
    assert len(created) == 1 and len(calls) == min(failures+1, 3)
    assert worker.status()['edit_retries'] == min(failures, 2)
    assert worker.status()['state'] == ('unavailable' if failures == 3 else 'closed')


def test_edit_retry_rechecks_exact_owner_and_never_retries_contract_rejection():
    calls = []
    allowed = [True]
    def update(text):
        calls.append(text); allowed[0] = False
        raise TimeoutError('late edit')
    worker = ChannelActivityWorker(None, SimpleNamespace(update=update), lambda: allowed[0], None, None,
                                   'op', edit_retry_delays=(0, 0))
    assert not worker._edit('text') and len(calls) == 1
    worker.transport.update = lambda text: (_ for _ in ()).throw(ContractError('wrong card'))
    assert not worker._edit('text') and worker.status()['edit_retries'] == 1


def test_stop_interrupts_edit_cooldown_and_no_new_card_is_created():
    worker = ChannelActivityWorker(None, SimpleNamespace(update=lambda text: (_ for _ in ()).throw(TimeoutError())),
        lambda: True, None, None, 'op', edit_retry_delays=(300, 300))
    finished = threading.Event()
    thread = threading.Thread(target=lambda: (worker._edit('text'), finished.set()))
    thread.start()
    worker._stop.set()
    assert finished.wait(1)
    thread.join(1)
    assert worker.status()['updates'] == 1
