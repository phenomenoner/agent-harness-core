import threading
import time
from dataclasses import replace
from types import SimpleNamespace

import pytest
from test_runtime_core import setup

from ahc.application.channel_ingress import ChannelIngress
from ahc.application.pending_approval import PendingApprovalPort
from ahc.application.runtime_service import RuntimeService
from ahc.domain.agent_backend import AgentTurnObservation
from ahc.domain.channel_ingress import ChannelInboundEvent, ChannelRoute
from ahc.domain.runtime_approval import ApprovalRequest
from ahc.domain.runtime_policy import EffectivePolicy
from ahc.domain.sessions import ContractError
from ahc.infrastructure.channel_control_store import ChannelControlStore
from ahc.infrastructure.runtime_approval_store import RuntimeApprovalStore
from ahc.infrastructure.runtime_inbox import RuntimeInbox
from ahc.infrastructure.threaded_runtime_repository import ThreadedRuntimeRepository


def make(tmp_path):
    original, current, request = setup(tmp_path)
    store = ThreadedRuntimeRepository(original.root)
    route = ChannelRoute("telegram", "1", "2", "3", current.session)
    policy = EffectivePolicy(1, "manual", "workspace-write", True, "a" * 64, 10)
    doc = {"channels": [{"route": route.to_dict(), "enabled": True}],
           "channel_control": {"enabled": True,
                               "models": {"luna-model": {"efforts": ["high", "max"],
                                                         "priority": True},
                                          "small": {"efforts": ["high"], "priority": False}},
                               "aliases": {"luna": "luna-model"}}}
    config = SimpleNamespace(to_dict=lambda: doc, session=current.session,
                             effective_policy=policy,
                             backend={"model": "luna-model", "reasoning_effort": "max"})
    controls = ChannelControlStore(store, config)
    store.channel_controls = controls
    ingress = ChannelIngress(RuntimeInbox(store), route)
    request = replace(request, deadline=time.monotonic()+30, effective_policy=policy)
    def submit(text, event_id=None, target=route):
        event = ChannelInboundEvent(target, event_id or str(time.monotonic_ns()), text)
        ChannelIngress(RuntimeInbox(store), target).submit(event, current.goal.digest)
        return event
    return store, current, request, route, controls, ingress, submit


def reply(store, event):
    with store._read() as c:
        return c.execute("SELECT reply FROM channel_controls WHERE operation_id=?",
                         (event.operation_id,)).fetchone()[0]


def test_toggle_duplicate_and_route_bound_preferences_survive_reopen(tmp_path):
    store, _, _, route, controls, _, submit = make(tmp_path)
    event = submit("/yolo", "once")
    assert "yolo" in reply(store, event)
    submit("/yolo", "once")
    normal = submit("hello")
    assert controls.preferences_for(normal.operation_id)["approval"] == "yolo"
    assert controls.preferences_for(normal.operation_id)["revision"] == 1
    assert "manual" in reply(store, submit("/yolo off"))
    reopened = ThreadedRuntimeRepository(store.root)
    restored = ChannelControlStore(reopened, controls.config)
    assert restored.preferences_for(normal.operation_id)["approval"] == "manual"
    other = replace(route, user_id="4")
    with pytest.raises(ContractError, match="not enabled"):
        submit("/yolo", target=other)


@pytest.mark.parametrize("previous", ["manual", "auto-approve", "always-approve"])
def test_yolo_feedback_matches_persisted_mode_across_new_and_explicit_switches(tmp_path, previous):
    store, _, _, _, controls, _, submit = make(tmp_path)
    submit("/approvals " + previous)
    for command in ("/yolo", "/yolo on", "/yolo on"):
        assert reply(store, submit(command)).startswith("YOLO 已開啟。\n")
    new_reply = reply(store, submit("/new"))
    assert "舊待辦" in new_reply and "旧" not in new_reply
    assert "核准：yolo" in reply(store, submit("/status"))
    expected = f"YOLO 已關閉，已恢復核准模式：{previous}。\n"
    assert reply(store, submit("/yolo")).startswith(expected)
    assert f"核准：{previous}" in reply(store, submit("/status"))
    assert reply(store, submit("/yolo on")).startswith("YOLO 已開啟。\n")
    assert reply(store, submit("/yolo off")).startswith(expected)
    assert controls.preferences_for(submit("fresh").operation_id)["approval"] == previous
    help_reply = reply(store, submit("/help"))
    assert "/yolo 不帶參數會反向切換" in help_reply
    assert "/new 保留模型、推理強度與核准設定" in help_reply


def test_model_effort_priority_are_independent_and_validate_catalog(tmp_path):
    store, _, _, _, controls, _, submit = make(tmp_path)
    assert "未變更" in reply(store, submit("/model small"))
    submit("/think high")
    submit("/fast on")
    assert "未變更" in reply(store, submit("/model small"))
    submit("/fast off")
    submit("/model small")
    normal = submit("hello")
    p = controls.preferences_for(normal.operation_id)
    assert (p["model"], p["effort"], p["fast"]) == ("small", "high", False)
    assert "未變更" in reply(store, submit("/fast on"))
    pending = RuntimeInbox(store).next_pending(normal.route.session)
    assert pending["operation_id"] == normal.operation_id


def test_approval_and_stop_are_responsive_while_turn_execution_lease_is_held(tmp_path):
    store, current, request, route, controls, _, submit = make(tmp_path)
    event = submit("run a tool")
    request = replace(request, operation_id=event.operation_id, input_text=event.input_text)
    published, release = threading.Event(), threading.Event()
    results, decisions = [], []
    class Backend:
        def invoke(self, bound):
            pending = ApprovalRequest(bound.session, bound.operation_id,
                                      bound.effective_policy.digest, "thread", "turn", "wire",
                                      "b" * 64, time.time()+5, "Execute the selected Python")
            def notify(p):
                controls.notify_approval(p)
                published.set()
            port = PendingApprovalPort(RuntimeApprovalStore(store), bound, notifier=notify)
            decisions.append(port.decide(pending, cancelled=lambda: False))
            release.wait(5)
            return AgentTurnObservation(bound, "completed", "fixture", 1, 1,
                                        "thread", "turn", "done")
    worker = threading.Thread(target=lambda: results.append(
        RuntimeService(store, Backend()).execute(current, request)))
    worker.start()
    try:
        assert published.wait(3)
        assert "核准這一次" in reply(store, submit("/approve"))
        assert "提出取消" in reply(store, submit("/stop"))
        assert controls.cancelled(request.operation_id)
        assert "仍有工作" in reply(store, submit("/new"))
        submit("/approvals always-approve")
        assert request.effective_policy.approval_mode == "manual"
    finally:
        release.set()
        worker.join(5)
    assert not worker.is_alive()
    assert decisions[0].outcome == "approve-once"
    assert results[0].result["accepted"] is True
    assert controls.preferences_for(request.operation_id)["approval"] == "always-approve"


def test_new_holds_queued_history_and_unknown_delivery_is_not_replayed(tmp_path):
    store, current, _, route, controls, _, submit = make(tmp_path)
    old = submit("old queued work")
    new = submit("/new")
    assert "新的對話" in reply(store, new)
    assert RuntimeInbox(store).next_pending(current.session) is None
    fresh = submit("fresh")
    assert RuntimeInbox(store).next_pending(current.session)["operation_id"] == fresh.operation_id
    assert controls.preferences_for(fresh.operation_id)["history_after"] > 0
    calls = []
    class Transport:
        def send(self, *args):
            calls.append(args)
            raise RuntimeError("ambiguous provider outcome")
    controls.deliver_one({route: Transport()})
    controls.deliver_one({route: Transport()})
    assert len(calls) == 1
    assert old.operation_id != fresh.operation_id


def test_restart_consumed_only_after_response_attempt_and_once(tmp_path):
    _, _, _, route, controls, _, submit = make(tmp_path)
    submit("/restart", "restart")
    assert not controls.restart_ready()
    class Transport:
        def send(self, *args):
            return "receipt"
    controls.deliver_one({route: Transport()})
    assert controls.restart_ready()
    assert not controls.restart_ready()
    submit("/restart", "restart")
    assert not controls.restart_ready()


@pytest.mark.parametrize("cleanup_fails", [False, True])
def test_service_restart_exit_requires_successful_workspace_cleanup(tmp_path, cleanup_fails):
    from test_foreground_runtime import setup as foreground_setup

    from ahc import AppConfig, Supervisor

    runtime, _, _, _ = foreground_setup(tmp_path, cleanup_fails=cleanup_fails)
    runtime.restart_requested = lambda: True
    code = Supervisor(AppConfig(state_dir=tmp_path / "service"), runtime=runtime).run(2)
    assert code == (4 if cleanup_fails else 75)


def test_new_fences_already_selected_submission_before_admission(tmp_path):
    from ahc.infrastructure.channel_control_store import ChannelSubmissionHeld

    store, current, request, _, _, _, submit = make(tmp_path)
    event = submit("selected work")
    request = replace(request, operation_id=event.operation_id, input_text=event.input_text)
    submit("/new")
    with pytest.raises(ChannelSubmissionHeld):
        store.begin(current, request)
    assert store.read_current(current.session.ref.virtual_session_id).active_turn_id is None


def test_child_approval_routes_to_parent_chat_without_fabricating_parent_request(tmp_path):
    from ahc.domain.child_work import ChildWorkEnvelope
    from ahc.infrastructure.child_approval_store import ChildApprovalStore
    from ahc.infrastructure.child_dispatch_repository import ChildDispatchRepository

    store, current, parent, _, controls, _, submit = make(tmp_path)
    event = submit("child work")
    parent = replace(parent, operation_id=event.operation_id, input_text=event.input_text)
    store.begin(current, parent)
    children = ChildDispatchRepository(store, parent)
    envelope = ChildWorkEnvelope("child", children.parent, "task", parent.workspace_scope,
                                 int((time.time()+10)*1000))
    children.admit(envelope)
    children.claim(envelope.parent, envelope.child_id, "token")
    child = replace(parent, operation_id="child", input_text="task")
    pending = ApprovalRequest(child.session, "child", child.effective_policy.digest,
                              "thread", "turn", "wire", "b"*64, time.time()+5, "Child write")
    approvals = ChildApprovalStore(children, envelope, "token", child)
    approvals.publish(child, pending)
    controls.notify_approval(pending, parent_operation=parent.operation_id, child_store=approvals)
    assert "拒絕這一次" in reply(store, submit("/deny " + pending.digest))
    assert approvals.consume(child.session, pending.digest).outcome == "deny"
    assert store.read_turn(child.session, "child") is None


def test_dynamic_tool_denial_prevents_actual_tool_invocation():
    from test_codex_workspace_tools import bridge, message

    adapter, tools = bridge()
    adapter.gate = SimpleNamespace(authorize=lambda *args: False)
    result = adapter.respond(message())
    assert result["result"]["success"] is False
    assert tools.calls == 0 and not adapter.unresolved


@pytest.mark.parametrize("unknown", [False, True])
def test_restart_fences_queued_and_selected_work_before_response_and_exit(tmp_path, unknown):
    from ahc.infrastructure.channel_control_store import ChannelSubmissionHeld

    store, current, request, route, controls, _, submit = make(tmp_path)
    ordinary = submit("queued")
    request = replace(request, operation_id=ordinary.operation_id, input_text=ordinary.input_text)
    submit("/restart")
    assert RuntimeInbox(store).next_pending(current.session) is None
    with pytest.raises(ChannelSubmissionHeld):
        store.begin(current, request)
    class Transport:
        def send(self, *args):
            if unknown:
                raise RuntimeError("response lost")
            return "receipt"
    controls.deliver_one({route: Transport()})
    assert RuntimeInbox(store).next_pending(current.session) is None
    with pytest.raises(ChannelSubmissionHeld):
        store.begin(current, request)
    from ahc.application.foreground_runtime import ForegroundRuntime

    def forbidden(*args, **kwargs):
        pytest.fail("restart allowed a new backend factory")
    runtime = ForegroundRuntime(store, current.session, lambda *_: request, forbidden)
    runtime.start()
    try:
        assert runtime.step() is None
    finally:
        runtime.close()
    assert controls.restart_ready()
    assert not controls.restart_ready()
    assert store.read_turn(request.session, request.operation_id) is None


def test_approvals_yolo_restores_previous_mode_after_reopen(tmp_path):
    store, _, _, _, controls, _, submit = make(tmp_path)
    submit("/approvals always-approve")
    submit("/approvals yolo")
    restored = ChannelControlStore(store, controls.config)
    store.channel_controls = restored
    submit("/yolo off")
    ordinary = submit("fresh")
    assert restored.preferences_for(ordinary.operation_id)["approval"] == "always-approve"


def test_status_uses_only_the_bound_current_runtime_cleanup_supplier(tmp_path):
    store, _, _, _, controls, _, submit = make(tmp_path)
    assert "觀測不可用" in reply(store, submit("/status"))
    controls.bind_cleanup_status(lambda: {
        "state": "pending", "automatic_attempts": 1,
        "automatic_attempt_limit": 2, "observation_scope": "current-runtime",
    })
    assert "待清理（自動 1/2）" in reply(store, submit("/status"))
    controls.bind_cleanup_status(lambda: {
        "state": "exhausted", "automatic_attempts": 2,
        "automatic_attempt_limit": 2, "observation_scope": "current-runtime",
    })
    assert "自動清理已停止" in reply(store, submit("/status"))
