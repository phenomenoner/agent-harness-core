"""Typing cleanup preserves the sealed answer and fences fresh backend admission."""

import threading
from types import SimpleNamespace

import pytest
from test_channel_outbox import state
from test_foreground_runtime import setup

from ahc.adapters.channel_typing_worker import ChannelTypingWorker
from ahc.application.working_continuity import WorkingContinuityService
from ahc.cli import foreground_typing
from ahc.domain.agent_backend import AgentTurnObservation


@pytest.mark.parametrize("fault", ["factory", "start", "status"])
def test_optional_presentation_failure_keeps_answer_and_next_turn(tmp_path, fault):
    from dataclasses import replace

    runtime, requests, invoked, request = setup(tmp_path)
    closed = []

    def fail():
        raise RuntimeError("PRIVATE presentation")

    class Worker:
        def start(self):
            if fault == "start":
                fail()

        def close(self):
            closed.append(True)

        def status(self):
            if fault == "status":
                fail()
            return {"state": "closed"}

    def factory(*_):
        if fault == "factory":
            fail()
        return Worker()

    runtime.channel_typing = factory
    runtime.start()
    try:
        requests.extend([request, replace(request, operation_id="next")])
        first = runtime.step()
        assert first.result["accepted"] and not first.result["unresolved_effects"]
        assert runtime.cleanup_status()["state"] == "clear"
        assert runtime.step().result["accepted"]
        assert invoked == [request.operation_id, "next"]
        assert runtime.admission_status()["ready"]
        assert "PRIVATE" not in str(runtime.channel_status())
        assert len(closed) == (0 if fault == "factory" else 2)
    finally:
        runtime.close()


def test_inflight_typing_preserves_answer_and_retains_cleanup_owner(tmp_path):
    runtime, requests, _, request = setup(tmp_path)
    entered, release = threading.Event(), threading.Event()
    events = []

    def send():
        entered.set()
        assert release.wait(3)
        events.append("typing-ended")

    worker = ChannelTypingWorker(SimpleNamespace(send=send), lambda: True, join_timeout=0.02)
    runtime.channel_typing = lambda *_: worker
    runtime.channel_delivery = lambda **kw: None

    class Backend:
        def invoke(self, admitted):
            assert entered.wait(2)
            return AgentTurnObservation(admitted, "completed", "fixture", 1, 1, output_text="done")

        def close(self):
            events.append("backend-closed")

    backend = Backend()
    runtime.backend_factory = lambda *a, **kw: backend
    runtime.start()
    try:
        requests.append(request)
        result = runtime.step()
        assert result.result["accepted"] is True and result.result["unresolved_effects"] is False
        assert runtime._typing is worker and runtime._backend is backend
        assert worker._thread.is_alive() and events == []
        assert runtime.step() is None
        assert runtime.cleanup_status()["state"] == "exhausted"
        assert events == []
    finally:
        release.set()
        worker.join_timeout = 2
        runtime.close()
    assert runtime.state == "closed" and not worker._thread.is_alive()
    assert runtime._typing is None and runtime._backend is None
    assert events == ["typing-ended", "backend-closed"]


@pytest.mark.parametrize("stop", ["cancelled", "deadline", "revoked"])
def test_composed_predicate_stops_refresh_when_turn_loses_authority(tmp_path, monkeypatch, stop):
    repo, snapshot, request, route = state(tmp_path)
    cancelled = [False]
    calls = []
    monkeypatch.setenv("AHC_TYPING_TEST", "fixture")
    monkeypatch.setattr(foreground_typing, "BotTypingTransport",
                        lambda *a: SimpleNamespace(send=lambda: calls.append("typing")))
    monkeypatch.setattr(foreground_typing.time, "monotonic", lambda: 0)
    config = SimpleNamespace(to_dict=lambda: {"channels": [{"route": route.to_dict(),
        "enabled": True, "typing": True, "token_env": "AHC_TYPING_TEST"}]})
    worker = foreground_typing.compose_typing(config, repo)(request, lambda: cancelled[0])
    try:
        assert worker.can_send() is False
        repo.begin(snapshot, request)
        assert worker._attempt() is True and calls == ["typing"]
        if stop == "cancelled":
            cancelled[0] = True
        elif stop == "deadline":
            monkeypatch.setattr(foreground_typing.time, "monotonic", lambda: request.deadline)
        else:
            WorkingContinuityService(repo).cancel(repo.read_current("session"))
        assert worker._attempt() is False
        assert calls == ["typing"]
    finally:
        worker.close()
