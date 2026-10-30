"""Review-discovered delivery and cleanup seams, without provider/native workloads."""

import threading
from dataclasses import replace
from types import SimpleNamespace

import pytest
from test_channel_controls import make
from test_runtime_core import Backend

from ahc.application.background_schedule import BackgroundSchedule
from ahc.application.foreground_runtime import ForegroundRuntime
from ahc.application.runtime_service import RuntimeService
from ahc.cli.foreground_channels import BotTextTransport, compose_channels
from ahc.domain.sessions import ContractError, VirtualSessionIdentity


def test_fresh_channel_composition_sends_foreground_answer_before_background_release(tmp_path, monkeypatch):
    store, current, template, route, _, _, submit = make(tmp_path)
    event = submit("answer the foreground question")
    request = replace(template, operation_id=event.operation_id, input_text=event.input_text)
    assert RuntimeService(store, Backend()).execute(current, request).result["accepted"]
    entered, release = threading.Event(), threading.Event()
    session = VirtualSessionIdentity.initial("background", current.session.agent_id, current.session.route_id)
    def model_lease():
        with store.execution_lock(session=session, workspace_id="background-workbench"):
            entered.set()
            assert release.wait(10)
    worker = threading.Thread(target=model_lease)
    sent = []
    monkeypatch.setenv("AHC_REVIEW_DELIVERY", "fixture")
    monkeypatch.setattr(BotTextTransport, "send", lambda self, destination, payload, operation:
        sent.append((destination, payload["output_text"])) or "fixture-receipt")
    config = SimpleNamespace(to_dict=lambda: {"channels":[{"route":route.to_dict(),
        "token_env":"AHC_REVIEW_DELIVERY", "enabled":True}]})
    worker.start()
    try:
        assert entered.wait(5)
        deliver = compose_channels(config, store)  # All three fresh-route reconciliations run.
        deliver(cancelled=lambda: False)
        assert sent == [(route, "💬 **回覆**\n\nanswer")]
        assert not release.is_set()
        deliver(cancelled=lambda: False)
        assert len(sent) == 1
    finally:
        release.set()
        worker.join(timeout=5)
    assert not worker.is_alive()


def test_background_cleanup_failure_stays_unverified_in_main_close_and_restart(tmp_path):
    store, current, _, _, controls, _, submit = make(tmp_path)
    manager = BackgroundSchedule(store, current.session, lambda binding: None,
                                 tick=lambda current: None)
    class BrokenWorker:
        held = True
        def close(self):
            raise ContractError("exact background custody cleanup unresolved")
    retained = BrokenWorker()
    manager._runtimes["retained-workbench"] = retained
    foreground = ForegroundRuntime(store, current.session, lambda *args: None,
        lambda *args: None, background_schedule=manager)
    controls.bind_background_status(manager.status)
    controls.prepare_background_restart = manager.prepare_restart
    foreground.start()
    with pytest.raises(ContractError, match="shutdown unverified"):
        foreground.close()
    assert manager.status()["state"] == "close-unverified"
    assert foreground.state == "close-unverified" and retained.held
    with pytest.raises(ContractError, match="shutdown unverified"):
        foreground.close()
    restart = submit("/restart")
    store._mutate(lambda c:c.execute("UPDATE channel_controls SET state='sent' WHERE operation_id=?",
                                    (restart.operation_id,)))
    assert controls.restart_ready() is False
