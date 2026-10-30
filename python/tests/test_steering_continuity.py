from dataclasses import replace
from types import SimpleNamespace

from test_channel_controls import make

from ahc.domain.agent_backend import AgentTurnObservation
from ahc.domain.runtime_turn import accept_observation
from ahc.infrastructure.runtime_conversation_context import conversation_context
from ahc.infrastructure.runtime_inbox import RuntimeInbox


def test_confirmed_steering_survives_next_turn_without_replaying_unknown(tmp_path):
    store, current, request, _, controls, _, submit = make(tmp_path)
    event = submit("original task")
    request = replace(request, operation_id=event.operation_id, input_text=event.input_text)
    store.begin(current, request)
    submit("/steer keep this correction")
    port = controls.steering_port(request)
    sent = []
    client = SimpleNamespace(thread="thread", turn="turn", _send=sent.append)
    port.poll(client)
    port.consume_response({"id": sent[0]["id"], "result": {"turnId": "turn"}})
    submit("/steer uncertain action")
    port.poll(client)
    port.reconcile_terminal()
    observed = AgentTurnObservation(request, "unknown", "fixture", 1, 1,
                                    unresolved_effects=True)
    store.finish(request, accept_observation(request, observed))
    next_event = submit("continue")
    history = conversation_context(store, current.session, next_event.operation_id,
                                   include_incomplete=True)
    assert history[0]["user_steering"] == "keep this correction"
    assert "uncertain action" not in str(history)


def test_late_unsent_steer_reconciles_after_terminal_without_restart(tmp_path):
    store, current, request, route, controls, _, submit = make(tmp_path)
    event = submit("original task")
    request = replace(request, operation_id=event.operation_id, input_text=event.input_text)
    store.begin(current, request)
    late = submit("/steer late supplement")
    observed = AgentTurnObservation(request, "unknown", "fixture", 1, 1,
                                    unresolved_effects=True)
    store.finish(request, accept_observation(request, observed))
    controls.deliver_one({route: SimpleNamespace(send=lambda *a: "receipt")})
    assert RuntimeInbox(store).query(current.session, late.operation_id)["status"] == "pending"
