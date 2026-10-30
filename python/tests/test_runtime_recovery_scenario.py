from test_channel_controls import make
from test_workspace_tools import Port

from ahc.adapters.wsl_unit_runner import WslControlTransportUnavailable
from ahc.application.foreground_runtime import ForegroundRuntime
from ahc.application.local_runtime_ingress import LocalRuntimeIngress
from ahc.application.runtime_service import RuntimeService
from ahc.domain.agent_backend import AgentTurnObservation
from ahc.domain.channel_ingress import ChannelInboundEvent
from ahc.infrastructure.runtime_inbox import RuntimeInbox


def test_completed_answer_cleanup_failure_quoted_queue_notice_and_safe_next_turn(tmp_path):
    store, current, _, route, controls, channel, submit = make(tmp_path)
    invoked, closed, probes, delivered = [], [], [], []
    now = [0]
    class Backend:
        def invoke(self, bound):
            invoked.append(bound.operation_id)
            return AgentTurnObservation(bound, "completed", "fixture", 1, 1,
                                        "thread", "turn", "answer")
        def close(self):
            closed.append(True)
            if len(closed) <= 2:
                raise WslControlTransportUnavailable(control_phase="receive")
        def recovery_ready(self):
            probes.append(True)
            return True
    runtime = ForegroundRuntime(store, current.session,
        LocalRuntimeIngress(RuntimeInbox(store), artifact_digest="a" * 64,
            configuration_digest=lambda _: "b" * 64, workspace_scope="work"),
        lambda *_args, **_kwargs: Backend(),
        open_workspace=lambda snapshot: RuntimeService(store, None).open_workspace(snapshot, "work", Port))
    controls.bind_admission_status(runtime.admission_status)
    class Transport:
        def send(self, target, payload, identifier):
            assert target == route
            delivered.append(payload["output_text"])
            return "receipt"
    runtime.start()
    try:
        first = submit("first task")
        assert runtime.step().result["accepted"]
        old_result = store.read_turn(current.session, first.operation_id).result
        runtime._recovery.clock = lambda: now[0]
        second = ChannelInboundEvent(route, "quoted-next", "continue useful work",
            context={"message_id": "next", "reply_to_id": "prior", "quote": "earlier answer",
                     "quote_status": "available"})
        channel.submit(second, current.goal.digest)
        runtime.step()  # second failed cleanup, no model replay
        runtime.step()  # schedules finite read-only recovery
        assert runtime.admission_status()["ready"] is False
        controls.deliver_one({route: Transport()})
        controls.deliver_one({route: Transport()})
        assert len(delivered) == 1 and "保留" in delivered[0]
        assert RuntimeInbox(store).next_pending(current.session)["operation_id"] == second.operation_id
        assert invoked == [first.operation_id]
        now[0] = 15
        runtime.step()  # new probe evidence; exact original cleanup only
        assert probes == [True] and len(closed) == 3
        assert invoked == [first.operation_id] and runtime.admission_status()["ready"]
        assert runtime.step().result["accepted"]
        assert invoked == [first.operation_id, second.operation_id]
        assert store.read_turn(current.session, first.operation_id).result == old_result
        assert RuntimeInbox(store).next_pending(current.session) is None
        controls.deliver_one({route: Transport()})
        assert len(delivered) == 1
    finally:
        runtime.close()
