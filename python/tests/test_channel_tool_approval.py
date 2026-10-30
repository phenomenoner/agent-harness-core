from dataclasses import replace
from types import SimpleNamespace

from test_codex_effective_policy import policy, request

from ahc.application.channel_tool_approval import ChannelToolGate
from ahc.domain.runtime_approval import ApprovalDecision


class Port:
    def __init__(self, outcome="approve-once", *, mutate=None):
        self.outcome, self.mutate, self.requests = outcome, mutate, []

    def decide(self, pending, *, cancelled):
        self.requests.append(pending)
        if self.mutate is not None:
            return self.mutate(pending)
        return ApprovalDecision(pending.digest, self.outcome)


def admitted(*, mode="manual", capabilities=("workspace.execute",), deadline=100):
    effective = policy(approval_mode="yolo" if mode == "yolo" else "manual")
    base = replace(
        request(SimpleNamespace(configuration_digest="c" * 64), policy()),
        capabilities=capabilities,
        deadline=deadline,
        effective_policy=effective,
    )
    if mode not in {"manual", "yolo"}:
        effective = SimpleNamespace(
            approval_mode=mode,
            approval_wait_seconds=30.0,
            digest=effective.digest,
        )
        return SimpleNamespace(
            session=base.session,
            operation_id=base.operation_id,
            capabilities=base.capabilities,
            deadline=base.deadline,
            effective_policy=effective,
        )
    return base


def authorize(gate, capability="workspace.execute", arguments=None, **kwargs):
    return gate.authorize(
        capability, {"code": "print('ok')"} if arguments is None else arguments,
        kwargs.get("thread", "thread"), kwargs.get("turn", "turn"), kwargs.get("call_id", "call"),
    )


def test_manual_dynamic_python_requires_correlated_port_decision():
    port = Port()
    assert authorize(ChannelToolGate(admitted(), port, clock=lambda: 10, wall_clock=lambda: 20))
    pending = port.requests[0]
    assert pending.backend_thread == "thread" and pending.backend_turn == "turn"
    assert pending.vendor_request_id == "tool:call"
    assert pending.action_digest != "0" * 64
    assert pending.expires_at == 50


def test_auto_approve_only_bypasses_port_for_known_readonly_capabilities():
    admitted_read = admitted(mode="auto-approve", capabilities=("workspace.inspect",))
    assert authorize(ChannelToolGate(admitted_read, None, clock=lambda: 10), "workspace.inspect")

    port = Port()
    admitted_write = admitted(mode="auto-approve", capabilities=("workspace.execute",))
    assert authorize(ChannelToolGate(admitted_write, port, clock=lambda: 10), "workspace.execute")
    assert len(port.requests) == 1


def test_always_approve_and_yolo_never_expand_capabilities():
    for mode in ("always-approve", "yolo"):
        gate = ChannelToolGate(
            admitted(mode=mode, capabilities=("workspace.execute",)), None, clock=lambda: 10
        )
        assert authorize(gate)
        assert not authorize(gate, "workspace.inspect")


def test_denied_cancelled_expired_or_mismatched_decision_cannot_authorize():
    assert not authorize(ChannelToolGate(admitted(), Port("deny"), clock=lambda: 10))
    assert not authorize(
        ChannelToolGate(admitted(), Port(), cancelled=lambda: True, clock=lambda: 10)
    )
    assert not authorize(ChannelToolGate(admitted(deadline=10), Port(), clock=lambda: 10))
    mismatch = Port(mutate=lambda pending: ApprovalDecision("d" * 64, "approve-once"))
    assert not authorize(ChannelToolGate(admitted(), mismatch, clock=lambda: 10))


def test_cancellation_or_expiry_after_wait_cannot_authorize():
    cancelled = [False]

    class CancellingPort(Port):
        def decide(self, pending, *, cancelled):
            cancelled_state[0] = True
            return ApprovalDecision(pending.digest, "approve-once")

    cancelled_state = cancelled
    assert not authorize(
        ChannelToolGate(
            admitted(), CancellingPort(), cancelled=lambda: cancelled[0], clock=lambda: 10
        )
    )

    clock = [10]

    class ExpiringPort(Port):
        def decide(self, pending, *, cancelled):
            clock[0] = 100
            return ApprovalDecision(pending.digest, "approve-once")

    assert not authorize(ChannelToolGate(admitted(), ExpiringPort(), clock=lambda: clock[0]))


def test_action_summary_is_bounded_and_redacts_obvious_secrets():
    port = Port()
    assert authorize(
        ChannelToolGate(admitted(), port, clock=lambda: 10),
        arguments={"token": "secret", "Authorization": "Bearer private", "code": "x" * 2000},
    )
    summary = port.requests[0].action_summary
    assert len(summary) <= 1024
    assert "secret" not in summary and "private" not in summary
