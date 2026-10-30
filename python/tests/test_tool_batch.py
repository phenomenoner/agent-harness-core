import pytest

from ahc.application.capability_view import _issue_agent_capability_view
from ahc.application.tool_batch import BatchCall, ReadOnlyBatch
from ahc.domain.capability_values import ApprovalMode, CapabilityDefinition, EffectClass
from ahc.domain.profile_values import ProfileResourceKind
from ahc.domain.sessions import ContractError, VirtualSessionIdentity


def fixture():
    session = VirtualSessionIdentity.initial("session", "agent", "route")
    kind = next(iter(ProfileResourceKind))
    view = _issue_agent_capability_view(
        (CapabilityDefinition("read", kind, EffectClass.READ_ONLY, ApprovalMode.NONE),),
        (("read", "resource"),),
    )

    class Port:
        seen = []

        def invoke(self, expected, call, deadline, cancelled):
            assert expected == session
            self.seen.append(call.call_id)
            if call.call_id == "lost":
                raise OSError("private transport detail")
            return {"raw": "retained local data"}

    port = Port()
    port.seen = []
    return session, ReadOnlyBatch(session, view, port, clock=lambda: 0), port


def call(identifier, resource="resource"):
    return BatchCall(identifier, "read", resource, "{}")


def test_mixed_receipts_grants_local_values_and_no_replay():
    session, batch, port = fixture()
    result = batch.run(session, (call("one"), call("denied", "other"), call("lost")), deadline=1)
    assert [r["state"] for r in result.receipts] == ["success", "rejected", "unknown"]
    assert port.seen == ["one", "lost"]
    assert result.values[0] == {"raw": "retained local data"}
    assert "retained local data" not in str(result.summary())
    assert "private transport detail" not in str(result)
    with pytest.raises(ContractError, match="replayed"):
        batch.run(session, (call("lost"),), deadline=1)


def test_scope_budget_cancel_and_late_result():
    session, batch, port = fixture()
    with pytest.raises(ContractError):
        other = VirtualSessionIdentity.initial("other", "agent", "route")
        batch.run(other, (call("one"),), deadline=1)
    result = batch.run(session, (call("cancel"),), deadline=1, cancelled=lambda: True)
    assert result.receipts[0]["state"] == "rejected"
    assert port.seen == []
    ticks = iter((0, 2))
    batch.clock = lambda: next(ticks)
    result = batch.run(session, (call("late"),), deadline=1)
    assert result.receipts[0]["state"] == "unknown"
    assert port.seen == ["late"]


@pytest.mark.parametrize("arguments", ["[]", '{"x":NaN}', "invalid", " " * 16385])
def test_invalid_arguments(arguments):
    with pytest.raises(ContractError):
        BatchCall("one", "read", "resource", arguments)


def test_effectful_grant_is_not_admitted_as_read_only():
    session, batch, port = fixture()
    batch.view = _issue_agent_capability_view(
        (CapabilityDefinition("read", next(iter(ProfileResourceKind)),
                              EffectClass.REVERSIBLE_LOCAL, ApprovalMode.NONE),),
        (("read", "resource"),),
    )
    result = batch.run(session, (call("effect"),), deadline=1)
    assert result.receipts[0]["reason"] == "effect-mismatch"
    assert port.seen == []


def test_batch_size_and_total_budget_are_enforced_before_dispatch():
    session, batch, port = fixture()
    with pytest.raises(ContractError):
        batch.run(session, tuple(call(f"item{i}") for i in range(17)), deadline=1)
    for wave in range(16):
        batch.run(session, tuple(call(f"w{wave}i{i}") for i in range(16)), deadline=1)
    with pytest.raises(ContractError, match="budget"):
        batch.run(session, (call("overflow"),), deadline=1)
    assert len(port.seen) == 256
