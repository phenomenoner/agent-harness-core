import pytest
from profile_test_support import profile_registry_payload

from ahc.application.memory_pack_service import MemoryPackService, MemoryPackUnavailable
from ahc.domain.capability_values import CapabilityGrant
from ahc.domain.profile_registry import AgentProfileRegistry
from ahc.domain.sessions import ContractError, VirtualSessionIdentity


def service(callback, *, granted=True, resource="hexi.durable-memory"):
    return MemoryPackService(
        registry=AgentProfileRegistry.from_dict(profile_registry_payload()),
        agent_id="hexi", resource_id=resource, scope="private-hexi",
        grants=(CapabilityGrant("hexi", "hexi.durable-memory", "memory.pack"),) if granted else (),
        pack=callback,
    )


@pytest.mark.parametrize("agent,query,budget,granted", [
    ("lizi", "hello", 500, True), ("hexi", "hello", 500, False),
    ("hexi", "hello", True, True), ("hexi", " hello ", 500, True),
])
def test_rejection_has_zero_provider_calls(agent, query, budget, granted):
    calls = []
    bound = service(lambda *a, **k: calls.append(k), granted=granted)
    session = VirtualSessionIdentity.initial("session", agent, "route")
    with pytest.raises(ContractError):
        bound.pack(session, query, budget_tokens=budget)
    assert calls == []


def test_cross_resource_binding_rejected_before_call():
    calls = []
    with pytest.raises(ContractError):
        service(lambda *a, **k: calls.append(k), resource="lizi.durable-memory")
    assert calls == []


def test_provider_failure_is_sanitized_and_not_retried():
    calls = []

    def fail(query, **options):
        calls.append(options)
        raise RuntimeError("PRIVATE-CREDENTIAL")

    with pytest.raises(MemoryPackUnavailable) as error:
        service(fail).pack(VirtualSessionIdentity.initial("session", "hexi", "route"), "hello")
    assert "PRIVATE" not in str(error.value)
    assert len(calls) == 1
    assert calls[0]["scope"] == "private-hexi" and calls[0]["use_tracking"] is False
