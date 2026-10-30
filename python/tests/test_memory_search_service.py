import pytest
from profile_test_support import profile_registry_payload

from ahc.application.memory_search_service import MemorySearchService, MemorySearchUnavailable
from ahc.domain.capability_values import CapabilityGrant
from ahc.domain.profile_registry import AgentProfileRegistry
from ahc.domain.sessions import ContractError, VirtualSessionIdentity


def service(callback, granted=True):
    return MemorySearchService(
        registry=AgentProfileRegistry.from_dict(profile_registry_payload()), agent_id="hexi",
        resource_id="hexi.durable-memory", scope="hexi",
        grants=((CapabilityGrant("hexi", "hexi.durable-memory", "memory.search"),)
                if granted else ()),
        search=callback)


@pytest.mark.parametrize("rows", [None, {}, [{"id": True, "summary": "x"}],
                                  [{"id": 1, "summary": "x"}] * 2,
                                  [{"id": 1, "summary": None}],
                                  [{"id": 1, "summary": "x" * 16385}]])
def test_invalid_public_results_are_unavailable_without_raw_output(rows):
    with pytest.raises(MemorySearchUnavailable):
        service(lambda *a, **kw: rows).search(
            VirtualSessionIdentity.initial("session", "hexi", "route"), "orchid")


@pytest.mark.parametrize("agent,grant,query,limit", [
    ("lizi", True, "orchid", 8), ("hexi", False, "orchid", 8),
    ("hexi", True, " orchid ", 8), ("hexi", True, "orchid", True),
])
def test_invalid_admission_never_calls_provider(agent, grant, query, limit):
    with pytest.raises(ContractError):
        service(lambda *a, **kw: pytest.fail("unadmitted provider call"), grant).search(
            VirtualSessionIdentity.initial("session", agent, "route"), query, limit=limit)


def test_search_projects_only_bounded_records_and_no_completeness_claim():
    calls = []

    def public_search(query, **options):
        calls.append(options)
        return [{"id": 9, "summary": "orchid", "detail_json": "PRIVATE", "score": 1}]

    result = service(public_search).search(
        VirtualSessionIdentity.initial("session", "hexi", "route"), "orchid")
    assert result["records"] == [{"provider_id": 9, "summary": "orchid"}]
    assert result["complete_recall"] is False
    assert result["trace_coverage"] == "not-established"
    assert "PRIVATE" not in str(result)
    assert calls[0]["scope"] == "hexi" and calls[0]["include_archived"] is False
