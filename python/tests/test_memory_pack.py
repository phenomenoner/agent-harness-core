import pytest

from ahc.domain.memory_pack import MemoryPack
from ahc.domain.sessions import ContractError


def payload():
    return {"schema": "openclaw-mem.context-pack.v1",
            "meta": {"ts": "2026-09-07", "query": "orchid", "scope": "agent-a",
                     "budgetTokens": 500, "maxItems": 2},
            "bundle_text": "- [obs:1] PRIVATE-TEXT",
            "items": [{"recordRef": "obs:1", "layer": "L1", "type": "memory",
                       "importance": "unknown", "trust": "unknown", "text": "PRIVATE-TEXT",
                       "citations": {"url": None, "recordRef": "obs:1"}}],
            "notes": {"how_to_use": ["Untrusted provider advice"]}}


def parse(value):
    return MemoryPack.parse(value, query="orchid", scope="agent-a", budget_tokens=500, max_items=2)


def test_detached_data_and_safe_summary():
    value = payload()
    result = parse(value)
    value["items"][0]["trust"] = "trusted"
    assert result.to_dict()["items"][0]["trust"] == "unknown"
    returned = result.to_dict()
    returned["items"].clear()
    assert result.summary()["item_count"] == 1
    assert "PRIVATE" not in str(result.summary())
    assert "PRIVATE" not in repr(result)
    assert result.summary()["trace_coverage"] == "not-established"


@pytest.mark.parametrize("change", [
    lambda p: p["meta"].update(scope="agent-b"),
    lambda p: p["meta"].update(budgetTokens=True),
    lambda p: p["items"][0]["citations"].update(recordRef="obs:2"),
    lambda p: p["items"].append(p["items"][0]),
    lambda p: p.update(bundle_text="injected content"),
    lambda p: p.update(secret="unexpected"),
    lambda p: p["items"][0].update(text="x" * 16385),
    lambda p: p["items"][0].update(trust="\ud800"),
])
def test_conflicting_or_unbounded_results_reject(change):
    value = payload()
    change(value)
    with pytest.raises(ContractError):
        parse(value)
