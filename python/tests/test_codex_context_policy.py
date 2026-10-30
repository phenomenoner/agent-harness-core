"""Selected-model compaction policy, native observation and legacy compatibility."""

import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest
from test_codex_client import setup

from ahc.adapters.codex_client import CodexProtocolClient
from ahc.adapters.codex_context_policy import catalog_window, compact_limit, context_policy
from ahc.domain.sessions import ContractError

POLICY = {"soft_limit_percent": 80, "headroom_tokens": 32768}


def usage(value, *, total=None):
    def counts(n):
        return {"inputTokens": n, "outputTokens": 0, "cachedInputTokens": 0,
                "reasoningOutputTokens": 0, "totalTokens": n}
    return {"method": "thread/tokenUsage/updated", "params": {"threadId": "thread",
        "turnId": "turn", "tokenUsage": {"last": counts(value),
            "total": counts(value if total is None else total), "modelContextWindow": 475000}}}


def test_native_limit_uses_effective_window_with_headroom_not_catalog_maximum(tmp_path):
    now = datetime.now(UTC)
    (tmp_path / "models_cache.json").write_text(json.dumps({"fetched_at": now.isoformat(),
        "models": [{"slug": "selected", "context_window": 500000,
                    "max_context_window": 872000, "effective_context_window_percent": 95}]}))
    window = catalog_window(tmp_path, "selected", now=now)
    assert window == 475000 and compact_limit(POLICY, window) == 380000
    assert catalog_window(tmp_path, "foreign", now=now) is None
    assert catalog_window(tmp_path, "selected", now=now + timedelta(days=2)) is None
    assert compact_limit(POLICY, 40000) == 7232
    assert compact_limit(POLICY, None) is None


def test_selected_native_threshold_and_context_metadata_reach_protocol_and_result():
    peer, _, request = setup()
    peer.messages[1]["result"]["model"] = "selected"
    client = CodexProtocolClient(peer, cwd="workspace", model="selected", clock=lambda: 0,
        context_policy=POLICY, context_window=475000)
    request = replace(request, configuration_digest=client.configuration_digest)
    peer.messages.insert(3, usage(146212, total=3377401))
    result = client.run_admitted(request)
    start = next(m for m in peer.sent if m.get("method") == "thread/start")
    assert start["params"]["config"] == {"model_auto_compact_token_limit": 380000}
    assert result.token_observation.model_context_window == 475000
    assert result.token_observation.model == "selected"
    assert result.token_observation.last.input_tokens == 146212
    assert result.token_observation.total.input_tokens == 3377401


def test_exact_compaction_boundary_records_before_and_first_following_usage():
    peer, client, request = setup()
    def boundary(method):
        return {"method": method, "params": {"threadId": "thread", "turnId": "turn",
            "item": {"id": "compact", "type": "contextCompaction"}}}
    peer.messages[3:3] = [usage(350000, total=5000000), boundary("item/started"),
        boundary("item/completed"), usage(60000, total=5060000), usage(70000, total=5130000)]
    result = client.run_admitted(request)
    assert result.disposition == "completed"
    compaction = result.token_observation.to_dict()["context_compaction"]
    assert compaction == {"count": 1, "before_tokens": 350000, "after_tokens": 60000}


def test_unmatched_compaction_end_does_not_fabricate_a_completed_compaction():
    peer, client, request = setup()
    peer.messages[3:3] = [{"method": "item/completed", "params": {"threadId": "thread",
        "turnId": "turn", "item": {"id": "compact", "type": "contextCompaction"}}}, usage(60000)]
    result = client.run_admitted(request)
    assert result.disposition == "completed"
    assert "context_compaction" not in result.token_observation.to_dict()


@pytest.mark.parametrize("policy", [None, {"soft_limit_percent": 49, "headroom_tokens": 32768},
    {"soft_limit_percent": True, "headroom_tokens": 32768},
    {"soft_limit_percent": 80, "headroom_tokens": 0}, {"unknown": 1}])
def test_context_policy_validation_and_native_default(policy):
    if policy is None:
        assert context_policy(policy) is None
    else:
        with pytest.raises(ContractError):
            context_policy(policy)
