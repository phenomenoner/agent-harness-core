"""Informational snapshots survive the backend boundary without becoming quotas."""

from dataclasses import FrozenInstanceError, replace

import pytest
from test_codex_client import setup

from ahc.domain.sessions import ContractError


def event(value=2):
    counts = {"inputTokens": 1, "outputTokens": 1, "cachedInputTokens": 0,
              "reasoningOutputTokens": 0, "totalTokens": value}
    return {"method": "thread/tokenUsage/updated", "params": {
        "threadId": "thread", "turnId": "turn",
        "tokenUsage": {"last": counts, "total": dict(counts, totalTokens=value + 10)}}}


def test_latest_snapshot_is_immutable_and_never_summed_or_complete():
    peer, client, request = setup()
    frame = event(4)
    peer.messages[3:3] = [event(), frame, frame]
    result = client.run_admitted(request)
    assert result.disposition == "completed" and result.vendor_usage is None
    tokens = result.token_observation
    assert tokens.last.total_tokens == 4 and tokens.total.total_tokens == 14
    frame["params"]["tokenUsage"]["last"]["totalTokens"] = 999
    assert tokens.last.total_tokens == 4
    with pytest.raises(FrozenInstanceError):
        tokens.last.total_tokens = 100
    public = result.public_dict()
    assert public["token_observation"]["complete"] is False
    assert public["token_observation"]["last"]["cache_write_input_tokens"] is None
    assert public["vendor_usage"] is None


@pytest.mark.parametrize("bad", ["thread", "turn", "bool", "negative", "oversized"])
def test_invalid_or_foreign_snapshot_is_not_projected(bad):
    peer, client, request = setup()
    frame = event()
    if bad in ("thread", "turn"):
        frame["params"][bad + "Id"] = "foreign"
    else:
        frame["params"]["tokenUsage"]["last"]["totalTokens"] = {
            "bool": True, "negative": -1, "oversized": 2**63}[bad]
    peer.messages.insert(3, frame)
    result = client.run_admitted(request)
    assert result.disposition == "completed" and result.token_observation is None
    assert "token_observation" not in result.public_dict()
    assert sum(item.get("method") == "turn/start" for item in peer.sent) == 1


def test_unknown_terminal_retains_last_observation_without_accepting_result():
    peer, client, request = setup()
    peer.messages.insert(3, event())
    peer.messages[-1] = {"method": "invalid"}
    result = client.run_admitted(request)
    assert result.disposition == "unknown" and result.unresolved_effects
    assert result.public_dict()["token_observation"]["last"]["total_tokens"] == 2
    assert result.output_text == ""


def test_absent_snapshot_and_foreign_projection_do_not_invent_usage():
    peer, client, request = setup()
    result = client.run_admitted(request)
    assert result.token_observation is None and "token_observation" not in result.public_dict()
    peer, client, request = setup()
    peer.messages.insert(3, event())
    result = client.run_admitted(request)
    with pytest.raises(ContractError, match="binding"):
        replace(result, backend_turn="foreign").public_dict()
    with pytest.raises(ContractError, match="binding"):
        replace(result, token_observation={"total": 0}).public_dict()
