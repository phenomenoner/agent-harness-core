"""Long valid conversations and aligned relay budgets, without provider calls."""
import pytest
from test_codex_client import setup


@pytest.mark.parametrize('count', [200, 5000])
def test_valid_notification_sequence_does_not_exhaust_short_turn_budget(count):
    peer, client, request = setup()
    event = {
        'method': 'remoteControl/status/changed',
        'params': {'installationId': 'PRIVATE', 'serverName': 'PRIVATE', 'status': 'disabled'},
    }
    peer.messages[1:1] = [event] * count
    result = client.run_admitted(request)
    assert result.disposition == 'completed'
    assert result.output_text == 'answer'
    assert client.frames == count + 4


def test_long_turn_deltas_reach_full_terminal():
    peer, client, request = setup()
    event = {'method': 'item/agentMessage/delta', 'params': {
        'threadId': 'thread', 'turnId': 'turn', 'delta': 'not final',
    }}
    peer.messages[-1:-1] = [event] * 5000
    result = client.run_admitted(request)
    assert result.disposition == 'completed'
    assert result.output_text == 'answer'
