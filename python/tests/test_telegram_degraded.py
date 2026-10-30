import pytest
from test_foreground_trusted import config_file

from ahc.adapters.telegram_poll import TelegramPollingUnavailable
from ahc.cli import foreground_telegram
from ahc.cli.foreground_config_v2 import parse_foreground_config_v2
from ahc.domain.channel_ingress import ChannelRoute
from ahc.domain.sessions import ContractError
from ahc.infrastructure.telegram_inbox import TelegramInbox


@pytest.mark.parametrize("recover", [False, True])
def test_poll_failure_budget_retains_cursor_and_exposes_state(tmp_path, monkeypatch, recover):
    runtime, request, value, _ = config_file(tmp_path, enabled=True)
    route = ChannelRoute("telegram", "123", "456", "789", request.session)
    value["channels"] = [{"route": route.to_dict(), "token_env": "AHC_POLL_TEST", "enabled": False}]
    value["telegram_polling"] = {"account_id": "123", "token_env": "AHC_POLL_TEST",
        "enabled": True, "exclusive_consumer": True, "interval_seconds": 2}
    config = parse_foreground_config_v2(value)
    calls, probes, clock = [], [], [10.0]

    class Poll:
        def __init__(self, *args, **kwargs):
            pass

        def poll(self, offset):
            calls.append(offset)
            if recover and len(calls) == 2:
                return []
            raise TelegramPollingUnavailable("fixture-private")

        def probe(self):
            probes.append(clock[0])
            raise TelegramPollingUnavailable("fixture-private")

    monkeypatch.setenv("AHC_POLL_TEST", "fixture")
    monkeypatch.setattr(foreground_telegram, "TelegramPoll", Poll)
    monkeypatch.setattr(foreground_telegram.time, "monotonic", lambda: clock[0])
    receive = foreground_telegram.compose_telegram(config, runtime.store)
    receive(cancelled=lambda: False)
    assert receive.status()["state"] == "degraded"
    receive(cancelled=lambda: False)
    assert len(calls) == 1
    clock[0] += 2
    receive(cancelled=lambda: False)
    assert receive.status()["state"] == ("ready" if recover else "backoff")
    assert "fixture-private" not in str(receive.status())
    assert TelegramInbox(runtime.store, [route]).offset() is None
    if not recover:
        clock[0] += 100
        receive(cancelled=lambda: False)
        assert len(calls) == 2 and probes == [112]
    else:
        # Store/contract failures are not mistaken for an unavailable provider.
        def broken(self, offset):
            raise ContractError("local invariant failed")
        monkeypatch.setattr(Poll, "poll", broken)
        clock[0] += 2
        with pytest.raises(ContractError, match="local invariant"):
            receive(cancelled=lambda: False)
