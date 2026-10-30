from types import SimpleNamespace

import pytest
from test_runtime_core import setup
from test_telegram_inbox import update

from ahc.adapters.telegram_poll import TelegramPoll, TelegramPollingUnavailable
from ahc.cli import foreground_telegram
from ahc.domain.channel_ingress import ChannelRoute
from ahc.infrastructure.telegram_inbox import TelegramInbox


@pytest.mark.parametrize("error,retryable", [(TimeoutError("secret"), True),
    (OSError("secret"), True), (ValueError("secret"), False)])
def test_only_transport_failures_are_recoverable(error, retryable):
    def request(*_):
        raise error
    with pytest.raises(TelegramPollingUnavailable) as observed:
        TelegramPoll("123", "fixture", request=request).poll()
    assert observed.value.retryable is retryable
    assert "secret" not in str(observed.value)


def receiver(tmp_path, monkeypatch, outcomes, *, recovery=None, probe_outcomes=()):
    store, snapshot, _ = setup(tmp_path)
    route = ChannelRoute("telegram", "123", "456", "789", snapshot.session)
    clock, calls = [0.0], []
    pending = iter(outcomes)
    probes, probe_values = [], iter(probe_outcomes)

    class Poll:
        def __init__(self, *_args, **_kwargs):
            pass

        def poll(self, offset):
            calls.append((clock[0], offset))
            value = next(pending)
            if isinstance(value, Exception):
                raise value
            return value

        def probe(self):
            probes.append(clock[0])
            value = next(probe_values)
            if isinstance(value, Exception):
                raise value

    monkeypatch.setenv("POLL_FIXTURE_TOKEN", "fixture")
    monkeypatch.setattr(foreground_telegram, "TelegramPoll", Poll)
    monkeypatch.setattr(foreground_telegram.time, "monotonic", lambda: clock[0])
    document = {"channels": [{"route": route.to_dict()}], "telegram_polling": {
        "enabled": True, "token_env": "POLL_FIXTURE_TOKEN", "account_id": "123",
        "interval_seconds": 1}}
    if recovery is not None:
        document["telegram_polling"]["transport_recovery"] = recovery
    receive = foreground_telegram.compose_telegram(SimpleNamespace(to_dict=lambda: document), store)
    receive.test_probes = probes
    return receive, clock, calls, TelegramInbox(store, [route])


def transient():
    return TelegramPollingUnavailable("unverified", retryable=True)


def test_recovery_backoff_preserves_offset_and_resets_after_success(tmp_path, monkeypatch):
    receive, clock, calls, inbox = receiver(tmp_path, monkeypatch, [
        transient(), transient(), [update(7)], transient(), transient(), []])
    for stamp in (0, 1, 2, 30):
        clock[0] = stamp
        receive(cancelled=lambda: False)
    assert receive.status()["state"] == "backoff"
    assert len(calls) == 2 and inbox.offset() is None
    clock[0] = 31
    receive(cancelled=lambda: False)
    assert receive.status()["state"] == "ready"
    assert receive.status()["recovery_attempts"] == 0
    assert calls[-1] == (31, None) and inbox.offset() == 8
    for stamp in (32, 33, 63):
        clock[0] = stamp
        receive(cancelled=lambda: False)
    assert calls[-1] == (63, 8)
    assert receive.status()["state"] == "ready"


def test_fast_burst_exhaustion_uses_default_probe_and_cancel_does_not_poll(tmp_path, monkeypatch):
    receive, clock, calls, _ = receiver(tmp_path, monkeypatch,
        [transient() for _ in range(4)] + [[]], probe_outcomes=[None])
    for stamp in (0, 1, 31):
        clock[0] = stamp
        receive(cancelled=lambda: False)
    assert receive.status()["recovery_attempts"] == 1
    clock[0] = 91
    receive(cancelled=lambda: True)
    assert len(calls) == 3
    receive(cancelled=lambda: False)
    assert receive.status()["state"] == "backoff"
    assert receive.status()["recovery_attempts"] == 2
    clock[0] = 1000
    receive(cancelled=lambda: False)
    assert len(calls) == 4
    assert receive.test_probes == [1000]
    clock[0] = 1001
    receive(cancelled=lambda: False)
    assert receive.status()["state"] == "ready" and len(calls) == 5


def test_nontransient_failure_uses_spaced_probe_instead_of_fast_retry(tmp_path, monkeypatch):
    receive, clock, calls, _ = receiver(tmp_path, monkeypatch, [
        TelegramPollingUnavailable("unverified"), transient()], probe_outcomes=[transient()])
    for stamp in (0, 1, 100):
        clock[0] = stamp
        receive(cancelled=lambda: False)
    assert receive.status()["state"] == "backoff"
    assert len(calls) == 2 and receive.test_probes == [100]


def exhaust(receive, clock):
    for stamp in (0, 1, 31, 91):
        clock[0] = stamp
        receive(cancelled=lambda: False)
    assert receive.status()["state"] == "backoff"


def test_opted_in_paused_transport_probes_then_reopens_with_same_durable_offset(tmp_path, monkeypatch):
    receive, clock, calls, inbox = receiver(tmp_path, monkeypatch,
        [transient() for _ in range(4)] + [[update(7)]],
        recovery={"interval_seconds": 300, "probe_limit": 2}, probe_outcomes=[None])
    exhaust(receive, clock)
    clock[0] = 390
    receive(cancelled=lambda: False)
    assert receive.test_probes == [] and len(calls) == 4 and inbox.offset() is None
    clock[0] = 391
    receive(cancelled=lambda: False)
    assert receive.test_probes == [391]
    assert receive.status()["state"] == "backoff"
    assert len(calls) == 4 and inbox.offset() is None
    clock[0] = 392
    receive(cancelled=lambda: False)
    assert calls[-1] == (392, None) and inbox.offset() == 8
    assert receive.status()["state"] == "ready"
    assert receive.status()["transport_probe_attempts"] == 1


def test_probe_failure_keeps_cooldown_and_never_acknowledges_offsets(tmp_path, monkeypatch):
    receive, clock, calls, inbox = receiver(tmp_path, monkeypatch, [transient() for _ in range(4)],
        recovery={"interval_seconds": 300, "probe_limit": 2},
        probe_outcomes=[transient() for _ in range(4)])
    exhaust(receive, clock)
    for stamp in (391, 691, 991, 100000):
        clock[0] = stamp
        receive(cancelled=lambda: False)
    assert receive.test_probes == [391, 691, 991, 100000]
    assert len(calls) == 4 and inbox.offset() is None
    assert receive.status()["state"] == "backoff"
    assert receive.status()["transport_probe_attempts"] == 4


def test_unverified_probe_keeps_admission_closed_but_does_not_disable_recovery(tmp_path, monkeypatch):
    receive, clock, calls, inbox = receiver(tmp_path, monkeypatch, [transient() for _ in range(4)],
        recovery={"interval_seconds": 300, "probe_limit": 2},
        probe_outcomes=[TelegramPollingUnavailable("wrong account") for _ in range(3)])
    exhaust(receive, clock)
    clock[0] = 391
    receive(cancelled=lambda: True)
    assert receive.test_probes == []
    receive(cancelled=lambda: False)
    for stamp in (691, 100000):
        clock[0] = stamp
        receive(cancelled=lambda: False)
    assert receive.test_probes == [391, 691, 100000]
    assert len(calls) == 4 and inbox.offset() is None
    assert receive.status()["failure_category"] == "unverified-response"


def test_transport_probe_validates_account_without_polling_or_acknowledging_updates():
    calls = []
    def request(token, method, body):
        calls.append((method, body))
        return {"ok": True, "result": {"id": 123, "is_bot": True}}
    TelegramPoll("123", "fixture", request=request).probe()
    assert calls == [("getMe", {})]
    with pytest.raises(TelegramPollingUnavailable) as error:
        TelegramPoll("456", "fixture", request=request).probe()
    assert error.value.retryable is False


def test_success_retains_observation_count_without_disabling_later_recovery(tmp_path, monkeypatch):
    receive, clock, calls, inbox = receiver(tmp_path, monkeypatch,
        [transient() for _ in range(4)] + [[]] + [transient() for _ in range(4)] + [[]],
        recovery={"interval_seconds": 300, "probe_limit": 1}, probe_outcomes=[None, None])
    exhaust(receive, clock)
    for stamp in (391, 392, 393, 394, 424, 484, 784, 100000):
        clock[0] = stamp
        receive(cancelled=lambda: False)
    assert receive.test_probes == [391, 784] and len(calls) == 10 and inbox.offset() is None
    assert receive.status()["state"] == "ready"
    assert receive.status()["transport_probe_attempts"] == 2


def test_explicit_transport_probe_policy_parses():
    from ahc.domain.sessions import VirtualSessionIdentity
    session = VirtualSessionIdentity.initial("test", "main", "test-route")
    route = ChannelRoute("telegram", "123", "456", "789", session)
    value = {"enabled": True, "exclusive_consumer": True, "token_env": "TEST", "account_id": "123",
             "interval_seconds": 1, "transport_recovery": {"interval_seconds": 300, "probe_limit": 2016}}
    raw = {"channels": [{"route": route.to_dict()}], "telegram_polling": value}
    assert foreground_telegram.parse_telegram(raw, session) == {"telegram_polling": value}


@pytest.mark.parametrize("policy", [{}, {"interval_seconds": True, "probe_limit": 2},
    {"interval_seconds": 0, "probe_limit": 2}, {"interval_seconds": 300, "probe_limit": 0},
    {"interval_seconds": 300, "probe_limit": 10001}])
def test_invalid_transport_probe_policy_is_rejected(policy):
    from ahc.domain.sessions import VirtualSessionIdentity, ContractError
    session = VirtualSessionIdentity.initial("test", "main", "test-route")
    route = ChannelRoute("telegram", "123", "456", "789", session)
    raw = {"channels": [{"route": route.to_dict()}], "telegram_polling": {
        "enabled": True, "exclusive_consumer": True, "token_env": "TEST", "account_id": "123",
        "interval_seconds": 1, "transport_recovery": policy}}
    with pytest.raises(ContractError):
        foreground_telegram.parse_telegram(raw, session)


def test_validation_outage_rechecks_identity_and_restores_the_only_entrance(tmp_path, monkeypatch):
    receive, clock, calls, inbox = receiver(tmp_path, monkeypatch,
        [TelegramPollingUnavailable("unverified") for _ in range(2)] + [[update(7)]],
        probe_outcomes=[TelegramPollingUnavailable("wrong identity"), None])
    for stamp in (0, 1, 60):
        clock[0] = stamp
        receive(cancelled=lambda: False)
    assert len(calls) == 2 and inbox.offset() is None
    assert receive.status()["automatic_recovery"] is True
    for stamp in (61, 120):
        clock[0] = stamp
        receive(cancelled=lambda: False)
    assert receive.test_probes == [61] and len(calls) == 2 and inbox.offset() is None
    clock[0] = 121
    receive(cancelled=lambda: False)
    assert receive.test_probes == [61, 121] and inbox.offset() is None
    clock[0] = 122
    receive(cancelled=lambda: False)
    assert calls[-1] == (122, None) and inbox.offset() == 8
    assert receive.status()["state"] == "ready"


def test_repeated_incidents_cannot_exhaust_a_lifetime_recovery_budget(tmp_path, monkeypatch):
    failure = TelegramPollingUnavailable("unverified")
    receive, clock, calls, inbox = receiver(tmp_path, monkeypatch,
        [failure, failure, failure, failure, [update(7)], failure, failure, []],
        recovery={"interval_seconds": 30, "probe_limit": 1},
        probe_outcomes=[None, None, None])
    for stamp in (0, 1, 31, 32, 33, 63, 64, 65, 66, 96, 97):
        clock[0] = stamp
        receive(cancelled=lambda: False)
    assert receive.test_probes == [31, 63, 96]
    assert calls[-1] == (97, 8) and inbox.offset() == 8
    assert receive.status()["state"] == "ready"
    assert receive.status()["transport_probe_attempts"] == 3


def test_long_outage_keeps_spaced_probes_after_the_legacy_limit(tmp_path, monkeypatch):
    failure = TelegramPollingUnavailable("unverified")
    receive, clock, calls, inbox = receiver(tmp_path, monkeypatch, [failure, failure, [update(7)]],
        recovery={"interval_seconds": 30, "probe_limit": 1},
        probe_outcomes=[failure, failure, failure, None])
    for stamp in (0, 1, 30, 31, 31, 60, 61, 90, 91, 120, 121):
        clock[0] = stamp
        receive(cancelled=lambda: False)
    assert receive.test_probes == [31, 61, 91, 121]
    assert len(calls) == 2 and inbox.offset() is None
    clock[0] = 122
    receive(cancelled=lambda: False)
    assert receive.status()["state"] == "ready" and inbox.offset() == 8


def test_rate_limit_hint_delays_polls_and_remains_observable(tmp_path, monkeypatch):
    failure = TelegramPollingUnavailable("unverified", retryable=True, category="rate-limit",
                                         http_status=429, retry_after=120)
    receive, clock, calls, inbox = receiver(tmp_path, monkeypatch, [failure, failure, []])
    for stamp in (0, 119, 120, 239):
        clock[0] = stamp
        receive(cancelled=lambda: False)
    assert calls == [(0, None), (120, None)] and inbox.offset() is None
    assert receive.status()["http_status"] == 429
    assert receive.status()["next_retry_seconds"] == 1
    clock[0] = 240
    receive(cancelled=lambda: False)
    assert receive.status()["state"] == "ready" and receive.status()["http_status"] is None


def test_failure_logs_are_sanitized_and_deduplicated_per_incident(tmp_path, monkeypatch, caplog):
    failure = TelegramPollingUnavailable("secret-token-and-message", retryable=True,
                                         category="service", http_status=503)
    receive, clock, _, _ = receiver(tmp_path, monkeypatch, [failure, failure, [], failure])
    for stamp in (0, 1, 31, 32):
        clock[0] = stamp
        receive(cancelled=lambda: False)
    assert len(caplog.records) == 2 and "http_status=503" in caplog.text
    assert "secret-token-and-message" not in caplog.text


def test_readiness_projects_recovery_and_http_diagnostics_without_private_fields(tmp_path):
    from test_foreground_runtime import setup as foreground_setup
    runtime, _, _, _ = foreground_setup(tmp_path)
    raw = {"state": "running", "telegram": {"state": "backoff", "failure_category": "rate-limit",
        "automatic_recovery": True, "http_status": 429, "next_retry_seconds": 120,
        "transport_probe_attempts": 3, "held_updates": 1, "private_payload": "secret"}}
    runtime.channel_receiver = SimpleNamespace(status=lambda: raw)
    runtime.state = "running"
    observed = runtime.admission_status()
    assert not observed["ready"] and observed["receiver"]["telegram_automatic_recovery"] is True
    assert observed["receiver"]["telegram_http_status"] == 429
    assert observed["receiver"]["telegram_next_retry_seconds"] == 120
    assert observed["receiver"]["telegram_held_updates"] == 1
    assert "secret" not in str(observed) and "private_payload" not in str(observed)


def test_malformed_remote_message_does_not_stop_the_real_control_pump(tmp_path, monkeypatch):
    from test_runtime_core import setup
    from ahc.cli.foreground_control_pump import ChannelControlPump
    from ahc.infrastructure.telegram_hold import inspect_held_updates
    from ahc.infrastructure.runtime_inbox import RuntimeInbox

    store, snapshot, _ = setup(tmp_path)
    route = ChannelRoute("telegram", "123", "456", "789", snapshot.session)
    clock, offsets = [0.0], []
    def request(token, method, body):
        if method == "getMe":
            return {"ok": True, "result": {"id": 123, "is_bot": True}}
        offsets.append(body.get("offset"))
        return {"ok": True, "result": [update(1, text=123)] if len(offsets) == 1
                else [update(2)]}
    monkeypatch.setenv("AHC_PUMP_TEST", "fixture")
    monkeypatch.setattr(foreground_telegram.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(foreground_telegram, "TelegramPoll",
                        lambda *a, **k: TelegramPoll(*a, **k, request=request))
    document = {"channels": [{"route": route.to_dict()}], "telegram_polling": {
        "enabled": True, "token_env": "AHC_PUMP_TEST", "account_id": "123",
        "interval_seconds": 1}}
    receive = foreground_telegram.compose_telegram(
        SimpleNamespace(to_dict=lambda: document), store, concurrent=True)
    controls = SimpleNamespace(document={"channels": []}, options={}, deliver_one=lambda _: None)
    pump = ChannelControlPump(controls, receive, None)
    def next_cycle(_timeout):
        clock[0] += 1
        if len(offsets) == 2:
            pump.stop.set()
    monkeypatch.setattr(pump.stop, "wait", next_cycle)
    pump._run()
    assert not pump.failed and offsets == [None, 2]
    assert receive.status()["state"] == "ready" and receive.status()["held_updates"] == 1
    assert TelegramInbox(store, [route]).offset() == 3
    assert inspect_held_updates(TelegramInbox(store, [route])) == {
        "held_count": 1, "update_ids": [1]}
    assert RuntimeInbox(store).next_pending(snapshot.session)["input_text"] == "hello"
