import pytest

from ahc.adapters.telegram_poll import TelegramPoll
from ahc.domain.sessions import ContractError


def test_account_verification_and_explicit_offset_without_implicit_advancement():
    calls = []

    def request(token, method, body):
        calls.append((method, body))
        assert token == "fixture"
        if method == "getMe":
            return {"ok": True, "result": {"id": 123, "is_bot": True}}
        return {"ok": True, "result": [{"update_id": 7}]}

    poller = TelegramPoll("123", "fixture", request=request)
    assert poller.poll() == [{"update_id": 7}]
    assert poller.poll() == [{"update_id": 7}]
    assert all("offset" not in body for method, body in calls if method == "getUpdates")
    assert poller.poll(7) == [{"update_id": 7}]
    assert calls[-1] == ("getUpdates", {"limit": 16, "timeout": 0,
                                      "allowed_updates": ["message"], "offset": 7})
    before = len(calls)
    with pytest.raises(ContractError):
        poller.poll(-1)
    assert len(calls) == before


@pytest.mark.parametrize("failure", ["account", "duplicate", "old", "error"])
def test_bad_identity_or_batch_is_not_retried(failure):
    calls = []

    def request(token, method, body):
        calls.append(method)
        if method == "getMe":
            return {"ok": True, "result": {"id": 999 if failure == "account" else 123,
                                            "is_bot": True}}
        if failure == "error":
            raise OSError("fixture-secret")
        return {"ok": True, "result": ([{"update_id": 7}, {"update_id": 7}]
                                        if failure == "duplicate" else [{"update_id": 6}])}

    with pytest.raises(ContractError, match="response unverified") as error:
        TelegramPoll("123", "fixture", request=request).poll(7)
    assert "fixture-secret" not in str(error.value)
    assert calls == (["getMe"] if failure == "account" else ["getMe", "getUpdates"])


@pytest.mark.parametrize("status,category,retryable,retry_after", [
    (503, "service", True, None), (429, "rate-limit", True, 120),
    (409, "conflict", False, None), (401, "authorization", False, None)])
def test_http_failure_retains_only_safe_diagnostics(monkeypatch, status, category,
                                                   retryable, retry_after):
    import json
    from ahc.adapters import telegram_poll

    class Response:
        def read(self, _limit):
            return json.dumps({"ok": False, "error_code": status,
                "description": "private-token-and-message",
                "parameters": {"retry_after": retry_after}}).encode()

    Response.status = status

    class Connection:
        def __init__(self, *_args, **_kwargs):
            pass

        def request(self, *_args):
            pass

        def getresponse(self):
            return Response()

        def close(self):
            pass

    monkeypatch.setattr(telegram_poll.http.client, "HTTPSConnection", Connection)
    with pytest.raises(telegram_poll.TelegramPollingUnavailable) as observed:
        TelegramPoll("123", "fixture").poll()
    error = observed.value
    assert error.category == category and error.retryable is retryable
    assert error.http_status == status and error.retry_after == retry_after
    assert "private-token-and-message" not in str(error)
