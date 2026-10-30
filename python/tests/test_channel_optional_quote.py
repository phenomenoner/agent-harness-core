"""Optional reply metadata cannot poison an authenticated channel's current input."""
import copy

import pytest
from test_channel_rich_events import (
    discord_message, discord_payload, route_for, telegram_message, telegram_payload,
)

from ahc.adapters.channel_events import decode_channel_event
from ahc.domain.sessions import ContractError


def reply_payload(platform, *, fault="shape", text="current question"):
    if platform == "telegram":
        nested = {"message_id": 8, "chat": {"id": 456}, "text": "PRIVATE quote"}
        if fault == "channel":
            nested["chat"] = []
        elif fault == "nested-channel":
            nested["chat"]["id"] = True
        message = telegram_message(text=text, quote={"text": "PRIVATE inline quote"},
                                   reply_to_message=nested)
        if fault in {"shape", "nested-shape"}:
            message["reply_to_message"] = [] if fault == "shape" else "PRIVATE malformed quote"
        return telegram_payload(message)
    reference = {"channel_id": "456", "message_id": "8"}
    nested = {"id": "8", "channel_id": "456", "content": "PRIVATE quote"}
    if fault == "shape":
        reference = []
    elif fault == "channel":
        reference["channel_id"] = 456
    elif fault == "nested-shape":
        nested = "PRIVATE malformed quote"
    elif fault == "nested-channel":
        nested["channel_id"] = 456
    return discord_payload(discord_message(content=text, message_reference=reference,
                                            referenced_message=nested))


@pytest.mark.parametrize("platform", ["telegram", "discord"])
@pytest.mark.parametrize("fault", ["shape", "channel", "nested-shape", "nested-channel"])
@pytest.mark.parametrize("text", ["current question", "/status"])
def test_malformed_optional_quote_preserves_current_input(tmp_path, platform, fault, text):
    route = route_for(tmp_path, platform)
    payload = reply_payload(platform, fault=fault, text=text)
    before = copy.deepcopy(payload)
    event = decode_channel_event(route, payload)
    assert event.input_text == text and not event.files
    assert event.context["quote_status"] == "unavailable"
    assert "quote" not in event.context and "reply_to_id" not in event.context
    assert "PRIVATE" not in str(event.to_dict())
    assert payload == before


@pytest.mark.parametrize("identifier", [None, "", "x" * 129, True, {}, []])
def test_unusable_discord_reference_id_does_not_admit_nested_content(tmp_path, identifier):
    route = route_for(tmp_path, "discord")
    payload = reply_payload("discord", fault="valid")
    payload["d"]["message_reference"]["message_id"] = identifier
    event = decode_channel_event(route, payload)
    assert event.input_text == "current question"
    assert event.context["quote_status"] == "unavailable"
    assert "PRIVATE" not in str(event.to_dict())


@pytest.mark.parametrize("platform", ["telegram", "discord"])
@pytest.mark.parametrize("fault", ["author", "channel", "text"])
def test_current_identity_and_input_are_still_validated(tmp_path, platform, fault):
    route = route_for(tmp_path, platform)
    payload = reply_payload(platform)
    message = payload["message"] if platform == "telegram" else payload["d"]
    if fault == "author":
        message["from" if platform == "telegram" else "author"] = []
    elif fault == "channel":
        if platform == "telegram":
            message["chat"] = []
        else:
            message["channel_id"] = 456
    else:
        message["text" if platform == "telegram" else "content"] = 123
    with pytest.raises(ContractError, match="channel platform event invalid"):
        decode_channel_event(route, payload)


@pytest.mark.parametrize("platform", ["telegram", "discord"])
def test_foreign_quote_is_dropped_without_reading_content_or_files(tmp_path, platform):
    route = route_for(tmp_path, platform)
    payload = reply_payload(platform, fault="valid")
    if platform == "telegram":
        payload["message"]["reply_to_message"]["chat"]["id"] = 999
        payload["message"]["reply_to_message"]["photo"] = ["PRIVATE foreign file"]
    else:
        payload["d"]["message_reference"]["channel_id"] = "999"
        payload["d"]["referenced_message"]["attachments"] = ["PRIVATE foreign file"]
    event = decode_channel_event(route, payload, include_files=True)
    assert event.input_text == "current question" and not event.files
    assert "quote" not in event.context and "reply_to_id" not in event.context
    assert "PRIVATE" not in str(event.to_dict())
