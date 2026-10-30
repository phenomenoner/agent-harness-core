import pytest
from test_runtime_core import setup

from ahc.adapters.channel_events import decode_channel_event
from ahc.adapters.channel_file_refs import MAX_FILE_BYTES
from ahc.domain.channel_ingress import ChannelRoute


def route_for(tmp_path, platform):
    _, snapshot, _ = setup(tmp_path)
    return ChannelRoute(platform, "123", "456", "789", snapshot.session)


def telegram_message(**fields):
    return {
        "message_id": 2,
        "chat": {"id": 456},
        "from": {"id": 789, "is_bot": False},
        **fields,
    }


def discord_message(**fields):
    return {
        "id": "2",
        "channel_id": "456",
        "author": {"id": "789"},
        "type": 0,
        "content": "hello",
        **fields,
    }


def telegram_payload(message, update_id=1):
    return {"update_id": update_id, "message": message}


def discord_payload(message):
    return {"op": 0, "t": "MESSAGE_CREATE", "d": message}


@pytest.mark.parametrize("platform", ["telegram", "discord"])
def test_quote_is_exact_context_and_never_becomes_current_command(tmp_path, platform):
    route = route_for(tmp_path, platform)
    if platform == "telegram":
        payload = telegram_payload({
            **telegram_message(text="continue", quote={"text": "/stop"},
                               reply_to_message={"message_id": 8, "text": "old"}),
        })
    else:
        payload = discord_payload(discord_message(
            content="continue",
            message_reference={"channel_id": "456", "message_id": "8"},
            referenced_message={"id": "8", "channel_id": "456", "content": "/stop"},
        ))
    event = decode_channel_event(route, payload)
    assert event.input_text == "continue"
    assert event.context["quote"] == "/stop"
    assert event.context["quote_status"] == "available"
    assert event.context["reply_to_id"] == "8"


@pytest.mark.parametrize("platform", ["telegram", "discord"])
def test_images_are_bounded_and_caption_is_preserved(tmp_path, platform):
    route = route_for(tmp_path, platform)
    if platform == "telegram":
        payload = telegram_payload(telegram_message(
            caption="photo caption",
            photo=[
                {"file_id": "too-big", "file_size": MAX_FILE_BYTES + 1,
                 "width": 4000, "height": 4000},
                {"file_id": "small", "file_size": 200, "width": 100, "height": 100},
                {"file_id": "large", "file_size": 300, "width": 200, "height": 200},
            ],
        ))
    else:
        payload = discord_payload(discord_message(
            content="image caption",
            attachments=[{
                "id": "3", "size": 300, "filename": "picture.png",
                "content_type": "image/png",
                "url": "https://cdn.discordapp.com/attachments/456/3/picture.png",
            }],
        ))
    event = decode_channel_event(route, payload, include_files=True)
    assert event.input_text in ("photo caption", "image caption")
    assert len(event.files) == 1
    assert event.files[0].byte_count == 300
    if platform == "telegram":
        assert event.files[0].file_id == "large"
        assert event.files[0].filename == "photo-2.jpg"
        assert event.files[0].media_type == "image/jpeg"
    else:
        assert event.files[0].filename == "picture.png"
        assert event.files[0].media_type == "image/png"


@pytest.mark.parametrize("platform", ["telegram", "discord"])
def test_unknown_reply_is_explicitly_unavailable(tmp_path, platform):
    route = route_for(tmp_path, platform)
    if platform == "telegram":
        payload = telegram_payload(telegram_message(
            text="next", reply_to_message={"message_id": 8},
        ))
    else:
        payload = discord_payload(discord_message(
            content="next", message_reference={"channel_id": "456", "message_id": "8"},
            referenced_message=None,
        ))
    event = decode_channel_event(route, payload)
    assert event.input_text == "next"
    assert event.context["quote_status"] == "unavailable"
    assert event.context["reply_to_id"] == "8"
    assert "quote" not in event.context


def test_telegram_voice_is_notice_and_keeps_caption(tmp_path):
    route = route_for(tmp_path, "telegram")
    event = decode_channel_event(route, telegram_payload(telegram_message(
        caption="please listen", voice={"file_id": "voice-file", "file_size": 12},
    )), include_files=True)
    assert event.input_text == "please listen"
    assert event.files == ()
    assert "未執行" in event.context["notice"]
    assert event.context["attachments"][0]["status"] == "unsupported"


def test_invalid_file_boundary_is_notice_and_route_mismatch_does_not_leak(tmp_path):
    route = route_for(tmp_path, "telegram")
    exact = decode_channel_event(route, telegram_payload(telegram_message(
        document={"file_id": "exact", "file_size": MAX_FILE_BYTES,
                  "file_name": "exact.bin"}, text="read")), include_files=True)
    assert len(exact.files) == 1
    oversize_payload = telegram_payload(telegram_message(
        document={"file_id": "oversize-secret", "file_size": MAX_FILE_BYTES + 1,
                  "file_name": "oversize.bin"}, text="read"), update_id=2)
    oversize = decode_channel_event(route, oversize_payload, include_files=True)
    assert oversize.files == ()
    assert "1 MiB" in oversize.context["notice"]
    foreign = telegram_payload(telegram_message(
        text="foreign", chat={"id": 999},
        reply_to_message={"message_id": 8, "text": "PRIVATE-FOREIGN-QUOTE"},
        document={"file_id": "foreign-secret", "file_size": 12, "file_name": "x.txt"},
    ), update_id=3)
    assert decode_channel_event(route, foreign, include_files=True) is None


@pytest.mark.parametrize("platform", ["telegram", "discord"])
def test_reply_image_is_imported_when_current_text_exists(tmp_path, platform):
    route = route_for(tmp_path, platform)
    if platform == "telegram":
        payload = telegram_payload(telegram_message(
            text="look at above",
            reply_to_message={
                "message_id": 8,
                "chat": {"id": 456},
                "photo": [{"file_id": "reply-photo", "file_size": 120,
                           "width": 100, "height": 100}],
            },
        ))
    else:
        payload = discord_payload(discord_message(
            content="look at above",
            message_reference={"channel_id": "456", "message_id": "8"},
            referenced_message={
                "id": "8", "channel_id": "456", "content": "",
                "attachments": [{
                    "id": "4", "size": 120, "filename": "above.png",
                    "content_type": "image/png",
                    "url": "https://cdn.discordapp.com/attachments/456/4/above.png",
                }],
            },
        ))
    event = decode_channel_event(route, payload, include_files=True)
    assert event.input_text == "look at above"
    assert len(event.files) == 1
    assert event.context["attachments"][0]["status"] == "available"


def test_mixed_supported_and_unsupported_media_never_stages_supported_file(tmp_path):
    route = route_for(tmp_path, "telegram")
    event = decode_channel_event(route, telegram_payload(telegram_message(
        text="inspect", document={"file_id": "doc", "file_size": 12, "file_name": "x.txt"},
        voice={"file_id": "voice", "file_size": 12},
    )), include_files=True)
    assert event.files == ()
    assert "整則訊息" in event.context["notice"]


def test_discord_nested_forward_snapshot_and_sticker_notice(tmp_path):
    route = route_for(tmp_path, "discord")
    event = decode_channel_event(route, discord_payload(discord_message(
        content="", message_snapshots=[{"message": {"content": "forwarded task"}}])),
        include_files=True)
    assert event.context["forwarded_text"] == "forwarded task"
    event = decode_channel_event(route, discord_payload(discord_message(
        content="", sticker_items=[{"id": "42", "name": "sticker"}])), include_files=True)
    assert "不支援" in event.context["notice"] and not event.files


def test_audio_document_gets_unsupported_notice(tmp_path):
    route = route_for(tmp_path, "telegram")
    event = decode_channel_event(route, telegram_payload(telegram_message(
        document={"file_id": "audio", "file_size": 12, "file_name": "voice.mp3",
                  "mime_type": "audio/mpeg"})), include_files=True)
    assert "不支援" in event.context["notice"] and not event.files
