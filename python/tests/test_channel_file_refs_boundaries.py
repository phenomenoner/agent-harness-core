"""Exercise bounded channel file metadata and reference boundaries."""

import pytest

from ahc.adapters.channel_file_refs import decode_file_refs
from ahc.domain.channel_ingress import ChannelRoute
from ahc.domain.sessions import ContractError, VirtualSessionIdentity


def _route(platform):
    return ChannelRoute(
        platform,
        "123",
        "456",
        "789",
        VirtualSessionIdentity.initial("session-test", "agent-test", "route-test"),
    )


def _discord_attachment(index=0, *, byte_count=6, media_type=None):
    file_id = str(index + 1)
    filename = f"report-{index}.txt"
    attachment = {
        "id": file_id,
        "filename": filename,
        "size": byte_count,
        "url": f"https://cdn.discordapp.com/attachments/456/{file_id}/{filename}",
    }
    if media_type is not None:
        attachment["content_type"] = media_type
    return attachment


def _message(platform, *, byte_count=6, media_type="application/octet-stream"):
    if platform == "telegram":
        return {
            "document": {
                "file_id": "file-1",
                "file_name": "report.txt",
                "file_size": byte_count,
                "mime_type": media_type,
            }
        }
    return {
        "attachments": [_discord_attachment(byte_count=byte_count, media_type=media_type)]
    }


@pytest.mark.parametrize(("count", "expected_error"), [(0, False), (8, False), (9, True)])
def test_discord_attachment_count_boundaries(count, expected_error):
    message = {"attachments": [_discord_attachment(index) for index in range(count)]}
    if expected_error:
        with pytest.raises(ContractError, match="inbound file metadata invalid"):
            decode_file_refs(_route("discord"), message)
    else:
        assert len(decode_file_refs(_route("discord"), message)) == count


def test_telegram_missing_document_is_empty():
    assert decode_file_refs(_route("telegram"), {}) == ()


def test_discord_query_is_accepted_but_hidden_from_repr():
    attachment = _discord_attachment()
    attachment["url"] += "?ex=fixture"
    (value,) = decode_file_refs(_route("discord"), {"attachments": [attachment]})
    assert value.download_url.endswith("?ex=fixture")
    assert "ex=fixture" not in repr(value)


@pytest.mark.parametrize("platform", ["telegram", "discord"])
@pytest.mark.parametrize("byte_count", [True, -1, 1048577])
def test_file_byte_count_is_strict(platform, byte_count):
    with pytest.raises(ContractError, match="inbound file metadata invalid"):
        decode_file_refs(_route(platform), _message(platform, byte_count=byte_count))


@pytest.mark.parametrize("platform", ["telegram", "discord"])
@pytest.mark.parametrize("media_type", ["text", "TEXT/PLAIN"])
def test_file_media_type_is_strict(platform, media_type):
    with pytest.raises(ContractError, match="inbound file metadata invalid"):
        decode_file_refs(_route(platform), _message(platform, media_type=media_type))


@pytest.mark.parametrize(
    ("platform", "message"),
    [
        ("telegram", {"document": None}),
        ("telegram", {"document": {}}),
        ("discord", {"attachments": None}),
        ("discord", {"attachments": [None]}),
        ("discord", {"attachments": [{}]}),
    ],
)
def test_malformed_file_metadata_is_contract_error(platform, message):
    with pytest.raises(ContractError, match="inbound file metadata invalid"):
        decode_file_refs(_route(platform), message)
