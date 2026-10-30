"""Exercise the production attachment HTTP path with a controlled connection boundary."""

import pytest

from ahc.adapters import channel_attachment
from ahc.application.channel_artifact_upload import ArtifactUpload
from ahc.domain.channel_ingress import ChannelRoute
from ahc.domain.sessions import VirtualSessionIdentity

BOUNDARY = "ahc-0123456789abcdef0123456789abcdef"
CONTENT_TYPE = f"multipart/form-data; boundary={BOUNDARY}"


@pytest.mark.parametrize("platform", ["telegram", "discord"])
@pytest.mark.parametrize(
    "failure", [None, "status", "oversize", "duplicate", "nonfinite", "request", "read"]
)
def test_attachment_http_request_response_and_cleanup(monkeypatch, platform, failure):
    route = ChannelRoute(
        platform,
        "123",
        "456",
        "789",
        VirtualSessionIdentity.initial("session-test", "agent-test", "route-test"),
    )
    body = (
        b"--ahc-0123456789abcdef0123456789abcdef\r\n"
        b'Content-Disposition: form-data; name="document"; filename="report.txt"\r\n'
        b"Content-Type: application/octet-stream\r\n\r\n"
        b"fixture-bytes\r\n"
        b"--ahc-0123456789abcdef0123456789abcdef--\r\n"
    )
    upload = ArtifactUpload(route, "artifact-test", "report.txt", 13, CONTENT_TYPE, body)
    events = []

    class Response:
        status = 500 if failure == "status" else 200

        def read(self, limit):
            events.append(("read", limit))
            assert limit == 65537
            if failure == "read":
                raise OSError("fixture read failed")
            if failure == "oversize":
                return b" " * 65537
            if failure == "duplicate":
                return b'{"id":1,"id":2}'
            if failure == "nonfinite":
                return b'{"id":NaN}'
            return b'{"id":"123"}'

    class Connection:
        def __init__(self, host, timeout):
            events.append(("open", host, timeout))

        def request(self, method, path, request_body, headers):
            events.append(("request", method, path, request_body, headers))
            if failure == "request":
                raise OSError("fixture request failed")

        def getresponse(self):
            events.append(("getresponse",))
            return Response()

        def close(self):
            events.append(("close",))

    monkeypatch.setattr(channel_attachment.http.client, "HTTPSConnection", Connection)

    if failure:
        with pytest.raises((ValueError, OSError)):
            channel_attachment._post(route, "fixture", upload)
    else:
        assert channel_attachment._post(route, "fixture", upload) == {"id": "123"}

    assert events[-1] == ("close",)
    assert [event[0] for event in events].count("open") == 1
    assert [event[0] for event in events].count("request") == 1
    assert [event[0] for event in events].count("close") == 1
    assert [event[0] for event in events].count("getresponse") == (0 if failure == "request" else 1)
    assert [event[0] for event in events].count("read") == (0 if failure == "request" else 1)
    assert events[0] == (
        "open",
        "api.telegram.org" if platform == "telegram" else "discord.com",
        10,
    )
    request = next(event for event in events if event[0] == "request")
    _, method, path, request_body, headers = request
    assert method == "POST" and request_body == body and request_body is upload.body
    if platform == "telegram":
        assert path == "/botfixture/sendDocument"
        assert headers == {"Content-Type": CONTENT_TYPE}
    else:
        assert path == "/api/v10/channels/456/messages"
        assert headers == {"Content-Type": CONTENT_TYPE, "Authorization": "Bot fixture"}
