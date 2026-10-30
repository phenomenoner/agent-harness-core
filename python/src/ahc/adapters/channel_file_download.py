"""Bounded file fetches; callers retain authenticated ingress and artifact ownership."""

import http.client
import json
import re
from urllib.parse import urlsplit

from ahc.adapters.channel_text import BotTextTransport, _invalid_constant, _unique_object
from ahc.domain.channel_files import ChannelFile
from ahc.domain.sessions import ContractError


def _request(host, method, path, body, limit):
    connection = http.client.HTTPSConnection(host, timeout=10)
    try:
        headers = {"User-Agent": "AHC/0.3.0", "Accept-Encoding": "identity"}
        if body is not None:
            headers["Content-Type"] = "application/json"
        connection.request(method, path, body, headers)
        response = connection.getresponse()
        data = response.read(limit + 1)
        if response.status != 200 or len(data) > limit:
            raise ValueError()
        if response.getheader("Content-Encoding", "identity") != "identity":
            raise ValueError()
        return data
    finally:
        connection.close()


class BotFileDownload:
    """No redirects or retries; Discord CDN requests never receive the bot token."""

    def __init__(self, route, token, *, request=_request):
        BotTextTransport(route, token)
        if not callable(request):
            raise ContractError("file download transport invalid")
        self._route, self._token, self._request = route, token, request

    def fetch(self, reference):
        if (type(reference) is not ChannelFile or reference.platform != self._route.platform
                or reference.channel_id != self._route.channel_id):
            raise ContractError("file download route mismatch")
        try:
            if reference.platform == "telegram":
                metadata = self._request(
                    "api.telegram.org", "POST", f"/bot{self._token}/getFile",
                    json.dumps({"file_id": reference.file_id}).encode(), 65536)
                result = json.loads(metadata, object_pairs_hook=_unique_object,
                                    parse_constant=_invalid_constant)
                item = result["result"]
                if (result["ok"] is not True or item["file_id"] != reference.file_id
                        or type(item["file_size"]) is not int
                        or item["file_size"] != reference.byte_count):
                    raise ValueError()
                file_path = item["file_path"]
                if (type(file_path) is not str or len(file_path) > 1024
                        or not re.fullmatch(r"[A-Za-z0-9_./-]+", file_path)
                        or any(part in {"", ".", ".."} for part in file_path.split("/"))):
                    raise ValueError()
                host, path = "api.telegram.org", f"/file/bot{self._token}/{file_path}"
            else:
                url = urlsplit(reference.download_url)
                host = "cdn.discordapp.com"
                path = url.path + ("?" + url.query if url.query else "")
            data = self._request(host, "GET", path, None, reference.byte_count)
            if type(data) is not bytes or len(data) != reference.byte_count:
                raise ValueError()
            return data
        except Exception:
            raise ContractError("inbound file download unverified") from None
