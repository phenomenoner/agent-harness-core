"""Bounded attachment references; a reference is not downloaded or authenticated bytes."""

import re
from dataclasses import dataclass, field
from urllib.parse import unquote, urlsplit

from .artifacts import _MEDIA_TYPE
from .result_artifacts import display_filename
from .sessions import ContractError


@dataclass(frozen=True, slots=True)
class ChannelFile:
    platform: str
    channel_id: str
    file_id: str
    filename: str
    byte_count: int
    media_type: str
    download_url: str | None = field(default=None, repr=False)

    def to_dict(self):
        return {"platform": self.platform, "channel_id": self.channel_id,
                "file_id": self.file_id, "filename": self.filename,
                "byte_count": self.byte_count, "media_type": self.media_type,
                "download_url": self.download_url}

    @classmethod
    def from_dict(cls, value):
        if type(value) is not dict or set(value) != {
                "platform", "channel_id", "file_id", "filename", "byte_count",
                "media_type", "download_url"}:
            raise ContractError("inbound file fields invalid")
        return cls(**value)

    def __post_init__(self):
        if type(self.platform) is not str or self.platform not in {"telegram", "discord"}:
            raise ContractError("file platform invalid")
        if type(self.channel_id) is not str or not re.fullmatch(r"-?[0-9]{1,20}", self.channel_id):
            raise ContractError("file channel invalid")
        display_filename(self.filename)
        if type(self.byte_count) is not int or not 0 <= self.byte_count <= 1048576:
            raise ContractError("inbound file exceeds byte bound")
        if (type(self.media_type) is not str or len(self.media_type) > 128
                or _MEDIA_TYPE.fullmatch(self.media_type) is None):
            raise ContractError("inbound media type invalid")
        if type(self.file_id) is not str:
            raise ContractError("inbound file ID invalid")
        if self.platform == "telegram":
            if (not re.fullmatch(r"[A-Za-z0-9_-]{1,512}", self.file_id)
                    or self.download_url is not None):
                raise ContractError("Telegram file reference invalid")
        else:
            if (not re.fullmatch(r"[1-9][0-9]{0,19}", self.file_id)
                    or not re.fullmatch(r"[1-9][0-9]{0,19}", self.channel_id)
                    or type(self.download_url) is not str or len(self.download_url) > 4096):
                raise ContractError("Discord file reference invalid")
            try:
                url = urlsplit(self.download_url)
                expected = f"/attachments/{self.channel_id}/{self.file_id}/{self.filename}"
                if (url.scheme != "https" or url.netloc != "cdn.discordapp.com"
                        or url.fragment or unquote(url.path, errors="strict") != expected
                        or any(ord(c) < 32 or ord(c) == 127 for c in self.download_url)):
                    raise ValueError()
            except (ValueError, UnicodeError):
                raise ContractError("Discord attachment URL binding invalid") from None
