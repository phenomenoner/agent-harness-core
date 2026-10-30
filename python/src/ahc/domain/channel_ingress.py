"""Strict route-bound channel ingress values for the local runtime."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from hashlib import sha256
from typing import cast

from .channel_files import ChannelFile
from .sessions import (
    ContractError,
    VirtualSessionIdentity,
    _exact_mapping,
)
from .skill_values import canonical_json

CHANNEL_PLATFORMS = frozenset({"telegram", "discord"})
CHANNEL_VALUE_MAX_LENGTH = 128


def _opaque(value: object, label: str) -> str:
    if type(value) is not str or not 1 <= len(value) <= CHANNEL_VALUE_MAX_LENGTH:
        raise ContractError(f"{label} must contain 1 to {CHANNEL_VALUE_MAX_LENGTH} characters")
    if any(0xD800 <= ord(character) <= 0xDFFF for character in value):
        raise ContractError(f"{label} must contain only Unicode scalar values")
    if any(ord(character) < 0x20 or 0x7F <= ord(character) <= 0x9F for character in value):
        raise ContractError(f"{label} must not contain control characters")
    return value


@dataclass(frozen=True, slots=True)
class ChannelRoute:
    """The trusted platform/account/channel/user and AHC session binding."""

    platform: str
    account_id: str
    channel_id: str
    user_id: str
    session: VirtualSessionIdentity

    def __post_init__(self) -> None:
        if type(self.platform) is not str or self.platform not in CHANNEL_PLATFORMS:
            raise ContractError("channel route platform is unsupported")
        for value, label in (
            (self.account_id, "channel route account_id"),
            (self.channel_id, "channel route channel_id"),
            (self.user_id, "channel route user_id"),
        ):
            _opaque(value, label)
        if type(self.session) is not VirtualSessionIdentity:
            raise ContractError("channel route session must be a VirtualSessionIdentity")

    @property
    def session_identity(self) -> VirtualSessionIdentity:
        return self.session

    def to_dict(self) -> dict[str, object]:
        return {
            "platform": self.platform,
            "account_id": self.account_id,
            "channel_id": self.channel_id,
            "user_id": self.user_id,
            "session": self.session.to_dict(),
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, object]) -> ChannelRoute:
        raw = _exact_mapping(
            value,
            frozenset({"platform", "account_id", "channel_id", "user_id", "session"}),
            "channel route",
        )
        try:
            session = VirtualSessionIdentity.from_dict(cast(Mapping[str, object], raw["session"]))
        except ContractError as exc:
            raise ContractError(f"session: {exc}") from exc
        return cls(
            cast(str, raw["platform"]),
            cast(str, raw["account_id"]),
            cast(str, raw["channel_id"]),
            cast(str, raw["user_id"]),
            session,
        )


@dataclass(frozen=True, slots=True)
class ChannelInboundEvent:
    """An independently observed channel event awaiting local admission."""

    route: ChannelRoute
    event_id: str
    input_text: object
    files: tuple[ChannelFile, ...] = ()
    context: dict | None = None

    def __post_init__(self) -> None:
        if type(self.route) is not ChannelRoute:
            raise ContractError("channel event route must be a ChannelRoute")
        _opaque(self.event_id, "channel event event_id")
        from .channel_context import message_context

        object.__setattr__(self, "context", message_context(self.context))
        if (type(self.files) is not tuple or len(self.files) > 8
                or any(type(item) is not ChannelFile or item.platform != self.route.platform
                       or item.channel_id != self.route.channel_id for item in self.files)
                or len({item.file_id for item in self.files}) != len(self.files)):
            raise ContractError("channel event files invalid")
        # RuntimeInbox._payload owns the existing input_text validation and length bound.

    @property
    def observed_route(self) -> ChannelRoute:
        return self.route

    @property
    def operation_id(self) -> str:
        return channel_operation_id(self.route, self.event_id)

    def to_dict(self) -> dict[str, object]:
        result = {
            "route": self.route.to_dict(),
            "event_id": self.event_id,
            "input_text": self.input_text,
        }
        if self.files:
            result["files"] = [item.to_dict() for item in self.files]
        if self.context is not None:
            result["context"] = self.context
        return result

    @classmethod
    def from_dict(cls, value: Mapping[str, object]) -> ChannelInboundEvent:
        fields = {"route", "event_id", "input_text"}
        if isinstance(value, Mapping) and "files" in value:
            fields.add("files")
        if isinstance(value, Mapping) and "context" in value:
            fields.add("context")
        raw = _exact_mapping(
            value,
            frozenset(fields),
            "channel inbound event",
        )
        try:
            route = ChannelRoute.from_dict(cast(Mapping[str, object], raw["route"]))
        except ContractError as exc:
            raise ContractError(f"route: {exc}") from exc
        files = raw.get("files", [])
        if type(files) is not list or len(files) > 8:
            raise ContractError("channel event files invalid")
        return cls(route, cast(str, raw["event_id"]), raw["input_text"],
                   tuple(ChannelFile.from_dict(item) for item in files), raw.get("context"))


def channel_operation_id(route: ChannelRoute, event_id: str) -> str:
    """Derive one stable operation identity without body, goal, or session generation."""

    if type(route) is not ChannelRoute:
        raise ContractError("channel operation route must be a ChannelRoute")
    event_id = _opaque(event_id, "channel event event_id")
    binding = {
        "platform": route.platform,
        "account_id": route.account_id,
        "channel_id": route.channel_id,
        "user_id": route.user_id,
        "event_id": event_id,
    }
    return f"channel-{sha256(canonical_json(binding)).hexdigest()}"


operation_id_for = channel_operation_id
ChannelEvent = ChannelInboundEvent
IncomingChannelEvent = ChannelInboundEvent

__all__ = [
    "CHANNEL_PLATFORMS",
    "CHANNEL_VALUE_MAX_LENGTH",
    "ChannelEvent",
    "ChannelInboundEvent",
    "ChannelRoute",
    "IncomingChannelEvent",
    "channel_operation_id",
    "operation_id_for",
]
