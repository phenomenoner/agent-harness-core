"""Explicit versioned HTTP server profile; secrets remain in the credential resolver."""

from dataclasses import dataclass
from typing import ClassVar
from urllib.parse import urlsplit

from .sessions import ContractError, _exact_mapping, _identifier
from .skill_values import digest_json
from .turns import _sha256_hex


@dataclass(frozen=True, slots=True)
class McpHttpProfile:
    server_id: str
    agent_id: str
    endpoint: str
    authorization_ref: str | None
    allowed_tools: tuple[str, ...]
    capability_digest: str | None
    enabled: bool = False
    schema_version: ClassVar[str] = "ahc.mcp-http-profile.v1"

    def __post_init__(self):
        _identifier(self.server_id, "MCP server")
        _identifier(self.agent_id, "MCP agent")
        if type(self.endpoint) is not str or len(self.endpoint) > 4096:
            raise ContractError("MCP HTTP profile endpoint invalid")
        try:
            url = urlsplit(self.endpoint)
            if (url.scheme not in ("http", "https") or not url.hostname
                    or url.username or url.password or url.fragment
                    or any(ord(c) < 33 for c in self.endpoint)
                    or (url.scheme == "http" and url.hostname not in
                        ("127.0.0.1", "::1", "localhost"))):
                raise ValueError()
            _ = url.port
        except ValueError:
            raise ContractError("MCP HTTP profile endpoint invalid") from None
        if (type(self.enabled) is not bool or type(self.allowed_tools) is not tuple
                or len(self.allowed_tools) > 256
                or any(type(v) is not str or not v or len(v) > 128 for v in self.allowed_tools)
                or len(set(self.allowed_tools)) != len(self.allowed_tools)):
            raise ContractError("MCP HTTP profile allowlist invalid")
        if self.authorization_ref is not None:
            _identifier(self.authorization_ref, "MCP credential reference")
        if self.capability_digest is not None:
            _sha256_hex(self.capability_digest)

    def to_dict(self):
        return {"schema_version": self.schema_version, "server_id": self.server_id,
                "agent_id": self.agent_id, "endpoint": self.endpoint,
                "authorization_ref": self.authorization_ref,
                "allowed_tools": list(self.allowed_tools),
                "capability_digest": self.capability_digest, "enabled": self.enabled}

    @property
    def digest(self):
        return digest_json(self.to_dict())

    @classmethod
    def from_dict(cls, value):
        raw = dict(_exact_mapping(value, set(cls.__dataclass_fields__), "MCP HTTP profile"))
        if (raw.pop("schema_version") != cls.schema_version
                or type(raw["allowed_tools"]) is not list):
            raise ContractError("MCP HTTP profile schema invalid")
        raw["allowed_tools"] = tuple(raw["allowed_tools"])
        return cls(**raw)
