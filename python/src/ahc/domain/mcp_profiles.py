"""Immutable owner-selected stdio server binding; discovery never grants tools."""

from dataclasses import dataclass
from pathlib import Path
from typing import ClassVar

from .sessions import ContractError, _exact_mapping, _identifier
from .skill_values import digest_json
from .turns import _sha256_hex


@dataclass(frozen=True, slots=True)
class McpStdioProfile:
    server_id: str
    agent_id: str
    command: tuple[str, ...]
    cwd: str
    environment_names: tuple[str, ...]
    allowed_tools: tuple[str, ...]
    capability_digest: str | None
    enabled: bool = False
    schema_version: ClassVar[str] = "ahc.mcp-stdio-profile.v1"

    def __post_init__(self):
        _identifier(self.server_id, "MCP server")
        _identifier(self.agent_id, "MCP agent")
        if (type(self.enabled) is not bool or type(self.command) is not tuple
                or not 1 <= len(self.command) <= 64
                or any(type(v) is not str or "\0" in v for v in self.command)
                or sum(len(v) for v in self.command) > 16384
                or not Path(self.command[0]).is_absolute()
                or type(self.cwd) is not str or not Path(self.cwd).is_absolute()):
            raise ContractError("MCP profile launch binding invalid")
        for values in (self.environment_names, self.allowed_tools):
            if (type(values) is not tuple or len(values) > 256
                    or any(type(v) is not str or not v or len(v) > 128 for v in values)
                    or len(set(values)) != len(values)):
                raise ContractError("MCP profile allowlist invalid")
        if any("=" in name or "\0" in name for name in self.environment_names):
            raise ContractError("MCP environment name invalid")
        if self.capability_digest is not None:
            _sha256_hex(self.capability_digest)

    def to_dict(self):
        return {"schema_version": self.schema_version, "server_id": self.server_id,
                "agent_id": self.agent_id, "command": list(self.command), "cwd": self.cwd,
                "environment_names": list(self.environment_names),
                "allowed_tools": list(self.allowed_tools),
                "capability_digest": self.capability_digest, "enabled": self.enabled}

    @property
    def digest(self):
        return digest_json(self.to_dict())

    @classmethod
    def from_dict(cls, value):
        raw = dict(_exact_mapping(value, set(cls.__dataclass_fields__), "MCP profile"))
        if raw.pop("schema_version") != cls.schema_version:
            raise ContractError("MCP profile schema invalid")
        for name in ("command", "environment_names", "allowed_tools"):
            if type(raw[name]) is not list:
                raise ContractError("MCP profile list invalid")
            raw[name] = tuple(raw[name])
        return cls(**raw)
