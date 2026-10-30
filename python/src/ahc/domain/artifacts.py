"""Immutable, content-addressed local workspace artifacts."""

import re
from dataclasses import dataclass
from hashlib import sha256

from .sessions import ContractError
from .skill_values import digest_json
from .turns import _sha256_hex
from .workspace import WorkspaceIdentity

_MEDIA_TYPE = re.compile(r"^[a-z0-9!#$%&'*+.^_`|~-]+/[a-z0-9!#$%&'*+.^_`|~-]+$")


class ArtifactNotPublished(ContractError):
    """The local publication did not happen; this is not an unknown write."""

    def __init__(self, reason):
        super().__init__(reason)
        self.reason = reason


@dataclass(frozen=True, slots=True)
class ArtifactManifest:
    producer: WorkspaceIdentity
    content_digest: str
    media_type: str
    byte_count: int
    lineage: tuple[str, ...] = ()

    def __post_init__(self):
        if type(self.producer) is not WorkspaceIdentity:
            raise ContractError("artifact producer invalid")
        _sha256_hex(self.content_digest)
        if (
            type(self.media_type) is not str
            or len(self.media_type) > 128
            or _MEDIA_TYPE.fullmatch(self.media_type) is None
        ):
            raise ContractError("artifact media type invalid")
        if type(self.byte_count) is not int or self.byte_count < 0:
            raise ContractError("artifact byte count invalid")
        if (
            type(self.lineage) is not tuple
            or any(type(item) is not str for item in self.lineage)
            or len(set(self.lineage)) != len(self.lineage)
        ):
            raise ContractError("artifact lineage invalid")
        for artifact_id in self.lineage:
            _sha256_hex(artifact_id)

    def to_dict(self):
        return {
            "producer": self.producer.to_dict(),
            "content_digest": self.content_digest,
            "media_type": self.media_type,
            "byte_count": self.byte_count,
            "lineage": list(self.lineage),
        }

    @classmethod
    def from_dict(cls, value):
        if type(value) is not dict or set(value) != {
            "producer", "content_digest", "media_type", "byte_count", "lineage"
        } or type(value["lineage"]) is not list:
            raise ContractError("artifact manifest fields invalid")
        return cls(
            WorkspaceIdentity.from_dict(value["producer"]),
            value["content_digest"],
            value["media_type"],
            value["byte_count"],
            tuple(value["lineage"]),
        )

    @property
    def artifact_id(self):
        return digest_json(self.to_dict())


def content_digest(content):
    if type(content) is not bytes:
        raise ContractError("artifact content must be bytes")
    return sha256(content).hexdigest()


__all__ = ["ArtifactManifest", "ArtifactNotPublished", "content_digest"]
