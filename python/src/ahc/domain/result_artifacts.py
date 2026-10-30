"""Explicit artifact selections in an accepted agent result."""

from dataclasses import dataclass

from .sessions import ContractError
from .turns import _sha256_hex


def display_filename(value):
    if (type(value) is not str or not 1 <= len(value) <= 128 or value in {".", ".."}
            or any(ord(c) < 32 or 0x7F <= ord(c) <= 0x9F or 0xD800 <= ord(c) <= 0xDFFF
                   or c in '\\/:"' for c in value)):
        raise ContractError("artifact upload filename invalid")
    return value


@dataclass(frozen=True, slots=True)
class ResultArtifact:
    artifact_id: str
    filename: str
    delivery_mode: str = "document"
    caption: str = ""

    def __post_init__(self):
        _sha256_hex(self.artifact_id)
        display_filename(self.filename)
        validate_delivery_mode(self.delivery_mode)
        validate_caption(self.caption)

    def to_dict(self):
        result = {"artifact_id": self.artifact_id, "filename": self.filename}
        if self.delivery_mode != "document":
            result["delivery_mode"] = self.delivery_mode
        if self.caption:
            result["caption"] = self.caption
        return result

    @classmethod
    def from_dict(cls, value):
        if (type(value) is not dict
                or not {"artifact_id", "filename"}.issubset(value)
                or set(value) - {"artifact_id", "filename", "delivery_mode", "caption"}):
            raise ContractError("result artifact fields invalid")
        return cls(value["artifact_id"], value["filename"],
                   value.get("delivery_mode", "document"), value.get("caption", ""))


def validate_delivery_mode(value):
    if type(value) is not str or value not in {"document", "photo"}:
        raise ContractError("artifact delivery mode invalid")
    return value


def validate_caption(value):
    if type(value) is not str or len(value) > 1024:
        raise ContractError("artifact caption invalid")
    if any(0xD800 <= ord(character) <= 0xDFFF for character in value):
        raise ContractError("artifact caption invalid")
    if len(value.encode("utf-16-le")) // 2 > 1024:
        raise ContractError("artifact caption invalid")
    return value


def result_artifacts(value):
    if (type(value) is not tuple
            or any(type(item) is not ResultArtifact for item in value)):
        raise ContractError("result artifacts must be an immutable tuple")
    if len({item.artifact_id for item in value}) != len(value):
        raise ContractError("duplicate result artifact")
    return [item.to_dict() for item in value]
