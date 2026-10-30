"""Immutable local child-work envelopes; execution remains adapter-owned."""

from dataclasses import dataclass
from hashlib import sha256

from .sessions import ContractError, VirtualSessionIdentity, _identifier
from .skill_values import digest_json
from .turns import _input_text, _sha256_hex


def _task_text(value):
    _input_text(value)
    try:
        value.encode("utf-8")
    except UnicodeError:
        raise ContractError("child task text encoding invalid") from None
    return value


@dataclass(frozen=True, slots=True)
class ChildParent:
    session: VirtualSessionIdentity
    operation_id: str
    request_digest: str

    def __post_init__(self):
        if type(self.session) is not VirtualSessionIdentity:
            raise ContractError("child parent session invalid")
        _identifier(self.operation_id, "child parent operation")
        _sha256_hex(self.request_digest)

    def to_dict(self):
        return {
            "session": self.session.to_dict(),
            "operation_id": self.operation_id,
            "request_digest": self.request_digest,
        }


@dataclass(frozen=True, slots=True)
class ChildWorkEnvelope:
    child_id: str
    parent: ChildParent
    task_text: str
    artifact_scope: str
    deadline_unix_ms: int
    execution_kind: str = "subagent"

    def __post_init__(self):
        _identifier(self.child_id, "child identifier")
        if type(self.parent) is not ChildParent:
            raise ContractError("child parent invalid")
        _task_text(self.task_text)
        _identifier(self.artifact_scope, "child artifact scope")
        if type(self.deadline_unix_ms) is not int or self.deadline_unix_ms <= 0:
            raise ContractError("child deadline invalid")
        if (type(self.execution_kind) is not str
                or self.execution_kind not in ("subagent", "minion")):
            raise ContractError("child execution kind unsupported")

    def to_dict(self):
        return {
            "child_id": self.child_id,
            "parent": self.parent.to_dict(),
            "task_text": self.task_text,
            "artifact_scope": self.artifact_scope,
            "deadline_unix_ms": self.deadline_unix_ms,
            "execution_kind": self.execution_kind,
        }

    @property
    def request_digest(self):
        return digest_json(self.to_dict())

    @property
    def task_digest(self):
        return sha256(self.task_text.encode("utf-8")).hexdigest()

    @classmethod
    def from_dict(cls, value):
        fields = {
            "child_id",
            "parent",
            "task_text",
            "artifact_scope",
            "deadline_unix_ms",
            "execution_kind",
        }
        if type(value) is not dict or set(value) != fields:
            raise ContractError("child envelope fields invalid")
        parent = value["parent"]
        if type(parent) is not dict or set(parent) != {
            "session",
            "operation_id",
            "request_digest",
        }:
            raise ContractError("child parent fields invalid")
        return cls(
            value["child_id"],
            ChildParent(
                VirtualSessionIdentity.from_dict(parent["session"]),
                parent["operation_id"],
                parent["request_digest"],
            ),
            value["task_text"],
            value["artifact_scope"],
            value["deadline_unix_ms"],
            value["execution_kind"],
        )


def child_output(value):
    if type(value) is not str:
        raise ContractError("child output exceeds bound")
    try:
        size = len(value.encode("utf-8"))
    except UnicodeError:
        raise ContractError("child output encoding invalid") from None
    if size > 4096:
        raise ContractError("child output exceeds bound")
    if any(0xD800 <= ord(char) <= 0xDFFF for char in value):
        raise ContractError("child output encoding invalid")
    return value
