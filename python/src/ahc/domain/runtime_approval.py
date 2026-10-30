"""Correlated approval values; receipt validation grants no replay authority."""

from dataclasses import dataclass
from math import isfinite

from .sessions import ContractError, VirtualSessionIdentity, _identifier
from .skill_values import digest_json
from .turns import _sha256_hex


@dataclass(frozen=True, slots=True)
class ApprovalRequest:
    session: VirtualSessionIdentity
    operation_id: str
    policy_digest: str
    backend_thread: str
    backend_turn: str
    vendor_request_id: str | int
    action_digest: str
    expires_at: float
    action_summary: str

    def __post_init__(self):
        if type(self.session) is not VirtualSessionIdentity:
            raise ContractError("approval session invalid")
        for value in (self.operation_id, self.backend_thread, self.backend_turn):
            _identifier(value, "approval binding")
        for value in (self.policy_digest, self.action_digest):
            _sha256_hex(value)
        if (
            type(self.action_summary) is not str
            or not self.action_summary.strip()
            or len(self.action_summary) > 4096
            or any(0xD800 <= ord(c) <= 0xDFFF for c in self.action_summary)
        ):
            raise ContractError("approval requires a bounded display summary")
        identity = self.vendor_request_id
        if type(identity) is int:
            if not -(2**53 - 1) <= identity <= 2**53 - 1:
                raise ContractError("approval request ID outside exact JSON integer range")
        elif type(identity) is str:
            if (
                not identity
                or len(identity) > 128
                or any(ord(c) < 32 or 0xD800 <= ord(c) <= 0xDFFF for c in identity)
            ):
                raise ContractError("approval request ID invalid")
        else:
            raise ContractError("approval request ID must be string or integer")
        if (
            type(self.expires_at) not in (int, float)
            or not isfinite(self.expires_at)
            or self.expires_at <= 0
        ):
            raise ContractError("approval expiration must be finite positive epoch seconds")
        object.__setattr__(self, "expires_at", float(self.expires_at))

    def to_dict(self):
        return {
            "session": self.session.to_dict(),
            "operation_id": self.operation_id,
            "policy_digest": self.policy_digest,
            "backend_thread": self.backend_thread,
            "backend_turn": self.backend_turn,
            "vendor_request_id": self.vendor_request_id,
            "action_digest": self.action_digest,
            "expires_at": self.expires_at,
            "action_summary": self.action_summary,
        }

    @property
    def digest(self):
        return digest_json(self.to_dict())

    @classmethod
    def from_dict(cls, value):
        fields = {
            "session",
            "operation_id",
            "policy_digest",
            "backend_thread",
            "backend_turn",
            "vendor_request_id",
            "action_digest",
            "expires_at",
            "action_summary",
        }
        if type(value) is not dict or set(value) != fields:
            raise ContractError("approval request fields invalid")
        return cls(**{**value, "session": VirtualSessionIdentity.from_dict(value["session"])})


@dataclass(frozen=True, slots=True)
class ApprovalDecision:
    request_digest: str
    outcome: str

    def __post_init__(self):
        _sha256_hex(self.request_digest)
        if type(self.outcome) is not str or self.outcome not in (
            "approve-once",
            "deny",
            "timeout",
            "unavailable",
            "cancelled",
        ):
            raise ContractError("approval outcome invalid")

    def resolve(self, request, *, now, cancelled=False):
        """Validate immediately before response; store/bridge separately consume once.

        Expiration uses wall-clock epoch seconds for cross-process persistence.
        Callers still bound waits with a monotonic deadline.
        """
        if type(request) is not ApprovalRequest or self.request_digest != request.digest:
            raise ContractError("approval decision correlation mismatch")
        if type(now) not in (int, float) or not isfinite(now) or type(cancelled) is not bool:
            raise ContractError("approval observation invalid")
        if cancelled:
            return "cancelled"
        if now >= request.expires_at:
            return "timeout"
        return self.outcome
