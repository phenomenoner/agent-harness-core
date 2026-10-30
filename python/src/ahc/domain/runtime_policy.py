"""Immutable owner-selected policy; approval is distinct from environment access.

These values describe admission, not proof that a vendor or OS enforces them.
Only explicit configuration creates a grant; model output cannot change it.
"""

from dataclasses import dataclass
from math import isfinite

from .sessions import ContractError
from .skill_values import digest_json
from .turns import _sha256_hex


@dataclass(frozen=True, slots=True)
class EffectivePolicy:
    revision: int
    approval_mode: str
    sandbox_mode: str
    workspace_granted: bool
    environment_digest: str
    approval_wait_seconds: float

    def __post_init__(self):
        if type(self.revision) is not int or self.revision < 1:
            raise ContractError("policy revision must be a positive integer")
        if type(self.approval_mode) is not str or self.approval_mode not in (
            "manual",
            "auto-approve",
            "always-approve",
            "yolo",
        ):
            raise ContractError("unsupported approval mode; Smart is not available")
        if type(self.sandbox_mode) is not str or self.sandbox_mode not in (
            "read-only",
            "workspace-write",
            "danger-full-access",
        ):
            raise ContractError("unsupported Codex sandbox mode")
        if type(self.workspace_granted) is not bool:
            raise ContractError("workspace grant must be explicit boolean")
        _sha256_hex(self.environment_digest)
        if (
            type(self.approval_wait_seconds) not in (int, float)
            or not isfinite(self.approval_wait_seconds)
            or self.approval_wait_seconds <= 0
        ):
            raise ContractError("approval wait must be positive and finite")
        object.__setattr__(self, "approval_wait_seconds", float(self.approval_wait_seconds))

    @property
    def codex_approval_policy(self):
        return "never" if self.approval_mode == "yolo" else "on-request"

    @property
    def codex_approvals_reviewer(self):
        return "auto_review" if self.approval_mode == "auto-approve" else None

    def to_dict(self):
        return {
            "schema_version": "ahc.effective-policy.v1",
            "revision": self.revision,
            "approval_mode": self.approval_mode,
            "codex_approval_policy": self.codex_approval_policy,
            "sandbox_mode": self.sandbox_mode,
            "workspace_granted": self.workspace_granted,
            "environment_digest": self.environment_digest,
            "approval_wait_seconds": self.approval_wait_seconds,
        }

    @property
    def digest(self):
        return digest_json(self.to_dict())

    @classmethod
    def from_dict(cls, value):
        fields = {
            "schema_version",
            "revision",
            "approval_mode",
            "codex_approval_policy",
            "sandbox_mode",
            "workspace_granted",
            "environment_digest",
            "approval_wait_seconds",
        }
        if type(value) is not dict or set(value) != fields:
            raise ContractError("effective policy fields invalid")
        if value["schema_version"] != "ahc.effective-policy.v1":
            raise ContractError("effective policy version unsupported")
        policy = cls(**{k: value[k] for k in fields - {"schema_version", "codex_approval_policy"}})
        if value["codex_approval_policy"] != policy.codex_approval_policy:
            raise ContractError("effective policy vendor mapping mismatch")
        return policy

    def require_workspace_grant(self, environment_digest):
        if not self.workspace_granted or environment_digest != self.environment_digest:
            raise ContractError("trusted workspace environment is not granted")
