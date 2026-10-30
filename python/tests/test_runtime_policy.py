"""Policy mode cannot broaden the independent interpreter/environment grant."""

from dataclasses import FrozenInstanceError, replace

import pytest

from ahc.domain.runtime_policy import EffectivePolicy
from ahc.domain.sessions import ContractError


def policy(**changes):
    return replace(EffectivePolicy(1, "manual", "read-only", False, "a" * 64, 30), **changes)


def test_yolo_changes_approval_only_and_never_grants_workspace():
    manual = policy()
    yolo = replace(manual, approval_mode="yolo")
    assert manual.codex_approval_policy == "on-request"
    assert yolo.codex_approval_policy == "never"
    assert yolo.sandbox_mode == manual.sandbox_mode == "read-only"
    assert yolo.environment_digest == manual.environment_digest
    with pytest.raises(ContractError, match="not granted"):
        yolo.require_workspace_grant("a" * 64)


def test_grant_is_bound_to_exact_environment():
    granted = policy(workspace_granted=True)
    granted.require_workspace_grant("a" * 64)
    with pytest.raises(ContractError):
        granted.require_workspace_grant("b" * 64)


@pytest.mark.parametrize(
    "changes",
    [
        {"revision": 2},
        {"approval_mode": "yolo"},
        {"sandbox_mode": "workspace-write"},
        {"workspace_granted": True},
        {"environment_digest": "b" * 64},
        {"approval_wait_seconds": 31},
    ],
)
def test_every_authority_field_changes_admission_digest(changes):
    assert policy(**changes).digest != policy().digest


def test_snapshot_is_immutable_and_projection_cannot_mutate_admission():
    original = policy()
    with pytest.raises(FrozenInstanceError):
        original.approval_mode = "yolo"
    projection = original.to_dict()
    projection["workspace_granted"] = True
    assert not original.workspace_granted
    assert EffectivePolicy.from_dict(original.to_dict()) == original
    assert policy(approval_wait_seconds=30.0).digest == original.digest


@pytest.mark.parametrize(
    "changes",
    [
        {"approval_mode": "smart"},
        {"approval_mode": []},
        {"revision": True},
        {"workspace_granted": 1},
        {"sandbox_mode": "unrestricted"},
        {"environment_digest": "not-a-digest"},
        {"approval_wait_seconds": float("nan")},
        {"approval_wait_seconds": float("inf")},
        {"approval_wait_seconds": 0},
        {"approval_wait_seconds": True},
    ],
)
def test_invalid_or_unavailable_policy_rejects_without_substitution(changes):
    with pytest.raises(ContractError):
        policy(**changes)


@pytest.mark.parametrize(
    "field,value",
    [
        ("schema_version", "ahc.effective-policy.v2"),
        ("codex_approval_policy", "never"),
        ("unknown", True),
    ],
)
def test_serialized_policy_rejects_drift(field, value):
    data = policy().to_dict()
    data[field] = value
    with pytest.raises(ContractError):
        EffectivePolicy.from_dict(data)
