import json
from dataclasses import FrozenInstanceError
from pathlib import Path

import pytest

from ahc.cli.foreground_config_v2 import parse_foreground_config_v2
from ahc.domain.runtime_policy import EffectivePolicy
from ahc.domain.sessions import ContractError, VirtualSessionIdentity
from ahc.domain.skill_values import digest_json


def valid_config():
    return {
        "schema_version": "ahc.foreground.config.v2",
        "runtime_root": "C:/ahc/runtime",
        "session": VirtualSessionIdentity.initial("session", "agent", "route").to_dict(),
        "workspace_id": "work",
        "checkpoint_digest": None,
        "skill_root": None,
        "workspace": {
            "kind": "wsl-python",
            "wsl_executable": "C:/Windows/System32/wsl.exe",
            "distribution": "AHC-Codex-Ubuntu-24.04",
            "user": "ahc",
            "cwd": "/home/ahc/work",
            "interpreter": "/opt/python/bin/python3",
            "execution_timeout": 30,
            "lifetime_seconds": 3600,
        },
        "backend": {
            "kind": "codex-wsl-trusted",
            "enabled": False,
            "bundle": "/opt/codex",
            "codex_home": "/home/ahc/codex",
            "model": "gpt-test",
            "turn_timeout": 30,
            "lifetime_seconds": 3600,
        },
        "policy": {
            "revision": 1,
            "approval_mode": "manual",
            "sandbox_mode": "workspace-write",
            "workspace_granted": True,
            "approval_wait_seconds": 30,
        },
    }


def replace(payload, path, value):
    target = payload
    for key in path[:-1]:
        target = target[key]
    target[path[-1]] = value
    return payload


def test_valid_roundtrip_preview_digest_and_fresh_immutable_projections():
    payload = valid_config()
    config = parse_foreground_config_v2(payload)
    assert config.to_dict() == payload
    assert config.session == VirtualSessionIdentity.from_dict(payload["session"])
    assert config.effective_policy == EffectivePolicy(
        1, "manual", "workspace-write", True, digest_json(payload["workspace"]), 30
    )
    assert config.configuration_digest == digest_json(
        {**payload, "effective_policy": config.effective_policy.to_dict()}
    )
    assert config.preview() == {
        "workspace": payload["workspace"],
        "backend": {"kind": "codex-wsl-trusted", "enabled": False, "model": "gpt-test"},
        "effective_policy": config.effective_policy.to_dict(),
    }
    assert {"bundle", "codex_home"}.isdisjoint(config.preview()["backend"])
    workspace, backend, document = config.workspace, config.backend, config.to_dict()
    workspace["cwd"], backend["model"], document["policy"]["workspace_granted"] = (
        "/changed",
        "changed",
        False,
    )
    assert config.workspace["cwd"] == "/home/ahc/work"
    assert config.backend["model"] == "gpt-test"
    assert config.to_dict()["policy"]["workspace_granted"] is True
    with pytest.raises(FrozenInstanceError):
        config.effective_policy.workspace_granted = False
    payload["workspace"]["cwd"] = "/mutated"
    payload["policy"]["approval_mode"] = "yolo"
    assert config.effective_policy.approval_mode == "manual"


def test_mode_changes_do_not_create_a_workspace_grant():
    payload = valid_config()
    payload["policy"].update(approval_mode="yolo", workspace_granted=False)
    config = parse_foreground_config_v2(payload)
    assert config.effective_policy.codex_approval_policy == "never"
    assert config.effective_policy.workspace_granted is False
    assert config.effective_policy.environment_digest == digest_json(payload["workspace"])


@pytest.mark.parametrize(
    "field",
    (
        "schema_version",
        "runtime_root",
        "session",
        "workspace_id",
        "checkpoint_digest",
        "workspace",
        "backend",
        "policy",
    ),
)
def test_missing_required_top_level_field_rejects(field):
    payload = valid_config()
    del payload[field]
    with pytest.raises(ContractError):
        parse_foreground_config_v2(payload)


@pytest.mark.parametrize("version", ("ahc.foreground.config.v1", "ahc.foreground.config.v3", None))
def test_v1_and_unknown_versions_reject(version):
    payload = valid_config()
    payload["schema_version"] = version
    with pytest.raises(ContractError):
        parse_foreground_config_v2(payload)


@pytest.mark.parametrize(
    "path, value",
    (
        (("runtime_root",), "relative"),
        (("runtime_root",), "C:relative"),
        (("runtime_root",), "/tmp/runtime"),
        (("runtime_root",), "C:/runtime\x00"),
        (("runtime_root",), "C:/runtime\n"),
        (("runtime_root",), "C:/runtime\ud800"),
        (("skill_root",), "relative"),
        (("workspace", "wsl_executable"), "relative/wsl.exe"),
        (("workspace", "wsl_executable"), "/usr/bin/wsl.exe"),
        (("workspace", "wsl_executable"), "C:/wsl\x00.exe"),
        (("workspace", "cwd"), "relative"),
        (("workspace", "cwd"), "C:/workspace"),
        (("workspace", "cwd"), "//server/work"),
        (("workspace", "cwd"), "/home/ahc/../other"),
        (("workspace", "cwd"), "/home//ahc"),
        (("workspace", "cwd"), "/home/ahc/"),
        (("workspace", "cwd"), "/home/ahc\\work"),
        (("workspace", "cwd"), "/home/ahc\nwork"),
        (("workspace", "cwd"), "/home/ahc\ud800"),
        (("workspace", "interpreter"), "/"),
        (("workspace", "interpreter"), "/opt/python/../bin/python3"),
        (("backend", "bundle"), "relative"),
        (("backend", "bundle"), "C:/codex"),
        (("backend", "bundle"), "//server/codex"),
        (("backend", "codex_home"), "/home/ahc/../other"),
        (("backend", "codex_home"), "/home/ahc\tprivate"),
    ),
)
def test_host_and_posix_paths_are_absolute_normalized_and_safe(path, value):
    with pytest.raises(ContractError):
        parse_foreground_config_v2(replace(valid_config(), path, value))


@pytest.mark.parametrize(
    "path, value",
    (
        (("workspace", "distribution"), ""),
        (("workspace", "distribution"), "Ubuntu-24.04"),
        (("workspace", "distribution"), "bad distro"),
        (("workspace", "distribution"), "-bad"),
        (("workspace", "distribution"), "a" * 129),
        (("workspace", "user"), "root"),
        (("workspace", "kind"), "wsl"),
        (("backend", "kind"), "codex-wsl-offline"),
        (("backend", "enabled"), 1),
        (("backend", "model"), None),
        (("backend", "model"), ""),
        (("backend", "model"), "m" * 4097),
        (("policy", "approval_mode"), "smart"),
        (("policy", "sandbox_mode"), "unrestricted"),
    ),
)
def test_unsupported_or_bounded_literal_fields_reject(path, value):
    with pytest.raises(ContractError):
        parse_foreground_config_v2(replace(valid_config(), path, value))


@pytest.mark.parametrize(
    "path, value",
    (
        (("workspace", "execution_timeout"), True),
        (("workspace", "execution_timeout"), 0),
        (("workspace", "execution_timeout"), -1),
        (("workspace", "execution_timeout"), float("nan")),
        (("workspace", "execution_timeout"), float("inf")),
        (("workspace", "execution_timeout"), 3601),
        (("workspace", "lifetime_seconds"), True),
        (("workspace", "lifetime_seconds"), 0),
        (("workspace", "lifetime_seconds"), 3601),
        (("workspace", "lifetime_seconds"), 1.0),
        (("backend", "turn_timeout"), True),
        (("backend", "turn_timeout"), 0),
        (("backend", "turn_timeout"), float("nan")),
        (("backend", "turn_timeout"), float("inf")),
        (("backend", "turn_timeout"), 3601),
        (("backend", "lifetime_seconds"), True),
        (("backend", "lifetime_seconds"), 0),
        (("backend", "lifetime_seconds"), 3601),
        (("policy", "revision"), True),
        (("policy", "revision"), 0),
        (("policy", "revision"), 1.0),
        (("policy", "workspace_granted"), 1),
        (("policy", "approval_wait_seconds"), True),
        (("policy", "approval_wait_seconds"), 0),
        (("policy", "approval_wait_seconds"), float("nan")),
        (("policy", "approval_wait_seconds"), float("inf")),
    ),
)
def test_numbers_are_strict_finite_positive_values(path, value):
    with pytest.raises(ContractError):
        parse_foreground_config_v2(replace(valid_config(), path, value))


def test_timeouts_must_not_outlive_matching_units_and_equal_is_allowed():
    payload = valid_config()
    payload["workspace"]["lifetime_seconds"] = 29
    with pytest.raises(ContractError):
        parse_foreground_config_v2(payload)
    payload = valid_config()
    payload["backend"]["lifetime_seconds"] = 29
    with pytest.raises(ContractError):
        parse_foreground_config_v2(payload)
    payload = valid_config()
    payload["workspace"].update(execution_timeout=3600, lifetime_seconds=3600)
    payload["backend"].update(turn_timeout=3600, lifetime_seconds=3600)
    assert parse_foreground_config_v2(payload).workspace["execution_timeout"] == 3600


@pytest.mark.parametrize(
    "path", (("workspace",), ("backend",), ("policy",), ("session",), ("session", "ref"))
)
def test_unknown_nested_fields_reject_at_every_level(path):
    payload = valid_config()
    target = payload
    for key in path:
        target = target[key]
    target["unknown"] = True
    with pytest.raises(ContractError):
        parse_foreground_config_v2(payload)


@pytest.mark.parametrize(
    "path, value",
    (
        (("workspace_id",), "not valid"),
        (("checkpoint_digest",), "x" * 64),
        (("checkpoint_digest",), 1),
        (("skill_root",), 1),
        (("skill_root",), "C:relative"),
        (("backend", "auth"), {}),
        (("workspace", "body"), "input"),
    ),
)
def test_scalar_and_private_auth_body_fields_reject(path, value):
    payload = valid_config()
    if path[-1] in ("auth", "body"):
        payload[path[0]][path[-1]] = value
    else:
        replace(payload, path, value)
    with pytest.raises(ContractError):
        parse_foreground_config_v2(payload)


def test_parser_has_no_filesystem_side_effects(monkeypatch):
    def forbidden(*_args, **_kwargs):
        raise AssertionError("parser performed filesystem work")

    for name in ("exists", "is_file", "is_dir", "mkdir", "resolve"):
        monkeypatch.setattr(Path, name, forbidden)
    config = parse_foreground_config_v2(valid_config())
    assert config.runtime_root == "C:/ahc/runtime"


def test_projection_is_json_safe_and_contains_no_derived_or_private_input():
    config = parse_foreground_config_v2(valid_config())
    encoded = json.dumps(config.to_dict(), allow_nan=False, sort_keys=True)
    assert "effective_policy" not in config.to_dict()
    assert all(word not in encoded for word in ("password", "auth", "body"))
