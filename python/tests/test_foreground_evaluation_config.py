"""An evaluator is explicit, finite, native-only and separate from promotion."""

from copy import deepcopy
from dataclasses import asdict

import pytest
from test_foreground_config_native import native_config

from ahc.cli.foreground_config_native import parse_foreground_config_native
from ahc.cli.foreground_config_v2 import parse_foreground_config_v2
from ahc.cli.foreground_evaluation import parse_evaluation
from ahc.domain.native_resource_limits import NativeResourceLimits
from ahc.domain.native_scratch_limits import NativeScratchLimits
from ahc.domain.sessions import ContractError


def evaluation_profile():
    return {"executable": "/opt/isolation/bwrap", "sha256": "a" * 64,
            "workspace": "/home/ahc/evaluation", "readonly_roots": ["/usr"],
            "protected_roots": [], "interpreter": "/usr/bin/python3",
            "resource_limits": asdict(NativeResourceLimits(128 * 1024**2, 16, 50000)),
            "scratch_limits": asdict(NativeScratchLimits(2 * 1024**2, 2 * 1024**2)),
            "lifetime_seconds": 30, "max_runs_per_trial": 2}


def configured():
    value = native_config()
    value["skill_root"] = "/home/ahc/product/skills"
    value["learning"] = {"mode": "proposal-only", "protected_skills": ["pmo-policy"],
                         "evaluation": evaluation_profile()}
    return value


def test_profile_roundtrip_binds_limits_and_preview_without_reading_host(monkeypatch):
    from pathlib import Path

    def no_read(*args, **kwargs):
        pytest.fail("parsing accessed the host")

    monkeypatch.setattr(Path, "exists", no_read)
    value = configured()
    config = parse_foreground_config_native(value)
    assert config.to_dict() == value
    assert config.preview()["learning"] == {
        "mode": "proposal-only", "isolated_evaluation_configured": True,
        "automatic_enabled": False, "automatic_configured": False,
        "observation_scope": "configuration-only"}
    changed = deepcopy(value)
    changed["learning"]["evaluation"]["max_runs_per_trial"] = 1
    other = parse_foreground_config_native(changed)
    assert other.configuration_digest != config.configuration_digest
    del value["learning"]["evaluation"]
    assert not parse_foreground_config_native(value).preview()["learning"][
        "isolated_evaluation_configured"]


@pytest.mark.parametrize("field,value", [
    ("workspace", "relative"), ("interpreter", "/usr/../python"), ("sha256", "wrong"),
    ("readonly_roots", []), ("protected_roots", ["/x"] * 17),
    ("lifetime_seconds", 0), ("lifetime_seconds", True), ("max_runs_per_trial", 33),
    ("scratch_limits", {"workspace_bytes": 1024}),
    ("resource_limits", {"memory_bytes": 16 * 1024**2}),
])
def test_invalid_profile_rejected_before_composition(field, value):
    profile = evaluation_profile()
    profile[field] = value
    with pytest.raises(ContractError):
        parse_evaluation(profile)


def test_evaluation_refuses_swap_automatic_mode_and_wsl():
    from test_foreground_config_v2 import valid_config

    value = configured()
    value["learning"]["evaluation"]["resource_limits"]["swap_bytes"] = 65536
    with pytest.raises(ContractError, match="swap"):
        parse_foreground_config_native(value)
    value = configured()
    value["learning"]["mode"] = "automatic"
    with pytest.raises(ContractError):
        parse_foreground_config_native(value)
    legacy = valid_config()
    legacy.update(skill_root="D:/skills", learning=configured()["learning"])
    with pytest.raises(ContractError, match="native"):
        parse_foreground_config_v2(legacy)
