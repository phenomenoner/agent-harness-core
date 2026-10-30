"""Explicit native configuration retains grants and rejects WSL/host ambiguity."""

import pytest
from test_foreground_config_v2 import valid_config

from ahc.cli.foreground_config_native import (
    parse_foreground_config_native, parse_trusted_foreground_config,
)
from ahc.cli.foreground_config_v2 import parse_foreground_config_v2
from ahc.domain.sessions import ContractError
from ahc.domain.skill_values import digest_json


def native_config():
    value = valid_config()
    value["schema_version"] = "ahc.foreground.config.v3"
    value["runtime_root"] = "/home/ahc/product/state"
    value["workspace"]["kind"] = "linux-ipython"
    for key in ("wsl_executable", "distribution", "user"):
        del value["workspace"][key]
    value["backend"]["kind"] = "codex-linux-trusted"
    return value


def test_native_roundtrip_exact_policy_and_explicit_version():
    value = native_config()
    config = parse_foreground_config_native(value)
    assert config.to_dict() == value
    assert parse_trusted_foreground_config(value) == config
    assert config.effective_policy.environment_digest == digest_json(value["workspace"])
    assert config.preview()["backend"]["kind"] == "codex-linux-trusted"
    with pytest.raises(ContractError):
        parse_foreground_config_v2(value)
    with pytest.raises(ContractError):
        parse_foreground_config_native(valid_config())
    assert parse_trusted_foreground_config(valid_config()) == parse_foreground_config_v2(valid_config())


@pytest.mark.parametrize("location", ["C:/ahc/runtime", "/", "/mnt/d/state", "/mnt", "/home/a/../b", "//host/state"])
def test_native_state_rejects_host_or_ambiguous_locations(location):
    value = native_config()
    value["runtime_root"] = location
    with pytest.raises(ContractError):
        parse_foreground_config_native(value)


@pytest.mark.parametrize("key,value", [("wsl_executable", "C:/Windows/wsl.exe"),
                                     ("distribution", "AHC-Codex-Ubuntu-24.04"),
                                     ("user", "ahc"), ("kind", "wsl-ipython")])
def test_native_workspace_rejects_bridge_fields(key, value):
    document = native_config()
    document["workspace"][key] = value
    with pytest.raises(ContractError):
        parse_foreground_config_native(document)


@pytest.mark.parametrize("section,key", [
    ("workspace", "cwd"), ("workspace", "interpreter"),
    ("backend", "bundle"), ("backend", "codex_home"),
])
def test_native_owned_paths_reject_windows_mounts(section, key):
    value = native_config()
    value[section][key] = "/mnt/c/ahc/core"
    with pytest.raises(ContractError):
        parse_foreground_config_native(value)


@pytest.mark.parametrize("field", ["skill_root", "native_memory"])
def test_native_owned_resource_rejects_windows_mounts(field):
    value = native_config()
    value[field] = ("/mnt/d/skills" if field == "skill_root" else
                    {"binding_file": "/mnt/d/bindings.json", "binding_digest": "a" * 64})
    with pytest.raises(ContractError):
        parse_foreground_config_native(value)


def test_native_memory_references_preserve_grants_and_require_posix():
    value = native_config()
    value["native_memory"] = {"binding_file": "/home/ahc/product/memory.json", "binding_digest": "a" * 64}
    value["memory"] = {"enabled": True, "kind": "openclaw-mem-snapshot-pack", "resource_id": "memory",
                       "scope": "agent", "snapshot": "/home/ahc/product/snapshot.json",
                       "provider_root": "/home/ahc/product/provider", "granted": False, "timeout": 5}
    config = parse_foreground_config_native(value)
    assert config.to_dict()["memory"]["granted"] is False
    value["native_memory"]["binding_file"] = "D:/memory.json"
    with pytest.raises(ContractError):
        parse_foreground_config_native(value)
