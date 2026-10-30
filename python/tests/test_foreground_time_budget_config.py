"""Adaptive host policy is explicit, native and tied to the owner lifecycle."""
from copy import deepcopy
import pytest
from test_foreground_config_native import native_config
from ahc.cli.foreground_config_native import parse_foreground_config_native
from ahc.domain.sessions import ContractError

def test_default_route_and_custom_model_effort_are_bound():
    value = native_config()
    value["workspace"]["lifetime_mode"] = "foreground"
    value["backend"]["time_budget"] = {"enabled": True}
    config = parse_foreground_config_native(value)
    policy = config.backend["time_budget"]
    assert (policy["model"], policy["reasoning_effort"], policy["extension_seconds"]) == ("gpt-6-luna", "low", 1800)
    other = deepcopy(value)
    other["backend"]["time_budget"].update(model="another-model", reasoning_effort="high", extension_seconds=3600)
    configured = parse_foreground_config_native(other)
    assert configured.configuration_digest != config.configuration_digest
    assert configured.backend["time_budget"]["extension_seconds"] == 3600

def test_legacy_roundtrip_and_disabled_mode():
    value = native_config()
    assert parse_foreground_config_native(value).to_dict() == value
    value["backend"]["time_budget"] = {"enabled": False}
    assert not parse_foreground_config_native(value).backend["time_budget"]["enabled"]

def test_enabled_requires_owner_lifetime():
    value = native_config()
    value["backend"]["time_budget"] = {"enabled": True}
    with pytest.raises(ContractError, match="foreground-owned"):
        parse_foreground_config_native(value)
