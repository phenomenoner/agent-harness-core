import json

import pytest
from test_foreground_trusted import config_file

from ahc.cli import foreground_trusted, main
from ahc.cli.foreground import compose_foreground
from ahc.cli.foreground_config_v2 import parse_foreground_config_v2
from ahc.domain.channel_ingress import ChannelRoute
from ahc.domain.sessions import ContractError


def configured(tmp_path, enabled=True):
    _, request, value, path = config_file(tmp_path, enabled=enabled)
    route = ChannelRoute("telegram", "123", "456", "789", request.session)
    value["channels"] = [{"route": route.to_dict(), "token_env": "AHC_CONFIG_FIXTURE",
                           "enabled": True}]
    return value, path


@pytest.mark.parametrize("enabled", [False, True])
def test_preview_shows_effect_without_credential_resolution(tmp_path, monkeypatch, capsys, enabled):
    value, path = configured(tmp_path, enabled)
    monkeypatch.delenv("AHC_CONFIG_FIXTURE", raising=False)
    path.write_text(json.dumps(value), encoding="utf-8")
    assert main(["runtime-config-preview", "--runtime-config", str(path)]) == 0
    output = capsys.readouterr().out
    assert "AHC_CONFIG_FIXTURE" not in output
    preview = parse_foreground_config_v2(value).preview()["channels"]
    assert preview["automatic_delivery_routes"] == int(enabled)
    assert preview["configured_routes"] == 1
    assert preview["credentials"] == "not-inspected"
    assert preview["routes"][0]["automatic_delivery"] is enabled


@pytest.mark.parametrize("defect", ["duplicate", "session", "enabled", "credential",
                                    "typing-type", "typing-disabled"])
def test_invalid_channel_bindings_rejected(tmp_path, defect):
    value, _ = configured(tmp_path)
    row = value["channels"][0]
    if defect == "duplicate":
        value["channels"].append(row.copy())
    elif defect == "session":
        row["route"]["session"]["agent_id"] = "another-agent"
    elif defect == "enabled":
        row["enabled"] = 1
    elif defect == "typing-type":
        row["typing"] = 1
    elif defect == "typing-disabled":
        row.update(typing=True, enabled=False)
    else:
        row["token_env"] = "INVALID=NAME"
    with pytest.raises(ContractError):
        parse_foreground_config_v2(value)


def test_missing_enabled_token_fails_before_backend_construction(tmp_path, monkeypatch):
    value, path = configured(tmp_path)
    path.write_text(json.dumps(value), encoding="utf-8")
    monkeypatch.delenv("AHC_CONFIG_FIXTURE", raising=False)

    def forbidden(*args, **kwargs):
        pytest.fail("missing token reached backend construction")

    monkeypatch.setattr(foreground_trusted, "TrustedCodexBackendFactory", forbidden)
    with pytest.raises(ContractError, match="credential unavailable"):
        compose_foreground(path)


@pytest.mark.parametrize("enabled", [False, True])
def test_progress_opt_in_preview_preserves_disabled_backend(tmp_path, enabled):
    value, _ = configured(tmp_path, enabled)
    value["channels"][0]["progress"] = True
    config = parse_foreground_config_v2(value)
    assert config.preview()["channels"]["progress_routes"] == int(enabled)
    value["channels"][0]["progress"] = 1
    with pytest.raises(ContractError):
        parse_foreground_config_v2(value)
