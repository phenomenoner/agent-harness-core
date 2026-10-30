import json
from dataclasses import asdict

import pytest
from test_aar_native_binding import candidate
from test_foreground_config_v2 import valid_config

from ahc.adapters.aar_native_binding import NATIVE_TOOLS
from ahc.cli.foreground_config_v2 import parse_foreground_config_v2
from ahc.cli.foreground_memory import inspect_foreground_config
from ahc.domain.mcp_profiles import McpStdioProfile
from ahc.domain.sessions import ContractError


def configured(tmp_path):
    raw = valid_config()
    pin, _ = candidate()
    raw["native_minion"] = {
        "profile": McpStdioProfile("native", "agent", (str(tmp_path / "python.exe"),),
            str(tmp_path), (), tuple(sorted(NATIVE_TOOLS)), "a" * 64, True).to_dict(),
        "pin": asdict(pin),
        "route": {"profile_id": "model", "catalog_digest": "sha256:" + "b" * 64,
                  "profile_digest": "sha256:" + "c" * 64},
        "principal_id": "host", "session_id": "native-session",
        "grants": {"execute": ["model", "rlm"], "cancel": [], "reconcile": []},
        "budget": {"model_requests": 1, "input_tokens": 1024, "output_tokens": 256,
                   "child_operations": 0, "artifact_bytes": 16384, "wall_time_ms": 10000},
    }
    return raw


def test_config_roundtrip_digest_and_inert_doctor(tmp_path, monkeypatch):
    import subprocess

    raw = configured(tmp_path)
    path = tmp_path / "config.json"
    path.write_text(json.dumps(raw))
    monkeypatch.setattr(subprocess, "Popen", lambda *a, **kw: pytest.fail("must not launch"))
    config = parse_foreground_config_v2(raw)
    assert (parse_foreground_config_v2(config.to_dict()).configuration_digest
            == config.configuration_digest)
    assert (config.configuration_digest
            != parse_foreground_config_v2(valid_config()).configuration_digest)
    preview = inspect_foreground_config(path)["native_minion"]
    assert preview == {"server_id": "native", "enabled": True,
                       "status": "configured", "execution_verified": False}
    assert "grants" not in json.dumps(config.preview())
    raw["native_minion"]["grants"]["execute"].append("later")
    assert config.to_dict()["native_minion"]["grants"]["execute"] == ["model", "rlm"]


@pytest.mark.parametrize("field,key,value", [
    ("profile", "agent_id", "foreign"), ("profile", "allowed_tools", []),
    ("profile", "capability_digest", None), ("pin", "package_version", "latest"),
    ("route", "catalog_digest", "b" * 64), ("grants", "execute", []),
    ("grants", "cancel", ["x", "x"]), ("grants", "reconcile", "inferred"),
    ("budget", "model_requests", True), ("budget", "model_requests", 17),
    ("budget", "wall_time_ms", 60001), ("budget", "child_operations", 1),
])
def test_bad_native_binding_rejected(tmp_path, field, key, value):
    raw = configured(tmp_path)
    raw["native_minion"][field][key] = value
    with pytest.raises(ContractError):
        parse_foreground_config_v2(raw)
