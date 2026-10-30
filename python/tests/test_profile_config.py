from __future__ import annotations

import json
from hashlib import sha256
from pathlib import Path
from typing import cast

import pytest
from profile_test_support import profile_registry_payload, standalone_config_text

from ahc.application.profile_doctor import inspect_profile_authority
from ahc.domain import AppConfig
from ahc.infrastructure.config import (
    ConfigError,
    ProfileRegistryLoadError,
    load_config,
    load_config_document,
    load_profile_registry,
)


def _write_config_root(tmp_path: Path, *, version: int, registry: object | None = None) -> Path:
    config = tmp_path / "ahc.toml"
    config.write_text(standalone_config_text(version=version), encoding="utf-8")
    if registry is not None:
        (tmp_path / "profiles.json").write_text(
            json.dumps(registry, ensure_ascii=False, indent=2), encoding="utf-8"
        )
    return config


def _inventory(root: Path) -> tuple[tuple[str, str, int, str | None], ...]:
    rows: list[tuple[str, str, int, str | None]] = []
    for path in sorted(root.rglob("*")):
        relative = path.relative_to(root).as_posix()
        if path.is_file():
            data = path.read_bytes()
            rows.append((relative, "file", len(data), sha256(data).hexdigest()))
        else:
            rows.append((relative, "dir", 0, None))
    return tuple(rows)


def test_v1_does_not_discover_adjacent_agent_observations(tmp_path: Path) -> None:
    config = _write_config_root(tmp_path, version=1, registry=profile_registry_payload())
    (tmp_path / "agents" / "lizi").mkdir(parents=True)
    (tmp_path / "agents" / "lizi" / "profile.json").write_text("{}", encoding="utf-8")

    document = load_config_document(config)

    assert document.profile_registry is None
    assert document.profile_registry_path is None
    assert inspect_profile_authority(None)["status"] == "not-configured"


def test_app_config_preserves_existing_positional_field_order(tmp_path: Path) -> None:
    config = AppConfig(tmp_path, (), 0.25, 2.0)

    assert config.poll_interval_seconds == 0.25
    assert config.child_stop_timeout_seconds == 2.0
    assert config.profile_registry is None


def test_v2_loads_only_explicit_registry_and_maps_it_to_app_config(tmp_path: Path) -> None:
    config = _write_config_root(tmp_path, version=2, registry=profile_registry_payload())

    document = load_config_document(config)
    app_config = load_config(config)

    assert document.profile_registry_path == (tmp_path / "profiles.json").resolve()
    assert document.profile_registry is not None
    assert document.profile_registry.profile("lizi").credential_ref == "lizi.credential"
    assert app_config.profile_registry is document.profile_registry or (
        app_config.profile_registry is not None
        and app_config.profile_registry.to_dict() == document.profile_registry.to_dict()
    )
    assert [service.name for service in app_config.services] == ["demo"]


@pytest.mark.parametrize("case", ["missing", "malformed", "unknown-root", "missing-reference"])
def test_v2_invalid_registry_rejects_without_fallback(tmp_path: Path, case: str) -> None:
    config = _write_config_root(tmp_path, version=2)
    registry_path = tmp_path / "profiles.json"
    if case == "malformed":
        registry_path.write_text("{", encoding="utf-8")
    elif case == "unknown-root":
        payload = profile_registry_payload()
        payload["legacy"] = True
        registry_path.write_text(json.dumps(payload), encoding="utf-8")
    elif case == "missing-reference":
        payload = profile_registry_payload()
        payload["profiles"][0]["workspace_ref"] = "not-known"
        registry_path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(ConfigError):
        load_config_document(config)


@pytest.mark.parametrize("scope", ["registry", "profile", "resource"])
def test_profile_registry_raw_json_rejects_duplicate_members(tmp_path: Path, scope: str) -> None:
    text = json.dumps(profile_registry_payload(), ensure_ascii=False, indent=2)
    if scope == "registry":
        target = '  "default_agent_id": "hexi",'
        replacement = target + '\n  "default_agent_id": "lizi",'
    elif scope == "profile":
        target = '      "agent_id": "hexi",'
        replacement = target + '\n      "agent_id": "lizi",'
    else:
        target = '      "owner_agent_id": "hexi"'
        replacement = target + ',\n      "owner_agent_id": "lizi"'
    registry_path = tmp_path / "profiles.json"
    registry_path.write_text(text.replace(target, replacement, 1), encoding="utf-8")

    with pytest.raises(ProfileRegistryLoadError, match="duplicate JSON object member"):
        load_profile_registry(registry_path)


@pytest.mark.parametrize(
    "registry_path",
    ["/other/root/profiles.json", "C:\\other\\profiles.json", "\\\\server\\share\\profiles.json"],
)
def test_v2_rejects_posix_windows_and_unc_absolute_registry_paths(
    tmp_path: Path, registry_path: str
) -> None:
    text = standalone_config_text(version=2).replace(
        'profile_registry = "profiles.json"', f"profile_registry = '{registry_path}'"
    )
    config = tmp_path / "ahc.toml"
    config.write_text(text, encoding="utf-8")

    with pytest.raises(ConfigError, match="must be a relative path"):
        load_config_document(config)


def test_registry_load_errors_do_not_echo_configured_path(tmp_path: Path) -> None:
    sentinel = "RAW-PATH-SENTINEL"
    text = standalone_config_text(version=2).replace("profiles.json", f"{sentinel}-missing.json")
    config = tmp_path / "ahc.toml"
    config.write_text(text, encoding="utf-8")

    with pytest.raises(ConfigError) as raised:
        load_config_document(config)

    assert sentinel not in str(raised.value)


def test_v2_requires_explicit_registry_field_and_v1_rejects_it(tmp_path: Path) -> None:
    missing = standalone_config_text(version=2).replace('profile_registry = "profiles.json"\n', "")
    config = tmp_path / "ahc.toml"
    config.write_text(missing, encoding="utf-8")
    with pytest.raises(ConfigError, match="profile_registry"):
        load_config_document(config)

    config.write_text(
        standalone_config_text(version=1).replace(
            'state_dir = "state"', 'state_dir = "state"\nprofile_registry = "profiles.json"'
        ),
        encoding="utf-8",
    )
    with pytest.raises(ConfigError, match="unknown field"):
        load_config_document(config)


def test_profile_doctor_projection_is_sanitized_and_read_only(tmp_path: Path) -> None:
    config = _write_config_root(tmp_path, version=2, registry=profile_registry_payload())
    before = _inventory(tmp_path)

    document = load_config_document(config)
    report = inspect_profile_authority(document.profile_registry)

    assert report["status"] == "ready"
    assert report["default_agent_id"] == "hexi"
    profiles = cast(list[dict[str, object]], report["profiles"])
    resources = cast(list[dict[str, object]], report["resources"])
    assert [profile["agent_id"] for profile in profiles] == ["hexi", "lizi"]
    assert not any("value" in resource for resource in resources)
    assert _inventory(tmp_path) == before
