from __future__ import annotations

from pathlib import Path

import pytest

from ahc.domain import AppConfig, RestartPolicy
from ahc.infrastructure.config import ConfigError, load_config, load_config_document


def _config_text(*, cwd: str = ".", enabled: bool = True) -> str:
    return f'''config_version = "ahc.standalone.config.v1"
state_dir = "state"

[[services]]
name = "demo"
command = ["{{python}}", "-c", "pass"]
cwd = "{cwd}"
enabled = {str(enabled).lower()}
restart_policy = "on-failure"
max_restarts = 2
restart_delay_seconds = 0.25
stop_timeout_seconds = 2.0
'''


def _write_config(tmp_path: Path, text: str | None = None) -> Path:
    path = tmp_path / "ahc.toml"
    path.write_text(text or _config_text(), encoding="utf-8")
    return path


def test_load_config_document_resolves_paths_and_preserves_python_placeholder(
    tmp_path: Path,
) -> None:
    document = load_config_document(_write_config(tmp_path))

    service = document.services[0]
    assert document.config_version == "ahc.standalone.config.v1"
    assert document.state_dir == (tmp_path / "state").resolve()
    assert service.cwd == tmp_path.resolve()
    assert service.command[0] == "{python}"
    assert service.restart_policy == "on-failure"
    assert service.max_restarts == 2


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("max_restarts", "-1"),
        ("max_restarts", "101"),
        ("restart_delay_seconds", "-0.1"),
        ("stop_timeout_seconds", "0.0"),
        ("stop_timeout_seconds", "3601.0"),
    ],
)
def test_rejects_invalid_numeric_bounds(tmp_path: Path, field: str, value: str) -> None:
    defaults = {
        "max_restarts": "2",
        "restart_delay_seconds": "0.25",
        "stop_timeout_seconds": "2.0",
    }
    text = _config_text().replace(f"{field} = {defaults[field]}", f"{field} = {value}")

    with pytest.raises(ConfigError, match=field):
        load_config_document(_write_config(tmp_path, text))


def test_rejects_unknown_root_field(tmp_path: Path) -> None:
    with pytest.raises(ConfigError, match="unknown field"):
        load_config_document(_write_config(tmp_path, _config_text() + "\nnot_allowed = true\n"))


def test_rejects_unknown_service_field(tmp_path: Path) -> None:
    with pytest.raises(ConfigError, match="unknown field"):
        load_config_document(
            _write_config(tmp_path, _config_text() + "\nextra_service_field = true\n")
        )


def test_rejects_duplicate_service_names(tmp_path: Path) -> None:
    text = (
        _config_text()
        + """
[[services]]
name = "demo"
command = ["{python}", "-c", "pass"]
cwd = "."
"""
    )
    with pytest.raises(ConfigError, match="duplicates service"):
        load_config_document(_write_config(tmp_path, text))


def test_rejects_invalid_restart_policy(tmp_path: Path) -> None:
    with pytest.raises(ConfigError, match="restart_policy"):
        load_config_document(
            _write_config(tmp_path, _config_text().replace('"on-failure"', '"sometimes"'))
        )


def test_accepts_when_no_optional_service_is_enabled(tmp_path: Path) -> None:
    document = load_config_document(_write_config(tmp_path, _config_text(enabled=False)))
    assert not any(service.enabled for service in document.services)


def test_rejects_nonexistent_cwd(tmp_path: Path) -> None:
    with pytest.raises(ConfigError, match="cwd"):
        load_config_document(_write_config(tmp_path, _config_text(cwd="does-not-exist")))


def test_toml_duplicate_keys_are_rejected(tmp_path: Path) -> None:
    text = _config_text().replace(
        'state_dir = "state"', 'state_dir = "state"\nstate_dir = "other-state"'
    )
    with pytest.raises(ConfigError, match="invalid TOML"):
        load_config_document(_write_config(tmp_path, text))


def test_load_config_constructs_exact_domain_values(tmp_path: Path) -> None:
    config = load_config(_write_config(tmp_path))

    assert isinstance(config, AppConfig)
    assert config.services[0].restart_policy is RestartPolicy.ON_FAILURE
    assert config.services[0].command[0] == "{python}"
    assert config.services[0].restart_delay_seconds == 0.25


def test_disabled_services_are_validated_but_not_spawned(tmp_path: Path) -> None:
    text = (
        _config_text()
        + """
[[services]]
name = "disabled"
command = ["{python}", "-c", "raise SystemExit(99)"]
cwd = "."
enabled = false
"""
    )
    config = load_config(_write_config(tmp_path, text))

    assert [service.name for service in config.services] == ["demo"]
