"""Focused R1 lifecycle regressions for the standalone Python composition."""

from __future__ import annotations

from pathlib import Path

import pytest

from ahc.application.supervisor import Supervisor
from ahc.domain import AppConfig, ProcessIdentity
from ahc.infrastructure.config import load_config


def _write_config(tmp_path: Path, text: str) -> Path:
    path = tmp_path / "ahc.toml"
    path.write_text(text, encoding="utf-8")
    return path


@pytest.mark.parametrize("empty", [False, True])
def test_config_accepts_zero_enabled_optional_services(tmp_path: Path, empty: bool) -> None:
    config = load_config(
        _write_config(
            tmp_path,
            '''config_version = "ahc.standalone.config.v1"
state_dir = "state"
services = []
''' if empty else '''config_version = "ahc.standalone.config.v1"
state_dir = "state"

[[services]]
name = "optional-worker"
command = ["{python}", "-c", "pass"]
cwd = "."
enabled = false
''',
        )
    )

    assert config.services == ()
    assert Supervisor(config).run(max_runtime_seconds=0.0) == 0


def test_stop_acknowledgement_accepts_requested_generation(monkeypatch, tmp_path: Path) -> None:
    from ahc.application import operator

    requested = ProcessIdentity(pid=101, start_time=10.0)
    observations = iter(
        (
            (operator.OK, {"status": "running", "supervisor": requested.to_dict()}),
            (operator.OK, {"status": "stopped", "supervisor": requested.to_dict()}),
        )
    )

    def read_status(_config: AppConfig) -> tuple[int, dict[str, object]]:
        return next(observations)

    def write_stop(self, target: ProcessIdentity, **_kwargs: object) -> dict[str, object]:
        return {"type": "stop", "target": target.to_dict()}

    monkeypatch.setattr(operator, "_read_status", read_status)
    monkeypatch.setattr(operator.ControlStore, "write_stop", write_stop)

    code, observation = operator.request_stop(AppConfig(state_dir=tmp_path), wait_seconds=1.0)

    assert code == operator.OK
    assert observation["status"] == "stopped"


@pytest.mark.parametrize("replacement_pid", [101, 202])
def test_stop_acknowledgement_rejects_replacement_generation(
    monkeypatch, tmp_path: Path, replacement_pid: int,
) -> None:
    from ahc.application import operator

    requested = ProcessIdentity(pid=101, start_time=10.0)
    replacement = ProcessIdentity(pid=replacement_pid, start_time=20.0)
    observations = iter(
        (
            (operator.OK, {"status": "running", "supervisor": requested.to_dict()}),
            (operator.OK, {"status": "stopped", "supervisor": replacement.to_dict()}),
        )
    )
    written: list[ProcessIdentity] = []

    def read_status(_config: AppConfig) -> tuple[int, dict[str, object]]:
        return next(observations)

    def write_stop(self, target: ProcessIdentity, **_kwargs: object) -> dict[str, object]:
        written.append(target)
        return {"type": "stop", "target": target.to_dict()}

    monkeypatch.setattr(operator, "_read_status", read_status)
    monkeypatch.setattr(operator.ControlStore, "write_stop", write_stop)

    code, observation = operator.request_stop(AppConfig(state_dir=tmp_path), wait_seconds=1.0)

    assert written == [requested]
    assert code == operator.STOP_SUPERSEDED
    assert observation["status"] == "stop-superseded"
    assert observation["target"] == requested.to_dict()
    assert observation["observed_status"] == "stopped"
    assert observation["observed_supervisor"] == replacement.to_dict()
