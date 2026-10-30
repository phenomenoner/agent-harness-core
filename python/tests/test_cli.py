from __future__ import annotations

import json
from hashlib import sha256
from pathlib import Path
from typing import cast

from profile_test_support import profile_registry_payload, standalone_config_text

import ahc.application.capability_composition as capability_composition
import ahc.application.lifecycle as lifecycle
from ahc.application.capability_composition import OptionalCapabilityComposition
from ahc.cli import main
from ahc.domain import AppConfig, FakeTurnResult
from ahc.domain.capability_values import (
    ApprovalMode,
    CapabilityDefinition,
    CapabilityGrant,
    EffectClass,
)
from ahc.domain.profile_values import ProfileResourceKind


def _write_config(tmp_path: Path) -> Path:
    path = tmp_path / "ahc.toml"
    path.write_text(
        """config_version = "ahc.standalone.config.v1"
state_dir = "state"

[[services]]
name = "demo"
command = ["{python}", "-c", "pass"]
cwd = "."
enabled = true
restart_policy = "never"
""",
        encoding="utf-8",
    )
    return path


def _write_v2_config(tmp_path: Path, registry: dict[str, object] | None = None) -> Path:
    path = tmp_path / "ahc-v2.toml"
    path.write_text(standalone_config_text(version=2), encoding="utf-8")
    payload = profile_registry_payload() if registry is None else registry
    (tmp_path / "profiles.json").write_text(json.dumps(payload), encoding="utf-8")
    return path


def _inventory(root: Path) -> tuple[tuple[str, int, str], ...]:
    rows: list[tuple[str, int, str]] = []
    for path in sorted(item for item in root.rglob("*") if item.is_file()):
        data = path.read_bytes()
        rows.append((path.relative_to(root).as_posix(), len(data), sha256(data).hexdigest()))
    return tuple(rows)


def _install_lifecycle_fakes(monkeypatch, calls: dict[str, object]) -> None:
    class Supervisor:
        def __init__(self, config):
            calls["run_config"] = config

        def run(self, max_runtime_seconds=None):
            calls["max_runtime_seconds"] = max_runtime_seconds
            return 0

    def inspect_status(config):
        calls["status_config"] = config
        return 0, {"state": "running", "path": str(config.state_dir)}

    def request_stop(config, wait_seconds):
        calls["stop_config"] = config
        calls["wait_seconds"] = wait_seconds
        return 0, {"state": "stopped"}

    monkeypatch.setattr(lifecycle, "Supervisor", Supervisor)
    monkeypatch.setattr(lifecycle, "inspect_status", inspect_status)
    monkeypatch.setattr(lifecycle, "request_stop", request_stop)


def _json_line(capsys) -> dict:
    output = capsys.readouterr().out.strip().splitlines()
    assert len(output) == 1
    return json.loads(output[0])


def test_doctor_emits_machine_readable_json(tmp_path: Path, capsys) -> None:
    code = main(["--config", str(_write_config(tmp_path)), "doctor"])

    payload = _json_line(capsys)
    assert code == 0
    assert payload["command"] == "doctor"
    assert payload["ok"] is True
    assert payload["config"] == {
        "schema_version": "ahc.config-doctor.v1",
        "config_version": "ahc.standalone.config.v1",
        "service_count": 1,
        "enabled_service_count": 1,
        "services": [{"name": "demo", "enabled": True, "restart_policy": "never"}],
    }
    assert payload["profiles"]["status"] == "not-configured"
    assert payload["capabilities"] == {"status": "not-configured", "agents": []}


def test_doctor_v2_reports_profile_authority_without_mutation(tmp_path: Path, capsys) -> None:
    config = _write_v2_config(tmp_path)
    before = _inventory(tmp_path)

    code = main(["--config", str(config), "doctor"])

    payload = _json_line(capsys)
    assert code == 0
    assert payload["profiles"]["status"] == "ready"
    assert payload["profiles"]["default_agent_id"] == "hexi"
    assert [profile["agent_id"] for profile in payload["profiles"]["profiles"]] == [
        "hexi",
        "lizi",
    ]
    assert payload["capabilities"] == {
        "status": "not-configured",
        "agents": [
            {
                "agent_id": "hexi",
                "configured_count": 0,
                "denied_count": 0,
                "approval_required_count": 0,
            },
            {
                "agent_id": "lizi",
                "configured_count": 0,
                "denied_count": 0,
                "approval_required_count": 0,
            },
        ],
    }
    assert _inventory(tmp_path) == before


def test_doctor_does_not_echo_service_command_or_cwd(tmp_path: Path, capsys) -> None:
    command_sentinel = "RAW-COMMAND-SENTINEL"
    cwd_sentinel = "RAW-CWD-SENTINEL"
    private_cwd = tmp_path / cwd_sentinel
    private_cwd.mkdir()
    config = _write_v2_config(tmp_path)
    text = config.read_text(encoding="utf-8")
    text = text.replace(
        'command = ["{python}", "-c", "pass"]',
        f'command = ["{{python}}", "-c", "{command_sentinel}"]',
    )
    text = text.replace('cwd = "."', f'cwd = "{cwd_sentinel}"')
    config.write_text(text, encoding="utf-8")

    code = main(["--config", str(config), "doctor"])

    output = json.dumps(_json_line(capsys), ensure_ascii=False)
    assert code == 0
    assert command_sentinel not in output
    assert cwd_sentinel not in output


def test_doctor_does_not_probe_service_cwd_or_state_dir(
    tmp_path: Path, capsys, monkeypatch
) -> None:
    state_path = (tmp_path / "UNPROBED-STATE").absolute()
    cwd_path = (tmp_path / "UNPROBED-CWD").absolute()
    config = _write_v2_config(tmp_path)
    text = config.read_text(encoding="utf-8")
    text = text.replace('state_dir = "state"', f'state_dir = "{state_path.name}"')
    text = text.replace('cwd = "."', f'cwd = "{cwd_path.name}"')
    config.write_text(text, encoding="utf-8")
    before = _inventory(tmp_path)
    forbidden = {state_path, cwd_path}
    original_resolve = Path.resolve
    original_exists = Path.exists
    original_is_dir = Path.is_dir

    def reject_resolve(path: Path, *args, **kwargs):
        if path.absolute() in forbidden:
            raise AssertionError("doctor resolved a lifecycle-only path")
        return original_resolve(path, *args, **kwargs)

    def reject_exists(path: Path):
        if path.absolute() in forbidden:
            raise AssertionError("doctor tested a lifecycle-only path")
        return original_exists(path)

    def reject_is_dir(path: Path):
        if path.absolute() in forbidden:
            raise AssertionError("doctor inspected a lifecycle-only path")
        return original_is_dir(path)

    monkeypatch.setattr(Path, "resolve", reject_resolve)
    monkeypatch.setattr(Path, "exists", reject_exists)
    monkeypatch.setattr(Path, "is_dir", reject_is_dir)

    code = main(["--config", str(config), "doctor"])

    payload = _json_line(capsys)
    assert code == 0
    assert payload["profiles"]["status"] == "ready"
    assert _inventory(tmp_path) == before


def test_doctor_rejects_secret_field_without_echoing_value(tmp_path: Path, capsys) -> None:
    sentinel = "RAW-SECRET-SENTINEL"
    registry = profile_registry_payload()
    registry["resources"][3]["value"] = sentinel
    config = _write_v2_config(tmp_path, registry)

    code = main(["--config", str(config), "doctor"])

    payload = _json_line(capsys)
    assert code == 2
    assert payload["error"]["kind"] == "config"
    assert sentinel not in json.dumps(payload)


def test_run_v2_carries_profile_registry_without_materializing_resources(
    tmp_path: Path, capsys, monkeypatch
) -> None:
    calls: dict[str, object] = {}
    _install_lifecycle_fakes(monkeypatch, calls)
    config = _write_v2_config(tmp_path)
    before = _inventory(tmp_path)

    assert main(["--config", str(config), "run", "--max-runtime", "0"]) == 0

    payload = _json_line(capsys)
    assert payload["ok"] is True
    run_config = cast(AppConfig, calls["run_config"])
    assert run_config.profile_registry is not None
    assert run_config.profile_registry.profile("lizi").workspace_ref == "lizi.workspace"
    assert _inventory(tmp_path) == before


def test_run_v2_composes_absent_capability_policy_before_supervisor(
    tmp_path: Path, capsys, monkeypatch
) -> None:
    calls: dict[str, object] = {}
    _install_lifecycle_fakes(monkeypatch, calls)
    original = capability_composition.compose_optional_capability_views

    def capture_optional_composition(**kwargs):
        result = original(**kwargs)
        calls["capability_composition"] = result
        return result

    monkeypatch.setattr(
        capability_composition,
        "compose_optional_capability_views",
        capture_optional_composition,
    )

    assert main(["--config", str(_write_v2_config(tmp_path)), "run", "--max-runtime", "0"]) == 0

    assert _json_line(capsys)["ok"] is True
    authority = cast(OptionalCapabilityComposition, calls["capability_composition"])
    assert authority.composition is None
    assert authority.snapshot.status.value == "not-configured"
    assert [item.agent_id for item in authority.snapshot.agents] == ["hexi", "lizi"]


def test_invalid_optional_capability_policy_keeps_doctor_and_run_available_without_views(
    tmp_path: Path, capsys, monkeypatch
) -> None:
    calls: dict[str, object] = {}
    _install_lifecycle_fakes(monkeypatch, calls)
    original = capability_composition.compose_optional_capability_views

    def inject_invalid_policy(**kwargs):
        result = original(
            profile_registry=kwargs["profile_registry"],
            definitions=[
                CapabilityDefinition(
                    "workspace.read",
                    ProfileResourceKind.WORKSPACE,
                    EffectClass.READ_ONLY,
                    ApprovalMode.NONE,
                )
            ],
            grants=[CapabilityGrant("hexi", "lizi.workspace", "workspace.read")],
        )
        invalid_compositions = cast(
            list[OptionalCapabilityComposition],
            calls.setdefault("invalid_capability_compositions", []),
        )
        invalid_compositions.append(result)
        return result

    monkeypatch.setattr(
        capability_composition,
        "compose_optional_capability_views",
        inject_invalid_policy,
    )
    config = _write_v2_config(tmp_path)
    before = _inventory(tmp_path)

    assert main(["--config", str(config), "doctor"]) == 0
    doctor = _json_line(capsys)
    assert doctor["capabilities"]["status"] == "invalid-policy"
    assert all(item["configured_count"] == 0 for item in doctor["capabilities"]["agents"])

    assert main(["--config", str(config), "run", "--max-runtime", "0"]) == 0
    assert _json_line(capsys)["ok"] is True
    invalid_compositions = cast(
        list[OptionalCapabilityComposition], calls["invalid_capability_compositions"]
    )
    assert len(invalid_compositions) == 2
    assert all(item.composition is None for item in invalid_compositions)
    assert _inventory(tmp_path) == before


def test_run_uses_foreground_supervisor_and_hidden_test_runtime(
    tmp_path: Path, capsys, monkeypatch
) -> None:
    calls: dict[str, object] = {}
    _install_lifecycle_fakes(monkeypatch, calls)

    code = main(["--config", str(_write_config(tmp_path)), "run", "--max-runtime", "0"])

    payload = _json_line(capsys)
    assert code == 0
    assert payload == {"command": "run", "exit_code": 0, "ok": True}
    assert calls["max_runtime_seconds"] == 0.0


def test_status_and_stop_forward_lifecycle_api(tmp_path: Path, capsys, monkeypatch) -> None:
    calls: dict[str, object] = {}
    _install_lifecycle_fakes(monkeypatch, calls)
    config_path = _write_config(tmp_path)

    assert main(["--config", str(config_path), "status"]) == 0
    assert _json_line(capsys)["report"]["state"] == "running"

    assert main(["--config", str(config_path), "stop", "--wait-seconds", "3.5"]) == 0
    assert _json_line(capsys)["report"] == {"state": "stopped"}
    assert calls["wait_seconds"] == 3.5


def test_cli_config_errors_are_json_and_nonzero(tmp_path: Path, capsys) -> None:
    sentinel = "RAW-CONFIG-PATH-SENTINEL"
    code = main(["--config", str(tmp_path / f"{sentinel}.toml"), "doctor"])

    payload = _json_line(capsys)
    assert code == 2
    assert payload["command"] == "doctor"
    assert payload["error"]["kind"] == "config"
    assert payload["ok"] is False
    assert sentinel not in json.dumps(payload)


def test_hidden_max_runtime_is_not_accepted_by_other_commands(tmp_path: Path, capsys) -> None:
    code = main(["--config", str(_write_config(tmp_path)), "doctor", "--max-runtime", "1"])

    payload = _json_line(capsys)
    assert code == 2
    assert payload["error"]["kind"] == "usage"
    assert payload["error"]["message"] == "invalid command usage"


def test_fake_turn_is_configuration_free_and_emits_exact_typed_result(capsys) -> None:
    code = main(
        [
            "fake-turn",
            "--virtual-session-id",
            "session-cli",
            "--agent-id",
            "agent-cli",
            "--route-id",
            "route-cli",
            "--turn-id",
            "turn-cli",
            "--input-text",
            "hello",
        ]
    )

    payload = _json_line(capsys)
    assert code == 0
    assert FakeTurnResult.from_dict(payload).to_dict() == payload
    assert payload["output_text"] == "FAKE: hello"


def test_fake_turn_rejects_config_without_loading_it(tmp_path: Path, capsys) -> None:
    code = main(
        [
            "--config",
            str(tmp_path / "missing.toml"),
            "fake-turn",
            "--input-text",
            "hello",
        ]
    )

    payload = _json_line(capsys)
    assert code == 2
    assert payload["command"] == "fake-turn"
    assert payload["error"]["kind"] == "usage"
    assert payload["error"]["message"] == "invalid command usage"


def test_fake_turn_reports_invalid_lineage_as_a_contract_error(capsys) -> None:
    code = main(
        [
            "fake-turn",
            "--generation",
            "2",
            "--input-text",
            "hello",
        ]
    )

    payload = _json_line(capsys)
    assert code == 2
    assert payload["command"] == "fake-turn"
    assert payload["error"]["kind"] == "contract"
    assert payload["error"]["message"] == "command contract violated"


def test_fake_turn_request_json_rejects_competing_builder_authority(capsys) -> None:
    request = {
        "schema_version": "ahc.fake-turn-request.v1",
        "session": {
            "schema_version": "ahc.virtual-session.v1",
            "ref": {
                "schema_version": "ahc.virtual-session-ref.v1",
                "virtual_session_id": "session-json",
                "generation": 1,
            },
            "agent_id": "agent-json",
            "route_id": "route-json",
            "predecessor": None,
        },
        "turn_id": "turn-json",
        "input_text": "hello",
    }

    code = main(
        [
            "fake-turn",
            "--request-json",
            json.dumps(request),
            "--generation",
            "2",
        ]
    )

    payload = _json_line(capsys)
    assert code == 2
    assert payload["error"]["kind"] == "usage"
    assert payload["error"]["message"] == "invalid command usage"
