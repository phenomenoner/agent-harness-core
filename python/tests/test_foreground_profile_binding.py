"""App profile authority must also govern the selected foreground session."""

import json

import pytest
from profile_test_support import profile_registry_payload
from test_foreground_trusted import config_file

from ahc.cli import main
from ahc.infrastructure.runtime_repository import RuntimeRepository


@pytest.mark.parametrize("command", ["doctor", "run"])
@pytest.mark.parametrize("member", [True, False])
def test_foreground_agent_must_belong_to_app_registry(
        tmp_path, monkeypatch, capsys, command, member):
    _, request, _, path = config_file(tmp_path)
    names = (request.session.agent_id, "second") if member else ("other", "second")
    (tmp_path / "profiles.json").write_text(json.dumps(profile_registry_payload(names)),
                                             encoding="utf-8")
    app = tmp_path / "app.toml"
    app.write_text('config_version="ahc.standalone.config.v2"\nstate_dir="state"\n'
                   'profile_registry="profiles.json"\nservices=[]\n', encoding="utf-8")
    if not member or command == "doctor":
        def forbidden(*args, **kwargs):
            pytest.fail("unadmitted or diagnostic path opened the runtime")
        monkeypatch.setattr(RuntimeRepository, "__init__", forbidden)
    args = [command, "--config", str(app), "--runtime-config", str(path)]
    if command == "run":
        args.extend(["--max-runtime", "0.02"])
    code = main(args)
    output = json.loads(capsys.readouterr().out)
    assert code == (0 if member else 2)
    assert output["ok"] is member


def test_profile_reference_changes_actual_backend_request_binding(tmp_path):
    from ahc.cli.foreground_config_v2 import parse_foreground_config_v2
    from ahc.cli.foreground_profiles import bind_profile_configuration
    from ahc.cli.trusted_codex_backend import TrustedCodexBackendFactory
    from ahc.domain.profile_registry import AgentProfileRegistry

    _, request, raw, _ = config_file(tmp_path)
    config = parse_foreground_config_v2(raw)
    payload = profile_registry_payload((request.session.agent_id,))
    first = AgentProfileRegistry.from_dict(payload)
    previous = payload["profiles"][0]["backend_profile_ref"]
    payload["profiles"][0]["backend_profile_ref"] = "replacement-backend"
    for resource in payload["resources"]:
        if resource["resource_id"] == previous:
            resource["resource_id"] = "replacement-backend"
    second = AgentProfileRegistry.from_dict(payload)
    one = TrustedCodexBackendFactory(None, bind_profile_configuration(config, first)).binding(None)
    two = TrustedCodexBackendFactory(None, bind_profile_configuration(config, second)).binding(None)
    assert one[0] == two[0]  # Same executable, different authority/configuration identity.
    assert one[1] != two[1]
    assert bind_profile_configuration(config, None) is config
