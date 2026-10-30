import json
from argparse import Namespace

import pytest
from profile_test_support import profile_registry_payload
from test_foreground_trusted import config_file

from ahc.cli import foreground_selection as selection
from ahc.cli import main
from ahc.cli.foreground import compose_foreground_document
from ahc.infrastructure.config import load_config


def configured(tmp_path):
    _, request, raw, path = config_file(tmp_path)
    name = request.session.agent_id
    (tmp_path / "profiles.json").write_text(
        json.dumps(profile_registry_payload((name, "second"))), encoding="utf-8")
    app = tmp_path / "app.toml"
    app.write_text('config_version="ahc.standalone.config.v2"\nstate_dir="state"\n'
                   'profile_registry="profiles.json"\nservices=[]\n'
                   f'[foreground_configs]\n{name}="{path.name}"\n', encoding="utf-8")
    return app, path, raw, name


@pytest.mark.parametrize("command", ["doctor", "run"])
@pytest.mark.parametrize("choice", ["declared", "unmapped", "conflict", "override"])
def test_agent_selection_reaches_cli_and_rejects_bypass(tmp_path, capsys, command, choice):
    app, path, raw, name = configured(tmp_path)
    args = [command, "--config", str(app)]
    if choice == "override":
        raw["backend"]["model"] = "different"
        override = tmp_path / "override.json"
        override.write_text(json.dumps(raw), encoding="utf-8")
        args += ["--runtime-config", str(override)]
    else:
        args += ["--agent", "second" if choice == "unmapped" else name]
        if choice == "conflict":
            args += ["--runtime-config", str(path)]
    if command == "run":
        args += ["--max-runtime", "0.02"]
    result = main(args)
    assert result == (0 if choice == "declared" else 2)
    assert json.loads(capsys.readouterr().out)["ok"] is (choice == "declared")


@pytest.mark.parametrize("choice", ["agent", "path"])
def test_retained_selection_is_used_after_file_changes(tmp_path, monkeypatch, choice):
    app, path, raw, name = configured(tmp_path)
    config = load_config(app)
    read = selection.read_foreground_config
    calls = []

    def replace_after_read(path):
        calls.append(path)
        retained = read(path)
        changed = dict(raw, backend={**raw["backend"], "enabled": True})
        path.write_text(json.dumps(changed), encoding="utf-8")
        return retained

    monkeypatch.setattr(selection, "read_foreground_config", replace_after_read)
    arguments = Namespace(agent=name if choice == "agent" else None,
                          runtime_config=path if choice == "path" else None)
    document = selection.select_foreground(arguments, config)
    runtime = compose_foreground_document(document, profile_registry=config.profile_registry)
    try:
        runtime.start()
        assert runtime.backend_name == "unavailable"
        assert runtime.step() is None
    finally:
        runtime.close()
    assert calls == [path]


def test_wrong_agent_in_declared_file_rejects(tmp_path, capsys):
    app, path, raw, name = configured(tmp_path)
    raw["session"]["agent_id"] = "second"
    path.write_text(json.dumps(raw), encoding="utf-8")
    assert main(["doctor", "--config", str(app), "--agent", name]) == 2
    assert json.loads(capsys.readouterr().out)["ok"] is False


@pytest.mark.parametrize("declaration", [
    '[foreground_configs]\nunknown="trusted.json"\n',
    '[foreground_configs]\nagent="../trusted.json"\n',
    '[foreground_configs]\nagent="C:/trusted.json"\n',
    '[foreground_configs]\nagent=42\n',
    '[foreground_configs]\n',
])
def test_invalid_mapping_rejects_during_app_load(tmp_path, declaration):
    from ahc.domain.sessions import ContractError

    app, _, _, _ = configured(tmp_path)
    text = app.read_text().split("[foreground_configs]")[0] + declaration
    app.write_text(text, encoding="utf-8")
    with pytest.raises(ContractError):
        load_config(app)
