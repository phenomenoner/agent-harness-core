"""An explicit full-user grant reaches the backend, not only its workbench."""

import sys
from types import SimpleNamespace

import pytest
from test_linux_native import native_owner  # noqa: F401
from test_trusted_codex_launch import command, payload_for
from test_user_full_access import full_access_config

from ahc.cli.foreground_config_native import parse_foreground_config_native
from ahc.cli.foreground_learning import _require_protection
from ahc.cli.trusted_codex_backend import TrustedCodexBackendFactory
from ahc.domain.sessions import ContractError


PUBLIC_ENVIRONMENT = {
    "HOME": "/home/agent", "USER": "agent", "LOGNAME": "agent",
    "PATH": "/usr/local/bin:/usr/bin:/bin", "WSL_INTEROP": "/run/WSL/2_interop",
}


def test_full_user_grant_selects_real_home_and_unwrapped_backend(monkeypatch):
    import ahc.cli.foreground_backend_protection as protection
    import ahc.cli.foreground_user_environment as environment
    import ahc.cli.trusted_codex_backend as backend

    value = full_access_config()
    value["policy"]["approval_mode"] = "yolo"
    value["backend"]["authentication"] = "staged-codex-file"
    config = parse_foreground_config_native(value)
    observed = []
    monkeypatch.setattr(backend, "trusted_codex_command",
                        lambda **options: observed.append(options) or ("/usr/bin/true",))
    monkeypatch.setattr(environment, "native_workspace_environment",
                        lambda document: PUBLIC_ENVIRONMENT.copy())
    factory = TrustedCodexBackendFactory(None, config)
    assert observed[0]["codex_home"] == value["backend"]["codex_home"]
    assert observed[0]["sealed_history"] is False
    assert observed[0]["staged_authentication"] is False
    assert observed[0]["approval_policy"] == "never"
    assert observed[0]["user_environment"] == PUBLIC_ENVIRONMENT
    assert observed[0]["sqlite_home"] == value["backend"]["codex_home"] + "/state"
    monkeypatch.setattr(protection, "native_backend_protection",
                        lambda *a, **k: pytest.fail("full-user grant acquired a hidden wrapper"))
    runner = factory.new_runner()
    assert runner._boundary is None and runner._resource_limits is None
    captured = []
    monkeypatch.setattr(backend, "CodexProtocolClient",
                        lambda *a, **k: captured.append(k) or SimpleNamespace())
    monkeypatch.setattr(backend, "compose_foreground_media", lambda *a: None)
    factory._client(None, None, lambda: False)
    assert captured[0]["isolated_provider_auth"] is False


def test_full_user_launch_keeps_public_environment_and_native_tools(monkeypatch):
    import os
    import tomllib
    from pathlib import Path

    payload = payload_for(command(approval_policy="never", user_environment=PUBLIC_ENVIRONMENT,
                                  sqlite_home="/home/ahc/codex/state"))
    assert payload["home"] == "/home/ahc/codex"
    assert payload["overrides"]["projects"] == {"/home/ahc/work": {"trust_level": "trusted"}}
    assert payload["overrides"]["sandbox_mode"] == "danger-full-access"
    assert payload["overrides"]["sqlite_home"] == "/home/ahc/codex/state"
    assert not any(key.startswith("features.") for key in payload["overrides"])
    payload["verifier"] = "def verify_linux_tree(*args, **kwargs): pass"
    launched = []
    with monkeypatch.context() as patch:
        patch.setattr(os, "getuid", lambda: 1000, raising=False)
        patch.setattr(Path, "is_dir", lambda path: True)
        patch.setattr(os, "chdir", lambda path: None)
        patch.setattr(os, "execve", lambda exe, argv, env: launched.append((argv, env)))
        exec(compile(payload["launcher"], "full-user-launch", "exec"),
             {"p": payload, "json": __import__("json")})
    arguments, actual = launched[0]
    assert actual == PUBLIC_ENVIRONMENT | {"LANG": "C.UTF-8", "CODEX_HOME": "/home/ahc/codex"}
    assert "approval_policy=\"never\"" in arguments
    project = next(value for value in arguments if value.startswith("projects="))
    assert tomllib.loads(project)["projects"]["/home/ahc/work"]["trust_level"] == "trusted"


@pytest.mark.parametrize("extra", ["sealed", "staged", "secret-environment"])
def test_full_user_launch_does_not_mix_in_an_isolated_authentication_contract(extra):
    options = {"user_environment": PUBLIC_ENVIRONMENT}
    if extra == "sealed":
        options["sealed_history"] = True
    elif extra == "staged":
        options.update(sealed_history=True, staged_authentication=True)
    else:
        options["user_environment"] = PUBLIC_ENVIRONMENT | {"TELEGRAM_BOT_TOKEN": "not-copied"}
    with pytest.raises(ContractError):
        command(**options)


def test_explicit_user_instruction_trials_need_no_backend_namespace():
    value = full_access_config()
    value["backend"].pop("protection")
    assert _require_protection(value) is True


@pytest.mark.skipif(sys.platform != "linux", reason="native process custody")
def test_full_user_backend_runs_external_venv_and_next_process(native_owner, tmp_path):
    work = tmp_path / "work"
    work.mkdir()
    external = tmp_path / "outside-workspace"
    external.write_text("user-granted-access")
    binary = work / "venv" / "bin"
    binary.mkdir(parents=True)
    (binary / "python").symlink_to(sys.executable)
    value = full_access_config()
    value["policy"]["approval_mode"] = "yolo"
    value["workspace"].update(cwd=str(work), interpreter=sys.executable)
    config = parse_foreground_config_native(value)
    factory = TrustedCodexBackendFactory(None, config)
    for generation in (1, 2):
        runner = factory.new_runner()
        code = (
            "from pathlib import Path; import subprocess; "
            f"assert Path({str(external)!r}).read_text()=='user-granted-access'; "
            "assert subprocess.check_output(['venv/bin/python','-I','-B','-c',"
            "'print(6*7)'],text=True).strip()=='42'; "
            f"Path('result-{generation}').write_text('completed')"
        )
        peer = ("/usr/bin/python3", "-I", "-B", "-c", code)
        attempt = runner.prepare_creation(peer, lifetime_seconds=15)
        try:
            runner.create_attempt(attempt, peer, lifetime_seconds=15)
            assert runner._owned.process.wait(timeout=10) == 0
        finally:
            runner.close()
        assert runner.observe(attempt.identity).state == "absent"
        assert (work / f"result-{generation}").read_text() == "completed"
    assert external.read_text() == "user-granted-access"
