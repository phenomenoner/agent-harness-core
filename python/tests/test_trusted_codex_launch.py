import ast
import base64
import json
import zlib
from dataclasses import replace

import pytest
from test_codex_effective_policy import Peer, policy, request

from ahc.adapters.codex_client import CodexProtocolClient
from ahc.adapters.trusted_codex_launch import trusted_codex_command
from ahc.domain.sessions import ContractError


def command(**changes):
    params = dict(
        bundle="/opt/codex",
        codex_home="/home/ahc/codex",
        cwd="/home/ahc/work",
        interpreter="/usr/bin/python3",
        approval_policy="on-request",
    )
    return trusted_codex_command(**(params | changes))


def payload_for(args):
    if len(args) > 5:
        encoded = ''.join(args[5:])
    else:
        encoded = next(node.value for node in ast.walk(ast.parse(args[4]))
                       if isinstance(node, ast.Constant) and isinstance(node.value, str)
                       and len(node.value) > 100)
    return json.loads(zlib.decompress(base64.b85decode(encoded)))


def test_launch_is_explicit_bounded_python_argument_and_never_shell():
    args = command()
    assert args[:4] == ("/usr/bin/python3", "-I", "-u", "-c")
    ast.parse(args[4])
    assert all(len(arg) <= 4096 and all(ord(char) >= 32 for char in arg) for arg in args)
    payload = payload_for(args)
    assert "os.execve" in payload["launcher"]
    assert "require_root_owned=False" in payload["launcher"]
    assert "auth.json" not in payload["launcher"] and "os.environ" not in payload["launcher"]
    assert payload["overrides"] == {}


def test_authenticated_launch_disables_goal_and_native_execution_routes():
    args = command(sealed_history=True, staged_authentication=True)
    overrides = payload_for(args)["overrides"]
    assert overrides["cli_auth_credentials_store"] == "file"
    assert overrides["features.skip_host_skill_discovery"] is True
    for name in ("goals", "shell_tool", "code_mode_host", "plugins", "hooks", "apps"):
        assert overrides["features." + name] is False
    assert overrides["features.code_mode"] == {
        "enabled": False, "direct_only_tool_namespaces": ["workspace"]}


@pytest.mark.parametrize("cwd", ["/work/staging", '/work/quoted"staging', "/work/.debug/staging"])
def test_authenticated_launch_trusts_only_the_configured_workspace(cwd):
    args = command(cwd=cwd, sealed_history=True, staged_authentication=True)
    overrides = payload_for(args)["overrides"]
    assert overrides["projects"] == {cwd: {"trust_level": "trusted"}}
    assert not any(key.startswith("projects.") for key in overrides)
    assert overrides["suppress_unstable_features_warning"] is True
    assert overrides["features.skip_host_skill_discovery"] is True
    assert all(overrides["features." + name] is False
               for name in ("shell_tool", "unified_exec", "plugins", "hooks", "apps", "goals"))
    assert all(len(arg) <= 4096 for arg in args)


def test_authenticated_launcher_project_override_is_valid_toml(monkeypatch):
    import os
    import tomllib
    from pathlib import Path

    cwd = '/work/.debug/quoted"staging'
    args = command(cwd=cwd, sealed_history=True, staged_authentication=True)
    payload = payload_for(args)
    payload["verifier"] = "def verify_linux_tree(*args, **kwargs): pass"
    launched = []
    with monkeypatch.context() as patch:
        patch.setattr(os, "getuid", lambda: 1000, raising=False)
        patch.setattr(Path, "is_dir", lambda _path: True)
        patch.setattr(os, "chdir", lambda _path: None)
        patch.setattr(os, "execve", lambda exe, argv, env: launched.append(argv))
        exec(compile(payload["launcher"], "trusted-launch-probe", "exec"),
             {"p": payload, "json": json})
    emitted = [value for value in launched[0] if value.startswith("projects=")]
    assert len(emitted) == 1
    assert tomllib.loads(emitted[0]) == {"projects": {cwd: {"trust_level": "trusted"}}}
    mode = [value for value in launched[0] if value.startswith("features.code_mode=")]
    assert len(mode) == 1
    assert tomllib.loads(mode[0]) == {"features": {"code_mode": {
        "enabled": False, "direct_only_tool_namespaces": ["workspace"]}}}
    assert "features.code_mode_host=false" in launched[0]


@pytest.mark.parametrize(
    "changes",
    [
        {"cwd": "relative"},
        {"cwd": "/a/../b"},
        {"bundle": "//remote/share"},
        {"codex_home": "/home/ahc\nsecret"},
        {"approval_policy": "smart"},
    ],
)
def test_bad_launch_inputs_reject_before_any_process(changes):
    with pytest.raises(ContractError):
        command(**changes)


def test_launch_identity_is_bound_before_vendor_bytes_are_sent():
    effective = policy()
    peer = Peer(effective)
    first = CodexProtocolClient(
        peer,
        cwd="workspace",
        model="fixture",
        clock=lambda: 0,
        effective_policy=effective,
        launch_configuration_digest="a" * 64,
    )
    second = CodexProtocolClient(
        Peer(effective),
        cwd="workspace",
        model="fixture",
        clock=lambda: 0,
        effective_policy=effective,
        launch_configuration_digest="b" * 64,
    )
    assert first.configuration_digest != second.configuration_digest
    altered = replace(request(first, effective), configuration_digest=second.configuration_digest)
    assert first.run_admitted(altered).disposition == "unsupported"
    assert peer.sent == []


def test_large_pin_reconstruction_preserves_every_member_and_exact_verifier():
    from importlib.resources import files
    resources = files('ahc.adapters')
    args = command(sealed_history=True,staged_authentication=True)
    assert len(args) > 5 and all(len(arg) <= 4096 for arg in args)
    payload = payload_for(args)
    assert payload['pin'] == json.loads(resources.joinpath('codex_linux_backend.json').read_text())
    assert payload['verifier'] == resources.joinpath('linux_codex_tree.py').read_text()
    # Run the real data-argument decoder without launching any process.
    import sys
    from unittest.mock import patch
    observed = []
    with patch.object(sys,'argv',['-c',*args[5:]]):
        exec(args[4],{'exec':lambda compiled: observed.append(compiled)})
    assert len(observed) == 1
