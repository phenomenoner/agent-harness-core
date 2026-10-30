"""Explicit authentication admission, with no credentials or provider requests."""

from dataclasses import replace
from types import SimpleNamespace

import pytest
from test_codex_effective_policy import Peer, policy, request
from test_foreground_backend_protection import protected_config

from ahc.adapters.codex_client import CodexProtocolClient
from ahc.adapters.codex_linux_bundle import LINUX_CODEX_PIN
from ahc.adapters.trusted_codex_launch import trusted_codex_command
from ahc.cli.foreground_backend_protection import native_backend_protection
from ahc.cli.foreground_config_native import parse_foreground_config_native
from ahc.cli.trusted_codex_backend import TrustedCodexBackendFactory
from ahc.domain.sessions import ContractError


def authenticated_config():
    value = protected_config()
    value["backend"]["authentication"] = "staged-codex-file"
    value["workspace"]["protection"]["protected_roots"].append(value["backend"]["codex_home"])
    return value


def test_authentication_is_explicit_bound_and_preview_does_not_open_paths():
    value = authenticated_config()
    config = parse_foreground_config_native(value)
    assert config.to_dict() == value
    assert config.configuration_digest != parse_foreground_config_native(
        protected_config()).configuration_digest
    assert "authentication" not in str(config.preview())
    factory = TrustedCodexBackendFactory(None, config)
    assert len(factory._command[-1]) <= 4096
    assert factory._client(None, None, lambda: False).isolated_provider_auth


@pytest.mark.parametrize("case", ["unprotected", "unknown", "bool", "visible-to-workspace"])
def test_authentication_cannot_bypass_protection_or_import_arbitrary_auth(case):
    value = authenticated_config()
    if case == "unprotected":
        value["backend"].pop("protection")
    elif case == "unknown":
        value["backend"]["authentication"] = {"source": "/foreign/auth.json"}
    elif case == "bool":
        value["backend"]["authentication"] = True
    else:
        value["workspace"]["protection"]["protected_roots"].remove(
            value["backend"]["codex_home"])
    with pytest.raises(ContractError, match="authentication"):
        parse_foreground_config_native(value)


def test_helper_profile_never_receives_the_model_credential_file(monkeypatch):
    from ahc.infrastructure.process import linux_backend_boundary

    observed = []
    monkeypatch.setattr(linux_backend_boundary, "LinuxBackendBoundary",
                        lambda **kw: observed.append(kw) or SimpleNamespace(**kw))
    config = parse_foreground_config_native(authenticated_config())
    native_backend_protection(config)
    native_backend_protection(config, provider_auth=True)
    assert observed[0]["staged_authentication"] is False
    assert observed[1]["staged_authentication"] is True
    assert observed[0]["state_root"] == observed[1]["state_root"]


def test_actual_factory_requests_the_authenticated_model_profile(monkeypatch):
    from ahc.adapters import native_process_runner
    from ahc.cli import foreground_backend_protection

    observed = []
    marker = {"boundary": object(), "resource_limits": object()}
    monkeypatch.setattr(foreground_backend_protection, "native_backend_protection",
                        lambda config, **kw: observed.append(kw) or marker)
    monkeypatch.setattr(native_process_runner, "NativeProcessRunner",
                        lambda **kw: SimpleNamespace(**kw))
    config = parse_foreground_config_native(authenticated_config())
    runner = TrustedCodexBackendFactory(None, config).new_runner()
    assert observed == [{"provider_auth": True}]
    assert runner.environment == {} and runner.boundary is marker["boundary"]


@pytest.mark.parametrize("value", [1, "file", None])
def test_launch_rejects_ambiguous_authentication(value):
    with pytest.raises(ContractError, match="authentication"):
        trusted_codex_command(bundle="/bundle", codex_home="/home", cwd="/work",
                              interpreter="/usr/bin/python3", approval_policy="never",
                              sealed_history=True, staged_authentication=value)


def test_authenticated_launch_with_representative_native_paths_stays_bounded():
    # Preserve representative native argument lengths without operator paths.
    bundle = "/opt/fixture/codex/releases/" + "b" * 46
    workspace = "/opt/fixture/workspaces/" + "w" * 104
    command = trusted_codex_command(
        bundle=bundle, codex_home="/tmp/fixture-backend-home", cwd=workspace,
        interpreter="/usr/bin/python3", approval_policy="never",
        sealed_history=True, staged_authentication=True)
    assert len(command[-1]) <= 4096


def test_authenticated_client_binds_and_sends_empty_native_environments():
    effective = policy()
    peer = Peer(effective)
    peer.messages[0]["result"].update(platformOs="linux", platformFamily="unix")
    client = CodexProtocolClient(peer, cwd="workspace", model="fixture", clock=lambda: 0,
                                effective_policy=effective, target=LINUX_CODEX_PIN["target"],
                                isolated_provider_auth=True)
    other = CodexProtocolClient(None, cwd="workspace", model="fixture", clock=lambda: 0,
                               effective_policy=effective, target=LINUX_CODEX_PIN["target"])
    assert client.configuration_digest != other.configuration_digest
    result = client.run_admitted(replace(request(client, effective),
                                        artifact_digest=client.executable_sha256))
    assert result.disposition == "completed"
    messages = {frame.get("method"): frame for frame in peer.sent}
    assert messages["thread/start"]["params"]["environments"] == []
    assert messages["turn/start"]["params"]["environments"] == []
    assert not any("auth" in (frame.get("method") or "") for frame in peer.sent)


def test_authentication_mode_does_not_silently_enter_windows_client():
    with pytest.raises(ContractError, match="Linux"):
        CodexProtocolClient(None, cwd="workspace", model="fixture", isolated_provider_auth=True)
