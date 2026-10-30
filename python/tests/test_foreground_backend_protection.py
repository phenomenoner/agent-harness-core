"""Portable admission tests; parsing and standby do not inspect native paths."""

import os
from copy import deepcopy

import pytest
from test_foreground_config_v2 import valid_config
from test_foreground_workspace_protection import native_config, protection

from ahc.cli.foreground_config_native import parse_foreground_config_native
from ahc.cli.foreground_config_v2 import parse_foreground_config_v2
from ahc.cli.trusted_codex_backend import TrustedCodexBackendFactory
from ahc.domain.sessions import ContractError
from ahc.infrastructure.process.linux_backend_boundary import _installation_file


def test_vendor_identity_preparation_reuses_only_one_bounded_regular_file(tmp_path):
    path = _installation_file(tmp_path)
    assert path == tmp_path / "installation_id" and path.read_bytes() == b""
    path.write_text("7ab373e8-36f6-4f56-a091-c53b7cfbe0e1")
    before = path.read_bytes()
    assert _installation_file(tmp_path) == path and path.read_bytes() == before
    assert not (tmp_path / "config.toml").exists()


@pytest.mark.parametrize("case", ["directory", "hardlink", "oversized"])
def test_vendor_identity_refuses_aliases_and_excess_data(tmp_path, case):
    path = tmp_path / "installation_id"
    if case == "directory":
        path.mkdir()
    elif case == "hardlink":
        original = tmp_path / "original"
        original.write_text("protected-original")
        os.link(original, path)
    else:
        path.write_bytes(b"x" * 65)
    with pytest.raises(ContractError, match="installation identity"):
        _installation_file(tmp_path)
    if case == "hardlink":
        assert original.read_text() == "protected-original"


def protected_config():
    value = native_config()
    value["workspace"]["protection"] = protection()
    value["backend"]["protection"] = protection() | {
        "network_mode": "shared",
        "home_mode": "sealed-history",
        "home_bytes": 32 * 1024**2,
        "temporary_bytes": 32 * 1024**2,
    }
    return value


@pytest.mark.parametrize("route", ["mcp", "minion"])
def test_protected_stdio_routes_require_exact_workspace_and_empty_environment(route):
    from ahc.cli.foreground_backend_protection import require_protected_routes

    value = protected_config()
    profile = {"schema_version": "ahc.mcp-stdio-profile.v1", "enabled": True,
               "cwd": value["workspace"]["cwd"], "environment_names": []}
    value["mcp" if route == "mcp" else "native_minion"] = (
        {"profiles": [profile], "credential_environment": {}} if route == "mcp"
        else {"profile": profile})
    require_protected_routes(value)
    for field, bad in (("cwd", "/foreign"), ("environment_names", ["HOME"]),
                       ("schema_version", "ahc.mcp-http-profile.v1")):
        changed = deepcopy(value)
        selected = (changed["mcp"]["profiles"][0] if route == "mcp"
                    else changed["native_minion"]["profile"])
        selected[field] = bad
        with pytest.raises(ContractError, match="unqualified"):
            require_protected_routes(changed)
    value["mcp"] = value.get("mcp", {"profiles": []}) | {
        "credential_environment": {"private": "SECRET_ENV"}}
    with pytest.raises(ContractError, match="credentials"):
        require_protected_routes(value)


def test_stdio_transport_never_falls_back_after_protection_refusal(tmp_path, monkeypatch):
    import sys

    from ahc.adapters import mcp_stdio, native_process_runner

    calls = []

    class Refused:
        def __init__(self, **options):
            calls.append(options)

        def prepare_creation(self, *args, **kwargs):
            raise ContractError("refused protected launch")

        def close(self):
            calls.append("closed")

    monkeypatch.setattr(native_process_runner, "NativeProcessRunner", Refused)
    monkeypatch.setattr(mcp_stdio, "spawn_managed", lambda *a, **kw: pytest.fail("unconfined"))
    with pytest.raises(ContractError, match="refused protected"):
        mcp_stdio.McpStdioTransport.launch((sys.executable, "-c", "pass"), cwd=tmp_path,
            env={}, protection={"boundary": object(), "resource_limits": object()},
            lifetime_seconds=5)
    assert calls[0]["environment"] == {} and calls[-1] == "closed"


def test_protected_minion_port_receives_the_parent_boundary(monkeypatch):
    from types import SimpleNamespace

    from ahc.cli import foreground_backend_protection, native_child_factory
    from ahc.cli.foreground_children import ForegroundChildren

    selected = {"boundary": object(), "resource_limits": object()}
    value = {"native_minion": {"profile": {"enabled": True, "environment_names": []}},
             "backend": {"protection": {}}}
    config = SimpleNamespace(to_dict=lambda: value)
    monkeypatch.setattr(foreground_backend_protection, "native_backend_protection",
                        lambda current: selected if current is config else pytest.fail("foreign"))
    captured = []
    monkeypatch.setattr(native_child_factory, "ForegroundNativeChild",
                        lambda *args, **kwargs: captured.append(kwargs) or "port")
    owner = ForegroundChildren(object(), SimpleNamespace(config=config))
    owner.workspace = SimpleNamespace(identity=object())
    assert owner._port(SimpleNamespace(execution_kind="minion"), "token") == "port"
    assert captured[0]["protection"] is selected and captured[0]["environment"] == {}


def test_profile_roundtrip_is_exact_and_binding_changes_without_path_reads():
    value = protected_config()
    config = parse_foreground_config_native(value)
    assert config.to_dict() == value
    changed = deepcopy(value)
    changed["backend"]["protection"]["temporary_bytes"] *= 2
    other = parse_foreground_config_native(changed)
    assert config.configuration_digest != other.configuration_digest
    assert config.backend["protection"]["temporary_bytes"] == 32 * 1024**2
    factory = TrustedCodexBackendFactory(None, config)
    assert (
        factory._command
        != TrustedCodexBackendFactory(
            None, parse_foreground_config_native(native_config())
        )._command
    )
    assert "backend.protection" not in str(config.preview())


@pytest.mark.parametrize(
    "case",
    [
        "network",
        "home",
        "empty",
        "unknown",
        "capacity",
        "bool",
        "runtime",
        "skills",
        "workspace",
        "minion",
        "mcp",
    ],
)
def test_unclosed_route_or_invalid_profile_refuses_before_composition(case):
    value = protected_config()
    p = value["backend"]["protection"]
    if case == "network":
        p["network_mode"] = "automatic"
    if case == "home":
        p["home_mode"] = "inherit"
    if case == "empty":
        p.clear()
    if case == "unknown":
        p["fallback"] = True
    if case == "capacity":
        p["temporary_bytes"] = 2**40
    if case == "bool":
        p["home_bytes"] = True
    if case == "runtime":
        p["protected_roots"] = ["/different"]
    if case == "skills":
        value["skill_root"] = "/home/ahc/skills"
    if case == "workspace":
        del value["workspace"]["protection"]
    if case in ("mcp", "minion"):
        # Inspect the cross-route validator separately from profile schema parsing.
        from ahc.cli.foreground_backend_protection import require_protected_routes

        value["mcp" if case == "mcp" else "native_minion"] = (
            {"profiles": [{"enabled": True}]} if case == "mcp" else {"profile": {"enabled": True}}
        )
        with pytest.raises(ContractError, match="unqualified"):
            require_protected_routes(value)
        return
    with pytest.raises(ContractError):
        parse_foreground_config_native(value)


def test_legacy_trusted_and_disabled_optional_routes_remain_explicit():
    value = valid_config()
    config = parse_foreground_config_v2(value)
    assert "protection" not in config.backend
    value["backend"]["protection"] = protected_config()["backend"]["protection"]
    with pytest.raises(ContractError, match="native"):
        parse_foreground_config_v2(value)


def test_parent_child_and_recovery_select_the_same_protected_options(monkeypatch):
    import ahc.adapters.native_process_runner as runners
    import ahc.cli.foreground_backend_protection as profile

    calls = []
    monkeypatch.setattr(
        profile,
        "native_backend_protection",
        lambda c: {"boundary": "sealed", "resource_limits": "finite"},
    )
    monkeypatch.setattr(runners, "NativeProcessRunner", lambda **kw: calls.append(kw) or kw)
    factory = TrustedCodexBackendFactory(None, parse_foreground_config_native(protected_config()))
    first = factory.new_runner()
    recovery = factory.new_runner(recovery=True)
    assert first == recovery and len(calls) == 2
    assert first["boundary"] == "sealed" and first["environment"] == {}


def test_no_unconfined_fallback_on_boundary_admission_error(monkeypatch):
    import ahc.adapters.native_process_runner as runners
    import ahc.cli.foreground_backend_protection as profile

    def denied(config):
        raise ContractError("refused boundary")

    monkeypatch.setattr(profile, "native_backend_protection", denied)
    monkeypatch.setattr(
        runners, "NativeProcessRunner", lambda **kw: pytest.fail("unconfined fallback")
    )
    factory = TrustedCodexBackendFactory(None, parse_foreground_config_native(protected_config()))
    with pytest.raises(ContractError, match="refused"):
        factory.new_runner()
