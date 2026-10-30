from dataclasses import replace
from types import SimpleNamespace

import pytest
from test_foreground_native_memory import configured

from ahc.adapters.memory_provider_identity import provider_digest
from ahc.application.native_memory_diagnostics import inspect_native_provider
from ahc.cli.foreground_config_v2 import parse_foreground_config_v2
from ahc.cli.foreground_native_memory import read_native_binding


@pytest.mark.parametrize("mode,status,reason", [
    ("absent", "unavailable", "provider-directory-absent"),
    ("invalid", "unavailable", "provider-source-invalid"),
    ("changed", "blocked", "provider-content-mismatch"),
    ("matching", "present-unverified", None),
    ("unreadable", "indeterminate", "provider-source-unreadable"),
])
def test_source_readback_never_imports_or_inspects_database(
        tmp_path, monkeypatch, mode, status, reason):
    root = tmp_path / "provider"
    binding = SimpleNamespace(provider_root=str(root), provider_digest="a" * 64)
    if mode != "absent":
        root.mkdir()
    if mode in ("changed", "matching"):
        api = root / "openclaw_mem" / "core" / "api.py"
        api.parent.mkdir(parents=True)
        api.write_text("raise RuntimeError('PRIVATE must never import')")
        if mode == "matching":
            binding.provider_digest = provider_digest(root)
    if mode == "unreadable":
        def forbidden(*args):
            raise PermissionError("PRIVATE")
        monkeypatch.setattr("ahc.application.native_memory_diagnostics.provider_digest", forbidden)
    before = {str(p): p.read_bytes() for p in root.rglob("*") if p.is_file()}
    report = inspect_native_provider(binding)
    assert report == {"observation_scope": "provider-source-identity", "execution_verified": False,
                      "database_inspected": False, "status": status, "reason_code": reason}
    assert "PRIVATE" not in str(report) and str(root) not in str(report)
    assert before == {str(p): p.read_bytes() for p in root.rglob("*") if p.is_file()}


def test_opt_in_doctor_adds_source_readback_but_default_does_not(tmp_path, monkeypatch):
    from ahc.cli.runtime_doctor import inspect_selected_runtime

    raw, registry, _, _ = configured(tmp_path)
    binding = read_native_binding(parse_foreground_config_v2(raw), registry)
    binding = replace(binding, provider_root=str(tmp_path / "missing-provider"))
    monkeypatch.setattr("ahc.cli.foreground_selection.select_foreground", lambda *a: raw)
    monkeypatch.setattr("ahc.cli.foreground_native_memory.read_native_binding", lambda *a: binding)
    document = SimpleNamespace(profile_registry=registry)
    default = inspect_selected_runtime(SimpleNamespace(inspect_runtime=False), document)
    assert "source" not in default["runtime"]["native_memory"]
    result = inspect_selected_runtime(SimpleNamespace(inspect_runtime=True), document)
    source = result["runtime"]["native_memory"]["source"]
    assert source["reason_code"] == "provider-directory-absent"
    assert result["runtime"]["native_memory"]["provider_status"] == "not-inspected"
    assert not (tmp_path / "missing-provider").exists()
