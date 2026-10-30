from __future__ import annotations

import importlib.util
from pathlib import Path


def test_python_architecture_gate() -> None:
    script = Path(__file__).resolve().parents[1] / "tools" / "check_architecture.py"
    spec = importlib.util.spec_from_file_location("check_architecture", script)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    assert module.violations() == []


def test_runtime_application_cannot_construct_concrete_custody(tmp_path, monkeypatch):
    script = Path(__file__).resolve().parents[1] / "tools" / "check_architecture.py"
    spec = importlib.util.spec_from_file_location("runtime_architecture", script)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    source = tmp_path / "src" / "ahc"
    application = source / "application"
    application.mkdir(parents=True)
    (application / "custodied_backend.py").write_text(
        "from ahc.infrastructure.backend_custody import BackendCustody\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(module, "ROOT", tmp_path)
    monkeypatch.setattr(module, "SOURCE", source)
    assert any("runtime application imports concrete assembly" in item
               for item in module.violations())
