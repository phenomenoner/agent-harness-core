from __future__ import annotations

import ast
from pathlib import Path

import pytest

from ahc.application import hosted_runtime


def test_hosted_runtime_is_absent_until_the_native_host_installs_it() -> None:
    assert hosted_runtime.inspect_hosted_runtime() == {
        "schema_version": "ahc.credential-effect-boundary-status.v1",
        "status": "not-configured",
        "purpose": None,
    }
    assert hosted_runtime.__all__ == ["inspect_hosted_runtime"]


def test_python_caller_cannot_commit_an_arbitrary_hosted_context() -> None:
    class Explosive:
        def __getattribute__(self, _name: str) -> object:
            raise AssertionError("arbitrary context attributes must not be read")

    snapshot = {
        "schema_version": "ahc.credential-effect-boundary-status.v1",
        "status": "unavailable",
        "purpose": "model.invoke",
    }
    with pytest.raises(TypeError, match="exact native hosted-context type"):
        hosted_runtime._commit_native_context(Explosive(), snapshot)
    assert hosted_runtime.inspect_hosted_runtime()["status"] == "not-configured"


def test_hosted_runtime_source_has_no_capsule_constructor_or_effect_seam() -> None:
    path = Path(hosted_runtime.__file__ or "")
    tree = ast.parse(path.read_text(encoding="utf-8"))
    imports = {
        alias.name.split(".", 1)[0]
        for node in ast.walk(tree)
        if isinstance(node, ast.Import)
        for alias in node.names
    }
    imports |= {
        (node.module or "").split(".", 1)[0]
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom)
    }
    assert not imports & {"ctypes", "importlib", "keyring", "socket", "subprocess"}
    source = path.read_text(encoding="utf-8")
    assert "PyCapsule_New" not in source
    assert "PyCapsule_GetPointer" not in source
    assert "turn/start" not in source
    assert "auth.json" not in source
