from __future__ import annotations

import ast
import importlib
import inspect

import pytest

from ahc.application.credential_effect_boundary import (
    CREDENTIAL_EFFECT_BOUNDARY_STATUS_SCHEMA_VERSION,
    inspect_credential_effect_binding,
)
from ahc.domain.credential_effect_values import (
    CredentialEffectBindingSnapshot,
    CredentialEffectResponse,
)

_FORBIDDEN_IMPORT_ROOTS = frozenset(
    {
        "ctypes",
        "importlib",
        "keyring",
        "socket",
        "subprocess",
    }
)
_FORBIDDEN_NAME_PARTS = frozenset(
    {
        "authorization",
        "callback",
        "command",
        "connection",
        "consumer",
        "credential_material",
        "descriptor",
        "endpoint",
        "factory",
        "handle",
        "issuer",
        "locator",
        "module_path",
        "permit",
        "pipe",
        "provider",
        "socket",
        "spawn",
        "token",
    }
)


def _guard_findings(source: str) -> set[str]:
    tree = ast.parse(source)
    findings: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            if any(alias.name.split(".", 1)[0] in _FORBIDDEN_IMPORT_ROOTS for alias in node.names):
                findings.add("forbidden-import")
        elif isinstance(node, ast.ImportFrom):
            root = (node.module or "").split(".", 1)[0]
            if root in _FORBIDDEN_IMPORT_ROOTS:
                findings.add("forbidden-import")
        names: list[str] = []
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            names.append(node.name)
        elif isinstance(node, ast.arg):
            names.append(node.arg)
        elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            names.append(node.target.id)
        elif isinstance(node, ast.Assign):
            names.extend(target.id for target in node.targets if isinstance(target, ast.Name))
        for name in names:
            lowered = name.lower()
            if any(part in lowered for part in _FORBIDDEN_NAME_PARTS):
                findings.add("forbidden-authority-name")
    return findings


def test_d1_no_binding_is_pure_not_configured_without_caller_invocation() -> None:
    calls = 0

    class Explosive:
        def __getattribute__(self, _name: str):
            nonlocal calls
            calls += 1
            raise AssertionError("caller object was inspected or invoked")

    assert inspect_credential_effect_binding(None) == {
        "schema_version": CREDENTIAL_EFFECT_BOUNDARY_STATUS_SCHEMA_VERSION,
        "status": "not-configured",
        "purpose": None,
    }
    assert calls == 0
    with pytest.raises(TypeError, match="exact CredentialEffectBindingSnapshot"):
        inspect_credential_effect_binding(Explosive())  # type: ignore[arg-type]
    assert calls == 0


def test_d1_ordinary_import_surface_has_no_live_capability_constructor() -> None:
    values = importlib.import_module("ahc.domain.credential_effect_values")
    boundary = importlib.import_module("ahc.application.credential_effect_boundary")
    assert set(values.__all__) == {
        "CREDENTIAL_EFFECT_BINDING_SNAPSHOT_SCHEMA_VERSION",
        "CREDENTIAL_EFFECT_RESPONSE_SCHEMA_VERSION",
        "MAX_CREDENTIAL_EFFECT_RESPONSE_BYTES",
        "CredentialEffectBindingSnapshot",
        "CredentialEffectBindingStatus",
        "CredentialEffectResponse",
        "CredentialEffectTransportStatus",
    }
    assert set(boundary.__all__) == {
        "CREDENTIAL_EFFECT_BOUNDARY_STATUS_SCHEMA_VERSION",
        "inspect_credential_effect_binding",
    }
    assert CredentialEffectBindingSnapshot.__module__ == values.__name__
    assert CredentialEffectResponse.__module__ == values.__name__
    public = set(values.__all__) | set(boundary.__all__)
    assert not any(part in name.lower() for name in public for part in _FORBIDDEN_NAME_PARTS)


def test_loaded_pure_credential_contract_modules_have_no_authority_seam() -> None:
    names = ("ahc.domain.credential_effect_values",
             "ahc.application.credential_effect_boundary")
    for name in names:
        module = importlib.import_module(name)
        # Inspect the loaded candidate, including zip imports during wheel tests.
        source = inspect.getsource(module)
        assert source and _guard_findings(source) == set()


def test_d1_structural_guard_rejects_each_forbidden_mutation_family() -> None:
    mutants = {
        "import socket\n",
        "from subprocess import run\n",
        "import ctypes\n",
        "provider: object\n",
        "credential_material: bytes\n",
        "def issue_factory():\n    return None\n",
        "def invoke(callback):\n    return callback()\n",
        "module_path: str\n",
        "command: tuple[str, ...]\n",
        "endpoint_handle: int\n",
        "authorization_token: str\n",
    }
    for source in mutants:
        assert _guard_findings(source), source
