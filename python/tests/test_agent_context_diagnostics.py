"""The operator preflight must agree with persona admission without activation."""

import pytest
from test_agent_context import configured

from ahc.cli.foreground_memory import inspect_foreground_document
from ahc.domain.sessions import ContractError


def test_doctor_rejects_foreign_persona_without_provider_activation(monkeypatch):
    import subprocess

    monkeypatch.setattr(subprocess, "Popen", lambda *a, **k: pytest.fail("unexpected process"))
    value, registry = configured()
    value["agent_context"]["resource_id"] = "foreign.persona"
    with pytest.raises(ContractError, match="matching profile persona"):
        inspect_foreground_document(value, profile_registry=registry)


def test_doctor_accepts_matching_persona_without_exposing_instructions(monkeypatch):
    import subprocess

    monkeypatch.setattr(subprocess, "Popen", lambda *a, **k: pytest.fail("unexpected process"))
    value, registry = configured()
    result = inspect_foreground_document(value, profile_registry=registry)
    assert result["configuration"] == "valid"
    assert value["agent_context"]["instructions"] not in str(result)
