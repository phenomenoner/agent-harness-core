"""The absent-memory adapter must not discover or invoke a provider."""

from __future__ import annotations

import importlib
import sqlite3
import subprocess
from dataclasses import FrozenInstanceError

import pytest

from ahc.adapters.unavailable_memory import UnavailableMemoryPort
from ahc.domain.memory_availability import MemoryAvailability, MemoryUnavailableOutcome
from ahc.domain.sessions import ContractError


class _EffectTrap:
    def __call__(self, *args: object, **kwargs: object) -> object:
        raise AssertionError("memory adapter performed an effect")


@pytest.mark.parametrize("status", ("absent", "disabled", "incompatible", "unavailable"))
def test_all_operations_return_the_configured_unavailable_status(
    monkeypatch: pytest.MonkeyPatch, status: str
) -> None:
    trap = _EffectTrap()
    monkeypatch.setattr(importlib, "import_module", trap)
    monkeypatch.setattr(sqlite3, "connect", trap)
    monkeypatch.setattr(subprocess, "run", trap)
    port = UnavailableMemoryPort(MemoryAvailability(status))

    assert port.status() == MemoryAvailability(status)
    for operation in (port.read, port.store, port.pack):
        result = operation(trap)
        assert isinstance(result, MemoryUnavailableOutcome)
        assert result.to_dict() == {
            "outcome": "unavailable",
            "operation": result.operation,
            "status": status,
            "reason_code": f"memory-{status}",
        }


@pytest.mark.parametrize("status", ("ready", "", None, "unavailable: secret-value"))
def test_invalid_status_never_becomes_a_public_diagnostic(status: object) -> None:
    with pytest.raises(ContractError, match="memory availability status is unsupported") as error:
        MemoryAvailability(status)  # type: ignore[arg-type]

    assert "secret-value" not in str(error.value)


def test_values_are_immutable_and_do_not_accept_arbitrary_reason_codes() -> None:
    availability = MemoryAvailability("absent")
    outcome = MemoryUnavailableOutcome("read", availability)

    with pytest.raises(FrozenInstanceError):
        availability.status = "ready"  # type: ignore[misc]
    with pytest.raises(FrozenInstanceError):
        outcome.operation = "store"  # type: ignore[misc]
    with pytest.raises(TypeError):
        MemoryAvailability("absent", "provider-error")  # type: ignore[call-arg]
    with pytest.raises(ContractError, match="memory operation is unsupported"):
        MemoryUnavailableOutcome("probe", availability)


def test_port_requires_explicit_typed_configured_status() -> None:
    with pytest.raises(ContractError, match="explicit MemoryAvailability"):
        UnavailableMemoryPort("absent")  # type: ignore[arg-type]
