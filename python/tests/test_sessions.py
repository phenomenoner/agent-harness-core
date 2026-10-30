from __future__ import annotations

from dataclasses import FrozenInstanceError

import pytest

from ahc.domain import (
    VIRTUAL_SESSION_REF_SCHEMA_VERSION,
    VIRTUAL_SESSION_SCHEMA_VERSION,
    ContractError,
    VirtualSessionIdentity,
    VirtualSessionRef,
)


def _initial_ref() -> dict[str, object]:
    return {
        "schema_version": VIRTUAL_SESSION_REF_SCHEMA_VERSION,
        "virtual_session_id": "session-main",
        "generation": 1,
    }


def _initial_identity() -> dict[str, object]:
    return {
        "schema_version": VIRTUAL_SESSION_SCHEMA_VERSION,
        "ref": _initial_ref(),
        "agent_id": "agent-main",
        "route_id": "route-main",
        "predecessor": None,
    }


def _successor_identity() -> dict[str, object]:
    return {
        "schema_version": VIRTUAL_SESSION_SCHEMA_VERSION,
        "ref": {
            "schema_version": VIRTUAL_SESSION_REF_SCHEMA_VERSION,
            "virtual_session_id": "session-main",
            "generation": 2,
        },
        "agent_id": "agent-main",
        "route_id": "route-main",
        "predecessor": _initial_ref(),
    }


def test_initial_and_successor_identities_round_trip_exactly() -> None:
    initial = VirtualSessionIdentity.from_dict(_initial_identity())
    successor = VirtualSessionIdentity.from_dict(_successor_identity())

    assert initial.to_dict() == _initial_identity()
    assert successor.to_dict() == _successor_identity()
    assert initial.ref.generation == 1
    assert initial.predecessor is None
    assert successor.ref.virtual_session_id == initial.ref.virtual_session_id
    assert successor.predecessor == initial.ref
    assert successor.ref.generation == initial.ref.generation + 1


def test_convenience_constructors_preserve_the_same_lineage_contract() -> None:
    initial = VirtualSessionIdentity.initial("session-main", "agent-main", "route-main")
    successor = initial.successor()

    assert successor.predecessor == initial.ref
    assert successor.ref == initial.ref.successor()
    assert VirtualSessionIdentity.from_dict(successor.to_dict()) == successor


def test_domain_values_are_immutable_and_nested_emission_is_detached() -> None:
    identity = VirtualSessionIdentity.from_dict(_initial_identity())
    with pytest.raises(FrozenInstanceError):
        identity.ref = identity.ref  # type: ignore[misc]
    with pytest.raises(FrozenInstanceError):
        identity.ref.generation = 2  # type: ignore[misc]

    emitted = identity.to_dict()
    emitted["ref"]["generation"] = 99  # type: ignore[index]
    assert identity.ref.generation == 1


@pytest.mark.parametrize("generation", [True, False, 0, -1])
def test_generation_rejects_bool_and_nonpositive_values(generation: object) -> None:
    payload = _initial_ref()
    payload["generation"] = generation

    with pytest.raises(ContractError, match="generation"):
        VirtualSessionRef.from_dict(payload)


@pytest.mark.parametrize(
    "virtual_session_id",
    [
        "",
        " ",
        " leading",
        "trailing ",
        "bad/id",
        "é-id",
        "\x00-id",
        "a" * 129,
    ],
)
def test_identifiers_reject_blank_whitespace_control_unicode_and_oversize(
    virtual_session_id: str,
) -> None:
    payload = _initial_ref()
    payload["virtual_session_id"] = virtual_session_id

    with pytest.raises(ContractError, match="virtual_session_id"):
        VirtualSessionRef.from_dict(payload)


@pytest.mark.parametrize(
    "mutator, expected",
    [
        (
            lambda payload: payload["predecessor"].update({"generation": 2}),
            "predecessor generation",
        ),
        (
            lambda payload: payload["predecessor"].update({"virtual_session_id": "other"}),
            "same virtual_session_id",
        ),
        (lambda payload: payload.__setitem__("predecessor", None), "requires a predecessor"),
    ],
)
def test_successor_lineage_rejects_one_valid_predecessor_mutation(mutator, expected: str) -> None:
    payload = _successor_identity()
    mutator(payload)

    with pytest.raises(ContractError, match=expected):
        VirtualSessionIdentity.from_dict(payload)


def test_exact_keys_are_required_at_each_identity_depth() -> None:
    missing = _initial_identity()
    del missing["route_id"]
    with pytest.raises(ContractError, match="missing keys"):
        VirtualSessionIdentity.from_dict(missing)

    unknown = _initial_identity()
    unknown["unexpected"] = True
    with pytest.raises(ContractError, match="unknown keys"):
        VirtualSessionIdentity.from_dict(unknown)

    nested_unknown = _initial_identity()
    nested_unknown["ref"]["unexpected"] = True  # type: ignore[index]
    with pytest.raises(ContractError, match="ref: .*unknown keys"):
        VirtualSessionIdentity.from_dict(nested_unknown)

    nested_missing = _initial_identity()
    del nested_missing["ref"]["generation"]  # type: ignore[index]
    with pytest.raises(ContractError, match="ref: .*missing keys"):
        VirtualSessionIdentity.from_dict(nested_missing)
