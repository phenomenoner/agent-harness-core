from __future__ import annotations

import json

import pytest

from ahc.domain.credential_effect_values import (
    CREDENTIAL_EFFECT_BINDING_SNAPSHOT_SCHEMA_VERSION,
    CREDENTIAL_EFFECT_RESPONSE_SCHEMA_VERSION,
    MAX_CREDENTIAL_EFFECT_RESPONSE_BYTES,
    CredentialEffectBindingSnapshot,
    CredentialEffectBindingStatus,
    CredentialEffectResponse,
    CredentialEffectTransportStatus,
)
from ahc.domain.sessions import ContractError


def _response_payload(**changes: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "schema_version": CREDENTIAL_EFFECT_RESPONSE_SCHEMA_VERSION,
        "runtime_generation": 7,
        "capsule_generation": 11,
        "operation_id": "operation-1",
        "attempt_id": "attempt-1",
        "request_digest": f"sha256:{'a' * 64}",
        "channel_sequence": 3,
        "transport_status": "unavailable",
        "reason_code": "provider-unavailable",
    }
    payload.update(changes)
    return payload


def _wire(payload: object) -> bytes:
    return json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()


def test_d1_binding_snapshot_is_strict_secret_free_and_round_trips() -> None:
    absent = CredentialEffectBindingSnapshot(
        CREDENTIAL_EFFECT_BINDING_SNAPSHOT_SCHEMA_VERSION,
        CredentialEffectBindingStatus.NOT_CONFIGURED,
        None,
    )
    assert CredentialEffectBindingSnapshot.from_dict(absent.to_dict()) == absent
    assert absent.to_dict() == {
        "schema_version": CREDENTIAL_EFFECT_BINDING_SNAPSHOT_SCHEMA_VERSION,
        "status": "not-configured",
        "purpose": None,
    }

    unavailable = CredentialEffectBindingSnapshot(
        CREDENTIAL_EFFECT_BINDING_SNAPSHOT_SCHEMA_VERSION,
        CredentialEffectBindingStatus.UNAVAILABLE,
        "model.invoke",
    )
    assert CredentialEffectBindingSnapshot.from_dict(unavailable.to_dict()) == unavailable

    with pytest.raises(ContractError, match="purpose must be absent"):
        CredentialEffectBindingSnapshot(
            CREDENTIAL_EFFECT_BINDING_SNAPSHOT_SCHEMA_VERSION,
            CredentialEffectBindingStatus.NOT_CONFIGURED,
            "model.invoke",
        )
    with pytest.raises(ContractError, match="purpose is required"):
        CredentialEffectBindingSnapshot(
            CREDENTIAL_EFFECT_BINDING_SNAPSHOT_SCHEMA_VERSION,
            CredentialEffectBindingStatus.DEAD,
            None,
        )
    with pytest.raises(ContractError, match="invalid fields"):
        CredentialEffectBindingSnapshot.from_dict(absent.to_dict() | {"endpoint": "forbidden"})


def test_d1_non_success_response_round_trips_all_supported_statuses() -> None:
    reasons = {
        CredentialEffectTransportStatus.DENIED: "binding-denied",
        CredentialEffectTransportStatus.UNAVAILABLE: "provider-unavailable",
        CredentialEffectTransportStatus.FAILED: "provider-failed",
        CredentialEffectTransportStatus.UNKNOWN: "response-lost",
        CredentialEffectTransportStatus.CANCELLED: "cancelled-before-write",
    }
    for status, reason in reasons.items():
        response = CredentialEffectResponse.from_wire(
            _wire(_response_payload(transport_status=status.value, reason_code=reason))
        )
        assert response.transport_status is status
        assert response.reason_code == reason
        assert CredentialEffectResponse.from_wire(response.to_wire()) == response
        assert set(response.to_dict()) == {
            "schema_version",
            "runtime_generation",
            "capsule_generation",
            "operation_id",
            "attempt_id",
            "request_digest",
            "channel_sequence",
            "transport_status",
            "reason_code",
        }


def test_d1_response_rejects_malformed_oversized_unknown_and_raw_content_without_retention() -> (
    None
):
    sentinel = "D1-CREDENTIAL-MATERIAL-SENTINEL"
    duplicate = (
        b'{"schema_version":"ahc.credential-effect-response.v1",'
        b'"schema_version":"ahc.credential-effect-response.v1"}'
    )
    cases = [
        b"",
        b"\xff",
        b"[]",
        duplicate,
        _wire(_response_payload(schema_version="ahc.credential-effect-response.v999")),
        _wire(_response_payload(transport_status="succeeded", reason_code="provider-succeeded")),
        _wire(_response_payload(runtime_generation=True)),
        _wire(_response_payload(capsule_generation=0)),
        _wire(_response_payload(channel_sequence=-1)),
        _wire(_response_payload(request_digest="sha256:not-a-digest")),
        _wire(_response_payload(reason_code="raw provider exception")),
        _wire(_response_payload(domain_result={"value": sentinel})),
        _wire(_response_payload(credential_material=sentinel)),
        _wire(_response_payload(provider_exception=sentinel)),
        b"{" + b" " * MAX_CREDENTIAL_EFFECT_RESPONSE_BYTES + b"}",
    ]
    for frame in cases:
        with pytest.raises(ContractError) as error:
            CredentialEffectResponse.from_wire(frame)
        rendered = f"{error.value!s} {error.value!r}"
        assert sentinel not in rendered
        assert "raw provider exception" not in rendered
        prefix = frame[:32].decode("utf-8", errors="ignore")
        if prefix:
            assert prefix not in rendered


def test_d1_status_reason_pair_is_closed() -> None:
    with pytest.raises(ContractError, match="reason_code is inconsistent"):
        CredentialEffectResponse.from_wire(
            _wire(_response_payload(transport_status="unknown", reason_code="provider-failed"))
        )
