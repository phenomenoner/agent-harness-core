"""Pure, secret-free M2.5 credential façade readiness projection."""

from __future__ import annotations

from typing import Any

from .credential_facade import CredentialUseFacade

CREDENTIAL_DOCTOR_SCHEMA_VERSION = "ahc.credential-doctor.v1"


def inspect_credential_facade(facade: CredentialUseFacade[Any] | None) -> dict[str, object]:
    if facade is None:
        return {
            "schema_version": CREDENTIAL_DOCTOR_SCHEMA_VERSION,
            "status": "not-configured",
            "purpose": None,
        }
    if type(facade) is not CredentialUseFacade:
        raise TypeError("facade must be a CredentialUseFacade or None")
    return {
        "schema_version": CREDENTIAL_DOCTOR_SCHEMA_VERSION,
        "status": "configured-unverified",
        "purpose": facade.purpose.value,
    }


__all__ = ["CREDENTIAL_DOCTOR_SCHEMA_VERSION", "inspect_credential_facade"]
