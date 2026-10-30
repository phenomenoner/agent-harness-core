"""Pure inspection of the secret-free B1 credential-effect binding boundary."""

from __future__ import annotations

from ahc.domain.credential_effect_values import CredentialEffectBindingSnapshot

CREDENTIAL_EFFECT_BOUNDARY_STATUS_SCHEMA_VERSION = "ahc.credential-effect-boundary-status.v1"


def inspect_credential_effect_binding(
    snapshot: CredentialEffectBindingSnapshot | None,
) -> dict[str, object]:
    """Project an existing snapshot without opening or discovering a capability."""

    if snapshot is None:
        return {
            "schema_version": CREDENTIAL_EFFECT_BOUNDARY_STATUS_SCHEMA_VERSION,
            "status": "not-configured",
            "purpose": None,
        }
    if type(snapshot) is not CredentialEffectBindingSnapshot:
        raise TypeError("snapshot must be an exact CredentialEffectBindingSnapshot or None")
    return {
        "schema_version": CREDENTIAL_EFFECT_BOUNDARY_STATUS_SCHEMA_VERSION,
        "status": snapshot.status.value,
        "purpose": snapshot.purpose,
    }


__all__ = [
    "CREDENTIAL_EFFECT_BOUNDARY_STATUS_SCHEMA_VERSION",
    "inspect_credential_effect_binding",
]
