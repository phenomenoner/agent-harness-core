"""Pure read-only doctor projection for M2.1 profile authority."""

from __future__ import annotations

from ahc.domain import AgentProfileRegistry

PROFILE_DOCTOR_SCHEMA_VERSION = "ahc.profile-doctor.v1"


def inspect_profile_authority(registry: AgentProfileRegistry | None) -> dict[str, object]:
    """Project validated authority without I/O, repair, or materialization."""

    if registry is None:
        return {
            "schema_version": PROFILE_DOCTOR_SCHEMA_VERSION,
            "status": "not-configured",
            "registry_schema_version": None,
            "default_agent_id": None,
            "profiles": [],
            "resources": [],
        }
    if type(registry) is not AgentProfileRegistry:
        raise TypeError("registry must be an AgentProfileRegistry or None")
    return {
        "schema_version": PROFILE_DOCTOR_SCHEMA_VERSION,
        "status": "ready",
        "registry_schema_version": registry.schema_version,
        "default_agent_id": registry.default_agent_id,
        "profiles": [profile.to_dict() for profile in registry.profiles],
        "resources": [resource.to_dict() for resource in registry.resources],
    }


__all__ = ["PROFILE_DOCTOR_SCHEMA_VERSION", "inspect_profile_authority"]
