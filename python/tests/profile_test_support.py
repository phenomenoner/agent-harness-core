from __future__ import annotations

from typing import Any

from ahc.domain import ProfileResourceKind

_SCALAR_FIELDS = {
    "persona_ref": ProfileResourceKind.PERSONA,
    "workspace_ref": ProfileResourceKind.WORKSPACE,
    "backend_profile_ref": ProfileResourceKind.BACKEND_PROFILE,
    "credential_ref": ProfileResourceKind.CREDENTIAL,
    "working_memory_ref": ProfileResourceKind.WORKING_MEMORY,
    "durable_memory_ref": ProfileResourceKind.DURABLE_MEMORY,
    "subagent_policy_ref": ProfileResourceKind.SUBAGENT_POLICY,
    "cron_policy_ref": ProfileResourceKind.CRON_POLICY,
}
_LIST_FIELDS = {
    "plugin_refs": ProfileResourceKind.PLUGIN,
    "channel_refs": ProfileResourceKind.CHANNEL,
}


def profile_registry_payload(agent_ids: tuple[str, ...] = ("hexi", "lizi")) -> dict[str, Any]:
    profiles: list[dict[str, Any]] = []
    resources: list[dict[str, str]] = []
    for agent_id in agent_ids:
        profile: dict[str, Any] = {
            "schema_version": "ahc.agent-profile.v1",
            "agent_id": agent_id,
        }
        for field_name, kind in _SCALAR_FIELDS.items():
            resource_id = _resource_id(agent_id, kind)
            profile[field_name] = resource_id
            resources.append(_resource(resource_id, kind, agent_id))
        for field_name, kind in _LIST_FIELDS.items():
            resource_id = _resource_id(agent_id, kind)
            profile[field_name] = [resource_id]
            resources.append(_resource(resource_id, kind, agent_id))
        profiles.append(profile)
    return {
        "schema_version": "ahc.agent-profile-registry.v1",
        "default_agent_id": agent_ids[0],
        "profiles": profiles,
        "resources": resources,
    }


def standalone_config_text(*, version: int = 1) -> str:
    profile_line = 'profile_registry = "profiles.json"\n' if version == 2 else ""
    return f"""config_version = "ahc.standalone.config.v{version}"
state_dir = "state"
{profile_line}
[[services]]
name = "demo"
command = ["{{python}}", "-c", "pass"]
cwd = "."
enabled = true
restart_policy = "never"
"""


def _resource_id(agent_id: str, kind: ProfileResourceKind) -> str:
    return f"{agent_id}.{kind.value}"


def _resource(resource_id: str, kind: ProfileResourceKind, owner_agent_id: str) -> dict[str, str]:
    return {
        "schema_version": "ahc.profile-resource.v1",
        "resource_id": resource_id,
        "kind": kind.value,
        "owner_agent_id": owner_agent_id,
    }
