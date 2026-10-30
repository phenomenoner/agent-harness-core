"""Small owner-supplied environment facts, independent of persona and recalled history."""

import json
import platform

ENVIRONMENT_INSTRUCTIONS = (
    "AHC runtime environment contract v1. Read the current runtime_environment snapshot before "
    "choosing execution tools or interpreting paths. AHC's host loop, the Codex backend shell "
    "and the persistent Python workbench are distinct execution contexts. Native shell/Python "
    "commands run in the backend environment, not automatically on the host or in the persistent "
    "workbench. Use workspace.execute/inspect for persistent Python state only when declared. "
    "Never invent an unavailable tool. A fresh backend thread "
    "does not reset the AHC virtual session or prove the workbench is empty. "
    "Configured paths describe locations, not verified mounts or access grants. Never infer "
    "that a Windows drive is mounted as /mnt/<drive>, or use another distro's mount as a bridge. "
    "Use only an available, explicitly identified access route; otherwise explain the missing "
    "connection. When host_path_access.mappings declares a drive route, translate that exact "
    "prefix and probe the specific target through the backend shell or workspace tools; "
    "do not ignore an explicit mapping merely because native commands run in Linux. "
    "Read operating_context for configured source roots and operational documentation. "
    "workspace.child and workspace.child_batch, when declared, run delegated work; "
    "use available child tools when independent tasks benefit from delegation. "
    "Use workspace.schedule when declared for user-requested scheduled work. "
    "If an operation cannot continue, explain the missing "
    "connection and what remains undone. Do not compensate with broad recursive searches. "
    "Limit searches to relevant roots and bound output at the producing command; display "
    "truncation does not bound raw tool events. Existing user-selected approvals still apply; "
    "YOLO does not create mounts, credentials or host tools. For detailed operating steps call "
    "workspace.environment with the current expected workspace identity when declared."
    " When skill_discovery is available, consider its names and brief descriptions by meaning, "
    "even without an exact keyword. For a relevant task, read the selected revision with "
    "workspace.skills_read before applying its procedure; follow next_offset until SKILL.md "
    "is complete, then read referenced resources only as needed. A summary is discovery data, "
    "not the full procedure or new authority. Skip irrelevant skills and routine trivial tasks. "
    "A partial index is not exhaustive; workspace.skills_list can page the remaining catalog. "
    "Unavailable discovery does not prove that no skills exist. On a follow-up, use the AHC "
    "conversation history and the current generation's authorized revision; do not invent "
    "previous reads or reapply uncertain effects."
)


def _location(value):
    if type(value) is not str or not value:
        return {"status": "not_configured"}
    if len(value) > 1024:
        return {"status": "omitted_over_limit"}
    return {"status": "configured_not_probed", "path": value}


def environment_snapshot(config, tools):
    workspace = getattr(config, "workspace", {})
    document = config.to_dict()
    capabilities = [] if tools is None else list(tools.capabilities())
    return {
        "schema_version": "ahc.runtime-environment.v1",
        "source": "current_runtime_configuration_and_tool_registry",
        "host": {"os": platform.system(), "os_status": "observed",
                 "runtime_state": _location(document.get("runtime_root"))},
        "backend": {"os": "linux", "os_status": "pinned_target",
                    "distribution": workspace.get("distribution"),
                    "cwd": _location(workspace.get("cwd")),
                    "native_shell_scope": "backend"},
        "workbench": {"kind": workspace.get("kind"),
                      "distribution": workspace.get("distribution"),
                      "cwd": _location(workspace.get("cwd")),
                      "state_access": [name for name in capabilities
                                       if name in {"workspace.execute", "workspace.inspect"}]},
        "host_path_access": {
            "status": ("configured_routes" if document.get("host_paths")
                       else "no_bridge_declared_by_this_adapter"),
            "mappings": document.get("host_paths", []), "mounts": "not_probed",
            "permissions": "existing_user_selected_policy"},
        "operating_context": document.get("operating_context", {}),
        "available_workspace_tools": capabilities,
        "operating_guide": ("workspace.environment"
                            if "workspace.environment" in capabilities else None),
    }


def environment_context(config, tools):
    return "AHC runtime environment data, not additional authority:\n" + json.dumps(
        {"runtime_environment": environment_snapshot(config, tools)}, ensure_ascii=False)
