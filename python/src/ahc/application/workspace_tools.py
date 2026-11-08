"""Agent-facing tools bound to one already admitted runtime workspace.

The foreground owner opens/restores the workspace. Tool input never selects a
runner, filesystem root, container ID, session replacement or execution backend.
"""

from ahc.domain.sessions import ContractError
from ahc.domain.workspace import (
    CheckpointNotSaved, CheckpointValueRejected, WorkspaceCodeRejected, WorkspaceIdentity,
    WorkspaceOperationNotStarted, workspace_failure_projection,
)
from ahc.domain.workspace_display import display_fields
from ahc.domain.workspace_inspection import inspection_names

OPERATIONS = ("workspace.execute", "workspace.inspect", "workspace.checkpoint",
              "workspace.environment")


def _rejection_feedback(arguments, workspace):
    """Advisory field paths and public schema literals; never identity values."""
    paths, versions = [], {}

    def compare(actual, expected, prefix):
        if len(paths) >= 16:
            return
        if type(expected) is dict:
            if type(actual) is not dict or set(actual) != set(expected):
                paths.append(prefix + ".fields")
                return
            for key, value in expected.items():
                compare(actual[key], value, prefix + "." + key)
        elif type(actual) is not type(expected) or actual != expected:
            paths.append(prefix)
            if prefix in {
                "expected.schema_version", "expected.session.schema_version",
                "expected.session.ref.schema_version",
                "expected.session.predecessor.schema_version",
            }:
                versions[prefix] = expected

    try:
        compare(arguments.get("expected") if type(arguments) is dict else None,
                workspace.identity.to_dict(), "expected")
    except Exception:
        return {}
    if not paths:
        return {}
    return {"invalid_fields": paths,
            **({"required_schema_versions": versions} if versions else {})}


class WorkspaceTools:
    def __init__(self, workspace=None, *, artifacts=None, mcp=None, progress=None, children=None,
                 memory_pack=None, memory_search=None, native_memory=None, scheduling=None,
                 channel_replies=None, skills=None, learning=None):
        self.workspace = workspace
        self.artifacts = artifacts
        self.mcp = mcp
        self.progress = progress
        self.children = children
        self.memory_pack = memory_pack
        self.memory_search = memory_search
        self.native_memory = native_memory
        self.scheduling = scheduling
        self.channel_replies = channel_replies
        self.skills = skills
        self.learning = learning

    def capabilities(self):
        if self.workspace is None:
            return ()
        jobs = getattr(self.workspace, "jobs", None)
        job_tools = (("workspace.job", "workspace.budget")
                     if jobs is not None and jobs.available else ())
        if self.workspace.closed or getattr(self.workspace, "unavailable", False):
            return job_tools
        if self.artifacts is not None and callable(
            getattr(getattr(self.workspace, "port", None), "export_artifact", None)
        ):
            available = OPERATIONS + ("workspace.artifact", "workspace.attach")
        else:
            available = OPERATIONS + (("workspace.attach",) if self.artifacts is not None else ())
        available += (("workspace.read_artifact", "workspace.incoming_artifacts")
                      if self.artifacts is not None else ())
        available += (("workspace.channel_messages", "workspace.reply")
                      if self.channel_replies is not None else ())
        available += (("workspace.skills_list", "workspace.skills_read",
                       "workspace.skills_resources")
                      if self.skills is not None else ())
        if (self.skills is not None and type(getattr(getattr(self.workspace, "port", None),
                                                    "skill_materialization_root", None)) is str):
            available += ("workspace.skills_materialize",)
        available += (("workspace.learning",) if self.learning is not None else ())
        return available + job_tools + (("workspace.mcp",) if self.mcp else ()) + (
            ("workspace.progress",) if self.progress is not None else ()) + (
            ("workspace.child",) if self.children is not None else ()) + (
            ("workspace.child_batch",)
            if getattr(self.children, "batch_configured", False) else ()) + (
            ("workspace.memory_pack",) if self.memory_pack is not None else ()) + (
            ("workspace.memory_search",) if self.memory_search is not None else ()) + (
            ("workspace.memory_native",) if self.native_memory is not None else ()) + (
            ("workspace.schedule",) if self.scheduling is not None else ())

    def preflight(self, name, arguments):
        # Pure admission-shape check only; invoke revalidates after approval.
        if name == "workspace.child" and self.children is not None:
            return self.children.preflight(self.workspace.identity, arguments)
        return None

    def invoke(self, name, arguments):
        if name not in self.capabilities():
            return {"disposition": "unsupported", "reason": "workspace-unavailable"}
        if name == "workspace.job":
            from .workspace_jobs import invoke_job

            return invoke_job(self.workspace, arguments)
        if name == "workspace.budget":
            from .workspace_budget import invoke_budget

            return invoke_budget(self.workspace, arguments)
        if name == "workspace.learning":
            return self.learning.invoke(self.workspace, arguments)
        if name in ("workspace.skills_list", "workspace.skills_read", "workspace.skills_resources",
                    "workspace.skills_materialize"):
            return self.skills.invoke(self.workspace, name, arguments,
                                      available_tools=self.capabilities())
        if name in ("workspace.channel_messages", "workspace.reply"):
            return self.channel_replies.invoke(self.workspace, name, arguments)
        if name == "workspace.environment":
            from importlib.resources import files

            try:
                if type(arguments) is not dict or set(arguments) != {"expected"}:
                    raise ContractError("environment guide fields invalid")
                if WorkspaceIdentity.from_dict(arguments["expected"]) != self.workspace.identity:
                    raise ContractError("environment guide owner mismatch")
                guide = files("ahc").joinpath("resources/runtime-environment.md").read_text(
                    encoding="utf-8")
                if len(guide) > 8192:
                    raise ContractError("environment guide exceeds limit")
                return {"disposition": "observed", "unresolved_effects": False,
                        "result": {"guide": guide,
                                   **({"runtime_status": self.runtime_status()}
                                      if callable(getattr(self, "runtime_status", None)) else {})}}
            except (ContractError, OSError):
                return {"disposition": "rejected", "unresolved_effects": False,
                        "reason": "environment-guide-unavailable"}
        activity = getattr(self, "activity_event", None)
        if callable(activity):
            try:
                activity("工具執行中")
            except Exception:
                pass
        if name == "workspace.memory_native":
            return self.native_memory.invoke(self.workspace, arguments)
        if name == "workspace.schedule":
            return self.scheduling.invoke(self.workspace, arguments)
        if name == "workspace.artifact":
            from .workspace_artifact_tools import capture

            return capture(self.workspace, self.artifacts, arguments)
        if name == "workspace.read_artifact":
            from .workspace_artifact_read import read_chunk

            return read_chunk(self.workspace, self.artifacts, arguments)
        if name == "workspace.incoming_artifacts":
            try:
                if type(arguments) is not dict or set(arguments) != {"expected"}:
                    raise ContractError("incoming artifact fields invalid")
                expected = WorkspaceIdentity.from_dict(arguments["expected"])
                if (expected != self.workspace.identity
                        or self.artifacts.store is not self.workspace.store):
                    raise ContractError("incoming artifact workspace mismatch")
                return {"disposition": "observed", "unresolved_effects": False,
                        "result": self.artifacts.incoming(expected)}
            except Exception:
                return {"disposition": "rejected", "unresolved_effects": False,
                        "reason": "incoming-artifacts-unavailable"}
        if name == "workspace.mcp":
            return self.mcp.invoke(self.workspace.identity, arguments)
        if name == "workspace.progress":
            return self.progress.invoke(self.workspace.identity, arguments)
        if name == "workspace.child":
            return self.children.invoke(self.workspace.identity, arguments)
        if name == "workspace.child_batch":
            return self.children.invoke_batch(self.workspace.identity, arguments)
        if name == "workspace.memory_pack":
            from .workspace_memory_pack import recall

            return recall(self.workspace, self.memory_pack, arguments)
        if name == "workspace.memory_search":
            from .workspace_memory_search import search

            return search(self.workspace, self.memory_search, arguments)
        if name == "workspace.attach":
            from .workspace_result_attachment import select_attachment

            return select_attachment(self.workspace, self.artifacts, arguments)
        workspace = self.workspace
        field = "code" if name == "workspace.execute" else "names"
        try:
            if type(arguments) is not dict or set(arguments) != {"expected", field}:
                raise ContractError("workspace tool fields invalid")
            expected = WorkspaceIdentity.from_dict(arguments["expected"])
            if expected != workspace.identity:
                raise ContractError("workspace reference stale")
            value = arguments[field]
            if field == "code":
                if type(value) is not str:
                    raise ContractError("workspace code invalid")
                value.encode("utf-8")
            else:
                if type(value) is not list:
                    raise ContractError("workspace names must be an array")
                value = inspection_names(tuple(value))
        except (ContractError, UnicodeError):
            return {"disposition": "rejected", "reason": "invalid-workspace-request",
                    **_rejection_feedback(arguments, workspace)}
        try:
            if name == "workspace.execute":
                result = workspace.execute(expected, value)
                if (
                    type(result) is not dict
                    or type(result.get("ok")) is not bool
                    or type(result.get("output")) is not str
                    or len(result["output"]) > 4096
                    or any(0xD800 <= ord(c) <= 0xDFFF for c in result["output"])
                    or type(result.get("output_truncated")) is not bool
                ):
                    raise ContractError("workspace output invalid")
                result = {
                    **display_fields(result),
                    "identity": result["identity"],
                    "ok": result["ok"],
                    "output": result["output"],
                    "output_truncated": result["output_truncated"],
                    "error": None if result["ok"] else "execution-error",
                }
            elif name == "workspace.inspect":
                result = workspace.inspect(expected, value)
            else:
                checkpoint = workspace.checkpoint(expected, value)
                result = {"origin": checkpoint.origin.to_dict(), "digest": checkpoint.digest}
            recovery = ({"workspace_recovery": "receipt-reconciled"}
                        if name == "workspace.execute" and
                        getattr(workspace, "last_recovery", None) == "receipt-reconciled" else {})
            return {"disposition": "observed", "result": result, "unresolved_effects": False,
                    **recovery}
        except WorkspaceOperationNotStarted:
            return {"disposition": "rejected", "reason": "workspace-operation-not-started",
                    "unresolved_effects": False,
                    "recovery": "The interpreter was not invoked. A running job owns its "
                                "namespace; use status/wait for that exact admitted job. "
                                "Otherwise inspect current identity before another call. "
                                "Do not repeat earlier writes to obtain a read result."}
        except WorkspaceCodeRejected as error:
            return {"disposition": "rejected", "reason": "workspace-code-frame-invalid",
                    "unresolved_effects": False, "invalid_fields": ["code"],
                    "request_bytes": error.request_bytes,
                    "max_request_bytes": error.max_request_bytes,
                    "recovery": "Code was not executed. Load existing files or split code "
                                "into smaller cells using the same current identity."}
        except CheckpointValueRejected:
            if name == "workspace.checkpoint":
                return {"disposition": "rejected", "reason": "checkpoint-values-not-portable",
                        "unresolved_effects": False, "invalid_fields": ["names"]}
            return {"disposition": "unknown", "reason": "workspace-operation-unverified",
                    "unresolved_effects": True}
        except CheckpointNotSaved:
            return {"disposition": "rejected", "reason": "checkpoint-capture-unavailable",
                    "unresolved_effects": False, "resource_available": False,
                    "recovery": "No checkpoint was saved. This workspace is unavailable. "
                                "Use already confirmed observations to finish a useful text "
                                "result; cleanup owns the next distinct workspace admission."}
        except Exception:
            failure = (workspace_failure_projection(getattr(workspace, "last_failure", None))
                       if name == "workspace.execute" else {})
            if (name == "workspace.execute" and failure.get("phase") == "admission"
                    and getattr(workspace, "execution_started", None) is False):
                return {"disposition": "rejected", "reason": "workspace-not-executed",
                        "unresolved_effects": False, "workspace_failure": failure,
                        "recovery": "The interpreter was not invoked. Inspect current workspace "
                                    "identity and existing state before a new call; temporary "
                                    "admission contention does not require repeating completed work."}
            # Unknown execution still cannot be replayed; preserve its causal phase.
            return {
                "disposition": "unknown",
                "reason": "workspace-operation-unverified",
                "unresolved_effects": True,
                **({"workspace_failure": failure} if failure else {}),
            }
