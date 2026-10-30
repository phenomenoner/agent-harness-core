"""Synchronous child tool composition bound only during an admitted parent turn."""

import math
import os
from uuid import uuid4

from ahc.application.child_dispatch import ChildDispatchService
from ahc.domain.child_work import ChildWorkEnvelope, _task_text
from ahc.domain.sessions import ContractError, _identifier
from ahc.domain.skill_values import digest_json
from ahc.domain.workspace import WorkspaceIdentity
from ahc.infrastructure.child_dispatch_repository import ChildDispatchRepository

from .foreground_child_limits import owner_limits
from .trusted_codex_child import TrustedCodexChildFactory


class ForegroundChildren:
    def __init__(self, store, owner):
        self.store, self.owner = store, owner
        limits = owner_limits(owner)
        self.max_children = limits["max_children"]
        self.max_concurrency = limits["max_concurrency"]
        self.max_artifacts = limits.get("max_artifacts", 0)
        self.artifact_bytes = limits.get("artifact_bytes", 0)
        self.children = self.workspace = None
        self.services = {}
        self.batch = None
        self.cancelled = lambda: False

    @property
    def native_configured(self):
        config = getattr(self.owner, "config", None)
        value = None if config is None else config.to_dict().get("native_minion")
        return value is not None and value["profile"]["enabled"]

    def bind(self, request, workspace, cancelled):
        if self.children is not None or workspace.store is not self.store:
            raise ContractError("child tool binding busy or foreign")
        self.children = ChildDispatchRepository(self.store, request)
        self.workspace, self.cancelled = workspace, cancelled

    @property
    def batch_configured(self):
        from ahc.infrastructure.threaded_runtime_repository import ThreadedRuntimeRepository

        return isinstance(self.store, ThreadedRuntimeRepository)

    def invoke_batch(self, identity, arguments):
        from .foreground_child_batch import invoke_batch

        return invoke_batch(self, identity, arguments)

    def prepare_artifacts(self, envelopes):
        if not self.max_artifacts:
            return
        from ahc.infrastructure.child_artifacts import ChildArtifacts

        artifacts = ChildArtifacts(self.children)
        for envelope in envelopes:
            if envelope.execution_kind != "subagent":
                continue
            record = self.children.admit(envelope)
            if record["state"] == "queued":
                artifacts.allocate(envelope.parent, envelope.child_id,
                    max_count=self.max_artifacts, max_bytes=self.artifact_bytes)

    def _port(self, envelope, token, *, children=None):
        children = self.children if children is None else children
        if envelope.execution_kind == "minion":
            from .native_child_factory import ForegroundNativeChild

            value = self.owner.config.to_dict().get("native_minion")
            if value is None or not value["profile"]["enabled"]:
                raise ContractError("native minion is not configured")
            environment = {name: os.environ[name] for name in value["profile"]["environment_names"]
                           if name in os.environ}
            options = {}
            if "protection" in self.owner.config.to_dict().get("backend", {}):
                from .foreground_backend_protection import native_backend_protection

                options["protection"] = native_backend_protection(self.owner.config)
            return ForegroundNativeChild(value, children, envelope, token,
                                         environment=environment, workspace=self.workspace.identity,
                                         **options)
        return TrustedCodexChildFactory(self.owner, children, envelope, token,
                                        self.workspace.identity).port()

    def preflight(self, identity, arguments):
        try:
            if (self.children is None or identity != self.workspace.identity
                    or type(arguments) is not dict
                    or not set(arguments) <= {
                        "expected", "child_id", "task_text", "timeout_ms", "execution_kind"}):
                raise ContractError("child tool reference invalid")
            if ("expected" in arguments
                    and WorkspaceIdentity.from_dict(arguments["expected"]) != identity):
                raise ContractError("child tool reference invalid")
            missing = sorted({"expected", "child_id", "task_text", "timeout_ms"} - set(arguments))
            if missing:
                return {"disposition": "rejected", "reason": "invalid-child-request",
                        "missing_fields": missing, "request_admitted": False,
                        "hint": "This call was rejected before dispatch. Supply the missing "
                                "fields. expected must contain the complete current workspace "
                                "identity (session, workspace_id, generation, revision); copy "
                                "the nested session unchanged. timeout_ms must be an integer "
                                "greater than zero, bounded by the parent deadline. "
                                "Changing child_id alone cannot fix this. "
                                "This does not authorize replay of an earlier unknown child.",
                        "unresolved_effects": False}
            _identifier(arguments["child_id"], "child identifier")
            _task_text(arguments["task_text"])
            kind = arguments.get("execution_kind", "subagent")
            if kind not in ("subagent", "minion"):
                raise ContractError("child execution kind invalid")
            if kind == "minion":
                value = self.owner.config.to_dict().get("native_minion")
                if value is None or not value["profile"]["enabled"]:
                    raise ContractError("native minion is not configured")
            timeout = arguments["timeout_ms"]
            if type(timeout) is not int or timeout < 1:
                raise ContractError("child timeout invalid")
        except Exception:
            return {"disposition": "rejected", "reason": "invalid-child-request",
                    "request_admitted": False, "unresolved_effects": False,
                    "hint": "No dispatch occurred. Check expected identity, configured execution "
                            "kind, task_text and timeout_ms (positive integer). "
                            "Never replay an earlier unknown child."}
        return None

    def invoke(self, identity, arguments):
        rejected = self.preflight(identity, arguments)
        if rejected is not None:
            return rejected
        try:
            local_id = arguments["child_id"]
            kind = arguments.get("execution_kind", "subagent")
            timeout = arguments["timeout_ms"]
            child_id = digest_json({"parent": self.children.parent.to_dict(), "child": local_id})
            records = self.children.read(self.children.parent)
            existing = next((row for row in records if row["child_id"] == child_id), None)
            if existing is not None:
                replay = ChildWorkEnvelope(child_id, self.children.parent, arguments["task_text"],
                    identity.workspace_id, existing["deadline_unix_ms"], kind)
                if replay.request_digest != existing["request_digest"]:
                    raise ContractError("child identifier task mismatch")
                return self._result(existing)
            if len(records) >= self.max_children:
                raise ContractError("child turn limit reached")
            now = self.children._monotonic_now()
            remaining = math.floor((self.children.request.effective_deadline - now) * 1000) - 1
            if remaining <= 1:
                raise ContractError("child parent deadline exhausted")
            envelope = ChildWorkEnvelope(child_id, self.children.parent, arguments["task_text"],
                identity.workspace_id, self.children._now() + min(timeout, remaining), kind)
        except Exception:
            return {"disposition": "rejected", "reason": "invalid-child-request",
                    "unresolved_effects": False}
        if (any(service.cleanup_pending for service in self.services.values())
                or self.batch is not None and self.batch.cleanup_pending):
            return {"disposition": "unknown", "reason": "child-dispatch-unverified",
                    "unresolved_effects": True}
        token = uuid4().hex
        service = ChildDispatchService(self.children, lambda item: self._port(item, token))
        self.services[envelope.child_id] = service
        try:
            self.prepare_artifacts((envelope,))
            return self._result(service.dispatch(envelope, token, cancelled=self.cancelled))
        except Exception:
            return {"disposition": "unknown", "reason": "child-dispatch-unverified",
                    "unresolved_effects": True}

    def _result(self, record):
        from ahc.infrastructure.native_artifact_import import NativeArtifactImport

        unresolved = record["state"] in ("unknown", "claimed")
        extras = ({"observations": record["observations"]} if record.get("observations") else {})
        if "token_observation" in record:
            extras["token_observation"] = record["token_observation"]
        if record.get("execution_kind") == "minion":
            rows = NativeArtifactImport(self.children).read(
                self.children.parent, record["child_id"])
            if rows:
                extras["artifacts"] = [{"artifact_id": digest_json(row["artifact"]),
                                        **row["artifact"]} for row in rows]
        return {"disposition": "unknown" if unresolved else "observed",
                "unresolved_effects": unresolved,
                "result": {"child_id": record["child_id"], "state": record["state"],
                           "result": record["result"], **extras}}

    def close(self):
        failed = False
        if self.batch is not None:
            try:
                self.batch.close()
            except Exception:
                failed = True
            else:
                self.batch = None
        for child_id, service in tuple(self.services.items()):
            try:
                service.close()
            except Exception:
                failed = True
            else:
                del self.services[child_id]
        if failed:
            raise ContractError("child cleanup remains pending")
        self.children = self.workspace = None


class ChildEnabledBackend:
    def __init__(self, backend, children, request, workspace, cancelled):
        self.backend, self.children = backend, children
        self.request, self.workspace, self.cancelled = request, workspace, cancelled

    def invoke(self, request):
        if request != self.request:
            raise ContractError("child-enabled parent request mismatch")
        self.children.bind(request, self.workspace, self.cancelled)
        return self.backend.invoke(request)

    def recovery_ready(self):
        probe = getattr(self.backend, "recovery_ready", None)
        return callable(probe) and probe()

    def close(self):
        self.children.close()
        close = getattr(self.backend, "close", None)
        if close is not None:
            close()
