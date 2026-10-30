"""Tool-free host model calls using the existing durable child/custody lifecycle."""
import math
import time
from dataclasses import replace
from uuid import uuid4
from ahc.domain.child_work import ChildWorkEnvelope
from ahc.domain.sessions import ContractError
from ahc.domain.skill_values import digest_json
from ahc.infrastructure.child_dispatch_repository import ChildDispatchRepository
from ahc.application.child_dispatch import ChildDispatchService

class HostModelCalls:
    def __init__(self, owner, parent_request, workspace):
        self.owner, self.parent_request, self.workspace = owner, parent_request, workspace

    def run(self, call_id, prompt, *, model, reasoning_effort, timeout_seconds,
            cancelled=lambda: False):
        from .foreground_config_native import parse_trusted_foreground_config
        from .trusted_codex_backend import TrustedCodexBackendFactory
        from .trusted_codex_child import TrustedCodexChildFactory
        if cancelled():
            raise ContractError("host model call cancelled")
        document = self.owner.config.to_dict()
        backend = document["backend"]
        backend.pop("time_budget", None)
        backend.pop("overload_recovery", None)
        backend.pop("context_policy", None)
        backend.pop("wrap_up_timeout", None)
        backend.update(model=model, reasoning_effort=reasoning_effort,
                       turn_timeout=timeout_seconds)
        backend["lifetime_seconds"] = max(backend["lifetime_seconds"],
            math.ceil(timeout_seconds + backend.get("settlement_timeout", 0) + 2))
        document.pop("agent_context", None)
        if "max_artifacts" in document.get("child_limits", {}):
            document["child_limits"].update(max_artifacts=0, artifact_bytes=0)
        owner = TrustedCodexBackendFactory(self.owner.store,
            parse_trusted_foreground_config(document), tool_free=True)
        # This is a separate host decision allowance, never a re-admission of user work.
        parent = replace(self.parent_request, deadline=time.monotonic() + timeout_seconds,
                         time_budget=None)
        children = ChildDispatchRepository(owner.store, parent)
        identifier = digest_json({"host_call": call_id, "parent": children.parent.to_dict()})
        envelope = ChildWorkEnvelope(identifier, children.parent, prompt,
            self.workspace.identity.workspace_id,
            children._now() + int(timeout_seconds * 1000) - 2)
        token = uuid4().hex
        service = ChildDispatchService(children, lambda admitted:
            TrustedCodexChildFactory(owner, children, admitted, token,
                                     self.workspace.identity).port())
        try:
            result = service.dispatch(envelope, token, cancelled=cancelled)
        finally:
            try:
                service.close()
            except Exception:
                pending = getattr(self.owner.store, "_host_model_pending_closes", None)
                if pending is None:
                    pending = self.owner.store._host_model_pending_closes = []
                if service not in pending:
                    pending.append(service)
                raise
        if result["state"] != "terminal" or result.get("result") is None:
            raise ContractError("host model outcome unverified; do not replay")
        return result["result"]["output_text"]
