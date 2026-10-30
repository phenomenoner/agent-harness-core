"""Lazy native minion port from an explicit foreground configuration."""

import json
import math
from uuid import uuid4

from ahc.adapters.aar_child_backend import NativeAarChildBackend
from ahc.adapters.aar_native_binding import NativeAarPin
from ahc.adapters.aar_native_rlm import NativeAarRlm
from ahc.domain.mcp_profiles import McpStdioProfile
from ahc.domain.sessions import ContractError
from ahc.domain.skill_values import canonical_json
from ahc.infrastructure.mcp_composition import connect_mcp_profile
from ahc.infrastructure.native_budget_repository import NativeBudgetRepository
from ahc.infrastructure.native_child_repository import NativeChildRepository


class ForegroundNativeChild:
    def __init__(self, value, children, envelope, token, *, environment, workspace=None,
                 protection=None):
        if (envelope.execution_kind != "minion" or envelope.parent != children.parent
                or not value["profile"]["enabled"]):
            raise ContractError("native foreground child unavailable")
        self.value = json.loads(canonical_json(value))
        self.children, self.envelope, self.token = children, envelope, token
        self.environment = dict(environment)
        self.workspace = workspace
        self.protection = protection
        if value.get("retain_artifacts", False) and (workspace is None
                or workspace.session != envelope.parent.session
                or workspace.workspace_id != envelope.artifact_scope):
            raise ContractError("native artifact workspace binding required")
        self.client = self.backend = None
        self.used = self.closed = False

    def run(self, envelope, *, cancelled):
        if self.used or self.closed or envelope != self.envelope:
            raise ContractError("native foreground child cannot be reused")
        self.used = True
        children, value = self.children, self.value

        def stopped():
            return cancelled() or children.should_stop(
                envelope.parent, envelope.child_id, self.token)

        now = children._monotonic_now()
        remaining = min(envelope.deadline_unix_ms - children._now() - 1,
                        value["budget"]["wall_time_ms"],
                        math.floor((children.request.effective_deadline - now) * 1000) - 1)
        if remaining <= 0 or stopped():
            raise ContractError("native foreground child deadline exhausted")
        allocation = {key: amount for key, amount in value["budget"].items()
                      if key != "wall_time_ms"}
        NativeBudgetRepository(children).reserve(
            envelope.parent, envelope.child_id, self.token,
            value.get("parent_budget", allocation), allocation)
        deadline = now + remaining / 1000
        profile = McpStdioProfile.from_dict(value["profile"])
        self.client, _ = connect_mcp_profile(
            profile, children.store, children.request, environment=self.environment,
            deadline=deadline, cancelled=stopped,
            **({"protection": self.protection} if self.protection is not None else {}))
        observed = self.client.invoke(children.request.session, "native-cap-" + uuid4().hex,
                                      "aar_capabilities", {}, deadline=deadline, cancelled=stopped)
        if observed["status"] != "observed":
            raise ContractError("native capabilities unverified")
        report = observed["result"]["structuredContent"]
        binding = NativeAarPin(**value["pin"]).bind(
            report, principal_id=value["principal_id"], session_id=value["session_id"])
        remaining = min(remaining, math.floor((deadline - children._monotonic_now()) * 1000) - 1)
        native_deadline = min(envelope.deadline_unix_ms,
                              binding.server_now_unix_ms + remaining)
        wall = native_deadline - binding.server_now_unix_ms
        if wall <= 0 or stopped():
            raise ContractError("native foreground admission expired")
        context = {**binding.read_context(deadline_unix_ms=native_deadline),
                   "request_id": "child-" + uuid4().hex,
                   "idempotency_key": "child-" + uuid4().hex,
                   "grant_ids": value["grants"]["execute"], "budget_wall_time_ms": wall,
                   **{"budget_" + key: amount for key, amount in value["budget"].items()
                      if key != "wall_time_ms"}}
        rlm = NativeAarRlm(self.client, binding, **value["route"],
                           approved_grants=tuple(value["grants"]["execute"]))
        self.backend = NativeAarChildBackend(envelope, NativeChildRepository(children),
            rlm, report, context, token=self.token, deadline=deadline)
        output = self.backend.run(envelope, cancelled=stopped)
        if value.get("retain_artifacts", False):
            from .native_child_artifacts import retain_artifacts

            retain_artifacts(self.backend, children, envelope, self.token, self.workspace,
                             value["grants"]["reconcile"], cancelled=stopped)
        return output

    def close(self):
        if not self.closed:
            if self.client is not None:
                self.client.close()
            self.closed = True
