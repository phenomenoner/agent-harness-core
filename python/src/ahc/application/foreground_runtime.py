"""In-process runtime owner; ingress and backend admission are injected by composition.

No queue, provider launcher, interpreter, or implicit checkpoint is created here.
The ingress callable must be nonblocking and return at most one bounded request.
"""

import threading
import time

from ahc.domain.agent_backend import AgentTurnObservation, AgentTurnRequest
from ahc.domain.sessions import ContractError
from ahc.infrastructure.working_repository import WorkingWriterBusy

from .runtime_service import RuntimeService
from .workspace_tools import WorkspaceTools


class ForegroundRuntime:
    def __init__(
        self,
        store,
        session,
        ingress,
        backend_factory,
        *,
        open_workspace=None,
        selector=None,
        memory=None,
        memory_pack=None,
        memory_search=None,
        native_memory=None,
        scheduling=None,
        artifacts=None,
        schedule=None,
        background_schedule=None,
        mcp=None,
        channel_delivery=None,
        channel_ingress=None,
        channel_receiver=None,
        channel_typing=None,
        progress=None,
        channel_files=None,
        children=None,
        local_reply=None,
        channel_replies=None,
        learning=None,
        canvas=None,
        lazy_skill_disclosure=False,
    ):
        self.store = store
        self.canvas = canvas
        self._canvas_status = {"status": "not-configured" if canvas is None else "idle"}
        self.session = session
        self.ingress = ingress
        self.backend_factory = backend_factory
        self.open_workspace = open_workspace
        self.selector = selector
        self.lazy_skill_disclosure = lazy_skill_disclosure
        self.memory = memory
        self.memory_pack = memory_pack
        self.memory_search = memory_search
        self.native_memory = native_memory
        self.scheduling = scheduling
        self.artifacts = artifacts
        self.schedule = schedule
        self.background_schedule = background_schedule
        self.mcp = mcp
        self.channel_delivery = channel_delivery
        self.channel_ingress = channel_ingress
        self.channel_receiver = channel_receiver
        self.channel_typing, self._typing = channel_typing, None
        self.progress = progress
        self.channel_files = channel_files
        self.children = children
        self.local_reply = local_reply
        self.channel_replies = channel_replies
        self.learning = learning
        self.workspace = None
        self._backend = None
        self._cleanup_attempts = 0
        self._cleanup_trace = None
        self._last_activity = None
        self._turn_timing = {}
        self.tools = WorkspaceTools()
        self.state = "new"
        self._thread = None
        self._root_lease = None
        self._workspace_unavailable = None
        self._workspace_failure_trace = None
        from .backend_recovery import BackendRecovery
        self._recovery = BackendRecovery()

    def _owner(self):
        if self._thread != threading.get_ident():
            raise ContractError("foreground runtime requires its owner thread")

    def memory_status(self):
        from .memory_diagnostics import inspect_memory

        return {**inspect_memory(self.memory),
                **({"pack": {"status": "configured", "observation_scope": "configuration-only"}}
                   if self.memory_pack is not None else {}),
                **({"search": {"status": "configured", "observation_scope": "configuration-only"}}
                   if self.memory_search is not None else {})}

    def schedule_status(self):
        if self.background_schedule is not None:
            return self.background_schedule.status()
        status = ("not-configured" if self.schedule is None
                  else getattr(self.schedule, "status", "observation-unavailable"))
        if status not in (
            "not-configured", "idle", "stale-goal", "submitted", "not-due",
            "admission-unavailable",
        ):
            status = "observation-unavailable"
        return {"status": status, "observation_scope": "last-idle-step",
                "execution_verified": False}

    def child_status(self):
        from .child_diagnostics import child_availability

        return child_availability(subagent_configured=self.children is not None,
            minion_configured=getattr(self.children, "native_configured", False))

    def channel_status(self):
        inspect = getattr(self.channel_ingress, "status", None)
        def observe(supplier):
            try:
                return supplier()
            except Exception:
                return {"state": "observation-unavailable"}
        return {"ingress": observe(inspect) if callable(inspect) else {"state": "not-configured"},
                **({"files": observe(self.channel_files.status)} if self.channel_files else {}),
                **({"receiver": observe(self.channel_receiver.status)}
                   if self.channel_receiver else {}),
                "activity": (observe(self._typing.status)
                             if callable(getattr(self._typing, "status", None))
                             else self._last_activity),
                "last_turn_timing": dict(self._turn_timing),
                **({"symbolic_canvas": dict(self._canvas_status)}
                   if self.canvas is not None else {}),
                "admission": self.admission_status()}

    def admission_status(self):
        """Readiness is separate from process liveness; no diagnostic runs here."""
        receiver = self._receiver_observation()
        reason = "ready"
        if self.state != "running":
            reason = "runtime-not-running"
        elif (getattr(self.channel_receiver, "failed", False)
              or any(value in {"paused", "failed", "closed", "close-unverified", "backoff",
                               "degraded", "observation-unavailable"}
                     for value in receiver.values())):
            reason = "receiver-unavailable"
        elif self._workspace_unavailable is not None:
            reason = "workspace-unavailable"
        elif self._cleanup_attempts:
            reason = ("backend-cleanup-exhausted" if self._cleanup_attempts >= 2
                      else "backend-cleanup-pending")
        return {"ready": reason == "ready", "reason": reason,
                "observation_scope": "current-runtime", "recovery": self._recovery.observe(),
                **({"receiver": receiver} if receiver else {})}

    def _receiver_observation(self):
        inspect = getattr(self.channel_receiver, "status", None)
        if not callable(inspect):
            return {}
        allowed = {"new", "running", "ready", "connecting", "reconnecting", "hello",
                   "identifying", "resuming", "paused", "failed", "closed",
                   "close-unverified", "idle", "polling", "backoff", "degraded"}
        def state(value):
            value = value.get("state") if type(value) is dict else None
            return value if type(value) is str and value in allowed else "observation-unavailable"
        try:
            raw = inspect()
            result = {"state": state(raw)}
            if type(raw) is dict:
                for platform in ("telegram", "discord"):
                    if raw.get(platform) is not None:
                        result[platform] = state(raw[platform])
                telegram = raw.get("telegram")
                if type(telegram) is dict:
                    if telegram.get("failure_category") in {
                            "none", "transport", "unverified-response", "service", "rate-limit",
                            "authorization", "conflict"}:
                        result["telegram_failure_category"] = telegram["failure_category"]
                    if type(telegram.get("automatic_recovery")) is bool:
                        result["telegram_automatic_recovery"] = telegram["automatic_recovery"]
                    for key in ("consecutive_failures", "recovery_attempts", "recovery_limit",
                                "transport_probe_attempts", "transport_probe_limit", "http_status",
                                "next_retry_seconds", "recovery_interval_seconds",
                                "retry_after_seconds", "held_updates"):
                        value = telegram.get(key)
                        if type(value) is int and 0 <= value <= 2**31 - 1:
                            result["telegram_" + key] = value
                discord = raw.get("discord")
                if type(discord) is dict:
                    reason = discord.get("reason")
                    if type(reason) is str and reason in {
                            "connection-budget-exhausted", "non-reconnectable-close",
                            "identify-admission-unavailable", "cancelled",
                            "session-invalidation-unavailable"}:
                        result["discord_reason"] = reason
                    for key in ("connection_attempts", "total_connection_attempts",
                                "connection_limit"):
                        value = discord.get(key)
                        if type(value) is int and 0 <= value <= 2**31 - 1:
                            result["discord_" + key] = value
            return result
        except Exception:
            return {"state": "observation-unavailable"}

    def cleanup_status(self):
        """Current in-process resource cleanup state; it is not historical turn evidence."""
        if self._backend is None and self._typing is None:
            state = "clear"
        elif self._cleanup_attempts >= 2:
            state = "exhausted"
        else:
            state = "pending"
        return {"state": state, "automatic_attempts": self._cleanup_attempts,
                "automatic_attempt_limit": 2, "observation_scope": "current-runtime",
                **({"failure_trace": self._cleanup_trace} if self._cleanup_trace else {})}

    def start(self):
        if self.state != "new":
            raise ContractError("foreground runtime already started")
        self._thread = threading.get_ident()
        self.state = "starting"
        self._root_lease = self.store.foreground_lease(self.session)
        current = self.store.read_current(self.session.ref.virtual_session_id)
        if current.session != self.session or current.status != "active":
            raise ContractError("foreground session is not current and active")
        if self.open_workspace is not None:
            from ahc.adapters.docker_workspace import WorkspaceUnavailable
            from ahc.domain.backend_creation import BackendCreationRejected
            from ahc.infrastructure.backend_custody import BackendCustodyUnavailable

            from .workspace_availability import WorkspacePreflightUnavailable

            try:
                self.workspace = self.open_workspace(current)
            except (WorkspacePreflightUnavailable, BackendCreationRejected,
                    WorkspaceUnavailable, BackendCustodyUnavailable) as error:
                from .failure_trace import failure_trace
                self._workspace_unavailable = getattr(
                    error, "reason_code", "workspace-start-unresolved")
                self._workspace_failure_trace = failure_trace(error)
                if getattr(self.channel_receiver, "workspace_independent", False):
                    self.channel_receiver.start()
                self.state = "running"
                if self.background_schedule is not None:
                    self.background_schedule.start()
                return
            if (
                self.workspace.store is not self.store
                or self.workspace.identity.session != self.session
            ):
                raise ContractError("foreground workspace owner mismatch")
            from .workspace_skills import WorkspaceSkillTools

            skill_repository = getattr(self.selector, "repository", None)
            self.tools = WorkspaceTools(self.workspace, artifacts=self.artifacts, mcp=self.mcp,
                                        progress=self.progress, children=self.children,
                                        memory_pack=self.memory_pack,
                                        memory_search=self.memory_search,
                                        native_memory=self.native_memory,
                                        scheduling=self.scheduling,
                                        channel_replies=self.channel_replies,
                                        learning=self.learning,
                                        skills=(WorkspaceSkillTools(skill_repository)
                                                if skill_repository is not None else None))
            self.tools.runtime_status = self.admission_status
            if self.learning is not None:
                self.learning.experiments.granted_tools = frozenset(self.tools.capabilities())
        if self.channel_receiver is not None:
            self.channel_receiver.start()
        self.state = "running"
        if self.background_schedule is not None:
            self.background_schedule.start()

    def step(self, *, cancelled=lambda: False):
        self._owner()
        if self.state != "running":
            raise ContractError("foreground runtime is not accepting requests")
        if getattr(self.channel_receiver, "failed", False):
            raise ContractError("channel control receiver failed")
        if cancelled():
            return None
        try:
            current = self.store.read_current(self.session.ref.virtual_session_id)
        except WorkingWriterBusy:
            # No admission or channel effect has occurred in this cycle.
            return None
        if current.session != self.session or current.status != "active":
            raise ContractError("foreground session changed")
        if self._workspace_unavailable is not None:
            return None
        if self.channel_delivery is not None and not cancelled():
            self.channel_delivery(cancelled=cancelled)
        if self._backend is not None or self._typing is not None:
            if self._cleanup_attempts >= 2:
                self._recovery.step(self._backend,
                    lambda: self._cleanup_after_answer(recovery=True), self._cleanup_trace)
                return None  # No model admission on an observation/reconciliation step.
            try:
                self._cleanup_after_answer()
            except Exception:
                return None  # Retain exact custody and fence fresh backend admission.
            return None  # Cleanup succeeded without a model turn; admit only on a later step.
        if self.workspace is not None and (self.workspace.closed or self.workspace.unavailable):
            # A terminal unknown cell is never replayed. Once its exact workspace
            # and backend cleanup have completed, a fresh generation can serve
            # the next distinct submission. Failed creation is not retried here.
            try:
                self.workspace.close()  # Unavailable ports retain exact cleanup custody.
                self.workspace = self.open_workspace(current)
                if (self.workspace.store is not self.store
                        or self.workspace.identity.session != self.session):
                    raise ContractError("foreground replacement workspace owner mismatch")
                self.tools.workspace = self.workspace
            except Exception as error:
                from .failure_trace import failure_trace

                self._workspace_unavailable = "workspace-replacement-unavailable"
                self._workspace_failure_trace = failure_trace(error)
                return None
        if self.channel_ingress is not None and not cancelled():
            self.channel_ingress(cancelled=cancelled)
        if self.channel_files is not None and not cancelled():
            self.channel_files(self.workspace, cancelled=cancelled)
        request = self.ingress(current, self.tools)
        if request is None and self.schedule is not None and not cancelled():
            # Only admit on idle steps. Durable admission is consumed next step,
            # after controls and cancellation have had another opportunity to run.
            self.schedule(current)
        if request is None or cancelled():
            controller = getattr(self.learning, "controller", None)
            if request is None and not cancelled() and controller is not None:
                controller.step(cancelled=cancelled)
            return None
        if (
            type(request) is not AgentTurnRequest
            or request.session != self.session
            or request.goal_digest != current.goal.digest
            or request.capabilities != tuple(self.tools.capabilities())
            or (
                self.workspace is not None
                and request.workspace_scope != self.workspace.identity.workspace_id
            )
        ):
            raise ContractError("foreground request binding mismatch")
        owner = self

        class AdmittedBackend:
            def invoke(self, bound):
                # Factory runs only after durable admission, never on operation replay.
                if cancelled() or time.monotonic() >= bound.deadline:
                    return AgentTurnObservation(bound, "not-submitted", "deadline-or-cancel", 0, 0)
                owner._turn_timing = {"operation_id": bound.operation_id}
                skills = getattr(owner.tools, "skills", None)
                if skills is not None and callable(getattr(skills, "bind_request", None)):
                    skills.bind_request(bound, owner.workspace)
                if owner.learning is not None:
                    owner.learning.record_input(bound)
                from .backend_recovery import BackendRecovery
                owner._recovery = BackendRecovery()
                if owner.channel_typing is not None:
                    try:
                        owner._typing = owner.channel_typing(bound, cancelled)
                        if owner._typing is not None:
                            owner._typing.start()
                    except Exception as error:
                        from .failure_trace import failure_trace

                        owner._turn_timing["presentation"] = {
                            "state": "unavailable", "failure_trace": failure_trace(error)}
                        # Retain any constructed UI owner for ordinary cleanup.
                owner.tools.activity_event = getattr(owner._typing, "observe", None)
                started = time.monotonic()
                backend = owner.backend_factory(bound, owner.tools, cancelled=cancelled)
                owner._turn_timing["backend_construct_seconds"] = time.monotonic() - started
                owner._backend = backend
                # Construction may take time; do not invoke after cancellation or expiry.
                if cancelled() or time.monotonic() >= bound.deadline:
                    if getattr(backend, "close", None) is None:
                        raise ContractError("cancelled backend construction cleanup unverified")
                    return AgentTurnObservation(
                        bound, "not-submitted", "deadline-or-cancel", 0, 0
                    )
                started = time.monotonic()
                try:
                    from contextlib import nullcontext

                    scope = (owner.workspace.turn_scope(bound.time_budget or bound.deadline)
                             if owner.workspace is not None else nullcontext())
                    with scope:
                        return backend.invoke(bound)
                finally:
                    owner._turn_timing["backend_invoke_seconds"] = time.monotonic() - started

            def answer_ready(self):
                finish = getattr(owner._typing, "finish", None)
                if finish is not None:
                    finish(True)

            def cleanup_after_answer(self):
                # Join UI before final delivery. Native backend cleanup can be slow;
                # retain its owner until the sealed answer has had a delivery chance.
                try:
                    owner._close_typing()
                except Exception:
                    owner._cleanup_attempts = 1
                    raise

        def local_reply(bound):
            if cancelled() or time.monotonic() >= bound.deadline:
                return None  # AdmittedBackend preserves the existing not-submitted result.
            return None if self.local_reply is None else self.local_reply(bound)

        from ahc.infrastructure.channel_control_store import ChannelSubmissionHeld

        try:
            result = RuntimeService(
                self.store, AdmittedBackend(), selector=self.selector, local_reply=local_reply
            ).execute_with_skills(current, request, tools=tuple(self.tools.capabilities()),
                                  lazy_disclosure=self.lazy_skill_disclosure)
            try:
                started = time.monotonic()
                if self.channel_delivery is not None and not cancelled():
                    self.channel_delivery(cancelled=cancelled)
            finally:
                self._turn_timing["delivery_step_seconds"] = time.monotonic() - started
                started = time.monotonic()
                try:
                    if self._cleanup_attempts == 0:
                        self._cleanup_after_answer()
                except Exception:
                    pass  # Exact owner retained; later step has one remaining attempt.
                finally:
                    self._turn_timing["cleanup_seconds"] = time.monotonic() - started
            if self.learning is not None:
                try:
                    self.learning.record_result(request, result)
                except Exception:
                    self.learning.capture_gap = True
            if self.canvas is not None:
                try:
                    self._canvas_status = self.canvas.observe(request, result)
                except Exception:
                    self._canvas_status = {"status": "observer-unavailable"}
            return result
        except ChannelSubmissionHeld:
            return None  # A concurrent /new held it before durable admission.

    def workspace_status(self):
        return {"observation_scope": "current-workspace-generation",
                "status": ("unavailable" if self._workspace_unavailable else
                           "closed" if self.workspace is not None and self.workspace.closed else
                           "opened" if self.workspace is not None else "not-configured"),
                "reason_code": self._workspace_unavailable, "automatic_retry": False,
                **({"failure_trace": self._workspace_failure_trace}
                   if self._workspace_failure_trace else {})}

    def _close_typing(self):
        if self._typing is not None:
            self._typing.close()
            status = getattr(self._typing, "status", None)
            self._typing = None
            if callable(status):
                try:
                    self._last_activity = status()
                except Exception:
                    self._last_activity = {"state": "closed", "observation": "unavailable"}

    def _close_backend(self):
        self._close_typing()
        if self._backend is not None:
            close = getattr(self._backend, "close", None)
            if close is not None:
                close()  # Failure retains the instance and fences the next admission.
            self._backend = None

    def _cleanup_after_answer(self, *, recovery=False):
        if self._backend is None and self._typing is None:
            return
        if not recovery:
            self._cleanup_attempts += 1
        try:
            self._close_backend()
        except Exception as error:
            from .failure_trace import failure_trace

            self._cleanup_trace = failure_trace(error)
            raise
        self._cleanup_attempts = 0
        self._cleanup_trace = None

    def close(self):
        controller = getattr(self.learning, "controller", None)
        if controller is not None:
            controller.close()
        if self.state == "closed":
            return
        if self.state == "new":
            if self.channel_receiver is not None:
                self.channel_receiver.close()
            self.state = "closed"
            return
        self._owner()
        self.state = "stopping"
        background_error = None
        if self.background_schedule is not None:
            try:
                self.background_schedule.close()
            except Exception as error:
                background_error = error
        if self.channel_receiver is not None:
            self.channel_receiver.close()
        self._close_backend()
        if self.workspace is not None:
            self.workspace.close()  # Failure retains workspace and its custody lease.
        if self._root_lease is not None:
            self._root_lease.release()
            self._root_lease = None
        if background_error is not None:
            self.state = "close-unverified"
            raise background_error
        self.state = "closed"
