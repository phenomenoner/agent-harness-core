"""One admitted Codex wire conversation; launch and policy admission belong to callers.

This client never constructs a transport, discovers auth or enables a runtime backend.
"""

import base64
import json
import time
from dataclasses import replace
from math import isfinite

from ahc.domain.agent_backend import AgentTurnObservation, AgentTurnRequest
from ahc.domain.sessions import ContractError, _identifier
from ahc.domain.skill_values import digest, digest_json

from .codex_bundle import CODEX_PIN
from .codex_linux_bundle import LINUX_CODEX_PIN
from .codex_protocol import (
    initialize_message,
    thread_message,
    turn_message,
    validate_initialize_response,
)
from .codex_reasoning import client_configuration, validate_effort


class CodexProtocolClient:
    def __init__(
        self,
        transport,
        *,
        cwd,
        model,
        reasoning_effort=None,
        clock=time.monotonic,
        cancelled=lambda: False,
        workspace_tools=None,
        permissions=None,
        policy_configuration_digest=None,
        effective_policy=None,
        service_tier=None,
        launch_configuration_digest=None,
        approval_port=None,
        wall_clock=time.time,
        target=CODEX_PIN["target"],
        developer_instructions=None,
        environment_instructions=None,
        tool_gate=None,
        answer_seal=None,
        turn_media=None,
        close_transport=True,
        isolated_provider_auth=False,
        startup_timeout=None,
        request_timeout=None,
        settlement_timeout=None,
        settlement_within_task_deadline=False,
        wrap_up_timeout=None,
        context_policy=None,
        context_window=None,
        overload_recovery=None,
        recovery_wait=time.sleep,
    ):
        from .codex_context_policy import compact_limit, context_policy as validate_context
        from ahc.domain.backend_recovery import recovery_policy

        self.context_policy = validate_context(context_policy)
        self.context_window = context_window
        self.compact_token_limit = compact_limit(self.context_policy, context_window)
        self.overload_recovery = recovery_policy(overload_recovery)
        if not callable(recovery_wait):
            raise ContractError("backend recovery wait invalid")
        self.recovery_wait = recovery_wait
        if target == CODEX_PIN["target"]:
            pin, self.platform_os = CODEX_PIN, "windows"
        elif target == LINUX_CODEX_PIN["target"]:
            pin, self.platform_os = LINUX_CODEX_PIN, "linux"
        else:
            raise ContractError("unsupported Codex artifact target")
        self.target = target
        if (type(isolated_provider_auth) is not bool
                or isolated_provider_auth and self.platform_os != "linux"):
            raise ContractError("isolated provider authentication requires Linux")
        self.isolated_provider_auth = isolated_provider_auth
        if developer_instructions is not None and (
            type(developer_instructions) is not str or not developer_instructions.strip()
            or len(developer_instructions) > 12000
            or any(0xD800 <= ord(c) <= 0xDFFF for c in developer_instructions)
        ):
            raise ContractError("Codex developer instructions invalid")
        self.developer_instructions = developer_instructions
        if environment_instructions is not None and (
            type(environment_instructions) is not str or not environment_instructions.strip()
            or len(environment_instructions) > 4096
            or any(0xD800 <= ord(c) <= 0xDFFF for c in environment_instructions)
        ):
            raise ContractError("Codex environment instructions invalid")
        self.environment_instructions = environment_instructions
        self.turn_context = None
        self.tool_gate = tool_gate
        if answer_seal is not None and not callable(answer_seal):
            raise ContractError("Codex answer seal callback invalid")
        self.answer_seal = answer_seal
        if turn_media is not None and not callable(turn_media):
            raise ContractError("Codex turn media callback invalid")
        self.turn_media = turn_media
        if type(close_transport) is not bool:
            raise ContractError("Codex transport cleanup ownership invalid")
        self.close_transport = close_transport
        phase_budgets = {'startup_timeout': startup_timeout, 'request_timeout': request_timeout,
                         'settlement_timeout': settlement_timeout,
                         'wrap_up_timeout': wrap_up_timeout}
        for name, value in phase_budgets.items():
            if value is not None and (type(value) not in (int, float) or not isfinite(value)
                                      or not 0 < value <= 86400):
                raise ContractError(f'Codex {name} invalid')
        self.startup_timeout, self.request_timeout = startup_timeout, request_timeout
        self.settlement_timeout = settlement_timeout
        self.wrap_up_timeout = wrap_up_timeout
        if type(settlement_within_task_deadline) is not bool:
            raise ContractError('Codex settlement deadline policy invalid')
        self.settlement_within_task_deadline = settlement_within_task_deadline
        self.executable_sha256 = next(
            member["sha256"] for member in pin["members"] if member["path"] == pin["entrypoint"]
        )
        thread_message(
            cwd=cwd,
            model=model,
            permissions=permissions,
            effective_policy=effective_policy,
            service_tier=service_tier,
        )
        if (permissions is None) != (policy_configuration_digest is None):
            raise ContractError("named policy requires profile and configuration digest")
        if permissions is not None and effective_policy is not None:
            raise ContractError("named profile and effective policy are ambiguous")
        if permissions is not None:
            digest(policy_configuration_digest, "policy configuration digest")
        if effective_policy is not None:
            from ahc.domain.runtime_policy import EffectivePolicy

            if type(effective_policy) is not EffectivePolicy:
                raise ContractError("effective policy invalid")
        if not callable(wall_clock):
            raise ContractError("wall clock invalid")
        self._permissions = permissions
        self._effective_policy = effective_policy
        self.service_tier = service_tier
        self._approval_port = approval_port
        self._wall_clock = wall_clock
        self.transport = transport
        self.cwd = cwd
        self.model = model
        self.reasoning_effort = validate_effort(reasoning_effort)
        self.clock = clock
        self.cancelled = cancelled
        self.used = False
        self.workspace_tools = workspace_tools
        self._bridge = None
        self._approval_bridge = None
        self._tool_names = () if workspace_tools is None else tuple(workspace_tools.capabilities())
        self._workspace_identity = (
            None if workspace_tools is None else workspace_tools.workspace.identity
        )
        self.configuration_digest = client_configuration(
            cwd, model, self.target, self.executable_sha256, reasoning_effort)
        selected_budgets = {key: value for key, value in phase_budgets.items() if value is not None}
        if selected_budgets:
            self.configuration_digest = digest_json({'base': self.configuration_digest,
                                                    'phase_budgets': selected_budgets})
        if settlement_within_task_deadline:
            self.configuration_digest = digest_json({'base': self.configuration_digest,
                'settlement_within_task_deadline': True})
        if isolated_provider_auth:
            self.configuration_digest = digest_json({
                "base": self.configuration_digest, "isolated_provider_auth": True,
            })
        if developer_instructions is not None:
            self.configuration_digest = digest_json({
                "base": self.configuration_digest, "developer_instructions": developer_instructions,
            })
        if permissions is not None:
            self.configuration_digest = digest_json(
                {
                    "base": self.configuration_digest,
                    "permissions": permissions,
                    "policy_configuration_digest": policy_configuration_digest,
                    "experimental": True,
                }
            )
        if environment_instructions is not None:
            self.configuration_digest = digest_json({
                "base": self.configuration_digest,
                "environment_instructions": environment_instructions,
            })
        if effective_policy is not None:
            from .codex_approval import effective_configuration_digest

            self.configuration_digest = effective_configuration_digest(
                self.configuration_digest, effective_policy
            )
        if service_tier is not None:
            self.configuration_digest = digest_json(
                {"base": self.configuration_digest, "service_tier": service_tier}
            )
        if launch_configuration_digest is not None:
            digest(launch_configuration_digest, "launch configuration digest")
            self.configuration_digest = digest_json(
                {"base": self.configuration_digest, "launch": launch_configuration_digest}
            )
        if self.context_policy is not None or self.overload_recovery is not None:
            self.configuration_digest = digest_json({
                "base": self.configuration_digest, "context_policy": self.context_policy,
                "context_window": self.context_window, "overload_recovery": self.overload_recovery,
            })
        if workspace_tools is not None:
            self.configuration_digest = digest_json(
                {
                    "base": self.configuration_digest,
                    "identity": self._workspace_identity.to_dict(),
                    "capabilities": self._tool_names,
                    "experimental": True,
                }
            )

    def run_admitted(self, request):
        if type(request) is not AgentTurnRequest or self.used:
            raise ContractError("Codex client request invalid or instance already consumed")
        self.used = True
        self.request = request
        from .codex_budget_wrap_up import BudgetWrapUp

        self._wrap_up = (None if self.wrap_up_timeout is None or request.time_budget is not None else
                         BudgetWrapUp(request.effective_deadline, self.wrap_up_timeout, self.clock()))
        self.admissions = self.accepted = self.frames = 0
        self.submitted = False
        self.thread = self.turn = None
        self._control_deadline = self._settlement_deadline = None
        self._native_terminal_received = False
        self._observed_tool_activity = False
        self._recovery_effect_items = set()
        self._recovery_seen_turns = set()
        self._recovery_waiting = False
        self._deferred_capacity_error = None
        self._recovery_record = (None if self.overload_recovery is None else {
            "attempts": 0, "state": "not-needed", "last_failure": None})
        self._compaction_count = 0
        self._compaction_pending = self._compaction_before = self._compaction_after = None
        self._pending_control_deadlines = {}
        self._startup_deadline = (None if self.startup_timeout is None
                                  else min(request.effective_deadline, self.clock() + self.startup_timeout))
        from .codex_native_collaboration import NativeCollaboration

        self._native_collaboration = NativeCollaboration(self)
        self.token_observation = None
        if request.time_budget is not None:
            request.time_budget.seed_snapshot()
        self._diagnostic = None
        self._advisory_diagnostic = None
        self._diagnostic_phase, self._diagnostic_method = "admission", "none"
        from .codex_diagnostics import remember

        try:
            try:
                from .memory_read_budget import memory_read_budget

                with memory_read_budget(request.deadline_source, clock=self.clock,
                                        cancelled=self.cancelled):
                    outcome = self._run()
            except Exception as error:
                remember(self, error=error)
                outcome = self._result(
                    "unknown" if self.submitted else "not-submitted", "protocol-or-transport"
                )
            if outcome.disposition == "completed":
                try:
                    if self.answer_seal is not None:
                        self._diagnostic_phase = "answer-seal"
                        self.answer_seal(outcome)
                except Exception:
                    remember(self, "answer-seal")
                    outcome = self._result("unknown", "answer-seal-unverified", unresolved=True)
        finally:
            try:
                if self.close_transport:
                    self.transport.close()
                    if self._diagnostic is not None:
                        self._diagnostic["cleanup"] = "client-close-returned"
            except Exception:
                self._diagnostic_phase = "cleanup"
                remember(self, "cleanup")
                self._diagnostic["cleanup"] = "unverified"
                if outcome.disposition == "completed":
                    outcome = replace(outcome, cleanup_pending=True)
                else:
                    outcome = self._result("unknown", "cleanup-unverified", unresolved=True)
        if self._recovery_record is not None:
            if outcome.disposition == "completed" and self._recovery_record["attempts"]:
                self._recovery_record["state"] = "recovered"
            outcome = replace(outcome, backend_recovery=dict(self._recovery_record))
        return replace(outcome, backend_diagnostic=self._diagnostic or self._advisory_diagnostic)

    def _result(self, status, reason, output="", *, unresolved=None):
        if self._recovery_record is not None:
            if status == "completed" and self._recovery_record["attempts"]:
                self._recovery_record["state"] = "recovered"
            elif status != "completed" and self._recovery_record["state"] in {
                    "continuing", "cooldown"}:
                self._recovery_record["state"] = "blocked"
        report = (status == "completed" and self._bridge is not None
                  and self._bridge.reporting)
        return AgentTurnObservation(
            self.request,
            status,
            reason,
            self.admissions,
            self.accepted,
            self.thread,
            self.turn,
            output,
            artifacts=(self._bridge.artifacts
                       if status == "completed" and self._bridge and not report else ()),
            reply_to_message_id=(self._bridge.reply_to_message_id
                                  if status == "completed" and self._bridge and not report
                                  else None),
            confirmed_work=(self._bridge.confirmed_work
                            if (status != "completed" or report) and self._bridge else None),
            token_observation=self.token_observation,
            backend_recovery=(None if self._recovery_record is None else
                              dict(self._recovery_record)),
            unresolved_effects=((self.submitted and status != "completed")
                                or bool(report and self._bridge.unresolved))
            if unresolved is None
            else unresolved,
            final_report=report,
        )

    def _stopped(self):
        from .codex_diagnostics import remember

        if self.clock() >= self.io_deadline:
            remember(self, "deadline", code=self.deadline_code())
            return True
        if self.cancelled():
            remember(self, "cancelled")
            return True
        return False

    @property
    def effects_open(self):
        return (not self._native_terminal_received and not self._recovery_waiting
                and self.clock() < self.request.effective_deadline
                and not self.cancelled())

    @property
    def io_deadline(self):
        deadline = self._settlement_deadline or self.request.effective_deadline
        budget = self.request.time_budget
        if (budget is not None and budget.status()["state"] == "finalizing"
                and self._wrap_up is None and not self._native_terminal_received):
            from .codex_budget_wrap_up import BudgetWrapUp
            self._wrap_up = BudgetWrapUp(deadline, budget.policy["wrap_up_seconds"], self.clock())
            self._wrap_up.notice_at = self.clock()
            self._wrap_up.poll(self)
        if self._control_deadline is not None:
            deadline = min(deadline, self._control_deadline)
        if self.accepted == 0 and self._startup_deadline is not None:
            deadline = min(deadline, self._startup_deadline)
        return deadline

    def deadline_code(self):
        if self._settlement_deadline is None and self.clock() >= self.request.effective_deadline:
            return 'deadline'
        if (self.accepted == 0 and self._startup_deadline is not None
                and self.clock() >= self._startup_deadline):
            return 'backend-startup-timeout'
        if self._control_deadline is not None:
            return 'control-request-timeout'
        return ('terminal-settlement-timeout'
                if self._settlement_deadline is not None else 'deadline')

    def begin_settlement(self, event):
        """Only an exact successful native terminal receipt may extend readback time."""
        params = event.get('params')
        turn = params.get('turn') if type(params) is dict else None
        if (set(event) == {'method', 'params'} and event.get('method') == 'turn/completed'
                and params.get('threadId') == self.thread and type(turn) is dict
                and turn.get('id') == self.turn and turn.get('status') == 'completed'
                and turn.get('error') is None):
            if self._native_terminal_received:
                return
            self._native_terminal_received = True
            if self.settlement_timeout is not None:
                deadline = self.clock() + self.settlement_timeout
                self._settlement_deadline = (min(deadline, self.request.effective_deadline)
                    if self.settlement_within_task_deadline else deadline)

    def _send(self, message):
        from .codex_diagnostics import method

        if (self._native_terminal_received and message.get('method') is not None
                and message['method'] not in {'thread/turns/list', 'turn/interrupt'}):
            raise ContractError('Codex new request after native completion')
        if message.get('method') in {'turn/start', 'turn/steer'} and not self.effects_open:
            raise ContractError('Codex execution authority expired or closed')
        self._diagnostic_phase = "send"
        self._diagnostic_method = method(message.get("method", "response"))
        deadline = self.io_deadline
        if self.request.time_budget is not None:
            cap = None if self.request_timeout is None else self.clock() + self.request_timeout
            def active_deadline():
                if cap is not None and self.clock() >= cap:
                    return cap
                value = self.io_deadline
                return min(value, cap) if cap is not None else value
            wire_deadline = active_deadline
            control_deadline = cap
        else:
            if self.request_timeout is not None:
                deadline = min(deadline, self.clock() + self.request_timeout)
            wire_deadline = control_deadline = deadline
        if 'id' in message and 'method' in message:
            self._pending_control_deadlines[message['id']] = control_deadline
        self.transport.send(message, deadline=wire_deadline)

    def _receive(self):
        from .codex_client_wire import receive

        return receive(self)

    def _response(self, identifier, allowed_notifications=()):
        from .codex_client_wire import response
        from .codex_diagnostics import remember
        from .codex_transport import CodexTransportError

        previous = self._control_deadline
        self._control_deadline = self._pending_control_deadlines.pop(identifier, self.io_deadline)
        try:
            return response(self, identifier, allowed_notifications)
        except CodexTransportError as error:
            if error.transport_error_type == 'deadline':
                remember(self, 'deadline', code=self.deadline_code())
            raise
        finally:
            self._control_deadline = previous

    @staticmethod
    def _validate_turn_media(value):
        if type(value) is not list or len(value) > 16:
            raise ContractError("Codex turn media projection invalid")
        for item in value:
            if type(item) is not dict or type(item.get("type")) is not str:
                raise ContractError("Codex turn media projection invalid")
            if item["type"] == "text":
                if (set(item) != {"type", "text"} or type(item["text"]) is not str
                        or not item["text"] or len(item["text"]) > 4096
                        or any(0xD800 <= ord(c) <= 0xDFFF for c in item["text"])):
                    raise ContractError("Codex turn media text invalid")
                continue
            if item["type"] != "image" or set(item) != {"type", "url"}:
                raise ContractError("Codex turn media projection invalid")
            url = item["url"]
            if type(url) is not str or "," not in url:
                raise ContractError("Codex turn media image invalid")
            header, encoded = url.split(",", 1)
            if header not in {
                "data:image/png;base64",
                "data:image/jpeg;base64",
                "data:image/gif;base64",
                "data:image/webp;base64",
            } or not encoded:
                raise ContractError("Codex turn media image invalid")
            try:
                base64.b64decode(encoded, validate=True)
            except (ValueError, TypeError):
                raise ContractError("Codex turn media image invalid") from None
        return value

    def _run(self):
        request = self.request
        if (
            request.artifact_digest != self.executable_sha256
            or request.configuration_digest != self.configuration_digest
            or request.capabilities != self._tool_names
            or request.upstream_request_limit is not None
            or request.effective_policy != self._effective_policy
        ):
            return self._result("unsupported", "binding-or-envelope-unsupported")
        if self.workspace_tools is not None and (
            not self._tool_names
            or request.backend_thread is not None
            or self.workspace_tools.workspace.identity != self._workspace_identity
            or request.session != self._workspace_identity.session
            or request.workspace_scope != self._workspace_identity.workspace_id
            or tuple(self.workspace_tools.capabilities()) != self._tool_names
        ):
            return self._result("unsupported", "workspace-binding-unsupported")
        if self._stopped():
            return self._result("not-submitted", "deadline-or-cancel")
        self.admissions = 1
        context = self.turn_context(request) if self.turn_context is not None else None
        if context is not None and (type(context) is not str or len(context) > 24000
                                   or any(0xD800 <= ord(c) <= 0xDFFF for c in context)):
            raise ContractError("Codex turn context invalid")
        if self._stopped():
            return self._result("not-submitted", "deadline-or-cancel")
        media = []
        if self.turn_media is not None:
            media = self._validate_turn_media(self.turn_media(request))
        initialize = initialize_message()
        if (self.workspace_tools is not None or self._permissions is not None
                or self.isolated_provider_auth):
            initialize["params"]["capabilities"]["experimentalApi"] = True
        self._send(initialize)
        response, _ = self._response(1)
        validate_initialize_response({"id": 1, "result": response})
        if response["platformOs"] != self.platform_os:
            raise ContractError("Codex initialized platform differs from pinned target")
        self._send({"method": "initialized", "params": {}})
        if self._stopped():
            return self._result("not-submitted", "deadline-or-cancel")
        start = thread_message(
            cwd=self.cwd,
            model=self.model,
            backend_thread=request.backend_thread,
            permissions=self._permissions,
            effective_policy=self._effective_policy,
            service_tier=self.service_tier,
        )
        if self.isolated_provider_auth:
            start["params"]["environments"] = []
        if self.compact_token_limit is not None:
            start["params"]["config"] = {
                "model_auto_compact_token_limit": self.compact_token_limit}
        if self.workspace_tools is not None:
            from .codex_workspace_tools import CodexWorkspaceTools

            start["params"]["dynamicTools"] = CodexWorkspaceTools.describe(
                request, self.workspace_tools
            )
        if self.developer_instructions is not None:
            start["params"]["developerInstructions"] = self.developer_instructions
        if self.environment_instructions is not None:
            start["params"]["developerInstructions"] = self.environment_instructions + (
                "\n\n" + self.developer_instructions if self.developer_instructions else "")
        if self._wrap_up is not None:
            instructions = start["params"].get("developerInstructions", "")
            start["params"]["developerInstructions"] = (
                instructions + "\n\n" if instructions else "") + self._wrap_up.INSTRUCTIONS
        self._send(start)
        response, pending = self._response(2, (
            "thread/started", "mcpServer/startupStatus/updated",
            "thread/status/changed", "thread/settings/updated",
        ))
        from .codex_approval import validate_thread_configuration

        validate_thread_configuration(self, response)
        thread = response.get("thread")
        if type(thread) is not dict:
            raise ContractError("Codex thread response invalid")
        self.thread = _identifier(thread.get("id"), "backend thread")
        if request.backend_thread is not None and self.thread != request.backend_thread:
            raise ContractError("Codex resumed a different thread")
        for event in pending:
            if event.get("method") != "thread/started":
                from .codex_status_notifications import consume_status

                if not consume_status(self, event):
                    raise ContractError("Codex pending status notification invalid")
                continue
            if (event.get("params", {}).get("thread", {}).get("id") != self.thread
                    or getattr(self, "_thread_started_seen", False)):
                raise ContractError("Codex thread notification mismatch")
            self._thread_started_seen = True
        if self._stopped():
            return self._result("not-submitted", "deadline-or-cancel")
        message = turn_message(
            request,
            self.thread,
            cwd=self.cwd,
            permissions=self._permissions,
            effective_policy=self._effective_policy,
            reasoning_effort=self.reasoning_effort,
            service_tier=self.service_tier,
        )
        if self.isolated_provider_auth:
            message["params"]["environments"] = []
        if context:
            message["params"]["input"].insert(0, {
                "type": "text",
                "text": context + "\n\nEND AHC BACKGROUND CONTEXT\n\nCURRENT USER REQUEST:\n",
            })
        if media:
            message["params"]["input"].extend(media)
        from .codex_transport import CodexStdioTransport

        try:
            frame_bytes = len(json.dumps(message, allow_nan=False).encode("utf-8")) + 1
        except (TypeError, ValueError, RecursionError):
            raise ContractError("Codex turn input invalid") from None
        if frame_bytes > CodexStdioTransport.MAX_OUTBOUND_FRAME:
            raise ContractError("Codex turn input exceeds outbound frame")
        self.submitted = True  # A failed write may have submitted; never retry it.
        self._send(message)
        response, pending = self._response(
            3,
            (
                "turn/started",
                "turn/completed",
                "item/started",
                "item/completed",
                "item/agentMessage/delta",
            ),
        )
        turn = response.get("turn")
        if type(turn) is not dict or turn.get("status") not in (
            "inProgress",
            "completed",
            "interrupted",
            "failed",
        ):
            raise ContractError("Codex turn response invalid")
        self.turn = _identifier(turn.get("id"), "backend turn")
        self.accepted = 1
        activity = getattr(self, "activity_event", None)
        if callable(activity):
            try:
                activity("模型處理中")
            except Exception:
                pass
        if self._effective_policy is not None:
            from .codex_approval import CodexApprovalBridge

            self._approval_bridge = CodexApprovalBridge(
                request,
                self._effective_policy,
                self.thread,
                self.turn,
                clock=self.clock,
                wall_clock=self._wall_clock,
                approval_port=self._approval_port,
            )
        if self.workspace_tools is not None:
            self._bridge = CodexWorkspaceTools(
                request, self.workspace_tools, self.thread, self.turn, clock=self.clock,
                gate=self.tool_gate
            )
        from .codex_terminal_readback import receive_terminal

        return receive_terminal(self, pending)
