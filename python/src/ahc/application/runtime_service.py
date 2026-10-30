"""One durable admission, one backend call, and AHC-owned terminal acceptance."""

from contextlib import AbstractContextManager
from dataclasses import replace
from typing import Protocol

from ahc.domain.agent_backend import AgentTurnRequest
from ahc.domain.runtime_turn import RuntimeTurn, accept_observation
from ahc.domain.sessions import ContractError
from ahc.domain.skill_models import SelectionRequest
from ahc.domain.working_sets import WorkingSetSnapshot
from ahc.domain.workspace import WorkspaceCheckpoint

from .agent_backend_port import AgentBackend


class RuntimeStore(Protocol):
    def execution_lock(self) -> AbstractContextManager[None]: ...
    def workspace_turn_scope(self, request: AgentTurnRequest) -> AbstractContextManager[None]: ...
    def begin(
        self, expected: WorkingSetSnapshot, request: AgentTurnRequest
    ) -> tuple[RuntimeTurn, bool]: ...
    def finish(self, request: AgentTurnRequest, result: dict[str, object]) -> RuntimeTurn: ...
    def seal_observation(self, request: AgentTurnRequest, result: dict[str, object]) -> None: ...
    def save_workspace_checkpoint(
        self, expected: WorkingSetSnapshot, checkpoint: WorkspaceCheckpoint
    ) -> str: ...


class RuntimeService:
    def __init__(self, store: RuntimeStore, backend: AgentBackend, *, selector=None,
                 local_reply=None) -> None:
        self.store = store
        self.backend = backend
        self.selector = selector
        self.local_reply = local_reply

    def open_workspace(self, expected, workspace_id, factory, *, checkpoint_digest=None):
        from .runtime_workspace import RuntimeWorkspace

        return RuntimeWorkspace.open(self.store, expected, workspace_id, factory, checkpoint_digest)

    def open_docker_workspace(
        self, expected, workspace_id, runner, *, checkpoint_digest=None, timeout=5
    ):
        from .custodied_workspace import open_custodied_workspace

        return open_custodied_workspace(
            self.store,
            expected,
            workspace_id,
            runner,
            checkpoint_digest=checkpoint_digest,
            timeout=timeout,
        )

    def checkpoint_workspace(self, expected, workspace, names):
        """Bind a live port's portable checkpoint to the existing runtime owner."""
        from .runtime_workspace import RuntimeWorkspace

        if isinstance(workspace, RuntimeWorkspace):
            if workspace.store is not self.store:
                raise ContractError("workspace belongs to a different runtime store")
            return workspace.checkpoint(workspace.identity, names, runtime_expected=expected).digest
        with self.store.execution_lock():
            identity = workspace.identity
            if identity.session != expected.session:
                raise ContractError("workspace differs from runtime session")
            checkpoint = workspace.checkpoint(identity, names)
            return self.store.save_workspace_checkpoint(expected, checkpoint)

    def execute_with_skills(self, expected, request, *, roles=("procedure",), tools=(),
                            lazy_disclosure=False):
        if self.selector is not None:
            selection = self.selector.select_and_pin(
                SelectionRequest(
                    request.session,
                    request.operation_id,
                    roles,
                    request.session.agent_id,
                    tuple(sorted(tools)),
                    request.input_text,
                    request.goal_digest,
                ),
                episode_id=request.operation_id,
                goal=expected.goal,
            )
            revisions = tuple(
                dict.fromkeys(entry.revision_digest for entry in selection["manifest"].entries)
            )
            request = replace(
                request,
                revision_digests=revisions,
                # Keep selection/episode custody while native discovery reads bodies on demand.
                # Backends without the admitted discovery pair retain legacy disclosure.
                skill_bodies=() if lazy_disclosure and {
                    "workspace.skills_list", "workspace.skills_read",
                }.issubset(set(tools) & set(request.capabilities)) else tuple(
                    (digest, selection["disclosure"][digest]) for digest in revisions
                ),
            )
        return self.execute(expected, request)

    def execute(self, expected: WorkingSetSnapshot, request: AgentTurnRequest) -> RuntimeTurn:
        scoped = getattr(self.store, "turn_execution_lock", None)
        with scoped(request) if callable(scoped) else self.store.execution_lock():
            return self._execute_locked(expected, request)

    def _execute_locked(self, expected, request):
        turn, created = self.store.begin(expected, request)
        if not created:
            # Even a fresh driver cannot re-submit a durable pending operation.
            return turn
        cleanup = getattr(self.backend, "cleanup_after_answer", None)
        accepted = False
        try:
            with self.store.workspace_turn_scope(request):
                try:
                    reply = None if self.local_reply is None else self.local_reply(request)
                    if reply is None:
                        result = accept_observation(request, self.backend.invoke(request))
                    else:
                        if (type(reply) is not str or not reply or len(reply) > 4096
                                or any(0xD800 <= ord(c) <= 0xDFFF for c in reply)):
                            raise ContractError("local command reply invalid")
                        result = {"result_schema_version": "ahc.runtime-result.v2",
                                  "disposition": "completed", "output_text": reply,
                                  "admissions": 0, "accepted_backend_turns": 0,
                                  "vendor_usage": None, "unresolved_effects": False,
                                  "accepted": True, "execution_kind": "local-command",
                                  "backend_completion": {"thread": None, "turn": None}}
                    accepted = (result.get("accepted") is True
                                or result.get("report_accepted") is True)
                    if accepted:
                        self.store.seal_observation(request, result)
                        ready = getattr(self.backend, "answer_ready", None)
                        if callable(ready):
                            try:
                                ready()
                            except Exception:
                                pass  # Optional presentation never invalidates the answer seal.
                except Exception as error:
                    from .failure_trace import failure_trace

                    result = {
                        "disposition": "unknown",
                        "output_text": "",
                        "admissions": None,
                        "accepted_backend_turns": None,
                        "vendor_usage": None,
                        "unresolved_effects": True,
                        "accepted": False,
                        "failure_trace": failure_trace(error),
                    }
                finally:
                    if callable(cleanup):
                        try:
                            cleanup()
                        except Exception as error:
                            if accepted:
                                from .failure_trace import failure_trace

                                result = {
                                    **result,
                                    "cleanup_pending": True,
                                    "cleanup_trace": failure_trace(error),
                                }
        except Exception as error:
            from .failure_trace import failure_trace

            result = {
                "disposition": "unknown",
                "output_text": "",
                "admissions": None,
                "accepted_backend_turns": None,
                "vendor_usage": None,
                "unresolved_effects": True,
                "accepted": False,
                "failure_trace": failure_trace(error),
            }
        return self.store.finish(request, result)
