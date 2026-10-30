"""Detached host decisions reuse runtime admission and native child custody."""
import time
import json
from types import SimpleNamespace

from ahc.application.runtime_service import RuntimeService
from ahc.domain.agent_backend import AgentTurnRequest
from ahc.domain.sessions import ContractError, VirtualSessionIdentity
from ahc.domain.skill_values import digest_json
from ahc.domain.working_sets import WorkingSetSnapshot
from ahc.infrastructure.working_repository import WorkingSetNotFound
from .host_model_calls import HostModelCalls


class HostLearningCalls:
    def __init__(self, store, config):
        from ahc.adapters.trusted_codex_launch import require_tool_free_configuration
        require_tool_free_configuration(config.backend["codex_home"], config.workspace["cwd"])
        self.store, self.config = store, config

    def run(self, source_session, source_operation, binding, prompt, *, cancelled):
        """A delivered/terminal source is never readmitted as a child parent."""
        from .foreground_config_native import parse_trusted_foreground_config
        from .trusted_codex_backend import TrustedCodexBackendFactory

        if cancelled():
            raise ContractError("host learning call cancelled before admission")
        source = self.store.read_current(source_session.ref.virtual_session_id)
        if source.session != source_session or source.status != "active":
            raise ContractError("host learning source session superseded")
        identity = digest_json([source_session.to_dict(), source_operation, binding["job_id"]])
        session = VirtualSessionIdentity.initial(
            "reflection-" + identity, source_session.agent_id, source_session.route_id)
        operation = "reflection-" + identity
        # A host job is once-only even if the controller loses its return path.
        try:
            self.store.read_current(session.ref.virtual_session_id)
        except WorkingSetNotFound:
            pass
        else:
            raise ContractError("host learning job already exists; do not replay")
        current = self.store.create(WorkingSetSnapshot.initial(session, source.goal))
        document = self.config.to_dict()
        document["session"] = session.to_dict()
        # Detached decisions have no channel receiver or channel control surface.
        for field in ("learning", "agent_context", "channels", "telegram", "discord",
                      "telegram_polling", "discord_gateway", "channel_control", "channel_controls"):
            document.pop(field, None)
        for field in ("time_budget", "overload_recovery", "context_policy", "wrap_up_timeout"):
            document["backend"].pop(field, None)
        config = parse_trusted_foreground_config(document)
        owner = TrustedCodexBackendFactory(self.store, config)
        workspace = SimpleNamespace(identity=self.store.reserve_workspace_generation(current, config.workspace_id))
        artifact, configuration = owner.binding(None, child=True)
        request = AgentTurnRequest(
            session=session, operation_id=operation, goal_digest=current.goal.digest,
            input_text="Run the reserved host reflection decision.",
            revision_digests=(), artifact_digest=artifact,
            configuration_digest=configuration, workspace_scope=config.workspace_id,
            deadline=time.monotonic() + binding["timeout_seconds"],
            effective_policy=config.effective_policy)
        def decide(bound):
            if cancelled():
                raise ContractError("host learning call cancelled before dispatch")
            return HostModelCalls(owner, bound, workspace).run(
                binding["job_id"], prompt, model=binding["model"],
                reasoning_effort=binding["effort"],
                timeout_seconds=max(0.001, min(binding["timeout_seconds"],
                    (binding["deadline_ms"] - time.time_ns() // 1000000) / 1000)),
                cancelled=cancelled)
        # The coordinator performs a host action; the native model execution is
        # independently accounted for as its durable child, not as user work.
        result = RuntimeService(self.store, None, local_reply=decide).execute(current, request)
        if not result.result or not result.result.get("accepted") or result.result.get("unresolved_effects"):
            raise ContractError("host learning outcome unverified; do not replay")
        return result.result["output_text"]

    def recover(self, source_session):
        """Reconcile retained detached custody only; never repeat its model decision."""
        pending = getattr(self.store, "_host_model_pending_closes", [])
        for service in tuple(pending):
            service.close()
            pending.remove(service)
        from .trusted_codex_backend import TrustedCodexBackendFactory
        from ahc.domain.skill_values import canonical_json
        with self.store._read() as db:
            exists = db.execute("SELECT 1 FROM sqlite_master WHERE name='runtime_learning_sources'").fetchone()
            rows = [] if exists is None else db.execute(
                "SELECT operation_id,binding_json,decision_json FROM runtime_learning_sources "
                "WHERE session_json=? AND binding_json IS NOT NULL AND state IN ('unknown','claimed') "
                "AND coalesce(json_extract(decision_json,'$.cleanup'),'')!='confirmed' "
                "ORDER BY created_ms LIMIT 128",
                (canonical_json(source_session.to_dict()).decode(),)).fetchall()
            if db.execute("SELECT 1 FROM sqlite_master WHERE name='runtime_learning_calls'").fetchone():
                rows = list(rows) + list(db.execute(
                    "SELECT source_operation AS operation_id,binding_json,NULL AS decision_json FROM runtime_learning_calls "
                    "WHERE session_json=? AND state IN ('unknown','claimed')",
                    (canonical_json(source_session.to_dict()).decode(),)))
        owner = None
        for row in rows:
            binding = json.loads(row["binding_json"])
            identity = digest_json([source_session.to_dict(), row["operation_id"], binding["job_id"]])
            session = VirtualSessionIdentity.initial(
                "reflection-" + identity, source_session.agent_id, source_session.route_id)
            try:
                self.store.read_current(session.ref.virtual_session_id)
            except WorkingSetNotFound:
                continue  # The original claim did not get as far as host admission.
            if owner is None:
                owner = TrustedCodexBackendFactory(self.store, self.config)
            owner.recover(session, self.config.workspace_id)
            def settled(db):
                if db.execute("SELECT 1 FROM sqlite_master WHERE name='runtime_learning_calls'").fetchone():
                    db.execute("UPDATE runtime_learning_calls SET state='cleanup-confirmed' WHERE job_id=? "
                               "AND session_json=? AND state IN ('unknown','claimed')",
                               (binding['job_id'], canonical_json(source_session.to_dict()).decode()))
                current = db.execute("SELECT decision_json FROM runtime_learning_sources WHERE operation_id=?",
                                     (row["operation_id"],)).fetchone()
                detail = json.loads(current[0]) if current[0] else {}
                detail["cleanup"] = "confirmed"
                db.execute("UPDATE runtime_learning_sources SET decision_json=? WHERE operation_id=? "
                           "AND state IN ('unknown','claimed')",
                           (canonical_json(detail).decode(), row["operation_id"]))
            self.store._mutate(settled)
        return True
