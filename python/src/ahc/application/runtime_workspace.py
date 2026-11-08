"""Runtime-owned admission around one isolated workspace port, without replay."""

from contextlib import contextmanager, nullcontext
from threading import local

from ahc.domain.sessions import ContractError
from ahc.domain.workspace import (
    WorkspaceCodeRejected, WorkspaceOperationNotStarted, workspace_failure_projection,
)
from ahc.infrastructure.working_repository import WorkingSetConflict, WorkingWriterBusy


def _failure(error, phase):
    code = "internal"
    for kind, label in ((WorkingWriterBusy, "writer-busy"), (WorkingSetConflict, "owner-conflict"),
                        (ContractError, "contract"), (OSError, "io")):
        if isinstance(error, kind):
            code = label
            break
    return {"phase": phase, "code": code}


class RuntimeWorkspace:
    def __init__(self, store, port):
        self.store = store
        self.port = port
        self.closed = False
        self.unavailable = False
        self._execution_observation = local()
        from .workspace_jobs import WorkspaceJobs

        self.jobs = WorkspaceJobs(self)

    @classmethod
    def open(cls, store, expected, workspace_id, factory, checkpoint_digest=None):
        with store.workspace_creation_lock(expected.session, workspace_id):
            checkpoint = None
            if checkpoint_digest is not None:
                checkpoint = store.read_workspace_checkpoint(
                    expected.session, workspace_id, checkpoint_digest
                )
                if checkpoint is None:
                    raise ContractError("requested workspace checkpoint is unavailable")
            identity = store.reserve_workspace_generation(expected, workspace_id, checkpoint)
            with store.workspace_startup_scope(identity):
                port = factory(identity, checkpoint)
                if port.identity != identity:
                    port.close()
                    raise ContractError("workspace factory returned a different owner")
            return cls(store, port)

    @property
    def identity(self):
        jobs = getattr(self, "jobs", None)
        identity = jobs.identity if jobs is not None else None
        return identity if identity is not None else self.port.identity

    @property
    def execution_started(self):
        return getattr(self._execution_observation, "current", {}).get("started")

    @property
    def last_failure(self):
        return getattr(self._execution_observation, "current", {}).get("failure")

    @property
    def last_recovery(self):
        return getattr(self._execution_observation, "current", {}).get("recovery")

    @contextmanager
    def turn_scope(self, deadline):
        scope = getattr(self.port, "turn_scope", None)
        with scope(deadline) if callable(scope) else nullcontext():
            try:
                yield
            finally:
                self.jobs.finish_turn()

    def _require(self, expected):
        if self.closed or self.unavailable or expected != self.identity:
            raise ContractError("workspace reference is stale or closed")
        return self.store.require_workspace(expected)

    @contextmanager
    def _observation_admission(self, expected, runtime_expected=None):
        admitted = False
        try:
            with self.store.workspace_execution_lock(expected):
                snapshot = self._require(expected)
                if runtime_expected is not None and snapshot != runtime_expected:
                    raise ContractError("checkpoint runtime snapshot is stale")
                admitted = True
                yield snapshot
        except Exception as error:
            if not admitted:
                raise WorkspaceOperationNotStarted(
                    "workspace operation rejected before interpreter admission") from error
            raise  # A lost reply or save receipt may follow an actual effect.

    def execute(self, expected, code):
        observation = {"started": False, "failure": None, "recovery": None, "phase": "admission"}
        self._execution_observation.current = observation
        try:
            with self.store.workspace_execution_lock(expected):
                return self._execute_admitted(expected, code, observation)
        except WorkspaceCodeRejected:
            raise
        except BaseException as error:
            if observation["failure"] is None:
                self._record_execution_failure(observation, error, observation["phase"])
            raise
        finally:
            # The synchronous tool consumes this on the calling thread. Rejected
            # competitors and nested calls cannot replace this invocation's record.
            self._execution_observation.current = observation

    def _execute_admitted(self, expected, code, observation):
        phase = "admission"
        try:
            self._require(expected)
            validate = getattr(self.port, "validate_execute", None)
            if callable(validate):
                validate(code)  # Validate the actual frame before reserving execution.
            successor = self.store.begin_workspace_step(expected)
            phase = observation["phase"] = "execute"
            observation["started"] = True
            result = self.port.execute(expected, code)
            phase = observation["phase"] = "result-validation"
            if self.port.identity != successor or result.get("identity") != successor.to_dict():
                raise ContractError("workspace result identity differs from admission")
            phase = observation["phase"] = "result-save"
            try:
                self.store.finish_workspace_step(successor)
            except WorkingWriterBusy:
                # One exact readback, then at most one same-owner receipt CAS.
                # Interpreter execution is never repeated. Both repository calls
                # revalidate the active session/generation/revision themselves.
                state = self.store.observe_workspace_step(successor)
                if state == "executing":
                    self.store.finish_workspace_step(successor)
                observation["recovery"] = "receipt-reconciled"
            return result
        except WorkspaceCodeRejected:
            raise
        except BaseException as error:
            # Fence and retain cleanup custody before leaving the admitted scope.
            self._record_execution_failure(observation, error, phase)
            raise

    def _record_execution_failure(self, observation, error, phase):
        original = (workspace_failure_projection(getattr(self.port, "last_failure", None))
                    if phase == "execute" else {})
        failure = observation["failure"] = original or _failure(error, phase)
        if observation["started"]:
            self.unavailable = True
            try:
                self.close()
                failure["cleanup"] = "confirmed"
            except BaseException:
                failure["cleanup"] = "unverified"

    def inspect(self, expected, names):
        from ahc.domain.workspace_inspection import inspection_result

        with self._observation_admission(expected):
            try:
                result = self.port.inspect(expected, names)
                if self.port.identity != expected or result.get("identity") != expected.to_dict():
                    raise ContractError("workspace inspection changed identity")
                return inspection_result(expected, names, result.get("variables"))
            except Exception:
                self.unavailable = True
                try:
                    self.close()
                except Exception:
                    pass  # Keep exact cleanup custody; no replacement without proof.
                raise

    def checkpoint(self, expected, names, *, runtime_expected=None):
        from ahc.domain.workspace import CheckpointNotSaved, CheckpointValueRejected

        with self._observation_admission(expected, runtime_expected) as snapshot:
            try:
                checkpoint = self.port.checkpoint(expected, names)
                if checkpoint.origin != expected or self.port.identity != expected:
                    raise ContractError("workspace checkpoint differs from admission")
            except CheckpointValueRejected:
                raise  # Exact known value rejection, interpreter remains serviceable.
            except Exception:
                self.unavailable = True
                try:
                    self.close()
                except Exception:
                    pass  # Keep the port and exact cleanup custody, without another attempt.
                raise CheckpointNotSaved("checkpoint capture failed before saving") from None
            self.store.save_workspace_checkpoint(snapshot, checkpoint)
            return checkpoint

    def close(self):
        if not self.closed:
            self.port.close()
            self.closed = True

    def capture_artifact(self, expected, name, publisher, media_type, lineage=()):
        from ahc.domain.artifacts import ArtifactManifest, ArtifactNotPublished
        from ahc.domain.workspace_inspection import inspection_names

        publishing = False
        try:
            inspection_names((name,))
            if getattr(publisher, "store", None) is not self.store:
                raise ContractError("artifact publisher belongs to a different runtime store")
            with self.store.workspace_execution_lock(expected):
                self._require(expected)
                export = getattr(self.port, "export_artifact", None)
                if not callable(export):
                    raise ContractError("workspace artifact export unavailable")
                try:
                    content = export(expected, name)
                except ContractError:
                    raise  # A known unsupported value does not damage the interpreter.
                except Exception:
                    self.unavailable = True
                    try:
                        self.close()
                    except Exception:
                        pass  # Retain the port and its custody for the owner's cleanup.
                    raise
                if self.port.identity != expected:
                    self.unavailable = True
                    try:
                        self.close()
                    except Exception:
                        pass
                    raise ContractError("workspace artifact export changed identity")
                # Only this call can publish. Earlier faults cannot mean a lost write.
                publishing = True
                manifest = publisher.publish(expected, content, media_type, lineage=lineage)
                if type(manifest) is not ArtifactManifest or manifest.producer != expected:
                    raise ContractError("workspace artifact publication binding invalid")
                return manifest
        except ArtifactNotPublished:
            raise
        except Exception as error:
            if not publishing:
                reason = ("artifact-export-unavailable" if self.unavailable
                          else "artifact-export-rejected")
                raise ArtifactNotPublished(reason) from error
            raise  # A publisher may have committed before losing its receipt.

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        self.close()
