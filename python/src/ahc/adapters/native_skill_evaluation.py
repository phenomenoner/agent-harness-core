"""Trusted composition for disposable, resource-bounded skill evaluation workspaces."""

from contextlib import contextmanager
from dataclasses import asdict, dataclass

from ahc.application.runtime_workspace import RuntimeWorkspace
from ahc.domain.native_resource_limits import NativeResourceLimits
from ahc.domain.runtime_policy import EffectivePolicy
from ahc.domain.sessions import ContractError
from ahc.domain.skill_values import digest_json
from ahc.infrastructure.process.linux_workspace_boundary import LinuxWorkspaceBoundary

from .native_process_runner import NativeProcessRunner
from .native_python_workspace import NativePythonWorkspace


@dataclass(frozen=True)
class NativeSkillEvaluationFactory:
    store: object
    expected: object
    skill_root: object
    boundary: LinuxWorkspaceBoundary
    limits: NativeResourceLimits
    policy: EffectivePolicy
    environment_digest: str
    interpreter: str
    lifetime_seconds: int

    def __post_init__(self):
        self._admit()

    @property
    def session(self):
        return self.expected.session

    def _admit(self):
        if (
            type(self.boundary) is not LinuxWorkspaceBoundary
            or self.boundary.scratch_limits is None
            or type(self.limits) is not NativeResourceLimits
            or self.limits.swap_bytes
            or type(self.policy) is not EffectivePolicy
            or type(self.lifetime_seconds) is not int
            or not 1 <= self.lifetime_seconds <= 3600
        ):
            raise ContractError(
                "evaluation requires ephemeral isolation and finite resource limits"
            )
        self.policy.require_workspace_grant(self.environment_digest)
        for root in (self.store.root, self.skill_root):
            if not any(
                root.resolve().is_relative_to(protected)
                for protected in self.boundary.protected_roots
            ):
                raise ContractError("evaluation runtime and learning stores must be protected")

    @property
    def binding_digest(self):
        self._admit()
        return digest_json(
            {
                "schema": "ahc.native-skill-evaluation.v1",
                "session": self.session.to_dict(),
                "runtime_root": str(self.store.root),
                "skill_root": str(self.skill_root),
                "isolation_binary": str(self.boundary.executable),
                "isolation_sha256": self.boundary.sha256,
                "workspace": str(self.boundary.workspace),
                "readonly_roots": list(map(str, self.boundary.readonly_roots)),
                "protected_roots": list(map(str, self.boundary.protected_roots)),
                "scratch_limits": asdict(self.boundary.scratch_limits),
                "limits": asdict(self.limits),
                "policy": self.policy.digest,
                "environment": self.environment_digest,
                "interpreter": self.interpreter,
                "lifetime_seconds": self.lifetime_seconds,
            }
        )

    def open(self, workspace_id):
        self._admit()
        return self._open(workspace_id)

    @contextmanager
    def _open(self, workspace_id):
        from ahc.infrastructure.runtime_execution_scope import evaluation_workspace

        self._admit()
        with evaluation_workspace(self.store, self.session, workspace_id) as remaining:
            expected = self.store.read_current(self.session.ref.virtual_session_id)
            if expected.session != self.session or expected.status != "active":
                raise ContractError("evaluation session is no longer current")
            lifetime = self.lifetime_seconds
            if remaining is not None:
                lifetime = min(lifetime, remaining)
            with self._workspace(expected, workspace_id, lifetime) as workspace:
                yield workspace

    def _workspace(self, expected, workspace_id, lifetime):
        owner = NativeProcessRunner(
            cwd=str(self.boundary.workspace),
            environment={},
            persistent=True,
            boundary=self.boundary,
            resource_limits=self.limits,
        )

        def create(identity, checkpoint):
            return NativePythonWorkspace(
                self.store,
                owner,
                identity,
                effective_policy=self.policy,
                environment_digest=self.environment_digest,
                cwd=str(self.boundary.workspace),
                interpreter=self.interpreter,
                user=None,
                timeout=min(30, lifetime),
                lifetime_seconds=lifetime,
                checkpoint=checkpoint,
            )

        return RuntimeWorkspace.open(self.store, expected, workspace_id, create)
