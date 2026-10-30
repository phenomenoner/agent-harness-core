from dataclasses import replace

import pytest
from test_backend_custody import Runner
from test_child_dispatch import state

from ahc.application.custodied_child_backend import CustodiedChildBackend
from ahc.domain.backend_identity import LinuxUnitIdentity
from ahc.domain.sessions import ContractError
from ahc.infrastructure.backend_custody import BackendCustody
from ahc.infrastructure.child_backend_custody import ChildBackendCustody


@pytest.mark.parametrize("mode", ["success", "attach-failed", "cancel-create", "wrong-token"])
def test_child_process_lifecycle_leaves_parent_owner_intact(tmp_path, mode):
    repo, parent, _, children, envelope = state(tmp_path)
    workspace = repo.reserve_workspace_generation(
        repo.read_current("session"), parent.workspace_scope)
    children.admit(envelope)
    children.claim(envelope.parent, envelope.child_id, "token")
    request = replace(parent, operation_id=envelope.child_id, input_text=envelope.task_text)
    parent_custody = BackendCustody(repo)
    parent_lease = parent_custody.acquire(parent.session, parent.workspace_scope)
    parent_unit = LinuxUnitIdentity("AHC-Codex-Ubuntu-24.04",
        "12345678-1234-1234-1234-123456789abc", "ahc-backend-" + "a" * 32 + ".service", "b" * 32)
    child_unit = replace(parent_unit, unit="ahc-backend-" + "c" * 32 + ".service")
    parent_custody.record_created(parent_lease, workspace, parent_unit)
    effects = []

    class ProcessRunner(Runner):
        def create(self, command, *, lifetime_seconds):
            effects.append("create")
            assert command == ("fixture",) and lifetime_seconds == 1
            if mode == "cancel-create":
                children.cancel(envelope.parent, envelope.child_id)
            return self.unit

        def close(self):
            effects.append("runner-close")

    runner = ProcessRunner(child_unit)
    custody = ChildBackendCustody(repo, envelope)

    class Client:
        def invoke(self, bound):
            assert bound == request
            effects.append("invoke")
            return "observed"

        def close(self):
            effects.append("client-close")

    def attach(unit):
        assert unit == child_unit
        effects.append("attach")
        if mode == "attach-failed":
            raise RuntimeError("fixture attach failure")
        return Client()

    backend = CustodiedChildBackend(children, envelope,
        "other" if mode == "wrong-token" else "token", request, workspace, runner,
        ("fixture",), 60, attach, custody=custody)
    try:
        with repo.execution_lock(), repo.workspace_turn_scope(parent):
            if mode == "success":
                assert backend.invoke(request) == "observed"
            else:
                with pytest.raises((ContractError, RuntimeError)):
                    backend.invoke(request)
            backend.close()
        assert backend.closed and backend.lease is None
        assert [row[2] for row in parent_custody._held(parent_lease)] == [parent_unit]
        assert ("create" in effects) is (mode != "wrong-token")
        assert ("invoke" in effects) is (mode == "success")
        assert runner.stopped is (mode != "wrong-token")
        assert repo.read_turn(parent.session, envelope.child_id) is None
    finally:
        parent_lease.close()
