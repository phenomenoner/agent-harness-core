"""Persistent interpreter custody coexists with per-turn backend custody."""

import threading
from contextvars import copy_context
from dataclasses import replace

import pytest
from test_backend_custody import Runner, setup

from ahc.domain.sessions import ContractError
from ahc.domain.skill_values import digest_json
from ahc.infrastructure.backend_custody import BackendCustody
from ahc.infrastructure.working_repository import WorkingWriterBusy


def test_roles_coexist_but_same_role_excludes_and_cleanup_cannot_cross(tmp_path):
    repo, backend, backend_lease, workspace, unit = setup(tmp_path)
    interpreter = BackendCustody(repo, resource_role="python-workspace")
    lease = interpreter.acquire(workspace.session, workspace.workspace_id)
    python_unit = replace(unit, unit="ahc-backend-" + "c" * 32 + ".service")
    try:
        assert backend_lease.key == digest_json(
            {"session": workspace.session.to_dict(), "workspace": workspace.workspace_id}
        )
        assert lease.key != backend_lease.key
        with pytest.raises(WorkingWriterBusy):
            BackendCustody(repo, resource_role="python-workspace").acquire(
                workspace.session, workspace.workspace_id
            )
        interpreter.record_created(lease, workspace, python_unit)
        python_runner = Runner(python_unit)
        assert interpreter.recover(lease, python_runner) == (python_unit,)
        assert backend._held(backend_lease)[0][2] == unit
        backend_runner = Runner(unit)
        assert backend.recover(backend_lease, backend_runner) == (unit,)
    finally:
        lease.close()
        backend_lease.close()


def test_direct_workspace_step_can_clean_its_interpreter_without_reentering_lock(tmp_path):
    repo, _, backend_lease, workspace, unit = setup(tmp_path)
    interpreter = BackendCustody(repo, resource_role="python-workspace")
    lease = interpreter.acquire(workspace.session, workspace.workspace_id)
    python_unit = replace(unit, unit="ahc-backend-" + "d" * 32 + ".service")
    try:
        interpreter.record_created(lease, workspace, python_unit)
        with repo.workspace_execution_lock(workspace):
            assert interpreter.recover(lease, Runner(python_unit)) == (python_unit,)
            with pytest.raises(WorkingWriterBusy):
                with repo.backend_execution_lock(workspace.session, workspace.workspace_id):
                    pass
    finally:
        lease.close()
        backend_lease.close()


def test_startup_grant_only_allows_own_interpreter_not_backend_or_foreign_context(tmp_path):
    repo, _, lease, workspace, _ = setup(tmp_path)
    lease.close()
    saved = []
    with pytest.raises(ContractError):
        with repo.workspace_startup_scope(workspace):
            pass
    with repo.execution_lock(), repo.workspace_startup_scope(workspace):
        with repo.backend_execution_lock(
            workspace.session, workspace.workspace_id, resource_role="python-workspace"
        ):
            pass
        with pytest.raises(WorkingWriterBusy):
            with repo.backend_execution_lock(workspace.session, workspace.workspace_id):
                pass
        with pytest.raises(ContractError):
            with repo.backend_execution_lock(
                workspace.session, "foreign", resource_role="python-workspace"
            ):
                pass
        with pytest.raises(WorkingWriterBusy):
            with repo.execution_lock():
                pass
        saved.append(copy_context())
        result = []

        def copied():
            try:
                with repo.backend_execution_lock(
                    workspace.session, workspace.workspace_id, resource_role="python-workspace"
                ):
                    result.append("unexpected")
            except ContractError:
                result.append("rejected")

        thread = threading.Thread(target=lambda: saved[0].run(copied))
        thread.start()
        thread.join(2)
        assert not thread.is_alive() and result == ["rejected"]
    with pytest.raises(ContractError):
        saved[0].run(
            lambda: repo.backend_execution_lock(
                workspace.session, workspace.workspace_id, resource_role="python-workspace"
            ).__enter__()
        )
