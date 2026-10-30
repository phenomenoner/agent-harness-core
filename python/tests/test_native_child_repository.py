from dataclasses import replace

import pytest
from test_child_work import envelope, setup

from ahc.domain.sessions import ContractError
from ahc.infrastructure.child_work_repository import ChildWorkRepository
from ahc.infrastructure.native_child_repository import NativeChildRepository
from ahc.infrastructure.runtime_repository import RuntimeRepository


def prepared(tmp_path):
    store, parent, clock, children = setup(tmp_path)
    child = replace(envelope(parent), execution_kind="minion")
    children.admit(child)
    children.claim(parent, child.child_id, "token")
    native = NativeChildRepository(children)
    binding = {key: "sha256:" + "a" * 64 for key in (
        "capability_digest", "tool_surface_digest", "schema_bundle_digest",
        "catalog_digest", "profile_digest", "submission_digest")}
    binding.update(server_id="aar", runtime_generation=1, principal_id="host", session_id="native")
    return store, parent, clock, native, binding


def receipt(state="running", revision=1):
    return {"operation": {"type": "operation", "value": "native-op"},
            "state": state, "certainty": "certain", "record_revision": revision,
            "reconciliation_required": False, "result": None, "artifacts": [], "failure": None}


def test_reservation_and_terminal_survive_reopen_no_replay(tmp_path):
    store, parent, _, native, binding = prepared(tmp_path)
    native.reserve(parent, "child", "token", binding)
    reopened = NativeChildRepository(
        ChildWorkRepository(RuntimeRepository(store.root), clock=lambda: 100))
    pending = reopened.read(parent, "child")
    assert pending["submission_reserved"] and pending["operation_id"] is None
    with pytest.raises(ContractError, match="do not replay"):
        reopened.reserve(parent, "child", "token", binding)
    reopened.observe(parent, "child", "token", receipt())
    reopened.observe(parent, "child", "token", receipt("succeeded", 2))
    assert native.read(parent, "child")["receipt"].terminal
    with pytest.raises(ContractError, match="regresses"):
        reopened.observe(parent, "child", "token", receipt("running", 3))
    assert native.read(parent, "child")["receipt"].state == "succeeded"


@pytest.mark.parametrize("token", [None, "wrong"])
def test_wrong_claim_cannot_reserve(tmp_path, token):
    _, parent, _, native, binding = prepared(tmp_path)
    with pytest.raises(ContractError):
        native.reserve(parent, "child", token, binding)
    assert native.read(parent, "child") is None


def test_parent_retirement_preserves_evidence_but_blocks_observation(tmp_path):
    store, parent, _, native, binding = prepared(tmp_path)
    native.reserve(parent, "child", "token", binding)
    native.observe(parent, "child", "token", receipt())
    store._mutate(lambda connection: ChildWorkRepository.retire_parent(
        connection, parent.operation_id))
    with pytest.raises(ContractError):
        native.observe(parent, "child", "token", receipt("succeeded", 2))
    assert native.read(parent, "child")["operation_id"] == "native-op"
    assert native.children.read(parent)[0]["state"] == "unknown"


def test_subagent_custody_cannot_accept_minion(tmp_path):
    from ahc.infrastructure.child_backend_custody import ChildBackendCustody

    store, parent, _, _, _ = prepared(tmp_path)
    with pytest.raises(ContractError):
        ChildBackendCustody(store, replace(envelope(parent), execution_kind="minion"))


def test_expired_claim_and_wrong_parent_preserve_reserved_evidence(tmp_path):
    _, parent, clock, native, binding = prepared(tmp_path)
    native.reserve(parent, "child", "token", binding)
    foreign = replace(parent, session=replace(parent.session, agent_id="other"))
    with pytest.raises(ContractError):
        native.read(foreign, "child")
    clock[0] = 201
    with pytest.raises(ContractError, match="not current"):
        native.observe(parent, "child", "token", receipt())
    assert native.read(parent, "child")["operation_id"] is None


def test_ordinary_subagent_port_rejects_minion_before_factory(tmp_path):
    from test_child_dispatch import state

    from ahc.adapters.child_agent_backend import ChildAgentBackend

    _, parent, clock, _, child = state(tmp_path)
    child = replace(child, execution_kind="minion")
    request = replace(parent, operation_id=child.child_id, input_text=child.task_text,
                      deadline=0.05, capabilities=(), backend_thread=None)
    with pytest.raises(ContractError, match="binding invalid"):
        ChildAgentBackend(child, request, lambda *a, **kw: pytest.fail("must not start"),
            parent_request=parent, monotonic=lambda: 0, wall_ms=lambda: clock[0])


def test_database_backup_retains_native_identity_and_receipt(tmp_path):
    import sqlite3

    from ahc.infrastructure.runtime_backup import backup_runtime_database, inspect_runtime_backup

    source = tmp_path / "source"
    source.mkdir()
    store, parent, _, native, binding = prepared(source)
    native.reserve(parent, "child", "token", binding)
    native.observe(parent, "child", "token", receipt("succeeded", 2))
    destination = tmp_path / "backup"
    manifest = backup_runtime_database(store, destination)
    assert inspect_runtime_backup(destination) == manifest
    with sqlite3.connect(destination / "runtime.sqlite") as connection:
        row = connection.execute("SELECT operation_id,receipt_json FROM runtime_native_children "
                                 "WHERE child_id='child'").fetchone()
        assert row == ("native-op", native.read(parent, "child")["receipt"].payload_json)
