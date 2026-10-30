from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from threading import Barrier

import pytest
from test_child_work import envelope, setup

from ahc.domain.sessions import ContractError
from ahc.infrastructure.child_work_repository import ChildWorkRepository
from ahc.infrastructure.native_budget_repository import NativeBudgetRepository
from ahc.infrastructure.runtime_repository import RuntimeRepository
from ahc.infrastructure.working_repository import WorkingSetConflict, WorkingWriterBusy


def budget():
    return {"model_requests": 1, "input_tokens": 20, "output_tokens": 10,
            "child_operations": 0, "artifact_bytes": 100}


def prepare(tmp_path):
    store, parent, clock, children = setup(tmp_path)
    for name in ("one", "two"):
        children.admit(replace(envelope(parent, child_id=name), execution_kind="minion"))
        children.claim(parent, name, "token")
    return store, parent, clock, children


def test_reservation_survives_cancel_and_reopen_without_refund(tmp_path):
    store, parent, _, children = prepare(tmp_path)
    native = NativeBudgetRepository(children)
    native.reserve(parent, "one", "token", budget(), budget())
    with pytest.raises(ContractError, match="do not replay"):
        native.reserve(parent, "one", "token", budget(), budget())
    children.cancel(parent, "one")
    reopened = NativeBudgetRepository(ChildWorkRepository(RuntimeRepository(store.root),
                                                         clock=lambda: 100))
    with pytest.raises(ContractError, match="exhausted"):
        reopened.reserve(parent, "two", "token", budget(), budget())
    with store._read() as connection:
        assert connection.execute("SELECT count(*) FROM runtime_native_budgets").fetchone()[0] == 1


def test_concurrent_children_cannot_oversubscribe_parent(tmp_path):
    store, parent, _, _ = prepare(tmp_path)
    barrier = Barrier(2)

    def reserve(name):
        children = ChildWorkRepository(RuntimeRepository(store.root), clock=lambda: 100)
        barrier.wait(timeout=5)
        try:
            NativeBudgetRepository(children).reserve(parent, name, "token", budget(), budget())
            return "reserved"
        except ContractError as error:
            assert "exhausted" in str(error)
            return "exhausted"
        except WorkingWriterBusy:
            return "busy"

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(reserve, ("one", "two")))
    assert results.count("reserved") == 1
    loser = ("one", "two")[0 if results[0] != "reserved" else 1]
    children = ChildWorkRepository(RuntimeRepository(store.root), clock=lambda: 100)
    with pytest.raises(ContractError, match="exhausted"):
        NativeBudgetRepository(children).reserve(parent, loser, "token", budget(), budget())


@pytest.mark.parametrize("mode", ["policy", "token", "expired", "foreign", "negative"])
def test_invalid_reservation_preserves_existing_budget(tmp_path, mode):
    store, parent, clock, children = prepare(tmp_path)
    native = NativeBudgetRepository(children)
    limits = {key: value * 2 for key, value in budget().items()}
    native.reserve(parent, "one", "token", limits, budget())
    token = "token"
    allocation = budget()
    if mode == "policy":
        limits["model_requests"] = 3
    if mode == "token":
        token = "wrong"
    if mode == "expired":
        clock[0] = 201
    if mode == "foreign":
        parent = replace(parent, session=replace(parent.session, agent_id="foreign"))
    if mode == "negative":
        allocation["input_tokens"] = -1
    with pytest.raises((ContractError, WorkingSetConflict)):
        native.reserve(parent, "two", token, limits, allocation)
    with store._read() as connection:
        assert connection.execute("SELECT count(*) FROM runtime_native_budgets").fetchone()[0] == 1
