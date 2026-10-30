"""Independent work keeps exclusive workspace/route and offline-recovery fences."""

import pytest

from ahc.domain.channel_ingress import ChannelRoute
from ahc.domain.sessions import ContractError, VirtualSessionIdentity
from ahc.infrastructure.channel_outbox import ChannelOutbox
from ahc.infrastructure.runtime_execution_scope import workspace_turn
from ahc.infrastructure.threaded_runtime_repository import ThreadedRuntimeRepository
from ahc.infrastructure.working_repository import WorkingWriterBusy


def owner(name):
    return VirtualSessionIdentity.initial(name, "agent", "route")


def test_independent_scopes_overlap_but_matching_scope_and_restore_remain_exclusive(tmp_path):
    store = ThreadedRuntimeRepository.initialize(tmp_path / "runtime")
    foreground, background = owner("foreground"), owner("background")
    with store.execution_lock(session=background, workspace_id="background"):
        with store.execution_lock(session=foreground, workspace_id="foreground"):
            pass
        with pytest.raises(WorkingWriterBusy):
            with store.execution_lock(session=background, workspace_id="background"):
                pytest.fail("matching model/workspace lease was duplicated")
        with pytest.raises(WorkingWriterBusy):
            with store.execution_lock(restore=True):
                pytest.fail("offline restore overlapped active work")
    with store.execution_lock(restore=True):
        with pytest.raises(WorkingWriterBusy):
            with store.execution_lock(session=foreground, workspace_id="foreground"):
                pytest.fail("new work crossed the restore fence")


def test_scoped_lease_cannot_authorize_a_different_workbench(tmp_path):
    from types import SimpleNamespace

    store = ThreadedRuntimeRepository.initialize(tmp_path / "runtime")
    with store.execution_lock(session=owner("background"), workspace_id="background"):
        request = SimpleNamespace(session=owner("foreground"), workspace_scope="foreground")
        with pytest.raises(ContractError):
            with workspace_turn(store, request):
                pytest.fail("wrong execution scope acquired a workspace grant")


def test_foreground_delivery_does_not_wait_for_a_background_model(tmp_path):
    store = ThreadedRuntimeRepository.initialize(tmp_path / "runtime")
    route = ChannelRoute("telegram", "bot", "user", "user", owner("foreground"))
    outbox = ChannelOutbox(store)
    with store.execution_lock(session=owner("background"), workspace_id="background"):
        with outbox.route_delivery_scope(route):
            with pytest.raises(WorkingWriterBusy):
                with outbox.route_delivery_scope(route):
                    pytest.fail("same-route delivery/reconciliation overlapped")
        with pytest.raises(WorkingWriterBusy):
            with store.execution_lock(restore=True):
                pytest.fail("restore was not fenced")
