"""Foreground admission, batch join, failure fencing and durable replay composition."""

from dataclasses import replace
from threading import Barrier, Event

import pytest
import test_foreground_runtime as fixture

from ahc.cli import foreground_children as composition
from ahc.domain.agent_backend import AgentTurnObservation
from ahc.domain.sessions import ContractError
from ahc.infrastructure.child_dispatch_repository import ChildDispatchRepository
from ahc.infrastructure.threaded_runtime_repository import ThreadedRuntimeRepository


@pytest.mark.parametrize("mode", ["success", "cancel", "cleanup"])
def test_foreground_batch_failure_fencing_and_reopen(tmp_path, monkeypatch, mode):
    monkeypatch.setattr(fixture, "RuntimeRepository", ThreadedRuntimeRepository)
    runtime, requests, _, request = fixture.setup(tmp_path)
    children = composition.ForegroundChildren(runtime.store, object())
    runtime.children = children
    stopped, failure = Event(), Event()
    if mode == "cleanup":
        failure.set()
    barrier = Barrier(2, timeout=5) if mode != "cancel" else None
    runs, closes, parent_closes, results = [], [], [], []

    class Factory:
        def __init__(self, owner, ledger, envelope, token, identity):
            self.ledger, self.envelope, self.token = ledger, envelope, token

        def port(self):
            owner = self

            class Port:
                def run(self, envelope, *, cancelled):
                    with owner.ledger.backend_admission(envelope, owner.token):
                        runs.append(envelope.child_id)
                        if barrier is not None:
                            barrier.wait()
                        else:
                            stopped.set()
                            assert cancelled()
                        return "child result"

                def close(self):
                    closes.append(owner.envelope.child_id)
                    if failure.is_set() and owner.envelope.task_text == "first":
                        raise ContractError("controlled cleanup failure")

            return Port()

    monkeypatch.setattr(composition, "TrustedCodexChildFactory", Factory)

    def factory(bound, tools, *, cancelled):
        class Backend:
            def invoke(self, admitted):
                result = tools.invoke("workspace.child_batch", {
                    "expected": tools.workspace.identity.to_dict(),
                    "concurrency": 1 if mode == "cancel" else 2,
                    "children": [{"child_id": name, "task_text": name, "timeout_ms": 10000}
                                 for name in ("first", "second")]})
                results.append(result)
                return AgentTurnObservation(admitted, "completed", "fixture", 1, 1,
                    output_text="parent result", unresolved_effects=result["unresolved_effects"])

            def close(self):
                parent_closes.append(True)

        return composition.ChildEnabledBackend(Backend(), children, bound,
                                               tools.workspace, cancelled)

    runtime.backend_factory = factory
    runtime.start()
    bound = replace(request, capabilities=runtime.tools.capabilities())
    assert "workspace.child_batch" in bound.capabilities
    ledger = ChildDispatchRepository(runtime.store, bound)
    requests.append(bound)
    try:
        outcome = runtime.step(cancelled=stopped.is_set)
        assert outcome.result["accepted"] == (mode == "success")
        if mode == "cleanup":
            assert runtime._backend is not None and not parent_closes
            assert runtime.step() is None
            assert runtime.cleanup_status()["state"] == "exhausted"
            failure.clear()
        else:
            assert runtime._backend is None and children.children is None
    finally:
        failure.clear()
        runtime.close()
    assert runtime.state == "closed" and parent_closes == [True]
    assert len(runs) == (1 if mode == "cancel" else 2)
    # First child: dispatch cleanup, two retained-parent attempts, explicit close.
    # Successfully closed sibling must never be retried.
    assert len(closes) == (5 if mode == "cleanup" else len(runs))
    if mode == "cleanup":
        assert sorted(closes.count(child) for child in set(closes)) == [1, 4]
    # Fresh repository sees the final parent/child states. Executing the exact
    # request again must return the persisted outcome without constructing a backend.
    reopened = ThreadedRuntimeRepository(runtime.store.root)
    restored = ChildDispatchRepository(reopened, bound).read(ledger.parent)
    expected = {"success": ["terminal", "terminal"],
                "cancel": ["unknown", "cancelled"], "cleanup": ["unknown", "terminal"]}
    assert [row["state"] for row in restored] == expected[mode]
    from ahc.application.runtime_service import RuntimeService

    class Never:
        def invoke(self, request):
            pytest.fail("durable parent replay invoked a backend")

    current = reopened.read_current(bound.session.ref.virtual_session_id)
    replay = RuntimeService(reopened, Never()).execute_with_skills(
        current, bound, tools=bound.capabilities)
    assert replay.result == outcome.result
