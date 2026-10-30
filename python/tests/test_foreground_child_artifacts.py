from types import SimpleNamespace

import pytest
from test_foreground_config_v2 import valid_config
from test_workspace_tools import setup

from ahc.cli import foreground_children as composition
from ahc.cli.foreground_config_v2 import parse_foreground_config_v2
from ahc.domain.agent_backend import AgentTurnRequest
from ahc.domain.child_result import ChildResult
from ahc.domain.result_artifacts import ResultArtifact
from ahc.infrastructure.child_artifacts import ChildArtifacts
from ahc.infrastructure.threaded_runtime_repository import ThreadedRuntimeRepository


@pytest.mark.parametrize("batch", [False, True])
def test_opted_in_dispatch_allocates_and_returns_owned_artifacts(tmp_path, monkeypatch, batch):
    base, original, _ = setup(tmp_path)
    store = ThreadedRuntimeRepository(base.root)
    current = store.read_current("session")
    request = AgentTurnRequest(current.session, "parent", current.goal.digest, "work", (),
                               "a" * 64, "b" * 64, "work", 100)
    raw = valid_config()
    raw["child_limits"] = {"max_children": 2, "max_concurrency": 2,
                           "max_artifacts": 1, "artifact_bytes": 100}
    owner = SimpleNamespace(config=parse_foreground_config_v2(raw))
    workspace = SimpleNamespace(store=store, identity=original.identity)
    children = composition.ForegroundChildren(store, owner)
    children.bind(request, workspace, lambda: False)
    children.children.clock = lambda: 100
    children.children.monotonic_clock = lambda: 0
    calls = []

    class Factory:
        def __init__(self, owner, worker, envelope, token, identity):
            self.worker, self.envelope, self.token = worker, envelope, token

        def port(self):
            factory = self

            class Port:
                def run(self, item, *, cancelled):
                    calls.append(item.child_id)
                    with factory.worker.backend_admission(item, factory.token):
                        manifest = ChildArtifacts(factory.worker).publish(item.parent,
                            item.child_id, factory.token, workspace.identity,
                            item.child_id.encode(), "text/plain")
                    return ChildResult("report", (ResultArtifact(manifest.artifact_id,
                                                                  "report.txt"),))

                def close(self):
                    pass

            return Port()

    monkeypatch.setattr(composition, "TrustedCodexChildFactory", Factory)
    items = [{"child_id": name, "task_text": "report", "timeout_ms": 1000}
             for name in ("first", "second")]
    try:
        with store.execution_lock():
            store.begin(current, request)
            with store.workspace_turn_scope(request):
                if batch:
                    args = {"expected": workspace.identity.to_dict(), "children": items,
                            "concurrency": 2}
                    result = children.invoke_batch(workspace.identity, args)
                    assert result["disposition"] == "observed"
                    assert children.invoke_batch(workspace.identity, args) == result
                    results = result["result"]
                else:
                    results = [children.invoke(workspace.identity,
                        {"expected": workspace.identity.to_dict(), **item}) for item in items]
                assert len({r["result"]["result"]["artifacts"][0]["artifact_id"]
                            for r in results}) == 2
        children.close()
        assert len(calls) == 2
        with store._read() as connection:
            row = connection.execute("SELECT count(*) FROM runtime_child_artifacts").fetchone()
            assert row[0] == 2
    finally:
        original.close()
