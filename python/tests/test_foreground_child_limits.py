"""Owner limits apply before dispatch across ordinary single and batch tools."""

from types import SimpleNamespace

import pytest
from test_child_worker_repository import prepared
from test_foreground_config_v2 import valid_config

from ahc.cli import foreground_children as composition
from ahc.cli.foreground_config_v2 import parse_foreground_config_v2
from ahc.domain.sessions import ContractError
from ahc.domain.workspace import WorkspaceIdentity


@pytest.mark.parametrize("count,size", [(True, 10), (-1, 10), (1, -1), (0, 10), (1, 0)])
def test_invalid_artifact_allocations_reject(count, size):
    raw = valid_config()
    raw["child_limits"] = {"max_children": 2, "max_concurrency": 1,
                           "max_artifacts": count, "artifact_bytes": size}
    with pytest.raises(ContractError):
        parse_foreground_config_v2(raw)


@pytest.mark.parametrize("limits", [None, {}, {"max_children": True, "max_concurrency": 1},
    {"max_children": -1, "max_concurrency": 1}, {"max_children": 1, "max_concurrency": 0},
    {"max_children": 1, "max_concurrency": 1, "vendor_tokens": 20}])
def test_invalid_or_unsupported_budget_rejects_configuration(limits):
    raw = valid_config()
    raw["child_limits"] = limits
    with pytest.raises(ContractError):
        parse_foreground_config_v2(raw)


def test_limits_are_previewed_and_bound_to_configuration_digest():
    raw = valid_config()
    old = parse_foreground_config_v2(raw)
    raw["child_limits"] = {"max_children": 2, "max_concurrency": 1}
    new = parse_foreground_config_v2(raw)
    assert old.configuration_digest != new.configuration_digest
    assert new.preview()["child_limits"] == raw["child_limits"]
    assert "child_limits" not in old.to_dict()


@pytest.mark.parametrize("limit", [0, 2])
def test_single_and_batch_share_owner_ceiling_before_factory(tmp_path, monkeypatch, limit):
    store, request, ledger, _ = prepared(tmp_path)
    raw = valid_config()
    raw["child_limits"] = {"max_children": limit, "max_concurrency": 1}
    owner = SimpleNamespace(config=parse_foreground_config_v2(raw))
    workspace = SimpleNamespace(store=store,
        identity=WorkspaceIdentity(request.session, "workspace"))
    children = composition.ForegroundChildren(store, owner)
    children.bind(request, workspace, lambda: False)
    children.children.clock = lambda: 100
    children.children.monotonic_clock = lambda: 0
    calls = []

    class Factory:
        def __init__(self, owner, worker, envelope, token, identity):
            self.worker, self.token = worker, token
            calls.append(envelope.child_id)

        def port(self):
            factory = self

            class Port:
                def run(self, item, *, cancelled):
                    with factory.worker.backend_admission(item, factory.token):
                        return "done"

                def close(self):
                    pass

            return Port()

    monkeypatch.setattr(composition, "TrustedCodexChildFactory", Factory)
    item = {"child_id": "one", "task_text": "work", "timeout_ms": 1000}
    expected = workspace.identity.to_dict()
    with store.execution_lock(), store.workspace_turn_scope(request):
        single = children.invoke(workspace.identity, {"expected": expected, **item})
        assert single["disposition"] == ("observed" if limit else "rejected")
        batch = {"expected": expected, "concurrency": 2,
                 "children": [{**item, "child_id": "two"}]}
        assert children.invoke_batch(workspace.identity, batch)["disposition"] == "rejected"
        assert len(calls) == (1 if limit else 0)
        batch["concurrency"] = 1
        result = children.invoke_batch(workspace.identity, batch)
        assert result["disposition"] == ("observed" if limit else "rejected")
        if limit:
            assert children.invoke(workspace.identity, {"expected": expected, **item}) == single
            assert children.invoke_batch(workspace.identity, batch) == result
        extra = {**item, "child_id": "three"}
        assert children.invoke(workspace.identity,
            {"expected": expected, **extra})["disposition"] == "rejected"
        batch["children"] = [extra]
        assert children.invoke_batch(workspace.identity, batch)["disposition"] == "rejected"
    children.close()
    assert len(calls) == len(ledger.read(ledger.parent)) == limit
