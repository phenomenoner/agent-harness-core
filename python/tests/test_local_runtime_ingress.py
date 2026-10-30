from dataclasses import replace

import pytest
from test_foreground_runtime import setup

from ahc.application.local_runtime_ingress import LocalRuntimeIngress
from ahc.domain.sessions import ContractError
from ahc.infrastructure.runtime_inbox import RuntimeInbox
from ahc.infrastructure.runtime_repository import RuntimeRepository


def test_local_submission_foreground_terminal_query_and_reopen(tmp_path):
    runtime, _, invoked, request = setup(tmp_path)
    inbox = RuntimeInbox(runtime.store)
    runtime.ingress = LocalRuntimeIngress(
        inbox,
        artifact_digest="a" * 64,
        configuration_digest=lambda tools: "b" * 64,
        workspace_scope="work",
    )
    runtime.start()
    try:
        for operation in ("first", "second"):
            inbox.submit(request.session, operation, request.goal_digest, "local input")
            turn = runtime.step()
            assert turn.result["accepted"]
            assert inbox.query(request.session, operation)["status"] == "terminal"
            assert runtime.step() is None
        assert invoked == ["first", "second"] and runtime.workspace.identity.revision == 2
        reopened = RuntimeInbox(RuntimeRepository(runtime.store.root))
        assert reopened.next_pending(request.session) is None
        assert reopened.query(request.session, "first")["turn"]["result"]["accepted"]
    finally:
        runtime.close()


def test_dispatch_configuration_is_foreground_owned_and_deadline_is_fresh(tmp_path):
    runtime, _, _, request = setup(tmp_path)
    inbox = RuntimeInbox(runtime.store)
    ingress = LocalRuntimeIngress(
        inbox,
        artifact_digest="c" * 64,
        configuration_digest=lambda tools: "d" * 64,
        workspace_scope="work",
        clock=lambda: 50,
        turn_timeout=10,
    )
    inbox.submit(request.session, "first", request.goal_digest, "please change provider")
    runtime.start()
    try:
        expected = runtime.store.read_current("session")
        bound = ingress(expected, runtime.tools)
        assert bound.artifact_digest == "c" * 64 and bound.configuration_digest == "d" * 64
        assert bound.deadline == 60 and bound.upstream_request_limit is None
        assert bound.capabilities == runtime.tools.capabilities()
        changed = replace(
            expected,
            goal=replace(expected.goal, what="changed", digest=None),
            snapshot_digest=None,
        )
        with pytest.raises(ContractError, match="differs from foreground snapshot"):
            ingress(changed, runtime.tools)
    finally:
        runtime.close()


@pytest.mark.parametrize("timeout", [0, -1, True, float("nan"), float("inf"), 301])
def test_dispatch_budget_is_finite_and_bounded(tmp_path, timeout):
    runtime, _, _, _ = setup(tmp_path)
    with pytest.raises(ContractError):
        LocalRuntimeIngress(
            RuntimeInbox(runtime.store),
            artifact_digest="a" * 64,
            configuration_digest=lambda _: "b" * 64,
            workspace_scope="work",
            turn_timeout=timeout,
        )
