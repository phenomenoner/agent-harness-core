"""Foreground configuration binds persistent per-turn skill selection."""

import json

import pytest
from skill_test_support import revision
from test_cli_foreground import configuration, wsl_backend
from test_skill_graph_convergence import _activate_with_report
from test_workspace_tools import Port

from ahc.cli import wsl_codex_backend
from ahc.application.runtime_service import RuntimeService
from ahc.cli.foreground import compose_foreground
from ahc.domain.agent_backend import AgentTurnObservation
from ahc.domain.sessions import ContractError
from ahc.infrastructure.runtime_inbox import RuntimeInbox


def test_foreground_reselects_from_pinned_repository_after_reopen(
    tmp_path, monkeypatch, repository
):
    original, request, payload, path, _ = configuration(tmp_path)
    candidate = revision()
    _activate_with_report(repository, candidate, "activate")
    payload.update(backend=wsl_backend(enabled=True), skill_root=str(repository.root))
    path.write_text(json.dumps(payload))
    inbox = RuntimeInbox(original.store)
    observed = []
    discovered = []

    class Factory:
        def recover(self, session, workspace_id):
            return ()

        def __init__(self, *args, **kwargs):
            pass

        def binding(self, tools):
            return "a" * 64, "b" * 64

        def __call__(self, bound, tools, **kwargs):
            class Backend:
                def invoke(self, selected):
                    observed.append((selected.revision_digests, selected.skill_bodies))
                    catalog = tools.invoke("workspace.skills_list", {
                        "expected": tools.workspace.identity.to_dict(), "query": "different words"})
                    row = catalog["result"]["skills"][0]
                    content = tools.invoke("workspace.skills_read", {
                        "expected": tools.workspace.identity.to_dict(),
                        "revision_digest": row["revision_digest"]})
                    discovered.append(content["result"]["text"])
                    return AgentTurnObservation(selected, "completed", "fixture", 1, 1)

            return Backend()

    monkeypatch.setattr(wsl_codex_backend, "WslCodexBackendFactory", Factory)
    monkeypatch.setattr(
        RuntimeService,
        "open_docker_workspace",
        lambda service, expected, workspace_id, runner, **kw: service.open_workspace(
            expected, workspace_id, Port
        ),
    )
    for operation, task in (("analyze", "analyze task"), ("other", "summarize task")):
        inbox.submit(request.session, operation, request.goal_digest, task)
        runtime = compose_foreground(path)
        runtime.start()
        try:
            result = runtime.step()
            assert result.result["accepted"]
            assert runtime.step() is None
            assert inbox.query(request.session, operation)["status"] == "terminal"
        finally:
            runtime.close()
    assert observed == [
        ((candidate.revision_digest,), ((candidate.revision_digest, candidate.body),)),
        ((), ()),
    ]
    assert repository.get_episode("analyze").surfaced_revisions == (candidate.revision_digest,)
    assert discovered == [candidate.body, candidate.body]


@pytest.mark.parametrize("value", [True, [], "relative", "missing"])
def test_invalid_skill_root_rejects_before_workspace(tmp_path, monkeypatch, value):
    _, _, payload, path, _ = configuration(tmp_path)
    payload["skill_root"] = str(tmp_path / "missing") if value == "missing" else value
    path.write_text(json.dumps(payload))
    monkeypatch.setattr(
        RuntimeService, "open_docker_workspace", lambda *a, **kw: pytest.fail("opened")
    )
    with pytest.raises(ContractError):
        compose_foreground(path)


def test_null_skill_root_preserves_disabled_selector(tmp_path):
    _, _, payload, path, _ = configuration(tmp_path)
    payload["skill_root"] = None
    path.write_text(json.dumps(payload))
    assert compose_foreground(path).selector is None
