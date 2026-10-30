"""Real host/child composition with only process/provider execution replaced."""
from types import SimpleNamespace
import pytest
from ahc.adapters.codex_client import CodexProtocolClient
from test_runtime_learning_queue import ready
from test_foreground_config_native import native_config
from test_trusted_codex_launch import command, payload_for
from ahc.application.child_dispatch import ChildDispatchService
from ahc.cli.foreground_config_native import parse_trusted_foreground_config
from ahc.cli.foreground_learning import compose_learning
from ahc.domain.sessions import ContractError
from ahc.adapters.trusted_codex_launch import require_tool_free_configuration

@pytest.mark.parametrize("limits", [
    {"max_children": 2, "max_concurrency": 1},
    {"max_children": 2, "max_concurrency": 1, "max_artifacts": 2, "artifact_bytes": 1024},
])
def test_real_host_child_factory_parses_limits_and_reflection_route(tmp_path, monkeypatch, limits):
    repo, req, queue, _ = ready(tmp_path)
    value = native_config()
    value["backend"]["enabled"] = True
    value["skill_root"] = str(repo.root)
    value["learning"] = {
        "mode": "proposal-only",
        "protected_skills": [],
        "reflection": {
            "enabled": True,
            "model": "gpt-6-sol",
            "reasoning_effort": "high",
        },
    }
    value.update(runtime_root=str(repo.root), session=req.session.to_dict(), child_limits=limits)
    config = parse_trusted_foreground_config(value)
    learning = compose_learning(config, SimpleNamespace(repository=repo), store=repo)
    child_client_routes = []
    client_init = CodexProtocolClient.__init__

    def capture_client_route(self, *args, **kwargs):
        client_init(self, *args, **kwargs)
        child_client_routes.append((self.model, self.reasoning_effort))

    monkeypatch.setattr(CodexProtocolClient, "__init__", capture_client_route)
    seen = []
    def construct_only(self, envelope, token, **kwargs):
        port = self.factory(envelope)
        assert port.request.artifact_digest == self.children.request.artifact_digest
        seen.append(port)
        return {"state": "terminal", "result": {"output_text": '{"action":"no-op","reason":"composition checked"}'}}
    monkeypatch.setattr(ChildDispatchService, "dispatch", construct_only)
    binding = queue.claim(req.session, req.operation_id, "owner", learning.controller.policy)
    assert binding["model"] == "gpt-6-sol" and binding["effort"] == "high"
    output = learning.controller.calls.run(
        req.session, req.operation_id, binding, "bounded reflection", cancelled=lambda: False)
    assert "no-op" in output and len(seen) == 1
    assert ("gpt-6-sol", "high") in child_client_routes
    identity = seen[0].request.session
    with repo._read() as db:
        assert db.execute("SELECT count(*) FROM workspace_heads WHERE session_json=?",
            (__import__("json").dumps(identity.to_dict(), sort_keys=True, separators=(",", ":")),)).fetchone()[0] == 1

def test_full_user_host_launch_disables_native_tools():
    args = command(user_environment={"HOME": "/home/user", "PATH": "/usr/bin:/bin"},
                   tool_free=True)
    payload = payload_for(args)
    overrides = payload["overrides"]
    for name in ("shell_tool", "unified_exec", "apply_patch_freeform", "apps", "plugins",
                 "multi_agent", "browser_use", "computer_use", "image_generation", "goals"):
        assert overrides["features." + name] is False
    assert overrides["web_search"] == "disabled" and overrides["mcp_servers"] == {}

@pytest.mark.parametrize("project", [False, True])
def test_configured_mcp_is_rejected_before_tool_free_launch(tmp_path, project):
    home, cwd = tmp_path / "home", tmp_path / "work"
    home.mkdir();cwd.mkdir()
    path = cwd / ".codex/config.toml" if project else home / "config.toml"
    path.parent.mkdir(exist_ok=True)
    path.write_text('[mcp_servers.custom]\ncommand = "/usr/bin/true"\n')
    with pytest.raises(ContractError, match="configured MCP"):
        require_tool_free_configuration(str(home), str(cwd))
