"""Detached host jobs never reopen a delivered source turn."""
from types import SimpleNamespace
import pytest
from test_runtime_learning_queue import ready, POLICY
from test_foreground_config_native import native_config
from ahc.cli.foreground_config_native import parse_trusted_foreground_config
from ahc.cli.host_learning_calls import HostLearningCalls
from ahc.cli.host_model_calls import HostModelCalls
from ahc.domain.sessions import ContractError
from ahc.cli import trusted_codex_backend

def test_detached_host_parent_is_distinct_durable_and_once_only(tmp_path, monkeypatch):
    repo, req, queue, clock = ready(tmp_path)
    value = native_config()
    value.update(runtime_root=str(repo.root), session=req.session.to_dict())
    from ahc.domain.channel_ingress import ChannelRoute
    with repo._read() as db:
        route = __import__("json").loads(db.execute("SELECT route_json FROM runtime_channel_outbox WHERE operation_id=?",(req.operation_id,)).fetchone()[0])
    route.update(channel_id="12345", user_id="67890")
    value["channels"] = [{"route": route, "token_env": "AHC_TEST_BOT_TOKEN", "enabled": True}]
    config = parse_trusted_foreground_config(value)
    monkeypatch.setattr(trusted_codex_backend, "TrustedCodexBackendFactory",
                        lambda store, cfg: SimpleNamespace(store=store, config=cfg,
                            binding=lambda *a, **k: ("a"*64, "b"*64)))
    seen = []
    def native_call(self, call_id, prompt, **kwargs):
        bound = self.parent_request
        assert bound.session != req.session and bound.operation_id != req.operation_id
        assert self.owner.store.read_turn(req.session, req.operation_id).status != "submitted"
        assert self.owner.store.read_turn(bound.session, bound.operation_id).status == "submitted"
        assert not bound.capabilities and not bound.revision_digests
        assert bound.artifact_digest == self.owner.binding(None, child=True)[0]
        assert "channels" not in self.owner.config.to_dict()
        self.owner.store.require_workspace(self.workspace.identity)
        seen.append(bound)
        return '{"action":"no-op","reason":"No lesson."}'
    monkeypatch.setattr(HostModelCalls, "run", native_call)
    binding = queue.claim(req.session, req.operation_id, "owner", POLICY)
    calls = HostLearningCalls(repo, config)
    before = repo.read_current(req.session.ref.virtual_session_id)
    assert "no-op" in calls.run(req.session, req.operation_id, binding, "bounded prompt",
                                cancelled=lambda: False)
    assert repo.read_current(req.session.ref.virtual_session_id) == before
    with pytest.raises(ContractError, match="do not replay"):
        calls.run(req.session, req.operation_id, binding, "bounded prompt", cancelled=lambda: False)
    assert len(seen) == 1

def test_failed_close_retains_owner_and_reopen_recovers_exact_detached_session(tmp_path, monkeypatch):
    from ahc.cli import host_model_calls
    from ahc.infrastructure.runtime_repository import RuntimeRepository
    repo, req, queue, clock = ready(tmp_path)
    value = native_config()
    value.update(runtime_root=str(repo.root), session=req.session.to_dict())
    config = parse_trusted_foreground_config(value)
    executions = []
    class Broken:
        def __init__(self, *args):
            self.closed = 0
        def dispatch(self, *args, **kwargs):
            executions.append('one-admission')
            raise OSError('controlled lost outcome')
        def close(self):
            self.closed += 1
            raise OSError('controlled cleanup failure')
    monkeypatch.setattr(host_model_calls, 'ChildDispatchService', Broken)
    binding = queue.claim(req.session, req.operation_id, 'owner', POLICY)
    with pytest.raises(ContractError):
        HostLearningCalls(repo, config).run(
            req.session, req.operation_id, binding, 'bounded prompt', cancelled=lambda: False)
    pending = repo._host_model_pending_closes
    assert len(pending) == 1 and executions == ['one-admission']
    reopened = RuntimeRepository(repo.root)
    recovered = []
    monkeypatch.setattr(trusted_codex_backend.TrustedCodexBackendFactory, 'recover',
                        lambda self, session, workspace: recovered.append((session, workspace)))
    assert HostLearningCalls(reopened, config).recover(req.session)
    assert len(recovered) == 1 and recovered[0][0] != req.session
    assert recovered[0][0].ref.virtual_session_id.startswith('reflection-')
    assert executions == ['one-admission']
