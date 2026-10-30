"""Detached reflection calls must not inherit foreground channel receivers."""
import json
from types import SimpleNamespace

import pytest

from test_foreground_config_native import native_config
from test_runtime_learning_queue import POLICY, ready
from ahc.cli.foreground_config_native import parse_trusted_foreground_config
from ahc.cli.host_learning_calls import HostLearningCalls
from ahc.cli.host_model_calls import HostModelCalls
from ahc.cli import trusted_codex_backend
from ahc.domain.sessions import ContractError


@pytest.mark.parametrize("platform", ["telegram", "discord"])
def test_detached_reflection_strips_channel_runtime_configuration(
    tmp_path, monkeypatch, platform
):
    repo, req, queue, _ = ready(tmp_path)
    document = native_config()
    document.update(runtime_root=str(repo.root), session=req.session.to_dict())
    document["backend"]["model"] = "gpt-source"
    document["backend"]["reasoning_effort"] = "medium"
    document["channel_control"] = {
        "enabled": True,
        "models": {"gpt-source": {"efforts": ["medium"], "priority": False}},
        "aliases": {},
    }

    with repo._read() as db:
        route = json.loads(
            db.execute(
                "SELECT route_json FROM runtime_channel_outbox WHERE operation_id=?",
                (req.operation_id,),
            ).fetchone()[0]
        )
    route.update(
        platform=platform,
        account_id="24680",
        channel_id="12345",
        user_id="67890",
    )
    document["channels"] = [{
        "route": route,
        "token_env": "AHC_TEST_BOT_TOKEN",
        "enabled": True,
    }]
    if platform == "telegram":
        document["telegram_polling"] = {
            "enabled": True,
            "account_id": route["account_id"],
            "token_env": "AHC_TEST_BOT_TOKEN",
            "exclusive_consumer": True,
            "interval_seconds": 1,
        }
    else:
        document["discord_gateway"] = {
            "enabled": True,
            "account_id": route["account_id"],
            "token_env": "AHC_TEST_BOT_TOKEN",
            "intents": 37377,
            "connection_limit": 1,
        }

    config = parse_trusted_foreground_config(document)
    source_config = config.to_dict()
    source_snapshot = repo.read_current(req.session.ref.virtual_session_id)
    detached_calls = []

    monkeypatch.setattr(
        trusted_codex_backend,
        "TrustedCodexBackendFactory",
        lambda store, cfg: SimpleNamespace(
            store=store,
            config=cfg,
            binding=lambda *args, **kwargs: ("a" * 64, "b" * 64),
        ),
    )

    def native_call(self, call_id, prompt, **kwargs):
        bound = self.parent_request
        assert bound.session != req.session
        assert bound.operation_id != req.operation_id
        assert self.owner.store.read_turn(req.session, req.operation_id).status != "submitted"
        assert self.owner.store.read_turn(bound.session, bound.operation_id).status == "submitted"
        assert not bound.capabilities and not bound.revision_digests
        detached = self.owner.config.to_dict()
        for key in ("channels", "telegram_polling", "discord_gateway", "channel_control"):
            assert key not in detached
        assert kwargs["model"] == "gpt-6-luna"
        assert kwargs["reasoning_effort"] == "max"
        self.owner.store.require_workspace(self.workspace.identity)
        detached_calls.append((bound.session, bound.operation_id))
        return '{"action":"no-op","reason":"No lesson."}'

    monkeypatch.setattr(HostModelCalls, "run", native_call)
    binding = queue.claim(req.session, req.operation_id, "owner", POLICY)
    calls = HostLearningCalls(repo, config)

    result = calls.run(
        req.session, req.operation_id, binding, "bounded prompt", cancelled=lambda: False
    )
    assert result == '{"action":"no-op","reason":"No lesson."}'
    assert len(detached_calls) == 1
    assert config.to_dict() == source_config
    assert repo.read_current(req.session.ref.virtual_session_id) == source_snapshot

    with pytest.raises(ContractError, match="do not replay"):
        calls.run(req.session, req.operation_id, binding, "bounded prompt", cancelled=lambda: False)
    assert len(detached_calls) == 1
    assert config.to_dict() == source_config
    assert repo.read_current(req.session.ref.virtual_session_id) == source_snapshot
