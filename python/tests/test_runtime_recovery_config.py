"""Foreground recovery config retains settings and requires actual recovery provenance."""

import json

import pytest
from test_foreground_config_v2 import valid_config
from test_runtime_core import setup
from test_runtime_restore_cli import cli

from ahc.application.working_continuity import WorkingContinuityService
from ahc.cli.foreground_config_v2 import parse_foreground_config_v2
from ahc.cli.runtime_recovery_config import prepare_recovered_config
from ahc.domain.sessions import ContractError
from ahc.domain.workspace import WorkspaceCheckpoint
from ahc.infrastructure.runtime_backup import _digest, backup_runtime_database
from ahc.infrastructure.runtime_restore import prepare_runtime_restore
from ahc.infrastructure.runtime_restore_activation import activate_runtime_restore
from ahc.infrastructure.runtime_restore_transform import transform_runtime_restore
from ahc.infrastructure.working_db import WORKING_DB_FILENAME


@pytest.mark.parametrize("enabled", [False, True])
@pytest.mark.parametrize("advanced", [False, True])
def test_recovered_config_binds_checkpoint_and_preserves_original(tmp_path, enabled, advanced):
    repo, saved, _ = setup(tmp_path)
    origin = repo.reserve_workspace_generation(saved, "work")
    checkpoint = WorkspaceCheckpoint(origin, '{"value":42}')
    repo.save_workspace_checkpoint(saved, checkpoint)
    config = valid_config()
    config["runtime_root"] = str(repo.root)
    config["backend"]["enabled"] = True
    config["channels"] = [{"route": {"session": saved.session.to_dict(), "platform": "telegram",
        "account_id": "123", "channel_id": "456", "user_id": "789"},
        "enabled": True, "token_env": "UNREAD_TOKEN", "typing": True, "progress": True}]
    parsed = parse_foreground_config_v2(config)
    original = tmp_path / "original.json"
    original.write_text(json.dumps(config), encoding="utf-8")
    before = original.read_bytes()
    backup, prep, transformed = (tmp_path / name for name in ("backup", "prep", "new"))
    source_digest = _digest(repo.root / WORKING_DB_FILENAME)
    manifest = backup_runtime_database(repo, backup)
    prepare_runtime_restore(backup, prep)
    current = saved
    if advanced:
        current = WorkingContinuityService(repo).continue_generation(
            saved, saved.session.successor(), "compaction")
        with pytest.raises(ContractError, match="provenance"):
            prepare_recovered_config(repo, original, parsed.configuration_digest,
                                     tmp_path / "ordinary", checkpoint_digest=None)
        config["session"] = current.session.to_dict()
        config["channels"][0]["route"]["session"] = current.session.to_dict()
        parsed = parse_foreground_config_v2(config)
        original.write_text(json.dumps(config), encoding="utf-8")
        before = original.read_bytes()
        source_digest = _digest(repo.root / WORKING_DB_FILENAME)
    receipt = transform_runtime_restore(prep, manifest["sha256"], (current,),
                                        source_digest, transformed)
    activate_runtime_restore(repo, tmp_path / "supervisor.lock", transformed,
                             receipt["sha256"], manifest["sha256"], source_digest)
    output = tmp_path / "config"
    result = prepare_recovered_config(repo, original, parsed.configuration_digest, output,
                                      checkpoint_digest=checkpoint.digest, enable_backend=enabled)
    successor = parse_foreground_config_v2(json.loads((output / "foreground.json").read_text()))
    assert successor.session == repo.read_current("session").session
    assert successor.checkpoint_digest == checkpoint.digest
    assert successor.to_dict()["policy"] == config["policy"]
    assert successor.to_dict()["workspace"] == config["workspace"]
    assert successor.backend["enabled"] is enabled
    row = successor.to_dict()["channels"][0]
    assert row["route"]["session"] == successor.session.to_dict()
    assert not row["enabled"] and not row["typing"] and not row["progress"]
    assert result["disabled_integrations"] == ["channels"] and not result["started"]
    assert original.read_bytes() == before
    if not advanced and not enabled:
        cli_result = cli("runtime-recovery-config", "--runtime-root", repo.root,
            "--original-config", original, "--expected-config-digest", parsed.configuration_digest,
            "--destination", tmp_path / "cli-config", "--checkpoint-digest", checkpoint.digest)
        assert cli_result["configuration"]["configuration_digest"] == result["configuration_digest"]
    with pytest.raises(ContractError):
        prepare_recovered_config(repo, original, "f" * 64, tmp_path / "bad",
                                 checkpoint_digest=checkpoint.digest)
