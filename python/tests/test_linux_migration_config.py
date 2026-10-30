import copy
import pytest
from test_foreground_config_v2 import valid_config
from ahc.cli.linux_migration_config import convert_foreground_to_linux
from ahc.domain.sessions import ContractError


PATHS = dict(runtime_root="/home/ahc/product/runtime", cwd="/home/ahc/workspace",
             interpreter="/opt/ahc/python", bundle="/opt/ahc/codex",
             codex_home="/home/ahc/product/codex-home", skill_root=None)


def test_migration_preserves_identity_and_policy_without_mutating_input():
    value = valid_config()
    before = copy.deepcopy(value)
    result = convert_foreground_to_linux(value, **PATHS)
    assert value == before
    assert result["session"] == before["session"]
    assert result["workspace_id"] == before["workspace_id"]
    assert result["checkpoint_digest"] == before["checkpoint_digest"]
    assert result["policy"] == {**before["policy"], "revision": 2}
    assert result["backend"]["enabled"] == before["backend"]["enabled"]
    assert result["backend"]["model"] == before["backend"]["model"]
    assert result["workspace"]["kind"] == "linux-python"
    assert "distribution" not in result["workspace"]


def test_existing_memory_requires_explicit_mapping():
    value = valid_config()
    value["native_memory"] = {"binding_file": "D:/binding.json", "binding_digest": "a" * 64}
    with pytest.raises(ContractError, match="explicit migration"):
        convert_foreground_to_linux(value, **PATHS)
    replacement = {"binding_file": "/home/ahc/product/binding.json", "binding_digest": "b" * 64}
    assert convert_foreground_to_linux(value, native_memory=replacement, **PATHS)["native_memory"] == replacement
