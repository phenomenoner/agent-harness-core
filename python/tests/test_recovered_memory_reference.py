import json

import pytest
from test_foreground_native_memory import configured

from ahc.cli.foreground_config_v2 import parse_foreground_config_v2
from ahc.cli.native_memory_binding import parse_native_memory_binding
from ahc.cli.runtime_memory_binding import recovered_memory_reference
from ahc.domain.sessions import ContractError


def test_recovery_requires_explicit_database_change_without_grant_expansion(tmp_path):
    raw, registry, session, path = configured(tmp_path)
    config = parse_foreground_config_v2(raw)
    with pytest.raises(ContractError):
        recovered_memory_reference(config, None, registry)
    with pytest.raises(ContractError):
        recovered_memory_reference(config, raw["native_memory"], registry)
    document = json.loads(path.read_text())
    database = tmp_path / "restored.sqlite"
    database.write_bytes(b"inspection belongs to preceding restore gate")
    document["bindings"]["hexi.durable-memory"]["database"] = str(database)
    new_path = tmp_path / "new-binding.json"
    new_path.write_text(json.dumps(document))
    binding = parse_native_memory_binding(document, registry=registry, agent_id="hexi")
    reference = {"binding_file": str(new_path), "binding_digest": binding.digest}
    assert recovered_memory_reference(config, reference, registry) == reference
    document["bindings"]["hexi.durable-memory"]["store_policy"] = "deny"
    new_path.write_text(json.dumps(document))
    changed = parse_native_memory_binding(document, registry=registry, agent_id="hexi")
    with pytest.raises(ContractError):
        recovered_memory_reference(
            config, {**reference, "binding_digest": changed.digest}, registry
        )
