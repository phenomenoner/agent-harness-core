"""Prepare an explicit successor foreground config without starting its effects."""

import json
from pathlib import Path

from ahc.domain.sessions import ContractError
from ahc.infrastructure.runtime_recovery_access import recovery_allows, verified_recovery_edges
from ahc.infrastructure.runtime_restore_activation import _write_json
from ahc.infrastructure.working_db import require_real_directory

from .foreground import read_foreground_config
from .foreground_config_native import parse_trusted_foreground_config


def prepare_recovered_config(store, original_path, expected_config_digest, destination, *,
                              checkpoint_digest, enable_backend=False,
                              native_memory_reference=None, profile_registry=None):
    original = parse_trusted_foreground_config(read_foreground_config(original_path))
    if (original.configuration_digest != expected_config_digest
            or Path(original.runtime_root).resolve() != store.root.resolve()
            or type(enable_backend) is not bool):
        raise ContractError("recovery configuration identity mismatch")
    from .runtime_memory_binding import recovered_memory_reference

    memory = recovered_memory_reference(original, native_memory_reference, profile_registry)
    with store.execution_lock():
        current = store.read_current(original.session.ref.virtual_session_id)
        if current.status != "active" or current.session == original.session:
            raise ContractError("recovery configuration requires an active successor")
        with store._read() as connection:
            if connection.execute("SELECT 1 FROM sqlite_master WHERE type='table' "
                                  "AND name='runtime_recovery_transitions'").fetchone() is None:
                raise ContractError("configuration session lacks recovery provenance")
            verified_recovery_edges(connection, current.session.ref.virtual_session_id)
            inherited = recovery_allows(store, connection, current.session, original.session)
            rows = connection.execute("SELECT transition_json FROM runtime_recovery_transitions "
                "WHERE virtual_session_id=?", (current.session.ref.virtual_session_id,))
            source_parent = any(
                data["recovered_session"] == current.session.to_dict()
                and data["source_session"] == original.session.to_dict()
                for data in (json.loads(row[0]) for row in rows))
            if not inherited and not source_parent:
                raise ContractError("configuration session lacks recovery provenance")
        if checkpoint_digest is not None and store.read_workspace_checkpoint(
                current.session, original.workspace_id, checkpoint_digest) is None:
            raise ContractError("selected recovery checkpoint unavailable")
        document = original.to_dict()
        if memory is not None:
            document["native_memory"] = memory
        document["session"] = current.session.to_dict()
        document["checkpoint_digest"] = checkpoint_digest
        document["backend"]["enabled"] = enable_backend
        disabled = []
        for row in document.get("channels", []):
            row["route"]["session"] = current.session.to_dict()
            row["enabled"] = False
            for flag in ("typing", "progress"):
                if flag in row:
                    row[flag] = False
        for name in ("channels", "telegram_polling", "discord_gateway", "mcp"):
            if name in document:
                disabled.append(name)
        for name in ("telegram_polling", "discord_gateway"):
            if name in document:
                document[name]["enabled"] = False
        for profile in document.get("mcp", {}).get("profiles", []):
            profile["enabled"] = False
        prepared = parse_trusted_foreground_config(document)
        destination = Path(destination).absolute()
        require_real_directory(destination.parent, allow_absent=False)
        destination.mkdir(exist_ok=False)
        _write_json(destination / "foreground.json", prepared.to_dict())
        report = {"schema": "ahc.recovered-foreground-config.v1",
                  "original_config_digest": expected_config_digest,
                  "configuration_digest": prepared.configuration_digest,
                  "session": current.session.to_dict(), "checkpoint_digest": checkpoint_digest,
                  "backend_enabled": enable_backend, "disabled_integrations": disabled,
                  "started": False}
        _write_json(destination / "recovery.json", report)
        return report
