from contextlib import contextmanager
from types import SimpleNamespace

import pytest
from test_memory_revision_service import correct, prepared

from ahc.domain.sessions import ContractError
from ahc.infrastructure.runtime_memory_backup import backup_runtime_memory


@pytest.mark.parametrize("action", ["revise", "tombstone"])
@pytest.mark.parametrize("state", ["in-flight", "unknown", "succeeded"])
def test_joint_backup_excludes_unresolved_revision_before_provider_effect(tmp_path, action, state):
    service, session, _, original, _, options = prepared(tmp_path)
    repo = options["repository"]
    effects = []
    receipt = {"sha256": "a" * 64, "activation_allowed": False}

    def backup():
        return backup_runtime_memory(
            repo, tmp_path / "joint", binding_digest="b" * 64,
            provider_backup=lambda path: effects.append(path) or receipt,
            provider_inspect=lambda *a, **kw: dict(receipt))

    real = service.revise

    def call(**request):
        if state == "in-flight":
            with pytest.raises(ContractError, match="reconciled"):
                backup()
            assert effects == [] and not (tmp_path / "joint").exists()
        result = real(**request)
        if state == "unknown":
            raise TimeoutError("provider committed; acknowledgment lost")
        return result

    service.revise = call
    if action == "revise":
        result = correct(service, session, original)
    else:
        result = service.revise_trusted(session, operation_id="tombstone",
                                       expected_ref=original["record_ref"])
    if state == "unknown":
        assert result["status"] == "unknown"
        with pytest.raises(ContractError, match="reconciled"):
            backup()
        assert effects == [] and not (tmp_path / "joint").exists()
    else:
        assert result["status"] == "succeeded"
        assert backup()["activation_allowed"] is False
        assert len(effects) == 1


def test_restore_command_checks_current_revision_custody_before_preparing(tmp_path, monkeypatch):
    from ahc.cli import runtime_memory_backup as command

    service, session, _, original, _, options = prepared(tmp_path)
    real = service.revise

    def lost(**request):
        real(**request)
        raise TimeoutError("lost reply")

    service.revise = lost
    assert correct(service, session, original)["status"] == "unknown"
    repo = options["repository"]
    registry = tmp_path / "registry.json"
    registry.write_text("{}")
    monkeypatch.setattr(command.AgentProfileRegistry, "from_dict", lambda value: object())
    monkeypatch.setattr(command, "read_foreground_config", lambda path: {})
    monkeypatch.setattr(command, "parse_trusted_foreground_config",
                        lambda value: SimpleNamespace(runtime_root=repo.root))
    binding = SimpleNamespace(database="unused", provider_root="unused", provider_digest="b" * 64,
                              timeout=10, digest="c" * 64)
    monkeypatch.setattr(command, "read_native_binding", lambda *a: binding)
    monkeypatch.setattr(command, "NativeMemoryProcess",
                        lambda **kw: SimpleNamespace(inspect_backup=lambda *a: None))

    @contextmanager
    def offline(store, *args):
        with store.execution_lock(), store.writer_lock():
            yield ()

    def forbidden(*args, **kwargs):
        pytest.fail("unresolved revision reached restore preparation")

    monkeypatch.setattr(command, "offline_restore_source", offline)
    monkeypatch.setattr(command, "prepare_runtime_memory_restore", forbidden)
    args = SimpleNamespace(command="runtime-memory-restore-prepare", profile_registry=registry,
                           foreground_config="unused", supervisor_lock="unused",
                           expected_source_digest="a" * 64, backup_root=tmp_path / "backup",
                           destination=tmp_path / "candidate", expected_manifest_digest="b" * 64)
    with pytest.raises(ContractError, match="reconcil"):
        command.run(args)
    assert not args.destination.exists()
