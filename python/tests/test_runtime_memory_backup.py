import pytest
from test_memory_store_approvals import prepared

from ahc.domain.sessions import ContractError
from ahc.infrastructure.runtime_memory_backup import backup_runtime_memory
from ahc.infrastructure.working_repository import WorkingWriterBusy


@pytest.mark.parametrize("state", ["in-flight", "unknown"])
def test_unresolved_memory_rejects_before_backup_effect(tmp_path, state):
    repo, snapshot, owner, digest = prepared(tmp_path)
    owner.approve(snapshot.session, digest, approval_expires=150)
    owner.consume(snapshot.session, digest)
    if state == "unknown":
        owner.finish(snapshot.session, digest, state)

    def forbidden(*args, **kw):
        pytest.fail("provider called")

    with pytest.raises(ContractError, match="reconciled"):
        backup_runtime_memory(
            repo,
            tmp_path / "joint",
            binding_digest="b" * 64,
            provider_backup=forbidden,
            provider_inspect=forbidden,
        )
    assert not (tmp_path / "joint").exists()


def test_backup_excludes_new_consumption_and_requires_provider_readback(tmp_path):
    repo, snapshot, owner, digest = prepared(tmp_path)
    owner.approve(snapshot.session, digest, approval_expires=150)
    receipt = {"sha256": "a" * 64, "activation_allowed": False}

    def backup(destination):
        with pytest.raises(WorkingWriterBusy):
            owner.consume(snapshot.session, digest)
        return receipt

    result = backup_runtime_memory(
        repo,
        tmp_path / "joint",
        binding_digest="b" * 64,
        provider_backup=backup,
        provider_inspect=lambda *a, **kw: dict(receipt),
    )
    assert result["activation_allowed"] is False
    assert result["runtime"]["restorable_runtime"] is False
    assert owner.query(snapshot.session, digest)["state"] == "approved"


@pytest.mark.parametrize("changed", ["none", "manifest", "runtime", "provider", "binding"])
def test_joint_inspection_checks_both_receipts_and_owner_digest(tmp_path, changed):
    import hashlib

    from ahc.infrastructure.runtime_memory_backup import inspect_runtime_memory_backup

    repo, _, _, _ = prepared(tmp_path)
    receipt = {"sha256": "a" * 64, "activation_allowed": False}
    destination = tmp_path / "joint"
    result = backup_runtime_memory(
        repo,
        destination,
        binding_digest="b" * 64,
        provider_backup=lambda path: receipt,
        provider_inspect=lambda *a, **kw: dict(receipt),
    )
    manifest = destination / "manifest.json"
    digest = hashlib.sha256(manifest.read_bytes()).hexdigest()
    if changed == "manifest":
        manifest.write_bytes(manifest.read_bytes() + b" ")
    elif changed == "runtime":
        with (destination / "runtime/runtime.sqlite").open("ab") as stream:
            stream.write(b"changed")

    def inspect(*a, **kw):
        assert kw["expected_digest"] == "a" * 64
        return {} if changed == "provider" else dict(receipt)

    if changed == "none":
        assert (
            inspect_runtime_memory_backup(
                destination,
                expected_digest=digest,
                expected_binding_digest=("c" if changed == "binding" else "b") * 64,
                provider_inspect=inspect,
            )
            == result
        )
    else:
        with pytest.raises(ContractError):
            inspect_runtime_memory_backup(
                destination,
                expected_digest=digest,
                expected_binding_digest=("c" if changed == "binding" else "b") * 64,
                provider_inspect=inspect,
            )
