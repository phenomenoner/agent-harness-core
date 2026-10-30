"""Bounded local artifact persistence over real runtime workspace state."""

from dataclasses import replace

import pytest
from test_runtime_core import setup as runtime_setup

from ahc.domain.artifacts import ArtifactManifest, ArtifactNotPublished, content_digest
from ahc.domain.sessions import ContractError
from ahc.infrastructure.artifact_repository import ArtifactRepository
from ahc.infrastructure.runtime_repository import RuntimeRepository
from ahc.infrastructure.working_repository import (
    WorkingRepositoryError,
    WorkingSetConflict,
    WorkingSetCorrupt,
)


def state(tmp_path):
    repo, snapshot, _ = runtime_setup(tmp_path)
    producer = repo.reserve_workspace_generation(snapshot, "workspace")
    return repo, snapshot, producer, ArtifactRepository(repo)


def test_manifest_is_immutable_canonical_and_content_addressed(tmp_path):
    _, _, producer, _ = state(tmp_path)
    manifest = ArtifactManifest(producer, content_digest(b"bytes"), "text/plain", 5)
    assert ArtifactManifest.from_dict(manifest.to_dict()) == manifest
    assert manifest.artifact_id == ArtifactManifest.from_dict(manifest.to_dict()).artifact_id
    with pytest.raises(ContractError):
        ArtifactManifest(producer, manifest.content_digest, "Text/plain", 5)
    with pytest.raises(ContractError):
        ArtifactManifest(producer, manifest.content_digest, "text/plain; charset=utf-8", 5)


def test_publish_roundtrip_reopen_and_idempotence(tmp_path):
    repo, _, producer, artifacts = state(tmp_path)
    first = artifacts.publish(producer, b"hello", "text/plain")
    assert artifacts.publish(producer, b"hello", "text/plain") == first
    manifest, content = ArtifactRepository(RuntimeRepository(repo.root)).read(
        producer.session, first.artifact_id
    )
    assert manifest == first and content == b"hello"


def test_stale_workspace_and_wrong_session_reject_before_publish(tmp_path):
    repo, snapshot, producer, artifacts = state(tmp_path)
    stale = repo.begin_workspace_step(producer)
    with pytest.raises(ArtifactNotPublished) as rejected:
        artifacts.publish(producer, b"bytes", "text/plain")
    assert isinstance(rejected.value.__cause__, WorkingSetConflict)
    with pytest.raises(ArtifactNotPublished):
        artifacts.publish(replace(stale, revision=0), b"bytes", "text/plain")
    repo.finish_workspace_step(stale)
    manifest = artifacts.publish(stale, b"bytes", "text/plain")
    with pytest.raises(ContractError):
        artifacts.read(replace(producer.session, agent_id="other"), manifest.artifact_id)
    assert artifacts.read(producer.session, "f" * 64) is None
    from ahc.application.working_continuity import WorkingContinuityService

    WorkingContinuityService(repo).continue_generation(
        repo.read_current("session"), producer.session.successor(), "compaction"
    )
    with pytest.raises(ArtifactNotPublished):
        artifacts.publish(stale, b"new", "text/plain")


def test_lineage_requires_existing_same_workspace_but_allows_prior_generation(tmp_path):
    repo, snapshot, producer, artifacts = state(tmp_path)
    ancestor = artifacts.publish(producer, b"ancestor", "text/plain")
    successor = repo.reserve_workspace_generation(snapshot, producer.workspace_id)
    child = artifacts.publish(successor, b"child", "text/plain", lineage=(ancestor.artifact_id,))
    assert child.lineage == (ancestor.artifact_id,)
    foreign = repo.reserve_workspace_generation(snapshot, "other")
    with pytest.raises(ContractError):
        artifacts.publish(foreign, b"bad", "text/plain", lineage=(ancestor.artifact_id,))
    with pytest.raises(ContractError):
        artifacts.publish(successor, b"bad", "text/plain", lineage=("f" * 64,))


def test_large_content_roundtrips_while_invalid_media_and_duplicate_lineage_reject(tmp_path):
    _, _, producer, artifacts = state(tmp_path)
    content = b"x" * 1_048_577
    manifest = artifacts.publish(producer, content, "text/plain")
    assert artifacts.read(producer.session, manifest.artifact_id)[1] == content
    with pytest.raises(ContractError):
        artifacts.publish(producer, b"x", "text/PLAIN")
    with pytest.raises(ContractError):
        ArtifactManifest(producer, "a" * 64, "text/plain", 0, ("a" * 64,) * 17)


def test_tamper_and_precommit_rollback_preserve_integrity(tmp_path):
    repo, _, producer, artifacts = state(tmp_path)
    with pytest.raises(ArtifactNotPublished, match="not-committed"):
        artifacts.publish(producer, b"rollback", "text/plain", fail_before_commit=True)
    manifest = artifacts.publish(producer, b"bytes", "text/plain")
    repo._mutate(
        lambda connection: connection.execute(
            "UPDATE runtime_artifacts SET content=? WHERE artifact_id=?",
            (b"tampered", manifest.artifact_id),
        )
    )
    with pytest.raises(WorkingSetCorrupt):
        artifacts.read(producer.session, manifest.artifact_id)


def test_commit_reply_loss_is_resolved_by_readback_without_another_write(tmp_path, monkeypatch):
    repo, _, producer, artifacts = state(tmp_path)
    mutate, calls = repo._mutate, []

    def lost_reply(operation, **kwargs):
        calls.append(1)
        mutate(operation, **kwargs)
        raise WorkingRepositoryError("PRIVATE lost commit reply")

    monkeypatch.setattr(repo, "_mutate", lost_reply)
    manifest = artifacts.publish(producer, b"committed once", "text/plain")
    assert artifacts.read(producer.session, manifest.artifact_id)[1] == b"committed once"
    assert calls == [1]
    with repo._read() as connection:
        assert connection.execute("SELECT count(*) FROM runtime_artifacts").fetchone()[0] == 1


def test_unreadable_commit_result_stays_unknown_and_is_never_written_twice(tmp_path, monkeypatch):
    repo, _, producer, artifacts = state(tmp_path)
    mutate, read, calls = repo._mutate, repo._read, []

    def lost_reply(operation, **kwargs):
        calls.append(1)
        mutate(operation, **kwargs)
        raise WorkingRepositoryError("lost commit reply")

    def unreadable():
        raise WorkingSetCorrupt("readback unavailable")

    with monkeypatch.context() as patch:
        patch.setattr(repo, "_mutate", lost_reply)
        patch.setattr(repo, "_read", unreadable)
        with pytest.raises(WorkingRepositoryError, match="lost commit"):
            artifacts.publish(producer, b"committed once", "text/plain")
    assert calls == [1]
    with read() as connection:
        assert connection.execute("SELECT count(*) FROM runtime_artifacts").fetchone()[0] == 1


def test_more_than_sixteen_valid_ancestors_remain_readable_and_owner_bound(tmp_path):
    _, _, producer, artifacts = state(tmp_path)
    lineage = tuple(artifacts.publish(producer, str(i).encode(), "text/plain").artifact_id
                    for i in range(17))
    manifest = artifacts.publish(producer, b"derived", "text/plain", lineage=lineage)
    assert artifacts.read(producer.session, manifest.artifact_id)[0].lineage == lineage
