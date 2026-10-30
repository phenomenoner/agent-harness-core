"""Doctor counts exact producer metadata without reading/publishing artifact content."""

import hashlib
from dataclasses import replace

import pytest
from test_artifacts import state

from ahc.domain.artifacts import ArtifactManifest, content_digest
from ahc.domain.skill_values import canonical_json
from ahc.infrastructure.runtime_health import inspect_runtime_health


def test_exact_producer_counts_are_read_only_and_not_content_verification(tmp_path):
    repo, snapshot, producer, _ = state(tmp_path)
    items = [(producer, b"private-one"), (producer, b"second"),
             (replace(producer, session=producer.session.successor()), b"foreign")]

    def populate(connection):
        for owner, content in items:
            manifest = ArtifactManifest(owner, content_digest(content), "text/plain", len(content))
            connection.execute("INSERT INTO runtime_artifacts VALUES(?,?,?)",
                (manifest.artifact_id, canonical_json(manifest.to_dict()).decode(), content))

    repo._mutate(populate)
    before = hashlib.sha256(repo.db_path.read_bytes()).hexdigest()
    report = inspect_runtime_health(repo, snapshot.session)["domains"]["artifacts"]
    assert report == {"status": "observed", "state_counts": {"recorded": 2},
        "stored_bytes": 17, "content_verified": False, "ownership_scope": "exact-producer-session"}
    assert hashlib.sha256(repo.db_path.read_bytes()).hexdigest() == before


@pytest.mark.parametrize("damage", ["absent", "invalid-json", "missing-owner"])
def test_absent_or_unassignable_metadata_preserves_other_domains(tmp_path, damage):
    repo, snapshot, _, _ = state(tmp_path)

    def mutate(connection):
        if damage == "absent":
            connection.execute("DROP TABLE runtime_artifacts")
        else:
            connection.execute("INSERT INTO runtime_artifacts VALUES(?,?,?)",
                ("bad", "not-json" if damage == "invalid-json" else "{}", b"secret"))

    repo._mutate(mutate)
    before = hashlib.sha256(repo.db_path.read_bytes()).hexdigest()
    report = inspect_runtime_health(repo, snapshot.session)["domains"]
    expected = "schema-absent" if damage == "absent" else "unreadable"
    assert report["artifacts"] == {"status": expected}
    assert report["turns"]["status"] == "observed"
    assert hashlib.sha256(repo.db_path.read_bytes()).hexdigest() == before
