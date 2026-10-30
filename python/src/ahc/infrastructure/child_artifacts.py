"""Explicit child artifact allocations and atomic publication in the parent workspace."""

from ahc.domain.artifacts import ArtifactManifest, ArtifactNotPublished, content_digest
from ahc.domain.sessions import ContractError

from .artifact_repository import ArtifactRepository


class ChildArtifacts:
    def __init__(self, children):
        self.children, self.store = children, children.store

    def allocate(self, parent, child_id, *, max_count, max_bytes):
        if (type(max_count) is not int or max_count < 0
                or type(max_bytes) is not int or max_bytes < 0):
            raise ContractError("child artifact allocation invalid")

        def write(connection):
            self.children._require_parent(connection, parent)
            row, envelope, _ = self.children._row(connection, parent, child_id)
            if envelope.execution_kind != "subagent" or row["state"] != "queued":
                raise ContractError("child artifact allocation requires queued ordinary child")
            previous = connection.execute(
                "SELECT max_count,max_bytes FROM runtime_child_artifact_limits WHERE child_id=?",
                (child_id,)).fetchone()
            if previous is not None:
                if tuple(previous) != (max_count, max_bytes):
                    raise ContractError("child artifact allocation already bound")
                return
            connection.execute("INSERT INTO runtime_child_artifact_limits VALUES(?,?,?)",
                               (child_id, max_count, max_bytes))
        self.store._mutate(write)

    def publish(self, parent, child_id, token, producer, content, media_type):
        mutating, manifest = False, None

        def write(connection):
            nonlocal mutating, manifest
            self.children._require_parent(connection, parent)
            row, envelope, _ = self.children._row(connection, parent, child_id)
            if (row["state"] != "claimed" or row["claim_token"] != token
                    or envelope.execution_kind != "subagent"
                    or envelope.deadline_unix_ms <= self.children._now()
                    or self.children._monotonic_now() >= self.children.request.effective_deadline
                    or producer.session != parent.session
                    or producer.workspace_id != envelope.artifact_scope):
                raise ContractError("child artifact publication owner mismatch")
            count, total, ids = validate_child_artifacts(connection, envelope)
            allocation = connection.execute(
                "SELECT max_count,max_bytes FROM runtime_child_artifact_limits WHERE child_id=?",
                (child_id,)).fetchone()
            proposed = ArtifactManifest(producer, content_digest(content), media_type, len(content))
            additional = proposed.artifact_id not in ids
            if (count + additional > allocation[0]
                    or total + (len(content) if additional else 0) > allocation[1]):
                raise ContractError("child artifact allocation exhausted")
            # Publish and budget enforcement share one rollback boundary.
            mutating = True
            manifest = ArtifactRepository(self.store).publish(
                producer, content, media_type, _transaction=connection)
            connection.execute("INSERT OR IGNORE INTO runtime_child_artifacts VALUES(?,?)",
                               (child_id, manifest.artifact_id))
            validate_child_artifacts(connection, envelope)
            return manifest
        try:
            return self.store._mutate(write)
        except ArtifactNotPublished:
            raise
        except Exception as error:
            if not mutating:
                raise ArtifactNotPublished("child-artifact-publication-rejected") from error
            if manifest is not None:
                try:
                    with self.store._read() as connection:
                        row = connection.execute(
                            "SELECT a.* FROM runtime_child_artifacts c JOIN runtime_artifacts a "
                            "ON a.artifact_id=c.artifact_id WHERE c.child_id=? AND a.artifact_id=?",
                            (child_id, manifest.artifact_id),).fetchone()
                        if row is None:
                            raise ArtifactNotPublished("child-artifact-not-committed") from error
                        observed, stored = ArtifactRepository._decode(row)
                        if observed == manifest and stored == content:
                            return observed
                except ArtifactNotPublished:
                    raise
                except Exception:
                    pass
            raise  # The allocation/record cannot be reconciled; never repeat the write.


def validate_child_artifacts(connection, envelope, selections=()):
    allocation = connection.execute(
        "SELECT max_count,max_bytes FROM runtime_child_artifact_limits WHERE child_id=?",
        (envelope.child_id,)).fetchone()
    if allocation is None:
        raise ContractError("child artifact allocation unavailable")
    rows = connection.execute(
        "SELECT a.* FROM runtime_child_artifacts c JOIN runtime_artifacts a "
        "ON a.artifact_id=c.artifact_id WHERE c.child_id=?", (envelope.child_id,)).fetchall()
    total, ids = 0, set()
    for row in rows:
        manifest, content = ArtifactRepository._decode(row)
        if (manifest.producer.session != envelope.parent.session
                or manifest.producer.workspace_id != envelope.artifact_scope):
            raise ContractError("child artifact producer mismatch")
        total += len(content)
        ids.add(manifest.artifact_id)
    if len(rows) > allocation[0] or total > allocation[1]:
        raise ContractError("child artifact allocation exhausted")
    if any(item.artifact_id not in ids for item in selections):
        raise ContractError("child result artifact not owned")
    return len(rows), total, ids
