"""Atomic local publication of a verified artifact retained by an exact native child."""

import json
from hashlib import sha256

from ahc.adapters.aar_native_artifacts import VerifiedNativeArtifact, _validate_reference
from ahc.domain.sessions import ContractError, _identifier
from ahc.domain.skill_values import canonical_json
from ahc.domain.workspace import WorkspaceIdentity

from .artifact_repository import ArtifactRepository
from .native_child_repository import NativeChildRepository


class NativeArtifactImport:
    def __init__(self, children):
        self.native, self.store = NativeChildRepository(children), children.store

    def publish(self, parent, child_id, token, producer, artifact, *, allow_redacted=False):
        _identifier(token, "native artifact claim")
        if (type(artifact) is not VerifiedNativeArtifact or type(artifact.content) is not bytes
                or type(producer) is not WorkspaceIdentity or producer.session != parent.session
                or type(allow_redacted) is not bool):
            raise ContractError("native artifact import binding invalid")
        reference = json.loads(artifact.reference_json)
        encoded = canonical_json(reference).decode()
        if encoded != artifact.reference_json:
            raise ContractError("native artifact reference not canonical")

        def write(connection):
            envelope = self.native._child(connection, parent, child_id, token)
            if producer.workspace_id != envelope.artifact_scope:
                raise ContractError("native artifact workspace scope mismatch")
            row = connection.execute("SELECT * FROM runtime_native_children WHERE child_id=?",
                                     (child_id,)).fetchone()
            retained = self.native._decode(row, envelope)
            if retained is None or retained["receipt"] is None:
                raise ContractError("native artifact receipt unavailable")
            _validate_reference(reference, retained["operation_id"], 16384, allow_redacted)
            if (reference not in json.loads(retained["receipt"].payload_json).get("artifacts", [])
                    or len(artifact.content) != reference["size_bytes"]
                    or "sha256:" + sha256(artifact.content).hexdigest() != reference["digest"]):
                raise ContractError("native artifact reference or bytes mismatch")
            native_id = reference["artifact"]["value"]
            existing = connection.execute("SELECT * FROM runtime_native_artifacts "
                "WHERE child_id=? AND native_artifact_id=?", (child_id, native_id)).fetchone()
            if existing is not None and existing["reference_json"] != encoded:
                raise ContractError("native artifact identity conflict")
            if existing is None:
                budget = connection.execute("SELECT allocation_json FROM runtime_native_budgets "
                    "WHERE child_id=?", (child_id,)).fetchone()
                maximum = 16384 if budget is None else json.loads(budget[0])["artifact_bytes"]
                used = connection.execute("SELECT COALESCE(SUM(length(a.content)),0) "
                    "FROM runtime_native_artifacts n JOIN runtime_artifacts a "
                    "ON a.artifact_id=n.artifact_id WHERE n.child_id=?", (child_id,)).fetchone()[0]
                if used + len(artifact.content) > maximum:
                    raise ContractError("native artifact allocation exhausted")
            manifest = ArtifactRepository(self.store).publish(
                producer, artifact.content, reference["media_type"], _transaction=connection)
            if existing is not None:
                if existing["artifact_id"] != manifest.artifact_id:
                    raise ContractError("native artifact local identity conflict")
            else:
                connection.execute("INSERT INTO runtime_native_artifacts VALUES(?,?,?,?)",
                    (child_id, native_id, encoded, manifest.artifact_id))
            return manifest
        return self.store._mutate(write)

    def read(self, parent, child_id):
        with self.store._read() as connection:
            envelope = self.native._child(connection, parent, child_id)
            retained = connection.execute("SELECT operation_id FROM runtime_native_children "
                "WHERE child_id=?", (child_id,)).fetchone()
            rows = connection.execute("SELECT n.reference_json,n.native_artifact_id,a.* "
                "FROM runtime_native_artifacts n "
                "JOIN runtime_artifacts a ON a.artifact_id=n.artifact_id WHERE n.child_id=? "
                "ORDER BY n.native_artifact_id", (child_id,)).fetchall()
            result = []
            for row in rows:
                reference = json.loads(row["reference_json"])
                manifest, _ = ArtifactRepository._decode(row)
                if retained is None:
                    raise ContractError("native artifact operation missing")
                _validate_reference(reference, retained[0], 16384, True)
                if (canonical_json(reference).decode() != row["reference_json"]
                        or reference["artifact"]["value"] != row["native_artifact_id"]
                        or reference["digest"] != "sha256:" + manifest.content_digest
                        or reference["media_type"] != manifest.media_type
                        or reference["size_bytes"] != manifest.byte_count
                        or manifest.producer.session != parent.session
                        or manifest.producer.workspace_id != envelope.artifact_scope):
                    raise ContractError("native artifact mapping corrupted")
                result.append({"native_reference": reference, "artifact": manifest.to_dict()})
            return tuple(result)
