"""Child-local artifact proposals; no workspace execution or channel delivery tools."""

from types import SimpleNamespace

from ahc.domain.artifacts import ArtifactNotPublished
from ahc.domain.result_artifacts import ResultArtifact
from ahc.domain.sessions import ContractError
from ahc.domain.workspace import WorkspaceIdentity

from .child_artifacts import ChildArtifacts, validate_child_artifacts


class ChildArtifactTools:
    def __init__(self, children, envelope, token, producer):
        self.children, self.envelope, self.token = children, envelope, token
        self.workspace = SimpleNamespace(identity=producer)

    def capabilities(self):
        return ("workspace.publish_text", "workspace.attach")

    def invoke(self, name, arguments):
        try:
            if (name not in self.capabilities() or type(arguments) is not dict
                    or WorkspaceIdentity.from_dict(arguments["expected"]) != self.workspace.identity
                    or self.children.should_stop(self.envelope.parent,
                                                 self.envelope.child_id, self.token)):
                raise ContractError("child artifact tool unavailable")
            if name == "workspace.publish_text":
                if (set(arguments) != {"expected", "text", "media_type"}
                        or type(arguments["text"]) is not str):
                    raise ContractError("child artifact text invalid")
                arguments["text"].encode("utf-8")
            else:
                if set(arguments) != {"expected", "artifact_id", "filename"}:
                    raise ContractError("child artifact selection invalid")
                selection = ResultArtifact(arguments["artifact_id"], arguments["filename"])
        except Exception:
            return {"disposition": "rejected", "unresolved_effects": False,
                    "reason": "child-artifact-input"}
        try:
            if name == "workspace.publish_text":
                manifest = ChildArtifacts(self.children).publish(self.envelope.parent,
                    self.envelope.child_id, self.token, self.workspace.identity,
                    arguments["text"].encode("utf-8"), arguments["media_type"])
                value = {"artifact_id": manifest.artifact_id, **manifest.to_dict()}
            else:
                with self.children.store._read() as connection:
                    self.children._require_parent(connection, self.envelope.parent)
                    self.children._row(connection, self.envelope.parent, self.envelope.child_id)
                    validate_child_artifacts(connection, self.envelope, (selection,))
                value = selection.to_dict()
            return {"disposition": "observed", "unresolved_effects": False, "result": value}
        except ArtifactNotPublished:
            return {"disposition": "rejected", "unresolved_effects": False,
                    "reason": "child-artifact-not-published",
                    "recovery": "Nothing was published. Check the parent allocation and "
                                "request, or return the confirmed text instead."}
        except Exception:
            if name == "workspace.attach":
                return {"disposition": "rejected", "unresolved_effects": False,
                        "reason": "child-artifact-selection-unavailable"}
            return {"disposition": "unknown", "unresolved_effects": True,
                    "reason": "child-artifact-unverified"}
