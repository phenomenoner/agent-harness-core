"""Child result proposals; artifact references require separate owner validation."""

from dataclasses import dataclass

from .child_work import child_output
from .result_artifacts import ResultArtifact, result_artifacts
from .sessions import ContractError


@dataclass(frozen=True, slots=True)
class ChildResult:
    output_text: str
    artifacts: tuple[ResultArtifact, ...] = ()

    def __post_init__(self):
        child_output(self.output_text)
        result_artifacts(self.artifacts)

    def to_dict(self):
        # Keep the existing durable text-only representation exactly compatible.
        return {"output_text": self.output_text,
                **({"artifacts": result_artifacts(self.artifacts)} if self.artifacts else {})}

    @classmethod
    def from_dict(cls, value):
        if (type(value) is not dict
                or set(value) not in ({"output_text"}, {"output_text", "artifacts"})):
            raise ContractError("child result fields invalid")
        artifacts = value.get("artifacts", [])
        if type(artifacts) is not list:
            raise ContractError("child result artifacts invalid")
        return cls(value["output_text"], tuple(ResultArtifact.from_dict(v) for v in artifacts))


def normalize_child_result(value):
    """Normalize port output; this does not authorize an artifact or final delivery."""
    if type(value) is str:
        return ChildResult(value)
    if type(value) is ChildResult:
        return value
    raise ContractError("child result type invalid")
