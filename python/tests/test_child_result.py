"""Shape compatibility is distinct from artifact ownership and byte-budget proof."""

from dataclasses import FrozenInstanceError

import pytest

from ahc.domain.child_result import ChildResult, normalize_child_result
from ahc.domain.result_artifacts import ResultArtifact
from ahc.domain.sessions import ContractError


def test_legacy_text_wire_shape_and_immutable_reference_roundtrip():
    assert normalize_child_result("hello").to_dict() == {"output_text": "hello"}
    assert ChildResult.from_dict({"output_text": "hello"}) == ChildResult("hello")
    value = ChildResult("report", (ResultArtifact("a" * 64, "report.txt"),))
    wire = value.to_dict()
    assert ChildResult.from_dict(wire) == value
    wire["artifacts"][0]["filename"] = "changed.txt"
    assert value.artifacts[0].filename == "report.txt"
    with pytest.raises(FrozenInstanceError):
        value.output_text = "changed"


@pytest.mark.parametrize("value", [
    {"output_text": "ok", "accepted": True},
    {"output_text": "ok", "artifacts": {}},
    {"output_text": "ok", "artifacts": [{"artifact_id": "a" * 64,
                                         "filename": "../escape"}]},
    {"output_text": "中" * 1400},
    {"output_text": "ok", "artifacts": [{"artifact_id": "a" * 64,
                                         "filename": "report.txt"}] * 2},
    {"output_text": "ok", "artifacts": [{"artifact_id": "a" * 64,
                                         "filename": "report.txt"}] * 9},
])
def test_invalid_child_result_rejects(value):
    with pytest.raises(ContractError):
        ChildResult.from_dict(value)


def test_port_result_does_not_accept_arbitrary_dict_or_list():
    for value in ({"output_text": "ok"}, ["ok"], None):
        with pytest.raises(ContractError):
            normalize_child_result(value)
