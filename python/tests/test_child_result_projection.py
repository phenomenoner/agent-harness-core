import json

from ahc.cli.foreground_child_batch import project_child_result
from ahc.domain.child_result import ChildResult
from ahc.domain.result_artifacts import ResultArtifact


def test_long_text_does_not_discard_artifact_references():
    value = ChildResult("x" * 3000, (ResultArtifact("a" * 64, "report.txt"),)).to_dict()
    result = project_child_result(value)
    assert result["output_truncated"] is True
    assert result["artifacts"] == value["artifacts"]
    assert value["output_text"] == "x" * 3000


def test_maximum_unicode_artifacts_compact_to_all_ids_with_bounded_batch():
    refs = tuple(ResultArtifact(f"{i:064x}", "中" * 124 + ".txt") for i in range(8))
    value = ChildResult("中" * 1000, refs).to_dict()
    result = project_child_result(value)
    assert result["artifact_ids"] == [item.artifact_id for item in refs]
    assert result["artifacts_compacted"] is True
    assert len(json.dumps(result)) <= 1600
    batch = {"disposition": "observed", "unresolved_effects": False, "result": [
        {"disposition": "observed", "unresolved_effects": False,
         "result": {"child_id": "a" * 64, "state": "terminal", "result": result}}
        for _ in range(16)]}
    assert len(json.dumps(batch).encode()) < 32768
    assert value["artifacts"] == [item.to_dict() for item in refs]
