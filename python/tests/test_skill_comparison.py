from __future__ import annotations

import hashlib

import pytest

from ahc.domain.sessions import ContractError
from ahc.domain.skill_comparison import BehaviorCase, assess_comparison, suite_digest
from ahc.domain.skill_values import canonical_json


def _json(value: object) -> str:
    return canonical_json(value).decode("utf-8")


def _case(case_id: str, prompt: str, expected: object) -> BehaviorCase:
    return BehaviorCase(case_id, prompt, expected)


def test_behavior_case_round_trips_detached_portable_json() -> None:
    source = {"ok": [True, 3, None]}
    case = _case("case_1", "Return a portable result.", source)
    source["ok"].append("later")

    value = case.to_dict()
    assert value == {
        "case_id": "case_1",
        "prompt": "Return a portable result.",
        "expected": {"ok": [True, 3, None]},
    }
    restored = BehaviorCase.from_dict(value)
    value["expected"]["ok"].append("outside")  # type: ignore[index]
    assert restored.to_dict()["expected"] == {"ok": [True, 3, None]}


@pytest.mark.parametrize(
    "expected",
    [1.25, {1: "numeric key"}, ("tuple",), {"nested": [float("nan")]}, {"s": "\ud800"}],
)
def test_behavior_case_rejects_nonportable_expected_values(expected: object) -> None:
    with pytest.raises(ContractError):
        _case("case", "Prompt", expected)


def test_behavior_case_rejects_invalid_identity_prompt_and_mapping_shape() -> None:
    for case_id in ("", "has space", ".", "x" * 65):
        with pytest.raises(ContractError):
            _case(case_id, "Prompt", None)
    for prompt in ("", "x" * 1_201, "\ud800"):
        with pytest.raises(ContractError):
            _case("case", prompt, None)
    with pytest.raises(ContractError):
        BehaviorCase.from_dict({"case_id": "case", "prompt": "Prompt", "expected": 1, "extra": 2})


def test_expected_canonical_size_is_bounded() -> None:
    with pytest.raises(ContractError):
        _case("case", "Prompt", "x" * 4_097)


def test_suite_digest_validates_bounds_identity_and_prompt_uniqueness() -> None:
    case = _case("one", "First prompt", 1)
    assert suite_digest([case]) == hashlib.sha256(canonical_json([case.to_dict()])).hexdigest()
    with pytest.raises(ContractError):
        suite_digest([])
    with pytest.raises(ContractError):
        suite_digest([case] * 9)
    with pytest.raises(ContractError):
        suite_digest([case, _case("one", "Second prompt", 2)])
    with pytest.raises(ContractError):
        suite_digest([case, _case("two", "First prompt", 2)])


def test_suite_digest_binds_case_prompt_and_expected() -> None:
    original = [_case("one", "Prompt", {"answer": 1})]
    assert suite_digest(original) != suite_digest([_case("one", "Changed", {"answer": 1})])
    assert suite_digest(original) != suite_digest([_case("one", "Prompt", {"answer": 2})])
    assert suite_digest(original) != suite_digest([_case("two", "Prompt", {"answer": 1})])


def test_strict_all_case_improvement_is_accepted_and_outputs_are_identity_bound() -> None:
    cases = [_case("one", "Add one", {"answer": 2}), _case("two", "Name color", "blue")]
    skill_digest = "a" * 64
    baseline = ['{"answer":1}', '"green"']
    candidate = [_json({"answer": 2}), _json("blue")]

    result = assess_comparison(skill_digest, cases, baseline, candidate)

    assert result["candidate_digest"] == skill_digest
    assert result["suite_digest"] == suite_digest(cases)
    assert result["accepted"] is True
    assert result["reason"] == "held-out-improvement"
    assert result["baseline_passed"] == 0
    assert result["candidate_passed"] == 2
    evidence = result["evidence"]
    assert evidence[0]["candidate_digest"] == hashlib.sha256(candidate[0].encode()).hexdigest()
    assert evidence[0]["candidate_digest"] != skill_digest
    assert evidence[0]["task_digest"] == hashlib.sha256(cases[0].prompt.encode()).hexdigest()
    expected_hash = hashlib.sha256(canonical_json(cases[0].expected)).hexdigest()
    assert evidence[0]["expected_digest"] == expected_hash
    assert all(item["held_out"] is True for item in evidence)


def test_equal_scores_are_not_an_improvement() -> None:
    cases = [_case("one", "Return 1", 1), _case("two", "Return 2", 2)]
    result = assess_comparison("b" * 64, cases, ["1", "2"], ["1", "2"])

    assert result["accepted"] is False
    assert result["reason"] == "no-measurable-improvement"
    assert result["baseline_passed"] == result["candidate_passed"] == 2


def test_any_wrong_candidate_fails_even_when_its_score_improves() -> None:
    cases = [_case("one", "Return 1", 1), _case("two", "Return 2", 2)]
    result = assess_comparison("c" * 64, cases, ["0", "0"], ["1", "0"])

    assert result["baseline_passed"] == 0
    assert result["candidate_passed"] == 1
    assert result["accepted"] is False
    assert result["reason"] == "candidate-failed"


def test_per_case_regression_fails_comparison() -> None:
    cases = [_case("one", "Return 1", 1), _case("two", "Return 2", 2)]
    result = assess_comparison("d" * 64, cases, ["1", "0"], ["0", "2"])

    assert result["accepted"] is False
    assert result["reason"] == "candidate-failed"
    assert result["baseline_passed"] == result["candidate_passed"] == 1


@pytest.mark.parametrize(
    "output",
    ["```json\n1\n```", '{"answer":1} trailing', '{"x":1,"x":1}', "NaN", "Infinity"],
)
def test_malformed_strict_json_is_known_incorrect(output: str) -> None:
    cases = [_case("one", "Return one", {"answer": 1})]
    result = assess_comparison("e" * 64, cases, ['{"answer":1}'], [output])

    assert result["accepted"] is False
    assert result["reason"] == "candidate-failed"
    assert result["candidate_passed"] == 0
    assert result["evidence"][0]["candidate_digest"] == hashlib.sha256(output.encode()).hexdigest()
    assert result["evidence"][0]["candidate_correct"] is False


def test_unavailable_output_is_unresolved_and_has_null_evidence_digest() -> None:
    cases = [_case("one", "Return one", 1)]
    result = assess_comparison("f" * 64, cases, ["1"], [None])

    assert result["accepted"] is False
    assert result["reason"] == "unresolved-output"
    assert result["baseline_passed"] == 1
    assert result["candidate_passed"] == 0
    assert result["evidence"][0]["candidate_digest"] is None
    assert result["evidence"][0]["candidate_correct"] is False


def test_unavailable_baseline_also_prevents_acceptance() -> None:
    cases = [_case("one", "Return one", 1)]
    result = assess_comparison("1" * 64, cases, [None], ["1"])

    assert result["accepted"] is False
    assert result["reason"] == "unresolved-output"
    assert result["evidence"][0]["baseline_digest"] is None
    assert result["candidate_passed"] == 1


@pytest.mark.parametrize(
    ("expected", "output"),
    [
        (True, "1"),
        (1, "true"),
        ({"value": [True]}, '{"value":[1]}'),
        ([1], "[1.0]"),
    ],
)
def test_json_comparison_uses_recursive_type_exact_equality(expected: object, output: str) -> None:
    cases = [_case("one", "Return the exact type", expected)]
    result = assess_comparison("2" * 64, cases, [output], [output])

    assert result["baseline_passed"] == 0
    assert result["candidate_passed"] == 0
    assert result["reason"] == "candidate-failed"


def test_comparison_validates_digest_and_output_shape() -> None:
    cases = [_case("one", "Return one", 1)]
    with pytest.raises(ContractError):
        assess_comparison("not-a-digest", cases, ["0"], ["1"])
    with pytest.raises(ContractError):
        assess_comparison("3" * 64, cases, [], ["1"])
    with pytest.raises(ContractError):
        assess_comparison("3" * 64, cases, ["0"], [1])  # type: ignore[list-item]
