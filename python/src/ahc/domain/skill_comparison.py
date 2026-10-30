"""Pure held-out behavior comparison for proposed skill revisions."""

from __future__ import annotations

import json
import math
import re
from dataclasses import dataclass

from .sessions import ContractError
from .skill_values import canonical_json, digest, digest_bytes

_CASE_ID_RE = re.compile(r"[A-Za-z0-9_-]{1,64}\Z")
_CASE_FIELDS = frozenset({"case_id", "prompt", "expected"})


def _portable_json_bytes(value: object) -> bytes:
    """Validate the narrower JSON value contract used for owner-set expectations."""

    active: set[int] = set()

    def visit(item: object) -> None:
        if item is None or type(item) in {str, int, bool}:
            return
        if type(item) is list:
            marker = id(item)
            if marker in active:
                raise ContractError("expected must not contain a cycle")
            active.add(marker)
            try:
                for child in item:
                    visit(child)
            finally:
                active.remove(marker)
            return
        if type(item) is dict:
            marker = id(item)
            if marker in active:
                raise ContractError("expected must not contain a cycle")
            active.add(marker)
            try:
                for key, child in item.items():
                    if type(key) is not str:
                        raise ContractError("expected object keys must be strings")
                    visit(child)
            finally:
                active.remove(marker)
            return
        raise ContractError("expected contains a non-portable JSON value")

    try:
        visit(value)
        encoded = canonical_json(value)
    except RecursionError as exc:
        raise ContractError("expected JSON is too deeply nested") from exc
    if len(encoded) > 4_096:
        raise ContractError("expected exceeds 4096 canonical JSON bytes")
    return encoded


def _copy_portable_json(value: object) -> object:
    """Return a detached plain JSON tree after checking its bounded encoding."""

    return json.loads(_portable_json_bytes(value))


@dataclass(frozen=True, slots=True)
class BehaviorCase:
    """One owner-configured prompt and expected portable JSON answer."""

    case_id: str
    prompt: str
    expected: object

    def __post_init__(self) -> None:
        if type(self.case_id) is not str or _CASE_ID_RE.fullmatch(self.case_id) is None:
            raise ContractError("case_id must match [A-Za-z0-9_-] with a maximum of 64 characters")
        if type(self.prompt) is not str or not self.prompt or len(self.prompt) > 1_200:
            raise ContractError("prompt must contain 1 to 1200 characters")
        try:
            self.prompt.encode("utf-8", errors="strict")
        except UnicodeEncodeError as exc:
            raise ContractError("prompt contains an unpaired surrogate") from exc
        object.__setattr__(self, "expected", _copy_portable_json(self.expected))

    def to_dict(self) -> dict[str, object]:
        """Emit a detached exact-key JSON-shaped mapping."""

        return {
            "case_id": self.case_id,
            "prompt": self.prompt,
            "expected": _copy_portable_json(self.expected),
        }

    @classmethod
    def from_dict(cls, value: dict[str, object]) -> BehaviorCase:
        """Parse an exact-key case mapping without coercion."""

        if type(value) is not dict or set(value) != _CASE_FIELDS:
            raise ContractError("invalid behavior case fields")
        if any(type(key) is not str for key in value):
            raise ContractError("behavior case keys must be strings")
        return cls(value["case_id"], value["prompt"], value["expected"])


def _cases_for_digest(
    cases: tuple[BehaviorCase, ...] | list[BehaviorCase],
) -> tuple[BehaviorCase, ...]:
    if type(cases) not in {tuple, list} or not 1 <= len(cases) <= 8:
        raise ContractError("behavior suite must contain 1 to 8 cases")
    if any(type(case) is not BehaviorCase for case in cases):
        raise ContractError("behavior suite must contain BehaviorCase values")

    records = tuple(cases)
    ids: set[str] = set()
    prompt_digests: set[str] = set()
    for case in records:
        if case.case_id in ids:
            raise ContractError("behavior suite contains duplicate case_id values")
        ids.add(case.case_id)
        prompt_digest = digest_bytes(case.prompt.encode("utf-8"))
        if prompt_digest in prompt_digests:
            raise ContractError("behavior suite contains duplicate prompts")
        prompt_digests.add(prompt_digest)
        case.to_dict()
    return records


def suite_digest(cases: tuple[BehaviorCase, ...] | list[BehaviorCase]) -> str:
    """Digest the ordered, validated case records that define one held-out suite."""

    records = _cases_for_digest(cases)
    return digest_bytes(canonical_json([case.to_dict() for case in records]))


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate object key")
        result[key] = value
    return result


def _reject_json_constant(_value: str) -> object:
    raise ValueError("non-standard JSON constant")


def _valid_parsed_json(value: object) -> bool:
    """Reject values Python's JSON decoder can construct outside portable JSON."""

    if value is None or type(value) in {int, bool}:
        return True
    if type(value) is float:
        return math.isfinite(value)
    if type(value) is str:
        return not any(0xD800 <= ord(char) <= 0xDFFF for char in value)
    if type(value) is list:
        return all(_valid_parsed_json(item) for item in value)
    if type(value) is dict:
        return all(
            type(key) is str
            and not any(0xD800 <= ord(char) <= 0xDFFF for char in key)
            and _valid_parsed_json(item)
            for key, item in value.items()
        )
    return False


def _strict_json_equal(actual: object, expected: object) -> bool:
    """Compare JSON trees without Python's bool/int equality shortcut."""

    if type(actual) is not type(expected):
        return False
    if type(actual) is dict:
        return actual.keys() == expected.keys() and all(
            _strict_json_equal(actual[key], expected[key]) for key in actual
        )
    if type(actual) is list:
        return len(actual) == len(expected) and all(
            _strict_json_equal(left, right) for left, right in zip(actual, expected, strict=True)
        )
    return actual == expected


def _assess_output(output: str | None, expected: object) -> tuple[str | None, bool]:
    if output is None:
        return None, False
    output_digest = digest_bytes(output.encode("utf-8", errors="surrogatepass"))
    try:
        parsed = json.loads(
            output,
            object_pairs_hook=_unique_object,
            parse_constant=_reject_json_constant,
        )
        if not _valid_parsed_json(parsed):
            return output_digest, False
    except (json.JSONDecodeError, ValueError, RecursionError):
        return output_digest, False
    return output_digest, _strict_json_equal(parsed, expected)


def _output_list(value: tuple[str | None, ...] | list[str | None], label: str, size: int):
    if type(value) not in {tuple, list} or len(value) != size:
        raise ContractError(f"{label} must contain one output per behavior case")
    if any(item is not None and type(item) is not str for item in value):
        raise ContractError(f"{label} values must be text or null")
    return tuple(value)


def assess_comparison(
    candidate_digest: str,
    cases: tuple[BehaviorCase, ...] | list[BehaviorCase],
    baseline_outputs: tuple[str | None, ...] | list[str | None],
    candidate_outputs: tuple[str | None, ...] | list[str | None],
) -> dict[str, object]:
    """Compare actual strict-JSON outputs and accept only resolved held-out gains."""

    digest(candidate_digest, "candidate_digest")
    records = _cases_for_digest(cases)
    baseline = _output_list(baseline_outputs, "baseline_outputs", len(records))
    candidate = _output_list(candidate_outputs, "candidate_outputs", len(records))
    expected_suite_digest = suite_digest(records)

    evidence: list[dict[str, object]] = []
    for case, baseline_output, candidate_output in zip(
        records, baseline, candidate, strict=True
    ):
        expected = case.expected
        baseline_hash, baseline_correct = _assess_output(baseline_output, expected)
        candidate_hash, candidate_correct = _assess_output(candidate_output, expected)
        evidence.append(
            {
                "task_digest": digest_bytes(case.prompt.encode("utf-8")),
                "expected_digest": digest_bytes(canonical_json(expected)),
                "baseline_digest": baseline_hash,
                "candidate_digest": candidate_hash,
                "baseline_correct": baseline_correct,
                "candidate_correct": candidate_correct,
                "held_out": True,
            }
        )

    baseline_passed = sum(item["baseline_correct"] is True for item in evidence)
    candidate_passed = sum(item["candidate_correct"] is True for item in evidence)
    unresolved = any(output is None for output in (*baseline, *candidate))
    regression = any(
        item["baseline_correct"] is True and item["candidate_correct"] is not True
        for item in evidence
    )
    candidate_failed = candidate_passed != len(records) or regression

    if unresolved:
        accepted, reason = False, "unresolved-output"
    elif candidate_failed:
        accepted, reason = False, "candidate-failed"
    elif candidate_passed <= baseline_passed:
        accepted, reason = False, "no-measurable-improvement"
    else:
        accepted, reason = True, "held-out-improvement"

    return {
        "candidate_digest": candidate_digest,
        "suite_digest": expected_suite_digest,
        "accepted": accepted,
        "reason": reason,
        "baseline_passed": baseline_passed,
        "candidate_passed": candidate_passed,
        "evidence": evidence,
    }


__all__ = ["BehaviorCase", "assess_comparison", "suite_digest"]
