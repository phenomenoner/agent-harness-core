"""Installed-surface JSON CLI witnesses for the active skill loop."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from skill_test_support import goal
from test_skill_active_loop import _lesson

from ahc.cli.main import main
from ahc.domain import OutcomeAttribution
from ahc.domain.judgment_records import ATTRIBUTION_SCHEMA
from ahc.domain.skill_behavior import HeldOutTaskEvidence
from ahc.domain.skill_values import digest_json


def _call(capsys: Any, *args: str) -> tuple[int, dict[str, object]]:
    exit_code = main(list(args))
    captured = capsys.readouterr()
    assert captured.err == ""
    lines = captured.out.splitlines()
    assert len(lines) == 1
    payload = json.loads(lines[0])
    assert isinstance(payload, dict)
    return exit_code, payload


def _learn_with_evidence(capsys, *args):
    # Stage one exposes the exact candidate; stage two supplies a separate held-out receipt.
    proposal_args = tuple(arg for arg in args if arg not in ("--activate", "--evaluate"))
    code, proposal = _call(capsys, *proposal_args)
    assert code == 0
    evidence = HeldOutTaskEvidence(
        proposal["candidate"]["revision_digest"],
        digest_json({"task": "held-out"}),
        digest_json({"answer": 42}),
        digest_json({"answer": 42}),
        ("cli-held-out-receipt",),
    )
    return _call(capsys, *args, "--held-out-evidence-json", json.dumps([evidence.to_dict()]))


def test_learning_control_read_is_nonmutating_and_stop_survives_reopen(repository,capsys):
    from ahc.application.skill_experiments import SkillExperiments
    args=('learning-control','--skill-root',str(repository.root),'--agent-id','main')
    with repository._read() as db:
        before=[tuple(row) for row in db.execute('SELECT type,name,sql FROM sqlite_master ORDER BY name')]
    code,value=_call(capsys,*args)
    assert code==0 and value['automatic_enabled'] is False and value['daily_limit'] is None
    with repository._read() as db:
        assert [tuple(row) for row in db.execute('SELECT type,name,sql FROM sqlite_master ORDER BY name')]==before
    assert _call(capsys,*args,'--enable')[1]['automatic_enabled'] is True
    assert _call(capsys,*args,'--disable')[1]['automatic_enabled'] is False
    SkillExperiments(repository,owner='main',granted_tools=(),protected_names=(),protection_check=lambda:False)
    assert _call(capsys,*args)[1]['automatic_enabled'] is False


def _turn_args(root: Path, generation: int, episode_id: str) -> tuple[str, ...]:
    return (
        "skill-turn",
        "--skill-root",
        str(root),
        "--virtual-session-id",
        "cli-session",
        "--generation",
        str(generation),
        "--agent-id",
        "agent-test",
        "--route-id",
        "route-test",
        "--turn-id",
        f"turn-{generation}",
        "--roles",
        "procedure",
        "--task-text",
        "analyze task",
        "--goal-json",
        json.dumps(goal().to_dict()),
        "--episode-id",
        episode_id,
    )


def test_source_cli_full_loop_and_exact_rollback(skill_root: Path, capsys: Any) -> None:
    code, first = _call(capsys, *_turn_args(skill_root, 1, "cli-episode-1"))
    assert code == 0
    assert first["manifest"]["entries"] == []

    attribution1 = OutcomeAttribution(
        ATTRIBUTION_SCHEMA,
        "cli-episode-1",
        "wrong-how",
        ("how-procedure",),
        ("cli-evidence-1",),
        "cli-verifier",
    )
    code, learned1 = _learn_with_evidence(
        capsys,
        "skill-learn",
        "--skill-root",
        str(skill_root),
        "--episode-id",
        "cli-episode-1",
        "--outcome",
        "verified-failure",
        "--evidence-ref",
        "cli-evidence-1",
        "--attribution-json",
        json.dumps(attribution1.to_dict()),
        "--lesson-json",
        json.dumps(_lesson()),
        "--reflect",
        "--evaluate",
        "--activate",
        "--operation-key",
        "cli-activate-1",
    )
    assert code == 0
    v1 = learned1["candidate"]["revision_digest"]
    assert learned1["evaluation"]["verdict"] == "accept"

    code, second = _call(capsys, *_turn_args(skill_root, 2, "cli-episode-2"))
    assert code == 0
    assert second["manifest"]["entries"][0]["revision_digest"] == v1

    attribution2 = OutcomeAttribution(
        ATTRIBUTION_SCHEMA,
        "cli-episode-2",
        "wrong-how",
        ("how-procedure",),
        ("cli-evidence-2",),
        "cli-verifier",
    )
    lesson2 = _lesson()
    lesson2["body"] = str(lesson2["body"]).replace(
        "Inspect the provided evidence.", "Inspect and compare the provided evidence."
    )
    code, learned2 = _learn_with_evidence(
        capsys,
        "skill-learn",
        "--skill-root",
        str(skill_root),
        "--episode-id",
        "cli-episode-2",
        "--outcome",
        "verified-failure",
        "--evidence-ref",
        "cli-evidence-2",
        "--used-revision",
        str(v1),
        "--attribution-json",
        json.dumps(attribution2.to_dict()),
        "--lesson-json",
        json.dumps(lesson2),
        "--reflect",
        "--evaluate",
        "--activate",
        "--operation-key",
        "cli-activate-2",
    )
    assert code == 0
    v2 = learned2["candidate"]["revision_digest"]
    assert v2 != v1

    code, rolled = _call(
        capsys,
        "skill-rollback",
        "--skill-root",
        str(skill_root),
        "--skill-name",
        "analyze-skill",
        "--expected-active-digest",
        str(v2),
        "--operation-key",
        "cli-rollback-1",
    )
    assert code == 0
    assert rolled["rollback"]["to_digest"] == v1


def test_request_json_rejects_competing_builder_authority(skill_root: Path, capsys: Any) -> None:
    request_json = json.dumps(
        {
            "schema_version": "ahc.fake-turn-request.v1",
            "session": {
                "schema_version": "ahc.virtual-session-identity.v1",
                "ref": {
                    "schema_version": "ahc.virtual-session-ref.v1",
                    "virtual_session_id": "cli-authority",
                    "generation": 1,
                },
                "agent_id": "agent-test",
                "route_id": "route-test",
                "predecessor": None,
            },
            "turn_id": "turn-authority",
            "input_text": "analyze task",
        }
    )
    code, payload = _call(
        capsys,
        "skill-turn",
        "--skill-root",
        str(skill_root),
        "--request-json",
        request_json,
        "--task-text",
        "competing task",
        "--goal-json",
        json.dumps(goal().to_dict()),
    )
    assert code == 2
    assert payload["error"]["message"] == "command contract violated"
