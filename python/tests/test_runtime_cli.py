"""Local CLI composition and fresh-process restart/readback, no provider calls."""

import json
import os
import subprocess
import sys
from pathlib import Path

from skill_test_support import goal

from ahc.cli.main import main
from ahc.domain.sessions import VirtualSessionIdentity
from ahc.infrastructure.skill_repository import SkillRepository


def call(capsys, *args):
    code = main(list(args))
    captured = capsys.readouterr()
    assert captured.err == ""
    return code, json.loads(captured.out)


def test_open_skills_inert_turn_and_fresh_process_status(tmp_path, skill_root, capsys):
    owner = VirtualSessionIdentity.initial("cli-session", "agent", "route")
    common = (
        "--runtime-root",
        str(tmp_path / "runtime"),
        "--session-json",
        json.dumps(owner.to_dict()),
    )
    code, opened = call(
        capsys, "runtime-open", *common, "--goal-json", json.dumps(goal().to_dict())
    )
    assert code == 0 and opened["working_set"]["status"] == "active"
    SkillRepository.create(skill_root)
    fixture = tmp_path / "peer.json"
    fixture.write_text(
        json.dumps(
            [
                {"id": 1, "result": {"protocol": "inert-v1"}},
                {"id": 2, "result": {"threadId": "thread"}},
                {"id": 3, "result": {"threadId": "thread", "turnId": "turn"}},
                {
                    "method": "turn/completed",
                    "params": {
                        "threadId": "thread",
                        "turnId": "turn",
                        "status": "completed",
                        "output": "answer",
                    },
                },
            ]
        ),
        encoding="utf-8",
    )
    args = (
        "runtime-turn",
        *common,
        "--operation-id",
        "operation",
        "--input-text",
        "analyze",
        "--backend",
        "inert",
        "--peer-json",
        str(fixture),
        "--skill-root",
        str(skill_root),
    )
    code, completed = call(capsys, *args)
    assert code == 0 and completed["turn"]["result"]["accepted"]
    assert completed["skills"] == "selected"
    code, replay = call(capsys, *args)
    assert replay == completed
    fixture.write_text(
        fixture.read_text(encoding="utf-8").replace("answer", "changed"), encoding="utf-8"
    )
    code, collision = call(capsys, *args)
    assert code == 2 and collision["error"]["kind"] == "contract"
    env = {
        **os.environ,
        "PYTHONPATH": str(Path(__file__).parents[1] / "src"),
        "PYTHONDONTWRITEBYTECODE": "1",
    }
    child = subprocess.run(
        [sys.executable, "-B", "-m", "ahc", "runtime-status", *common],
        env=env,
        cwd=tmp_path,
        capture_output=True,
        text=True,
        timeout=15,
    )
    assert child.returncode == 0 and child.stderr == ""
    status = json.loads(child.stdout)
    assert len(status["intents"]) == 1
    assert status["working_set"]["active_turn_id"] is None
    assert set(status["capabilities"].values()) == {"not-inspected"}


def test_unavailable_backend_is_parked_and_cancel_is_readable(tmp_path, capsys):
    owner = VirtualSessionIdentity.initial("session", "agent", "route")
    common = (
        "--runtime-root",
        str(tmp_path / "runtime"),
        "--session-json",
        json.dumps(owner.to_dict()),
    )
    call(capsys, "runtime-open", *common, "--goal-json", json.dumps(goal().to_dict()))
    code, result = call(
        capsys, "runtime-turn", *common, "--operation-id", "op", "--input-text", "hello"
    )
    assert code == 2 and result["turn"]["result"]["disposition"] == "unsupported"
    _, status = call(capsys, "runtime-status", *common)
    assert status["intents"][0]["kind"] == "parked"
    _, cancelled = call(capsys, "runtime-cancel", *common)
    assert cancelled["working_set"]["status"] == "cancelled"


def test_explicit_generation_continuation_and_completion(tmp_path, capsys):
    owner = VirtualSessionIdentity.initial("session", "agent", "route")
    root = ("--runtime-root", str(tmp_path / "runtime"))
    common = (*root, "--session-json", json.dumps(owner.to_dict()))
    _, opened = call(capsys, "runtime-open", *common, "--goal-json", json.dumps(goal().to_dict()))
    successor = owner.successor()
    code, continued = call(
        capsys,
        "runtime-continue",
        *common,
        "--successor-session-json",
        json.dumps(successor.to_dict()),
        "--reason",
        "compaction",
    )
    assert code == 0
    assert continued["working_set"]["goal"] == opened["working_set"]["goal"]
    code, rejected = call(capsys, "runtime-status", *common)
    assert code == 2 and rejected["error"]["kind"] == "contract"
    code, completed = call(
        capsys, "runtime-complete", *root, "--session-json", json.dumps(successor.to_dict())
    )
    assert code == 0 and completed["working_set"]["status"] == "completed"


def test_process_exit_releases_execution_lock_and_does_not_replay(tmp_path):
    from test_runtime_core import setup

    from ahc.infrastructure.runtime_repository import RuntimeRepository

    repo, snapshot, req = setup(tmp_path)
    # The child exits after durable admission while holding the real OS lock.
    script = """
import os, sys
from ahc.infrastructure.runtime_repository import RuntimeRepository
from ahc.domain.agent_backend import AgentTurnRequest
repo = RuntimeRepository(sys.argv[1])
snapshot = repo.read_current('session')
request = AgentTurnRequest(snapshot.session, 'operation', snapshot.goal.digest,
                           'question', (), 'a'*64, 'b'*64, 'workspace', 100)
with repo.execution_lock():
    repo.begin(snapshot, request)
    os._exit(17)
"""
    env = {
        **os.environ,
        "PYTHONPATH": str(Path(__file__).parents[1] / "src"),
        "PYTHONDONTWRITEBYTECODE": "1",
    }
    child = subprocess.run(
        [sys.executable, "-B", "-c", script, str(repo.root)],
        cwd=tmp_path,
        env=env,
        capture_output=True,
        timeout=15,
    )
    assert child.returncode == 17 and child.stdout == child.stderr == b""
    reopened = RuntimeRepository(repo.root)
    assert reopened.read_turn(req.session, req.operation_id).status == "submitted"
    assert reopened.intents(req.session) == ()
    recovered = reopened.reconcile(req.session)
    assert recovered[0].result["disposition"] == "unknown"
    assert reopened.read_current("session").active_turn_id is None
    assert len(reopened.intents(req.session)) == 1
