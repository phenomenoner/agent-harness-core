"""Abrupt foreground exit after an observed local effect must never replay it."""

import os
import subprocess
import sys
from pathlib import Path

import pytest
from test_runtime_core import setup

from ahc.application.foreground_runtime import ForegroundRuntime
from ahc.application.local_runtime_ingress import LocalRuntimeIngress
from ahc.domain.agent_backend import AgentTurnObservation
from ahc.infrastructure.runtime_inbox import RuntimeInbox
from ahc.infrastructure.runtime_repository import RuntimeRepository

CHILD = r"""
import os, sys
from pathlib import Path
from ahc.application.foreground_runtime import ForegroundRuntime
from ahc.application.local_runtime_ingress import LocalRuntimeIngress
from ahc.domain.agent_backend import AgentTurnObservation
from ahc.infrastructure.runtime_inbox import RuntimeInbox
from ahc.infrastructure.runtime_repository import RuntimeRepository

repo = RuntimeRepository(sys.argv[1])
current = repo.read_current('session')
marker = Path(sys.argv[2])
phase = sys.argv[3]
ingress = LocalRuntimeIngress(RuntimeInbox(repo), artifact_digest='a'*64,
    configuration_digest=lambda _: 'b'*64, workspace_scope='workspace')

class Backend:
    def invoke(self, request):
        # Durable local effect proves invoke was reached, independent of SQLite acceptance.
        with marker.open('xb') as stream:
            stream.write(request.operation_id.encode('ascii'))
            stream.flush()
            os.fsync(stream.fileno())
        if phase == 'before-terminal':
            os._exit(17)
        return AgentTurnObservation(request, 'completed', 'local-fixture', 1, 1,
                                    'thread', 'turn', 'answer')

runtime = ForegroundRuntime(repo, current.session, ingress, lambda *a, **k: Backend())
runtime.start()
result = runtime.step()
assert result.result['accepted'] is True
# Abruptly lose the caller response after the terminal transaction is committed.
os._exit(18)
"""


@pytest.mark.parametrize("phase", ["before-terminal", "after-terminal"])
def test_foreground_effect_survives_process_exit_without_replay(tmp_path, phase):
    repo, snapshot, request = setup(tmp_path)
    inbox = RuntimeInbox(repo)
    submission = inbox.submit(
        request.session, request.operation_id, request.goal_digest, request.input_text
    )
    marker = tmp_path / "effect"
    env = {
        **os.environ,
        "PYTHONPATH": str(Path(__file__).parents[1] / "src"),
        "PYTHONDONTWRITEBYTECODE": "1",
    }
    child = subprocess.run(
        [sys.executable, "-B", "-c", CHILD, str(repo.root), str(marker), phase],
        cwd=tmp_path,
        env=env,
        capture_output=True,
        timeout=15,
    )
    assert child.returncode == (17 if phase == "before-terminal" else 18), child.stderr
    assert child.stdout == child.stderr == b""
    assert marker.read_bytes() == request.operation_id.encode("ascii")

    reopened = RuntimeRepository(repo.root)
    inbox = RuntimeInbox(reopened)
    observed = inbox.query(request.session, request.operation_id)
    assert observed["status"] == ("submitted" if phase == "before-terminal" else "terminal")
    assert len(reopened.intents(request.session)) == (0 if phase == "before-terminal" else 1)
    calls = []

    def factory(bound, tools, *, cancelled):
        calls.append(bound.operation_id)
        assert bound.operation_id != request.operation_id, "replayed an observed effect"

        class Backend:
            def invoke(self, bound):
                return AgentTurnObservation(
                    bound, "completed", "local-fixture", 1, 1, "thread2", "turn2", "next"
                )

        return Backend()

    runtime = ForegroundRuntime(
        reopened,
        request.session,
        LocalRuntimeIngress(
            inbox,
            artifact_digest="a" * 64,
            configuration_digest=lambda _: "b" * 64,
            workspace_scope="workspace",
        ),
        factory,
    )
    runtime.start()
    try:
        assert runtime.step() is None and calls == []
        recovered = reopened.reconcile(request.session)
        if phase == "before-terminal":
            assert len(recovered) == 1
            assert recovered[0].result["disposition"] == "unknown"
            assert recovered[0].result["accepted"] is False
            assert recovered[0].result["unresolved_effects"] is True
            assert reopened.intents(request.session)[0]["kind"] == "parked"
        else:
            assert recovered == ()
            assert observed["turn"]["result"]["output_text"] == "answer"
            assert reopened.intents(request.session)[0]["kind"] == "mailbox"
        assert reopened.reconcile(request.session) == ()
        assert len(reopened.intents(request.session)) == 1
        # A caller retry gets the original receipt and cannot enqueue another execution.
        assert (
            inbox.submit(
                request.session, request.operation_id, request.goal_digest, request.input_text
            )
            == submission
        )
        assert runtime.step() is None and calls == []
        assert reopened.read_current("session").active_turn_id is None
        inbox.submit(request.session, "next", request.goal_digest, "next input")
        assert runtime.step().result["accepted"] is True
        assert calls == ["next"] and len(reopened.intents(request.session)) == 2
        assert marker.read_bytes() == request.operation_id.encode("ascii")
    finally:
        runtime.close()
