"""Opt-in real service/kernel lifecycle; controlled backend, no provider or Telegram IO.

Set AHC_RUN_SKILL_LIFECYCLE=1 and AHC_SKILL_CATALOG_FIXTURE to an offline
seven-core SkillRepository. Run using the isolated installed candidate's Python.
"""

import hashlib
import json
import os
import select
import shutil
import subprocess
import sys
from dataclasses import asdict, replace
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pytest

from ahc.adapters.native_process_runner import NativeProcessRunner
from ahc.adapters.native_python_workspace import NativePythonWorkspace
from ahc.adapters.native_retirement_observer import NativeRetirementObserver
from ahc.application.runtime_workspace import RuntimeWorkspace
from ahc.application.workspace_skills import WorkspaceSkillTools
from ahc.application.workspace_tools import WorkspaceTools
from ahc.cli.runtime_skill_discovery import skill_discovery_snapshot
from ahc.domain.channel_ingress import ChannelRoute
from ahc.domain.judgment_models import GOAL_SCHEMA, GoalContract
from ahc.domain.runtime_policy import EffectivePolicy
from ahc.domain.schedules import IntervalSchedule
from ahc.domain.sessions import ContractError, VirtualSessionIdentity
from ahc.domain.working_sets import WorkingSetSnapshot
from ahc.infrastructure.artifact_repository import ArtifactRepository
from ahc.infrastructure.backend_custody import BackendCustody, BackendCustodyUnavailable
from ahc.infrastructure.channel_continuation import (
    continuation_blockers,
    continue_discord_session,
    continue_telegram_session,
    read_skill_continuation_receipt,
)
from ahc.infrastructure.channel_schedule_routes import ChannelScheduleRoutes
from ahc.infrastructure.native_custody_retirement import retire_native_custody
from ahc.infrastructure.runtime_repository import RuntimeRepository
from ahc.infrastructure.schedule_repository import ScheduleRepository
from ahc.infrastructure.skill_repository import SkillRepository
from ahc.infrastructure.telegram_inbox import TelegramInbox
from ahc.infrastructure.working_repository import WorkingWriterBusy

FOUR = {"claim-verification", "root-cause-diagnosis", "humanizer-zh", "geo-content-engineering"}
EXTRA = {"pm-feature-spec", "reply-image-disambiguation", "engineering-delivery"}


def _native_readback(workspace, repository, expected):
    tools = WorkspaceTools(
        workspace,
        artifacts=ArtifactRepository(workspace.store),
        skills=WorkspaceSkillTools(repository),
    )
    revisions = repository.active_revisions()
    assert {tool for item in revisions for tool in item.required_tools}.issubset(
        tools.capabilities()
    )

    def invoke(name, **arguments):
        result = tools.invoke(
            name,
            {"expected": workspace.identity.to_dict(), **arguments},
        )
        assert result["disposition"] == "observed", result
        return result["result"]

    listing = invoke("workspace.skills_list", limit=32)
    actual = {item["skill_name"]: item["revision_digest"] for item in listing["skills"]}
    assert actual == expected and listing["next_offset"] is None
    discovery = skill_discovery_snapshot(
        tools, SimpleNamespace(capabilities=tools.capabilities(), session=workspace.identity.session)
    )
    assert discovery["status"] == "available" and discovery["next_offset"] is None
    assert {r["skill_name"]: r["revision_digest"] for r in discovery["skills"]} == expected
    assert all(set(row) == {"skill_name", "revision_digest", "description"}
               for row in discovery["skills"])
    reads = {}
    for name, digest in sorted(expected.items()):
        offset, chunks = 0, []
        while True:
            item = invoke(
                "workspace.skills_read", revision_digest=digest, offset=offset, limit=4096
            )
            assert item["revision_digest"] == digest and item["offset"] == offset
            chunks.append(item["text"])
            if item["next_offset"] is None:
                break
            assert item["next_offset"] > offset
            offset = item["next_offset"]
        body = "".join(chunks)
        assert body == repository.get_revision(digest).body
        reads[name] = {
            "revision": digest,
            "chars": len(body),
            "chunks": len(chunks),
            "body_sha256": hashlib.sha256(body.encode()).hexdigest(),
            "eof": True,
        }
    return reads, discovery


def _worker(config_path):
    config = json.loads(Path(config_path).read_text())
    root = Path(config["root"])
    store = RuntimeRepository(root / "runtime")
    current = store.read_current(config["session_id"])
    cwd = root / "workspace"
    cwd.mkdir(exist_ok=True)
    runner = NativeProcessRunner(cwd=str(cwd), environment={}, persistent=True, owner_lifetime=True)
    policy = EffectivePolicy(1, "manual", "workspace-write", True, "a" * 64, 30)

    def create(identity, checkpoint):
        return NativePythonWorkspace(
            store,
            runner,
            identity,
            effective_policy=policy,
            environment_digest="a" * 64,
            cwd=str(cwd),
            interpreter=sys.executable,
            user=None,
            timeout=15,
            lifetime_seconds=120,
            checkpoint=checkpoint,
        )

    workspace = RuntimeWorkspace.open(store, current, "work", create, config["checkpoint"])
    code = "answer=40" if config["checkpoint"] is None else "answer+=1"
    assert workspace.execute(workspace.identity, code)["ok"]
    result = workspace.execute(workspace.identity, "print(answer)")
    assert result["ok"] and str(config["answer"]) in result["output"]
    checkpoint = workspace.checkpoint(workspace.identity, ("answer",))
    reads, discovery = _native_readback(
        workspace, SkillRepository(config["catalog"]), config["expected"]
    )
    custody = BackendCustody(store)
    lease = custody.acquire(current.session, "work")
    backend = NativeProcessRunner(cwd=str(cwd), environment={})
    unit = custody.create(
        lease,
        workspace.identity,
        backend,
        (sys.executable, "-I", "-u", "-c", "import time;time.sleep(110)"),
        lifetime_seconds=120,
    )
    assert backend.observe(unit).state == "running"
    with store._read() as connection:
        records = [
            json.loads(row[0])
            for row in connection.execute(
                "SELECT record_json FROM backend_custody WHERE state='held'"
            )
        ]
    print(
        json.dumps(
            {
                "generation": current.session.ref.generation,
                "checkpoint": checkpoint.digest,
                "answer": config["answer"],
                "reads": reads,
                "discovery": discovery,
                "resources": records,
                "owner_pid": os.getpid(),
                "invocation_id": os.environ["INVOCATION_ID"],
                "owner_cgroup": Path("/proc/self/cgroup").read_text().strip(),
                "installed_module": __import__("ahc").__file__,
            }
        ),
        flush=True,
    )
    command = sys.stdin.readline().strip()
    if command == "crash":
        os._exit(0)  # systemd terminates descendants; persistent custody remains held.
    assert command == "drain"
    try:
        backend.close()
        custody.recover(lease, backend)
    finally:
        lease.close()
        workspace.close()


def _start_owner(root, config, index):
    config_path = root / f"owner-{index}.json"
    config_path.write_text(json.dumps(config))
    name = "ahc-skill-lifecycle-" + uuid4().hex
    stderr = (root / f"owner-{index}.stderr").open("w")
    process = subprocess.Popen(
        (
            "/usr/bin/systemd-run",
            "--user",
            "--wait",
            "--pipe",
            "--collect",
            "--unit=" + name,
            "-p",
            "Delegate=yes",
            "-p",
            "KillMode=control-group",
            "-p",
            "SendSIGKILL=yes",
            "-p",
            "RuntimeMaxSec=120",
            "-p",
            "MemoryMax=536870912",
            sys.executable,
            "-I",
            str(Path(__file__).absolute()),
            "worker",
            str(config_path),
        ),
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=stderr,
        text=True,
        env={**os.environ, "XDG_RUNTIME_DIR": f"/run/user/{os.geteuid()}"},
    )
    try:
        assert select.select([process.stdout], [], [], 60)[0], "native owner startup timed out"
        line = process.stdout.readline()
        assert line, (root / f"owner-{index}.stderr").read_text()
        return process, stderr, name + ".service", json.loads(line)
    except BaseException:
        if process.poll() is None:
            process.communicate("drain\n", timeout=130)
        stderr.close()
        raise


def _finish_owner(process, stderr, command):
    output, _ = process.communicate(command + "\n", timeout=35)
    stderr.close()
    assert process.returncode == 0, output


@pytest.mark.skipif(
    sys.platform != "linux" or os.environ.get("AHC_RUN_SKILL_LIFECYCLE") != "1",
    reason="explicit isolated native lifecycle opt-in required",
)
@pytest.mark.parametrize("platform", ["telegram", "discord"])
def test_real_owner_four_seven_four(tmp_path, platform):
    fixture = Path(os.environ["AHC_SKILL_CATALOG_FIXTURE"])
    assert os.geteuid() != 0 and fixture.is_dir()
    root = tmp_path
    for name in ("four", "seven"):
        shutil.copytree(fixture, root / name)
    four, seven = SkillRepository(root / "four"), SkillRepository(root / "seven")
    assert {item.skill_name for item in seven.active_revisions()} == FOUR | EXTRA
    for name in ("engineering-delivery", "reply-image-disambiguation", "pm-feature-spec"):
        value = four.get_active(name)
        four.deactivate(
            skill_name=name,
            expected_active_digest=value.revision_digest,
            operation_key="isolated-remove-" + name,
        )
    old_four = {item.skill_name: item.revision_digest for item in four.active_revisions()}
    store = RuntimeRepository.initialize(root / "runtime")
    identity = VirtualSessionIdentity.initial("isolated-" + uuid4().hex, "main", "isolated-route")
    task_goal = GoalContract(
        GOAL_SCHEMA,
        "isolated-adoption",
        1,
        None,
        "Verify native skill adoption",
        "Preserve the isolated working context",
        ("Four to seven to four exact revision pickup",),
        ("isolated runtime only",),
        ("no external effects",),
        ("local authority only",),
        (),
        "acknowledged",
    )
    original = WorkingSetSnapshot.initial(identity, task_goal)
    store.create(original)
    route = ChannelRoute(platform, "123", "456", "789", identity)
    from ahc.domain.skill_values import canonical_json
    from ahc.infrastructure.telegram_hold import hold_updates

    if platform == "telegram":
        inbox = TelegramInbox(store, [route])
        inbox.admit([{"update_id": 1, "message": {"message_id": 1, "chat": {"id": 456},
                     "from": {"id": 999, "is_bot": False}, "text": "unrouted fixture"}}],
                     expected_offset=None)
        hold_updates(inbox, [{"update_id": 2, "message": {"unsupported": True}}], 2)
        with store._read() as connection:
            original_holds = [
                tuple(row) for row in connection.execute("SELECT * FROM runtime_telegram_holds")
            ]
        continuation, position = continue_telegram_session, {"expected_offset": 3}
    else:
        from ahc.infrastructure.discord_inbox import DiscordInbox
        from ahc.infrastructure.discord_resume import DiscordResume

        resume = DiscordResume(DiscordInbox(store, [route]))
        resume.save_ready("isolated-gateway", "wss://gateway.discord.gg", 3, expected=None)
        continuation = continue_discord_session
        position = {"expected_resume": resume.read()}
        original_holds = None
    preference = canonical_json(
        {"model": "fixture-choice", "effort": "max", "revision": 7}
    ).decode()

    def seed_preferences(connection):
        connection.execute(
            "CREATE TABLE channel_preferences (route_json TEXT PRIMARY KEY,"
            "payload_json TEXT NOT NULL)"
        )
        connection.execute(
            "INSERT INTO channel_preferences VALUES(?,?)",
            (canonical_json(route.to_dict()).decode(), preference),
        )

    store._mutate(seed_preferences)
    schedule = IntervalSchedule(
        "retained", identity, original.goal.digest, 1000, 100, "retained input"
    )
    ScheduleRepository(store).create(schedule)
    store._mutate(lambda connection: ChannelScheduleRoutes.bind(connection, schedule, route))
    records, checkpoint, current = [], None, original
    for index, repository in enumerate((four, seven, four)):
        expected = {item.skill_name: item.revision_digest for item in repository.active_revisions()}
        config = {
            "root": str(root),
            "session_id": identity.ref.virtual_session_id,
            "catalog": str(repository.root),
            "expected": expected,
            "checkpoint": checkpoint,
            "answer": 40 + index,
        }
        process, stderr, service, observed = _start_owner(root, config, index)
        try:
            assert observed["generation"] == index + 1
            assert observed["owner_pid"] != os.getpid()
            assert observed["installed_module"] == __import__("ahc").__file__
            parent_cgroup = Path("/proc/self/cgroup").read_text().strip()
            assert observed["owner_cgroup"] != parent_cgroup
            with store._read() as connection:
                assert "backend_custody" in continuation_blockers(connection, current)
            with pytest.raises((ContractError, WorkingWriterBusy)):
                continuation(
                    store, root / "supervisor.lock", current, [route], **position
                )
            with pytest.raises(WorkingWriterBusy):
                retire_native_custody(store, current, "work")
            for resource in observed["resources"]:
                native = __import__(
                    "ahc.domain.native_process_identity", fromlist=["identity_from_dict"]
                )
                bound = native.identity_from_dict(resource["unit"])
                assert NativeRetirementObserver().observe(bound).state == "running"
                with pytest.raises(BackendCustodyUnavailable, match="no stop authority"):
                    NativeRetirementObserver().stop(bound)
        finally:
            _finish_owner(process, stderr, "crash" if index == 0 else "drain")
        with store._read() as connection:
            held_before = connection.execute(
                "SELECT count(*) FROM backend_custody WHERE state='held'"
            ).fetchone()[0]
        if index == 0:
            assert held_before == 2  # Real killed resources, not hand-authored idle rows.
        recovered = retire_native_custody(store, current, "work")
        assert len(recovered) == (2 if index == 0 else 0)
        assert retire_native_custody(store, current, "work") == ()
        checkpoint = observed["checkpoint"]
        records.append(
            {
                "owner": observed,
                "service": service,
                "held_before": held_before,
                "recovered": [asdict(value) for value in recovered],
            }
        )
        if index < 2:
            target = seven if index == 0 else four
            kwargs = {
                **position,
                "skill_repository": target,
                "expected_skill_revisions": tuple(
                    item.revision_digest for item in target.active_revisions()
                ),
            }
            receipt = continuation(
                store, root / "supervisor.lock", current, [route], **kwargs
            )
            assert (
                continuation(
                    store, root / "supervisor.lock", current, [route], **kwargs
                )["state"]
                == "already-applied"
            )
            records[-1]["continuation"] = receipt
            current = store.read_current(identity.ref.virtual_session_id)
            route = replace(route, session=current.session)
            retained = ScheduleRepository(store).read(current.session, "retained")
            assert retained == replace(schedule, session=current.session, revision=index + 2)
        if platform == "telegram":
            assert TelegramInbox(store, [route]).offset() == 3
        else:
            assert DiscordResume(DiscordInbox(store, [route])).read() == position["expected_resume"]
    with store._read() as connection:
        if original_holds is not None:
            assert [
                tuple(row) for row in connection.execute("SELECT * FROM runtime_telegram_holds")
            ] == original_holds
        exists = connection.execute(
            "SELECT name FROM sqlite_master WHERE name='runtime_submissions'"
        ).fetchone()
        assert (
            not exists
            or connection.execute("SELECT count(*) FROM runtime_submissions").fetchone()[0] == 0
        )
        preferences = [
            row[0] for row in connection.execute("SELECT payload_json FROM channel_preferences")
        ]
        assert preferences == [preference] * 3
    assert current.session.ref.generation == 3
    historical = read_skill_continuation_receipt(store, original)
    assert historical["working_set"]["session"]["ref"]["generation"] == 2
    assert {
        name: value["revision"] for name, value in records[-1]["owner"]["reads"].items()
    } == old_four
    final_services = []
    for record in records:
        for resource in record["owner"]["resources"]:
            assert not Path("/sys/fs/cgroup" + resource["unit"]["cgroup"]).exists()
        result = subprocess.run(
            (
                "/usr/bin/systemctl",
                "--user",
                "show",
                record["service"],
                "--property=LoadState,ActiveState,MainPID,ControlGroup",
            ),
            capture_output=True,
            text=True,
            env={**os.environ, "XDG_RUNTIME_DIR": f"/run/user/{os.geteuid()}"},
            timeout=5,
        )
        fields = dict(line.split("=", 1) for line in result.stdout.splitlines() if "=" in line)
        assert fields.get("MainPID", "0") == "0"
        assert fields.get("ActiveState") in ("inactive", "failed", None)
        final_services.append(fields)
    with store._read() as connection:
        assert continuation_blockers(connection, current) == ()
    (root / "ACCEPTANCE.json").write_text(
        json.dumps(
            {
                "verdict": "PASS",
                "sequence": [4, 7, 4],
                "generations": [1, 2, 3],
                "checkpoint_answers": [40, 41, 42],
                "platform": platform,
                "cursor": position.get("expected_offset"),
                "resume": position.get("expected_resume"),
                "records": records,
                "final_services": final_services,
                "zero_residual_cgroups": True,
                "evidence_origin": (
                    "real native service/kernel/backend processes; controlled non-model backend"
                ),
                "unverified": ["natural model selection", "Telegram IO", "live deployment"],
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    assert sys.argv[1] == "worker"
    _worker(sys.argv[2])
