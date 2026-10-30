"""Prepare a new disabled foreground and local runtime; never start a provider."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from ahc.cli.bundled_backend_paths import bundled_backend_paths
from ahc.cli.foreground_config_v2 import parse_foreground_config_v2
from ahc.domain.judgment_goal import GoalContract
from ahc.domain.sessions import VirtualSessionIdentity
from ahc.domain.working_sets import WorkingSetSnapshot
from ahc.infrastructure.runtime_repository import RuntimeRepository


def prepare(destination, *, goal, agent_id, session_id, route_id, bundle=None, codex_home=None,
            model, cwd, interpreter="/usr/bin/python3", reasoning_effort=None):
    destination = Path(destination).absolute()
    goal = GoalContract.from_dict(goal)
    session = VirtualSessionIdentity.initial(session_id, agent_id, route_id)
    if bundle is None or codex_home is None:
        paths = bundled_backend_paths(cwd)
        bundle = paths["bundle"] if bundle is None else bundle
        codex_home = paths["codex_home"] if codex_home is None else codex_home
    value = {
        "schema_version": "ahc.foreground.config.v2",
        "runtime_root": str(destination / "runtime"),
        "session": session.to_dict(), "workspace_id": "work",
        "checkpoint_digest": None,
        "workspace": {
            "kind": "wsl-python", "wsl_executable": "C:/Windows/System32/wsl.exe",
            "distribution": "AHC-Codex-Ubuntu-24.04", "user": "ahc", "cwd": cwd,
            "interpreter": interpreter, "execution_timeout": 30, "lifetime_seconds": 300,
        },
        "backend": {
            "kind": "codex-wsl-trusted", "enabled": False, "bundle": bundle,
            "codex_home": codex_home, "model": model, "turn_timeout": 120,
            "lifetime_seconds": 300,
        },
        "policy": {
            "revision": 1, "approval_mode": "manual", "sandbox_mode": "workspace-write",
            "workspace_granted": False, "approval_wait_seconds": 30,
        },
        "memory": {"enabled": False},
    }
    if reasoning_effort is not None:
        value["backend"]["reasoning_effort"] = reasoning_effort
    config = parse_foreground_config_v2(value)
    snapshot = WorkingSetSnapshot.initial(session, goal)
    # Validate everything before claiming a new directory. Never reuse/overwrite
    # an existing destination, even if it looks empty or partly prepared.
    destination.mkdir(exist_ok=False)
    store = RuntimeRepository.initialize(destination / "runtime")
    store.create(snapshot)
    documents = {"session.json": session.to_dict(), "goal.json": goal.to_dict(),
                 "foreground.json": config.to_dict()}
    for name, document in documents.items():
        (destination / name).write_text(json.dumps(document, indent=2), encoding="utf-8")
    (destination / "app.toml").write_text(
        'config_version="ahc.standalone.config.v1"\nstate_dir="state"\nservices=[]\n',
        encoding="utf-8",
    )
    receipt = {"status": "prepared-disabled", "configuration_digest": config.configuration_digest,
               "goal_digest": goal.digest, "backend_enabled": False, "workspace_granted": False,
               "native_paths_verified": False, "authentication_verified": False}
    (destination / "prepared.json").write_text(json.dumps(receipt, indent=2), encoding="utf-8")
    return receipt


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("destination", type=Path)
    parser.add_argument("--goal-json", type=Path, required=True)
    for name in ("agent-id", "session-id", "route-id", "model", "cwd"):
        parser.add_argument("--" + name, required=True)
    parser.add_argument("--bundle")
    parser.add_argument("--codex-home")
    parser.add_argument("--interpreter", default="/usr/bin/python3")
    parser.add_argument("--reasoning-effort")
    args = vars(parser.parse_args())
    goal = json.loads(args.pop("goal_json").read_text(encoding="utf-8"))
    print(json.dumps(prepare(**args, goal=goal)))


if __name__ == "__main__":
    main()
