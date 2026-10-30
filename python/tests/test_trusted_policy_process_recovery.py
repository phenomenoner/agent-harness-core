"""Apply the real abrupt-exit/reopen scenario to policy-bearing admissions."""

import sqlite3

import pytest
import test_foreground_process_recovery as scenario

from ahc.infrastructure.runtime_repository import RuntimeRepository


@pytest.mark.parametrize("phase", ["before-terminal", "after-terminal"])
def test_policy_bearing_turn_survives_lost_response_without_replay(tmp_path, monkeypatch, phase):
    script = scenario.CHILD.replace(
        "from ahc.application.foreground_runtime import ForegroundRuntime",
        "from ahc.application.foreground_runtime import ForegroundRuntime\n"
        "from ahc.domain.runtime_policy import EffectivePolicy",
    ).replace(
        "workspace_scope='workspace')",
        "workspace_scope='workspace', effective_policy="
        "EffectivePolicy(1,'manual','workspace-write',True,'c'*64,30))",
    )
    assert script != scenario.CHILD and "effective_policy=" in script
    monkeypatch.setattr(scenario, "CHILD", script)
    scenario.test_foreground_effect_survives_process_exit_without_replay(tmp_path, phase)
    store = RuntimeRepository(tmp_path / "runtime")
    with sqlite3.connect(store.db_path) as connection:
        assert (
            connection.execute("SELECT count(*) FROM runtime_effective_policies").fetchone()[0] == 1
        )
