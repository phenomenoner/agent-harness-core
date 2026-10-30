"""User-selected daily access is independent of isolated evaluation admission."""
from copy import deepcopy
from types import SimpleNamespace

import pytest

from ahc.cli.foreground_config_native import parse_foreground_config_native
from ahc.cli.foreground_config_v2 import parse_foreground_config_v2
from ahc.cli.foreground_learning import compose_learning
from ahc.cli.foreground_workspace_protection import native_workspace_protection
from ahc.domain.sessions import ContractError
from test_reversible_learning_config import protected_config


def full_access_config():
    value = protected_config()
    value["execution_profile"] = "user-full-access"
    del value["workspace"]["protection"]
    value["policy"]["sandbox_mode"] = "danger-full-access"
    value["policy"]["workspace_granted"] = True
    return value


@pytest.mark.parametrize("engine", ["linux-python", "linux-ipython"])
def test_daily_full_access_keeps_backend_home_and_learning_controls(engine):
    value = full_access_config()
    value["workspace"]["kind"] = engine
    config = parse_foreground_config_native(value)
    assert config.to_dict() == value
    assert config.preview()["execution_profile"] == "user-full-access"
    assert native_workspace_protection(config.workspace) == {}
    assert config.backend["protection"] == value["backend"]["protection"]
    assert config.to_dict()["learning"] == value["learning"]


@pytest.mark.parametrize("gap", ["opt-in", "read-only", "workspace-write", "grant", "mixed", "unknown"])
def test_daily_access_never_appears_from_a_missing_or_ambiguous_grant(gap):
    value = full_access_config()
    if gap == "opt-in":
        del value["execution_profile"]
    elif gap in ("read-only", "workspace-write"):
        value["policy"]["sandbox_mode"] = gap
    elif gap == "grant":
        value["policy"]["workspace_granted"] = False
    elif gap == "mixed":
        value["workspace"]["protection"] = protected_config()["workspace"]["protection"]
    else:
        value["execution_profile"] = "root"
    with pytest.raises(ContractError):
        parse_foreground_config_native(value)


def test_full_access_does_not_reinterpret_windows_configuration():
    value = full_access_config()
    with pytest.raises(ContractError):
        parse_foreground_config_v2(value)


def test_instruction_trials_keep_operator_stop_and_protected_names(repository):
    from skill_test_support import session

    value = full_access_config()
    value["session"] = session(1).to_dict()
    parsed = parse_foreground_config_native(value)
    config = SimpleNamespace(session=session(1), to_dict=parsed.to_dict)
    selector = SimpleNamespace(repository=repository)
    first = compose_learning(config, selector)
    assert first.experiments.protection_check() is True
    assert first.experiments.protected_names == frozenset(value["learning"]["protected_skills"])
    first.experiments.configure(enabled=False, daily_limit=2)
    compose_learning(config, selector)
    with repository._read() as db:
        assert db.execute("SELECT enabled FROM learning_control").fetchone()[0] == 0


def test_explicit_daily_access_does_not_disable_isolated_evaluation(monkeypatch):
    from ahc.cli import foreground_evaluation
    from ahc.domain.native_scratch_limits import NativeScratchLimits
    from ahc.infrastructure.process import linux_workspace_boundary
    import ahc.adapters.native_skill_evaluation as evaluation_adapter
    import ahc.application.skill_evaluation_runs as evaluation_runs
    from skill_test_support import session

    calls = {}
    value = full_access_config()
    value["session"] = session(1).to_dict()
    config = parse_foreground_config_native(value)
    profile = {"executable": "/opt/bwrap", "sha256": "a" * 64, "workspace": "/home/ahc/evaluation",
        "readonly_roots": ["/usr"], "protected_roots": [], "interpreter": "/usr/bin/python3",
        "resource_limits": {"memory_bytes": 256 * 1024**2, "pids": 16, "cpu_quota_us": 100000,
                            "cpu_period_us": 100000, "swap_bytes": 0},
        "scratch_limits": {"workspace_bytes": 4 * 1024**2, "temporary_bytes": 4 * 1024**2},
        "lifetime_seconds": 20, "max_runs_per_trial": 1}
    profile = foreground_evaluation.parse_evaluation(profile)

    class Boundary:
        def __init__(self, **options):
            calls["boundary"] = options

    monkeypatch.setattr(linux_workspace_boundary, "LinuxWorkspaceBoundary", Boundary)
    monkeypatch.setattr(evaluation_adapter, "NativeSkillEvaluationFactory", lambda *args: calls.update(factory=args))
    monkeypatch.setattr(evaluation_runs, "SkillEvaluationRuns", lambda *args, **kwargs: (args, kwargs))
    from pathlib import Path
    store = SimpleNamespace(root=Path("/home/ahc/runtime"),
        read_current=lambda _: SimpleNamespace(session=session(1), status="active"))
    experiments = SimpleNamespace(repository=SimpleNamespace(root=Path("/home/ahc/skills")))
    foreground_evaluation.compose_evaluation(config, store, experiments, profile)
    options = calls["boundary"]
    assert type(options["scratch_limits"]) is NativeScratchLimits
    assert options["readonly_roots"] == ["/usr"]
    assert set(options["protected_roots"]) == {str(store.root.resolve()), str(experiments.repository.root.resolve())}
