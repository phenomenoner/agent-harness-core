"""Explicit offline evaluator configuration; parsing never opens an environment."""

from dataclasses import asdict, replace

from ahc.domain.native_resource_limits import NativeResourceLimits
from ahc.domain.native_scratch_limits import NativeScratchLimits
from ahc.domain.sessions import ContractError
from ahc.domain.skill_values import digest, digest_json


def parse_evaluation(value):
    from .foreground_config_v2 import _posix_path

    fields = {"executable", "sha256", "workspace", "readonly_roots", "protected_roots",
              "interpreter", "resource_limits", "scratch_limits", "lifetime_seconds",
              "max_runs_per_trial"}
    if type(value) is not dict or set(value) != fields:
        raise ContractError("learning evaluation profile fields invalid")
    result = {key: _posix_path(value[key], "evaluation." + key, executable=True)
              for key in ("executable", "workspace", "interpreter")}
    digest(value["sha256"], "evaluation isolation digest")
    result["sha256"] = value["sha256"]
    for key in ("readonly_roots", "protected_roots"):
        paths = value[key]
        minimum = 1 if key == "readonly_roots" else 0
        if type(paths) is not list or not minimum <= len(paths) <= 16:
            raise ContractError("evaluation mount list invalid")
        result[key] = sorted({_posix_path(path, "evaluation." + key) for path in paths})
    for key, contract in (("resource_limits", NativeResourceLimits),
                          ("scratch_limits", NativeScratchLimits)):
        limits = value[key]
        if type(limits) is not dict or set(limits) != set(contract.__dataclass_fields__):
            raise ContractError("evaluation limits must be explicit")
        result[key] = asdict(contract(**limits))
    if result["resource_limits"]["swap_bytes"] != 0:
        raise ContractError("evaluation cannot enable swap")
    for key, maximum in (("lifetime_seconds", 3600), ("max_runs_per_trial", 32)):
        if type(value[key]) is not int or not 1 <= value[key] <= maximum:
            raise ContractError("evaluation lifetime or run budget invalid")
        result[key] = value[key]
    return result


def compose_evaluation(config, store, experiments, profile):
    from ahc.adapters.native_skill_evaluation import NativeSkillEvaluationFactory
    from ahc.application.skill_evaluation_runs import SkillEvaluationRuns
    from ahc.infrastructure.process.linux_workspace_boundary import LinuxWorkspaceBoundary

    if store is None or config.backend["kind"] != "codex-linux-trusted":
        raise ContractError("evaluation requires an explicit native runtime store")
    config.effective_policy.require_workspace_grant(digest_json(config.workspace))
    current = store.read_current(config.session.ref.virtual_session_id)
    if current.session != config.session or current.status != "active":
        raise ContractError("evaluation session is no longer current")
    protected = sorted(set(profile["protected_roots"]) | {
        str(store.root.resolve()), str(experiments.repository.root.resolve())})
    boundary = LinuxWorkspaceBoundary(
        executable=profile["executable"], sha256=profile["sha256"],
        workspace=profile["workspace"], readonly_roots=profile["readonly_roots"],
        protected_roots=protected, scratch_limits=NativeScratchLimits(**profile["scratch_limits"]),
    )
    environment = digest_json(profile)
    # The operator explicitly selected this separate, ephemeral environment.
    # It inherits the granted parent's approval policy, never new model authority.
    policy = replace(config.effective_policy, environment_digest=environment)
    factory = NativeSkillEvaluationFactory(
        store, current, experiments.repository.root, boundary,
        NativeResourceLimits(**profile["resource_limits"]), policy, environment,
        profile["interpreter"], profile["lifetime_seconds"],
    )
    return SkillEvaluationRuns(experiments, factory,
                               max_runs_per_trial=profile["max_runs_per_trial"])
