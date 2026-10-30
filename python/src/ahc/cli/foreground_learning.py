"""Bounded instruction learning under isolated or explicitly trusted daily routes."""

from ahc.application.skill_experiments import SkillExperiments
from ahc.application.workspace_learning import WorkspaceLearningTools
from ahc.domain.sessions import ContractError
from ahc.domain.skill_values import digest_json, skill_name


def parse_learning(raw):
    value = raw.get("learning")
    if value is None:
        return {}
    if (
        type(value) is not dict
        or not {"mode", "protected_skills"} <= set(value) <= {
            "mode", "protected_skills", "evaluation", "daily_limit", "reflection", "closed_loop"}
        or value["mode"] not in ("proposal-only", "reversible-trials")
        or raw.get("skill_root") is None
        or type(value["protected_skills"]) is not list
    ):
        raise ContractError("learning requires explicit instruction mode and a skill repository")
    for name in value["protected_skills"]:
        skill_name(name)
    result = {
        "learning": {
            "mode": value["mode"],
            "protected_skills": sorted(set(value["protected_skills"])),
        }
    }
    if "daily_limit" in value:
        if type(value["daily_limit"]) is not int or value["daily_limit"] < 1:
            raise ContractError("learning daily limit invalid")
        result["learning"]["daily_limit"] = value["daily_limit"]
    if value["mode"] == "reversible-trials":
        _require_protection(raw)
    if "evaluation" in value:
        from .foreground_evaluation import parse_evaluation

        if raw.get("schema_version") != "ahc.foreground.config.v3":
            raise ContractError("learning evaluation requires native foreground configuration")
        result["learning"]["evaluation"] = parse_evaluation(value["evaluation"])
    if "reflection" in value:
        result["learning"]["reflection"] = _parse_reflection(value["reflection"], raw)
    if "closed_loop" in value:
        result["learning"]["closed_loop"] = _parse_closed_loop(value["closed_loop"], raw, result["learning"])
    return result


def _parse_closed_loop(value, raw, learning):
    from ahc.domain.skill_comparison import BehaviorCase, suite_digest
    defaults = {"enabled": False, "model": "gpt-6-luna", "reasoning_effort": "max",
                "job_timeout_seconds": 180, "tokens_per_job": 4000,
                "daily_token_limit": 32000, "daily_call_limit": 16,
                "allow_assets": False, "suites": {}}
    if type(value) is not dict or set(value) - set(defaults):
        raise ContractError("closed-loop configuration fields invalid")
    result = {**defaults, **value}
    if any(type(result[key]) is not bool for key in ("enabled", "allow_assets")):
        raise ContractError("closed-loop switches invalid")
    from ahc.infrastructure.runtime_learning_queue import ReflectionPolicy
    ReflectionPolicy(result["job_timeout_seconds"], result["tokens_per_job"], result["daily_token_limit"],
                     result["daily_call_limit"], result["model"], result["reasoning_effort"])
    suites = result["suites"]
    if type(suites) is not dict or len(suites) > 16:
        raise ContractError("closed-loop suites invalid")
    normalized = {}
    for name, suite in suites.items():
        skill_name(name)
        if (type(suite) is not dict or not {"cases"} <= set(suite) <= {"cases", "retire"}
                or type(suite["cases"]) is not list):
            raise ContractError("owner-held-out suite invalid")
        cases = [BehaviorCase.from_dict(case) for case in suite["cases"]]
        suite_digest(cases)
        retire = suite.get("retire", {})
        if type(retire) is not dict or len(retire) > 8 or name in retire:
            raise ContractError("scoped retirement allowlist invalid")
        from ahc.domain.skill_values import digest
        for retired, revision in retire.items():
            skill_name(retired)
            digest(revision, "retired revision")
            if retired in learning["protected_skills"]:
                raise ContractError("cannot retire protected skill")
        normalized[name] = {"cases": [case.to_dict() for case in cases], "retire": dict(retire)}
    result["suites"] = normalized
    if result["enabled"]:
        if (learning["mode"] != "reversible-trials" or not learning.get("reflection", {}).get("enabled")
                or not learning["reflection"]["create_proposals"] or not suites):
            raise ContractError("closed-loop requires enabled reflection, trials and independent owner suites")
        _require_protection(raw)
    return result


def _parse_reflection(value, raw):
    from ahc.infrastructure.runtime_learning_queue import ReflectionPolicy
    defaults = {"enabled": False, "model": "gpt-6-luna", "reasoning_effort": "max",
                "job_timeout_seconds": 180, "tokens_per_job": 4000,
                "daily_token_limit": 32000, "daily_call_limit": 16,
                "create_proposals": True}
    if type(value) is not dict or set(value) - set(defaults):
        raise ContractError("learning reflection fields invalid")
    result = {**defaults, **value}
    if any(type(result[key]) is not bool for key in ("enabled", "create_proposals")):
        raise ContractError("learning reflection switches invalid")
    ReflectionPolicy(result["job_timeout_seconds"], result["tokens_per_job"],
                     result["daily_token_limit"], result["daily_call_limit"],
                     result["model"], result["reasoning_effort"])
    if result["enabled"] and (raw.get("schema_version") != "ahc.foreground.config.v3"
            or raw.get("backend", {}).get("kind") != "codex-linux-trusted"):
        raise ContractError("learning reflection requires native trusted backend")
    return result

def _require_protection(document):
    from .foreground_backend_protection import require_protected_routes
    from .foreground_user_access import USER_FULL_ACCESS, require_user_full_access

    if document.get("execution_profile") == USER_FULL_ACCESS:
        return require_user_full_access(document)

    if (document.get("schema_version") != "ahc.foreground.config.v3"
            or document.get("backend", {}).get("kind") != "codex-linux-trusted"
            or "protection" not in document.get("backend", {})):
        raise ContractError("instruction activation requires protected native execution routes")
    try:
        require_protected_routes(document)
    except (KeyError,TypeError) as exc:
        raise ContractError('instruction activation route configuration invalid') from exc
    # user-full-access is the owner's explicit trusted instruction-trial grant,
    # not an OS containment claim. Separately configured code evaluations still
    # use compose_evaluation's ephemeral, offline namespace and owned custody.
    return True


def compose_learning(config, selector, *, store=None):
    value = config.to_dict().get("learning")
    if value is None:
        return None
    if selector is None:
        raise ContractError("learning repository is not configured")
    experiments = SkillExperiments(
        selector.repository,
        owner=config.session.agent_id,
        granted_tools=(),
        protected_names=value["protected_skills"],
        protection_check=(lambda: _require_protection(config.to_dict()))
        if value["mode"] == "reversible-trials" else (lambda: False),
    )
    experiments.configure_policy(
        policy_digest=digest_json(value),
        enabled=value["mode"] == "reversible-trials",
        daily_limit=value.get("daily_limit", 3),
    )
    evaluations = None
    if "evaluation" in value:
        from .foreground_evaluation import compose_evaluation

        evaluations = compose_evaluation(config, store, experiments, value["evaluation"])
    from ahc.infrastructure.runtime_learning_queue import RuntimeLearningQueue

    learning = WorkspaceLearningTools(experiments, config.session, evaluations=evaluations,
                                     handoff=RuntimeLearningQueue(store) if store is not None else None)
    reflection = value.get("reflection", {})
    if reflection.get("enabled"):
        if store is None:
            raise ContractError("learning reflection requires a durable runtime store")
        from ahc.application.learning_reflection import LearningReflectionController
        from ahc.infrastructure.runtime_learning_queue import ReflectionPolicy
        from .host_learning_calls import HostLearningCalls
        policy = ReflectionPolicy(reflection["job_timeout_seconds"], reflection["tokens_per_job"],
                                  reflection["daily_token_limit"], reflection["daily_call_limit"],
                                  reflection["model"], reflection["reasoning_effort"])
        calls = HostLearningCalls(store, config)
        closed_loop = None
        loop = value.get("closed_loop", {})
        if loop.get("enabled"):
            from ahc.application.skill_closed_loop import LearningClosedLoop
            loop_policy = ReflectionPolicy(loop["job_timeout_seconds"], loop["tokens_per_job"],
                                           loop["daily_token_limit"], loop["daily_call_limit"],
                                           loop["model"], loop["reasoning_effort"])
            closed_loop = LearningClosedLoop(learning, loop, calls, loop_policy)
            selector.scoped_learning = True
            selector.scoped_route = learning.handoff.route_for_operation
        learning.controller = LearningReflectionController(
            learning, policy, calls, closed_loop=closed_loop,
            create_proposals=reflection["create_proposals"])
    return learning
