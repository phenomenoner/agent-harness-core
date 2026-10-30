"""Opt-in reflection retains legacy collection and exposes model policy."""
import pytest
from ahc.cli.foreground_learning import parse_learning
from ahc.domain.sessions import ContractError

def config():
    return {"schema_version": "ahc.foreground.config.v3", "skill_root": "/skills",
            "backend": {"kind": "codex-linux-trusted"},
            "learning": {"mode": "proposal-only", "protected_skills": [],
                         "reflection": {"enabled": True}}}

def test_explicit_reflection_defaults_and_custom_route():
    raw = config()
    result = parse_learning(raw)["learning"]["reflection"]
    assert result["model"] == "gpt-6-luna" and result["reasoning_effort"] == "max"
    assert result["job_timeout_seconds"] == 180 and result["create_proposals"]
    raw["learning"]["reflection"].update(model="gpt-6-sol", reasoning_effort="high")
    result = parse_learning(raw)["learning"]["reflection"]
    assert (result["model"], result["reasoning_effort"]) == ("gpt-6-sol", "high")

def test_absent_or_disabled_keeps_collection():
    raw = config();raw["learning"].pop("reflection")
    assert "reflection" not in parse_learning(raw)["learning"]
    raw["learning"]["reflection"] = {"enabled": False}
    assert not parse_learning(raw)["learning"]["reflection"]["enabled"]

@pytest.mark.parametrize("key,value", [("enabled", 1), ("job_timeout_seconds", 0),
    ("job_timeout_seconds", 601), ("tokens_per_job", 40000), ("model", ""),
    ("model", "bad model"), ("reasoning_effort", "guess"), ("daily_call_limit", True),
    ("poll_interval", 5), ("create_proposals", "yes")])
def test_invalid_policy_rejected(key, value):
    raw = config();raw["learning"]["reflection"][key] = value
    with pytest.raises(ContractError):parse_learning(raw)

def test_enabled_reflection_requires_native_route():
    raw = config();raw["backend"]["kind"] = "codex-wsl-trusted"
    with pytest.raises(ContractError):parse_learning(raw)


@pytest.mark.parametrize('change', ['proposal-only', 'no-reflection', 'no-cases', 'no-proposals'])
def test_closed_loop_requires_owner_suites_reflection_and_trial_grant(change, monkeypatch):
    from ahc.cli import foreground_learning as module
    monkeypatch.setattr(module, '_require_protection', lambda document: True)
    raw = config()
    raw['learning']['mode'] = 'reversible-trials'
    raw['learning']['closed_loop'] = {'enabled': True, 'suites': {'sample': {'cases': [
        {'case_id':'one', 'prompt':'Return task output as JSON', 'expected':42}]}}}
    if change == 'proposal-only': raw['learning']['mode'] = 'proposal-only'
    elif change == 'no-reflection': raw['learning']['reflection']['enabled'] = False
    elif change == 'no-cases': raw['learning']['closed_loop']['suites'] = {}
    else: raw['learning']['reflection']['create_proposals'] = False
    with pytest.raises(ContractError): parse_learning(raw)


def test_closed_loop_custom_route_is_independent_and_absent_defaults_are_inert(monkeypatch):
    from ahc.cli import foreground_learning as module
    monkeypatch.setattr(module, '_require_protection', lambda document: True)
    raw = config()
    assert 'closed_loop' not in parse_learning(raw)['learning']
    raw['learning'].update(mode='reversible-trials', closed_loop={'enabled': True, 'model':'custom-route',
        'reasoning_effort':'xhigh', 'suites': {'sample': {'cases': [
        {'case_id':'one', 'prompt':'Return task output as JSON', 'expected':42}]}}})
    value = parse_learning(raw)['learning']
    assert value['reflection']['model'] == 'gpt-6-luna'
    assert value['closed_loop']['model'] == 'custom-route'
    assert value['closed_loop']['reasoning_effort'] == 'xhigh'
    raw['learning']['closed_loop']['suites']['sample']['cases'][0]['expected'] = 99
    assert value['closed_loop']['suites']['sample']['cases'][0]['expected'] == 42


@pytest.mark.parametrize('change', ['bad-model','bad-effort','bad-assets','same-retirement','protected-retirement','duplicate-case'])
def test_closed_loop_invalid_owner_policy_has_no_activation(change, monkeypatch):
    from ahc.cli import foreground_learning as module
    monkeypatch.setattr(module, '_require_protection', lambda document: True)
    raw = config(); raw['learning']['mode'] = 'reversible-trials'
    case = {'case_id':'one', 'prompt':'Return task output as JSON', 'expected':42}
    loop = {'enabled':True,'suites':{'sample':{'cases':[case]}}}
    raw['learning']['closed_loop'] = loop
    if change == 'bad-model': loop['model'] = 'bad route'
    elif change == 'bad-effort': loop['reasoning_effort'] = 'guess'
    elif change == 'bad-assets': loop['allow_assets'] = 1
    elif change == 'same-retirement': loop['suites']['sample']['retire'] = {'sample':'a'*64}
    elif change == 'protected-retirement':
        raw['learning']['protected_skills'] = ['governance']
        loop['suites']['sample']['retire'] = {'governance':'a'*64}
    else: loop['suites']['sample']['cases'].append(dict(case))
    with pytest.raises(ContractError): parse_learning(raw)
