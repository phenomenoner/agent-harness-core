"""Protected instruction activation, durable stop and shared Python engines."""
from copy import deepcopy
from types import SimpleNamespace

import pytest
from skill_test_support import session
from test_workspace_learning import request, setup, invoke
from test_foreground_config_native import native_config

from ahc.cli.foreground_config_native import parse_foreground_config_native
from ahc.cli.foreground_learning import compose_learning
from ahc.domain.sessions import ContractError


def protected_config():
    value = native_config()
    value['skill_root'] = '/home/ahc/skills'
    profile = {'executable':'/opt/bwrap', 'sha256':'a'*64,
        'readonly_roots':['/usr'],
        'protected_roots':[value['runtime_root'],value['skill_root']],
        'resource_limits':{'memory_bytes':256*1024**2,'pids':16,
            'cpu_quota_us':100000,'cpu_period_us':100000,'swap_bytes':0}}
    value['workspace']['protection']=deepcopy(profile)
    value['backend']['protection']=profile | {'network_mode':'shared',
        'home_mode':'sealed-history','home_bytes':4*1024**2,'temporary_bytes':4*1024**2}
    value['learning']={'mode':'reversible-trials','protected_skills':['pmo-policy'],'daily_limit':2}
    return value


@pytest.mark.parametrize('engine',['linux-python','linux-ipython'])
def test_native_engines_admit_explicit_protected_instruction_trials(engine):
    value=protected_config()
    value['workspace']['kind']=engine
    config=parse_foreground_config_native(value)
    assert config.to_dict()==value
    assert config.preview()['learning']['automatic_configured'] is True
    assert config.preview()['learning']['automatic_enabled'] is False


@pytest.mark.parametrize('gap',['backend','workspace','runtime','skills','mcp'])
def test_activation_requires_all_enabled_routes_protected(gap):
    value=protected_config()
    if gap in ('backend','workspace'):
        del value[gap]['protection']
    elif gap in ('runtime','skills'):
        hidden=value['runtime_root' if gap=='runtime' else 'skill_root']
        value['backend']['protection']['protected_roots'].remove(hidden)
    else:
        value['mcp']={'profiles':[{'enabled':True,'schema_version':'unsupported'}]}
    with pytest.raises(ContractError):
        parse_foreground_config_native(value)


def test_operator_stop_survives_reopen_until_explicit_policy_change(repository):
    value=protected_config()
    config=SimpleNamespace(session=session(1),to_dict=lambda:value)
    selector=SimpleNamespace(repository=repository)
    first=compose_learning(config,selector)
    assert first.experiments.protection_check() is True
    first.experiments.configure(enabled=False,daily_limit=2)
    second=compose_learning(config,selector)
    with repository._read() as db:
        assert db.execute('SELECT enabled FROM learning_control').fetchone()[0]==0
    value['learning']['mode']='proposal-only'
    compose_learning(config,selector)
    value['learning']['mode']='reversible-trials'
    compose_learning(config,selector)
    with repository._read() as db:
        assert db.execute('SELECT enabled FROM learning_control').fetchone()[0]==1


def test_terminal_receipts_are_grounded_bounded_and_capture_failure_is_optional(repository,monkeypatch):
    tools=setup(repository)
    turn=SimpleNamespace(result={'accepted':False,'disposition':'unknown','unresolved_effects':True,
                                'output_text':'private answer'})
    ref=tools.learning.record_result(request(session(1)),turn)
    assert ref
    facts=invoke(tools,'status')['result']['experiences'][0]['facts']
    assert facts['unresolved_effects'] is True
    assert 'output_text' not in facts
    assert facts['delivery_or_material_use_verified'] is False
    monkeypatch.setattr(tools.learning.experiments,'record_experience',
        lambda **kw: (_ for _ in ()).throw(OSError('unavailable')))
    assert tools.learning.record_result(request(session(1),'later'),turn) is None
    assert tools.learning.capture_gap


def test_existing_instruction_revision_can_keep_tools_but_never_expand_them(repository):
    from dataclasses import replace
    from test_skill_experiments import candidate
    from test_skill_graph_convergence import _activate_with_report
    old=candidate()
    _activate_with_report(repository,old,'existing-tool-contract')
    value=protected_config()
    config=SimpleNamespace(session=session(1),to_dict=lambda:value)
    learning=compose_learning(config,SimpleNamespace(repository=repository))
    revised=replace(old,version='1.0.1',predecessor_digest=old.revision_digest,
                    body='# Procedure\nUse the requested terminology.',body_digest=None,revision_digest=None)
    assert learning.experiments._candidate(revised)==old
    expanded=replace(revised,required_tools=tuple(sorted((*old.required_tools,'ungranted.tool'))),
                     revision_digest=None)
    with pytest.raises(ContractError,match='expand'):
        learning.experiments._candidate(expanded)
