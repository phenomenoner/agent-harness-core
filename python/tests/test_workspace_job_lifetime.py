"""Lifetime regressions proposed by Luna/max and integrated against actual API."""
import time
from types import SimpleNamespace

import pytest
from test_workspace_jobs import owned, call


def budget(tools):
    return tools.invoke('workspace.budget', {'expected':tools.workspace.identity.to_dict()})['result']


def test_owner_lifetime_ignores_bounded_metadata(owned):
    _,workspace,tools=owned
    port=workspace.port
    port._runner=SimpleNamespace(owner_lifetime=True)
    port._opened_at=time.monotonic()-58; port._lifetime=60
    result=call(tools,'start',code='count-once',expected_seconds=10,wait_seconds=0)
    try:
        assert result['disposition']=='observed'
        assert port.entered.wait(1)
        assert result['result']['budget']['hard_remaining_ms']>35000
        assert budget(tools)['lifetime']['remaining_ms'] is None
    finally:
        port.release.set()


@pytest.mark.parametrize('limiter',['turn','lifetime'])
def test_bounded_lifetime_caps_admission_and_budget(owned,limiter):
    _,workspace,tools=owned; port=workspace.port; now=time.monotonic()
    port._runner=SimpleNamespace(owner_lifetime=False)
    port._opened_at=now-(30 if limiter=='lifetime' else 10); port._lifetime=60
    port._turn_deadline=now+(60 if limiter=='lifetime' else 30)
    result=call(tools,'start',code='count-once',expected_seconds=5,wait_seconds=0)
    try:
        assert result['disposition']=='observed'
        assert port.entered.wait(1)
        report=budget(tools)
        effective=report['deadline']['effective_remaining_ms']
        hard=result['result']['budget']['hard_remaining_ms']
        assert 20000<effective<=30000
        assert abs(hard-effective)<3000
        assert effective<=min(report['deadline']['remaining_ms'],report['lifetime']['remaining_ms'])+100
    finally:
        port.release.set()


def test_oversized_estimate_rejects_before_reservation(owned):
    _,workspace,tools=owned; port=workspace.port
    port._runner=SimpleNamespace(owner_lifetime=False)
    port._opened_at=time.monotonic()-58; port._lifetime=60
    identity=workspace.identity
    result=call(tools,'start',code='count-once',expected_seconds=10,wait_seconds=0)
    assert result['disposition']=='rejected' and not result['unresolved_effects']
    assert workspace.identity==identity and port.calls==0
    assert 'work1' not in workspace.jobs._workers


@pytest.mark.parametrize('field,value',[('_opened_at',None),('_opened_at',float('nan')),
    ('_opened_at',float('inf')),('_lifetime',None),('_lifetime',float('inf')),
    ('_lifetime',-1)])
def test_unverified_bounded_lifetime_rejects_without_effect(owned,field,value):
    _,workspace,tools=owned; port=workspace.port
    port._runner=SimpleNamespace(owner_lifetime=False)
    port._opened_at=time.monotonic(); port._lifetime=60
    if value is None: delattr(port,field)
    else: setattr(port,field,value)
    identity=workspace.identity
    result=call(tools,'start',code='count-once',expected_seconds=1,wait_seconds=0)
    assert result['disposition']=='rejected' and not result['unresolved_effects']
    assert workspace.identity==identity and port.calls==0
