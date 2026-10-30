"""Real SQLite closed-loop custody with controlled model outputs, no provider effects."""

import json
from dataclasses import replace
from types import SimpleNamespace

import pytest
from test_runtime_learning_queue import ready, delivered

from ahc.application.learning_reflection import LearningReflectionController
from ahc.application.skill_closed_loop import LearningClosedLoop
from ahc.application.skill_experiments import SkillExperiments
from ahc.application.workspace_learning import WorkspaceLearningTools
from ahc.domain.sessions import ContractError
from ahc.domain.skill_values import digest_json
from ahc.infrastructure.runtime_learning_queue import ReflectionPolicy
from ahc.infrastructure.runtime_inbox import RuntimeInbox
from ahc.infrastructure.scoped_skill_trials import pin_operation_catalog, read_operation_catalog

POLICY = ReflectionPolicy(180, 100, 2000, 20, 'fixture-route', 'high')
CASE = {'case_id': 'double', 'prompt': 'Return twice the integer 21 as JSON.', 'expected': 42}
PROPOSAL = {'action': 'propose', 'reason': 'The original instruction omitted the arithmetic rule.',
            'proposal': {'skill_name': 'arithmetic', 'body': 'Return twice the supplied integer as JSON.',
                         'description': 'Apply the arithmetic rule', 'cause': 'missing rule',
                         'hypothesis': 'The explicit rule corrects the held-out task.'}}


class Calls:
    def __init__(self, outputs):
        self.outputs, self.seen, self.effect = list(outputs), [], lambda *a: None
    def run(self, session, operation, binding, prompt, *, cancelled):
        self.seen.append((binding, prompt))
        assert not cancelled()
        self.effect(session, operation, binding, prompt)
        output = self.outputs.pop(0)
        if isinstance(output, Exception):
            raise output
        return output


def setup_loop(tmp_path, repository, *, outputs=None, assets=False, proposal=None):
    repo, request, queue, clock = ready(tmp_path)
    experiments = SkillExperiments(repository, owner=request.session.agent_id, granted_tools=(),
                                   protected_names=('governance',), protection_check=lambda: True)
    experiments.configure(enabled=True)
    learning = WorkspaceLearningTools(experiments, request.session, handoff=queue)
    config = {'enabled': True, 'model': POLICY.model, 'reasoning_effort': POLICY.effort,
              'job_timeout_seconds': 180, 'tokens_per_job': 100, 'daily_token_limit': 2000,
              'daily_call_limit': 20, 'allow_assets': assets,
              'suites': {'arithmetic': {'cases': [dict(CASE)], 'retire': {}}}}
    calls = Calls(outputs if outputs is not None else [json.dumps(proposal or PROPOSAL), '21', '42'])
    loop = LearningClosedLoop(learning, config, calls, POLICY)
    control = LearningReflectionController(learning, POLICY, calls, closed_loop=loop)
    return repo, request, queue, clock, learning, loop, control, calls


def adopt(parts):
    repo, request, queue, clock, learning, loop, control, calls = parts
    first = control.step()
    assert first['state'] == 'complete' and first['closed_loop']['state'] == 'pending'
    trial = first['proposal']['trial_id']
    result = control.step()
    assert result['state'] == 'active'
    return trial


def test_configured_comparison_scoped_adoption_and_old_catalog_stability(tmp_path, repository):
    parts = setup_loop(tmp_path, repository)
    repo, request, queue, clock, learning, loop, control, calls = parts
    before = repo.read_turn(request.session, request.operation_id).result
    trial = adopt(parts)
    candidate = repository.get_revision(loop.trials.get(trial)['plan']['candidate'])
    assert len(calls.seen) == 3
    assert {(b['model'], b['effort']) for b, _ in calls.seen} == {(POLICY.model, POLICY.effort)}
    assert all('"expected"' not in prompt for _, prompt in calls.seen)
    assert repository.get_active('arithmetic') is None
    assert repository.pin_revision_availability(request.session)[0] == ()
    assert read_operation_catalog(repository, request.session, request.operation_id) == ()
    assert pin_operation_catalog(repository, request.session, 'next', route=queue.route_for_operation(request.session, request.operation_id)) == (candidate,)
    foreign = replace(request.session, route_id='foreign')
    with pytest.raises(ContractError, match='identity'):
        pin_operation_catalog(repository, foreign, 'foreign-operation')
    from ahc.domain.sessions import VirtualSessionIdentity
    other = VirtualSessionIdentity.initial('foreign-session', request.session.agent_id, 'foreign')
    assert pin_operation_catalog(repository, other, 'foreign-operation') == ()
    assert repo.read_turn(request.session, request.operation_id).result == before
    assert control.step() is None and len(calls.seen) == 3
    # Reopening reads active evidence without running a model again.
    LearningClosedLoop(learning, loop.config, calls, POLICY)
    assert len(calls.seen) == 3


@pytest.mark.parametrize('outputs,expected', [(['42', '42'], 'rejected'), (['21', '0'], 'rejected'),
    (['21', OSError('lost output')], 'unknown'), (['21', 'not JSON'], 'rejected')])
def test_no_gain_failure_or_uncertainty_never_adopts_or_replays(tmp_path, repository, outputs, expected):
    parts = setup_loop(tmp_path, repository, outputs=[json.dumps(PROPOSAL), *outputs])
    repo, request, queue, clock, learning, loop, control, calls = parts
    first = control.step()
    trial = first['proposal']['trial_id']
    assert control.step()['state'] == expected
    assert loop.trials.get(trial)['state'] == expected
    assert loop.trials.current(request.session)[1] == ()
    count = len(calls.seen)
    assert control.step() is None and len(calls.seen) == count
    LearningClosedLoop(learning, loop.config, calls, POLICY).step(cancelled=lambda: False)
    assert len(calls.seen) == count


@pytest.mark.parametrize('change', ['input', 'stop', 'deadline', 'disabled'])
def test_result_before_adoption_is_fenced_by_new_input_stop_deadline_and_operator_switch(tmp_path, repository, change, monkeypatch):
    parts = setup_loop(tmp_path, repository)
    repo, request, queue, clock, learning, loop, control, calls = parts
    first = control.step()
    original = loop.trials.finish
    def finish(*args):
        result = original(*args)
        if change == 'input':
            current = repo.read_current(request.session.ref.virtual_session_id)
            RuntimeInbox(repo).submit(request.session, 'priority-input', current.goal.digest, 'new user work')
        elif change == 'stop':
            queue.stop(request.session, request.operation_id)
        elif change == 'deadline':
            clock[0] += POLICY.job_timeout_seconds * 1000
        else:
            learning.experiments.configure(enabled=False)
        return result
    monkeypatch.setattr(loop.trials, 'finish', finish)
    assert control.step()['state'] == 'unknown'
    assert loop.trials.current(request.session)[1] == ()
    assert repository.get_active('arithmetic') is None


def test_stale_epoch_rollback_cannot_undo_aba_replacement(tmp_path, repository):
    parts = setup_loop(tmp_path, repository)
    repo, request, queue, clock, learning, loop, control, calls = parts
    trial = adopt(parts)
    repository._mutate(lambda db: db.execute('UPDATE learning_scoped_heads SET generation=generation+2'))
    with pytest.raises(ContractError, match='epoch'):
        loop.trials.rollback(request.session, trial, lambda action: queue.guard_effect(request.session, request.operation_id, 0, action))
    assert loop.trials.get(trial)['state'] == 'active'


def test_resources_and_scripts_are_versioned_inert_and_not_executed(tmp_path, repository):
    marker = tmp_path / 'must-not-exist'
    proposal = json.loads(json.dumps(PROPOSAL))
    code = 'from pathlib import Path; Path(' + repr(str(marker)) + ').write_text("executed")'
    proposal['proposal']['assets'] = [{'path': 'scripts/example.py', 'text': code}]
    parts = setup_loop(tmp_path, repository, assets=True, proposal=proposal)
    trial = adopt(parts)
    loop = parts[5]
    candidate = repository.get_revision(loop.trials.get(trial)['plan']['candidate'])
    assert repository.read_asset(candidate.revision_digest, 'scripts/example.py').decode() == code
    assert not marker.exists()
    assert code in json.loads(parts[-1].seen[-1][1].split('\n', 1)[1])['resources']['scripts/example.py']


def test_asset_proposal_without_explicit_owner_switch_remains_inert(tmp_path, repository):
    proposal = json.loads(json.dumps(PROPOSAL))
    proposal['proposal']['assets'] = [{'path': 'script.py', 'text': 'raise RuntimeError()'}]
    parts = setup_loop(tmp_path, repository, proposal=proposal)
    assert parts[6].step()['state'] == 'complete'  # reflection was durably recorded before proposal rejection
    assert parts[5].trials.pending(parts[1].session) == ()
    assert repository.get_active('arithmetic') is None


def test_comparison_reservation_is_once_only_and_shares_daily_usage(tmp_path, repository):
    parts = setup_loop(tmp_path, repository)
    repo, request, queue, clock, learning, loop, control, calls = parts
    control.step()
    binding = queue.reserve_followup(request.session, request.operation_id, 0, POLICY, 'same-job', deadline_ms=clock[0]+1000)
    assert binding['model'] == POLICY.model
    assert queue.reserve_followup(request.session, request.operation_id, 0, POLICY, 'same-job', deadline_ms=clock[0]+1000) is None
    queue.reconcile_followups(request.session)
    assert queue.reserve_followup(request.session, request.operation_id, 0, POLICY, 'same-job', deadline_ms=clock[0]+1000) is None
    with repo._read() as db:
        assert db.execute('SELECT calls FROM runtime_learning_usage').fetchone()[0] == 2


def next_turn(parts, text, *, read=True):
    from ahc.application.channel_ingress import ChannelIngress
    from ahc.application.runtime_service import RuntimeService
    from ahc.application.skill_service import SkillTurnService
    from ahc.application.workspace_skills import WorkspaceSkillTools
    from ahc.domain.agent_backend import AgentTurnObservation
    from ahc.domain.channel_ingress import ChannelInboundEvent, ChannelRoute
    from ahc.domain.workspace import WorkspaceIdentity
    repo, request, queue, clock, learning, loop, control, calls = parts
    current = repo.read_current(request.session.ref.virtual_session_id)
    route = ChannelRoute('telegram', 'account', 'channel', 'user', request.session)
    event = ChannelInboundEvent(route, 'later-'+digest_json([text, read]), CASE['prompt'])
    ChannelIngress(RuntimeInbox(repo), route).submit(event, current.goal.digest)
    bound = replace(request, operation_id=event.operation_id, input_text=event.input_text,
                    goal_digest=current.goal.digest, capabilities=('workspace.learning','workspace.skills_list','workspace.skills_read'))
    candidate = loop.catalog()[0]
    skills = WorkspaceSkillTools(learning.experiments.repository)
    workspace = SimpleNamespace(identity=WorkspaceIdentity(request.session, 'work'), closed=False)
    class Backend:
        def invoke(self, bound):
            skills.bind_request(bound, workspace)
            learning.record_input(bound)
            if read:
                arguments = {'expected': workspace.identity.to_dict(), 'revision_digest': candidate.revision_digest}
                result = skills.invoke(workspace, 'workspace.skills_read', arguments, available_tools=bound.capabilities)
                assert result['disposition'] == 'observed'
                learning.record_tool(bound, 'read-skill', 'workspace.skills_read', result)
            return AgentTurnObservation(bound, 'completed', 'controlled', 1, 1, output_text=text)
    selector = SkillTurnService(learning.experiments.repository)
    selector.scoped_learning = True
    selector.scoped_route = queue.route_for_operation
    result = RuntimeService(repo, Backend(), selector=selector).execute_with_skills(current, bound, tools=bound.capabilities, lazy_disclosure=True)
    assert result.result['accepted']
    delivered(repo, bound, route)
    return bound


def test_next_turn_real_read_performance_and_regression_rollback_preserve_old_operation(tmp_path, repository):
    parts = setup_loop(tmp_path, repository)
    trial = adopt(parts)
    loop = parts[5]
    good = next_turn(parts, '42')
    result = loop.step(cancelled=lambda: False)
    assert result['observation']['state'] == 'passed'
    bad = next_turn(parts, '0')
    result = loop.step(cancelled=lambda: False)
    assert result['observation']['state'] == 'regressed'
    assert result['observation']['rollback']['state'] == 'rolled-back'
    assert loop.catalog() == ()
    assert len(read_operation_catalog(repository, parts[1].session, good.operation_id)) == 1
    assert len(read_operation_catalog(repository, parts[1].session, bad.operation_id)) == 1
    assert pin_operation_catalog(repository, parts[1].session, 'after-rollback') == ()
    assert len(parts[-1].seen) == 3


def test_output_without_a_complete_skill_read_is_not_claimed_as_a_regression(tmp_path, repository):
    parts = setup_loop(tmp_path, repository)
    trial = adopt(parts)
    next_turn(parts, '0', read=False)
    assert parts[5].step(cancelled=lambda: False) is None
    assert parts[5].trials.get(trial)['state'] == 'active'


def test_overlarge_resource_comparison_stops_before_any_model_call(tmp_path, repository):
    proposal = json.loads(json.dumps(PROPOSAL))
    proposal['proposal']['assets'] = [{'path': 'resource.txt', 'text': 'x'*5000}]
    parts = setup_loop(tmp_path, repository, assets=True, proposal=proposal)
    assert parts[6].step()['closed_loop']['state'] == 'pending'
    assert parts[6].step()['state'] == 'unknown'
    assert len(parts[-1].seen) == 1


def test_catalog_tamper_is_rejected_before_any_new_disclosure(tmp_path, repository):
    parts = setup_loop(tmp_path, repository)
    trial = adopt(parts)
    session = parts[1].session
    pin_operation_catalog(repository, session, 'pinned', route=parts[2].route_for_operation(session, parts[1].operation_id))
    repository._mutate(lambda db: db.execute("UPDATE learning_operation_catalogs SET revisions_json='[]' WHERE operation_id='pinned'"))
    with pytest.raises(ContractError, match='digest'):
        read_operation_catalog(repository, session, 'pinned')
    with pytest.raises(ContractError, match='digest'):
        pin_operation_catalog(repository, session, 'pinned', route=parts[2].route_for_operation(session, parts[1].operation_id))


def test_verified_comparison_reopens_only_adoption_without_model_replay(tmp_path, repository):
    parts = setup_loop(tmp_path, repository)
    repo, request, queue, clock, learning, loop, control, calls = parts
    first = control.step()
    trial = first['proposal']['trial_id']
    loop.trials.start(request.session, trial, loop.policy_digest, deadline_ms=clock[0]+1000)
    loop.trials.finish(trial, ['21'], ['42'])
    reopened = LearningClosedLoop(learning, loop.config, calls, POLICY)
    assert reopened.step(cancelled=lambda: False)['state'] == 'active'
    assert len(calls.seen) == 1


def test_expired_verified_comparison_cannot_renew_its_deadline(tmp_path, repository):
    parts = setup_loop(tmp_path, repository)
    repo, request, queue, clock, learning, loop, control, calls = parts
    first = control.step(); trial = first['proposal']['trial_id']
    loop.trials.start(request.session, trial, loop.policy_digest, deadline_ms=clock[0]+1)
    loop.trials.finish(trial, ['21'], ['42'])
    clock[0] += 2
    reopened = LearningClosedLoop(learning, loop.config, calls, POLICY)
    assert reopened.step(cancelled=lambda: False)['state'] == 'unknown'
    assert loop.catalog() == () and len(calls.seen) == 1


def test_model_tool_cannot_bypass_scoped_comparison_through_global_promote(tmp_path, repository):
    from ahc.domain.workspace import WorkspaceIdentity
    parts = setup_loop(tmp_path, repository)
    repo, request, queue, clock, learning, loop, control, calls = parts
    learning.controller = control
    first = control.step(); trial = first['proposal']['trial_id']
    workspace = SimpleNamespace(identity=WorkspaceIdentity(request.session, 'work'), closed=False)
    result = learning.invoke(workspace, {'expected': workspace.identity.to_dict(), 'action':'promote', 'payload':{'trial_id':trial}})
    assert result['disposition'] == 'rejected'
    assert repository.get_active('arithmetic') is None
    assert loop.trials.get(trial)['state'] == 'pending'


@pytest.mark.parametrize('dependent', [False, True])
def test_dependency_aware_scoped_merge_retirement_and_reversible_retention(tmp_path, repository, dependent):
    from test_skill_graph_convergence import _activate_with_report
    from test_skill_markdown_resources import modern
    from ahc.domain.skill_models import SkillRelationship
    support = modern(skill_name='old-helper', agent_allowlist=())
    _activate_with_report(repository, support, 'helper')
    if dependent:
        child = modern(skill_name='dependent-helper', agent_allowlist=(),
                       relationships=(SkillRelationship('depends_on', support.revision_digest),))
        _activate_with_report(repository, child, 'dependent')
    parts = setup_loop(tmp_path, repository)
    repo, request, queue, clock, learning, loop, control, calls = parts
    loop.config['suites']['arithmetic']['retire'] = {'old-helper':support.revision_digest}
    loop.policy_digest = digest_json(loop.config)
    first = control.step(); trial = first['proposal']['trial_id']
    result = control.step()
    if dependent:
        assert result['state'] == 'unknown'
        assert any(r.skill_name == 'old-helper' for r in loop.catalog())
    else:
        assert result['state'] == 'active'
        assert all(r.skill_name != 'old-helper' for r in loop.catalog())
        assert repository.get_active('old-helper') == support
        loop.trials.rollback(request.session, trial, lambda action: queue.guard_effect(request.session, request.operation_id, 0, action))
        assert support in loop.catalog()
    assert repository.get_revision(support.revision_digest) == support


def test_cleanup_owner_after_comparison_failure_remains_reachable_without_redispatch(tmp_path, repository):
    parts = setup_loop(tmp_path, repository, outputs=[json.dumps(PROPOSAL), OSError('lost model return')])
    repo, request, queue, clock, learning, loop, control, calls = parts
    cleaned = []
    calls.recover = lambda session: cleaned.append(session)
    control.step()
    assert control.step()['state'] == 'unknown'
    count = len(calls.seen)
    assert control.step()['state'] == 'cleanup-only'
    assert len(calls.seen) == count and cleaned


def test_disabling_scoped_learning_changes_only_new_operation_views(tmp_path, repository):
    from ahc.application.skill_service import SkillTurnService
    from ahc.application.workspace_skills import WorkspaceSkillTools
    from ahc.domain.skill_models import SelectionRequest
    from ahc.domain.workspace import WorkspaceIdentity
    parts=setup_loop(tmp_path,repository)
    trial=adopt(parts)
    request=parts[1]
    service=SkillTurnService(repository)
    service.scoped_learning=True
    service.scoped_route=lambda session,operation:parts[2].route_for_operation(session,request.operation_id)
    selection=SelectionRequest(request.session,'enabled-turn',('procedure',),request.session.agent_id,(),
                               'when useful',request.goal_digest)
    enabled=service.select_and_pin(selection,episode_id='enabled-turn')
    assert enabled['manifest'].entries
    service.scoped_learning=False
    disabled=service.select_and_pin(replace(selection,turn_id='disabled-turn'),episode_id='disabled-turn')
    assert disabled['manifest'].entries==()
    assert read_operation_catalog(repository,request.session,'enabled-turn')
    workspace=SimpleNamespace(identity=WorkspaceIdentity(request.session,'work'),closed=False)
    tools=WorkspaceSkillTools(repository)
    tools.bind_request(SimpleNamespace(session=request.session,operation_id='disabled-turn'),workspace)
    result=tools.invoke(workspace,'workspace.skills_list',{'expected':workspace.identity.to_dict()},available_tools=())
    assert result['result']['skills']==[]


@pytest.mark.parametrize('field', ['platform', 'account_id', 'channel_id', 'user_id'])
def test_same_virtual_session_cannot_share_an_overlay_with_another_physical_chat(tmp_path, repository, field):
    from ahc.application.channel_ingress import ChannelIngress
    from ahc.application.skill_service import SkillTurnService
    from ahc.application.workspace_skills import WorkspaceSkillTools
    from ahc.domain.channel_ingress import ChannelInboundEvent, ChannelRoute
    from ahc.domain.skill_models import SelectionRequest
    from ahc.domain.workspace import WorkspaceIdentity
    parts=setup_loop(tmp_path,repository)
    adopt(parts)
    repo,request,queue,clock,learning,loop,control,calls=parts
    original=ChannelRoute('telegram','account','channel','user',request.session)
    foreign=replace(original, **{field: 'discord' if field == 'platform' else 'another-value'})
    current=repo.read_current(request.session.ref.virtual_session_id)
    event=ChannelInboundEvent(foreign,'different-physical-chat','when useful')
    ChannelIngress(RuntimeInbox(repo),foreign).submit(event,current.goal.digest)
    selector=SkillTurnService(repository);selector.scoped_learning=True
    selector.scoped_route=queue.route_for_operation
    selected=selector.select_and_pin(SelectionRequest(request.session,event.operation_id,('procedure',),
        request.session.agent_id,(),'when useful',request.goal_digest),episode_id=event.operation_id)
    assert selected['manifest'].entries==()
    workspace=SimpleNamespace(identity=WorkspaceIdentity(request.session,'work'),closed=False)
    tools=WorkspaceSkillTools(repository)
    tools.bind_request(SimpleNamespace(session=request.session,operation_id=event.operation_id),workspace)
    listing=tools.invoke(workspace,'workspace.skills_list',{'expected':workspace.identity.to_dict()},available_tools=())
    assert listing['result']['skills']==[]


def test_physical_chats_sharing_a_virtual_session_keep_independent_adoption_and_rollback(tmp_path, repository):
    from ahc.application.channel_ingress import ChannelIngress
    from ahc.domain.channel_ingress import ChannelInboundEvent, ChannelRoute
    from test_channel_outbox import accepted
    parts=setup_loop(tmp_path,repository)
    first_trial=adopt(parts)
    repo,request,queue,clock,learning,loop,control,calls=parts
    original=queue.route_for_operation(request.session,request.operation_id)
    foreign=ChannelRoute('telegram','account','another-chat','user',request.session)
    current=repo.read_current(request.session.ref.virtual_session_id)
    event=ChannelInboundEvent(foreign,'second-physical-source','when useful')
    ChannelIngress(RuntimeInbox(repo),foreign).submit(event,current.goal.digest)
    source=replace(request,operation_id=event.operation_id,input_text=event.input_text,goal_digest=current.goal.digest)
    assert accepted(repo,current,source).result['accepted']
    delivered(repo,source,foreign)
    proposal=json.loads(json.dumps(PROPOSAL))
    proposal['proposal']['body']='Multiply the supplied integer by two and return JSON.'
    calls.outputs.extend([json.dumps(proposal),'21','42'])
    second_trial=adopt(parts)
    first=repository.get_revision(loop.trials.get(first_trial)['plan']['candidate'])
    second=repository.get_revision(loop.trials.get(second_trial)['plan']['candidate'])
    assert first.revision_digest != second.revision_digest
    assert loop.trials.current(request.session,route=original)[1] == (first,)
    assert loop.trials.current(request.session,route=foreign.to_dict())[1] == (second,)
    loop.trials.rollback(request.session,second_trial,lambda action:queue.guard_effect(request.session,source.operation_id,0,action))
    assert loop.trials.current(request.session,route=foreign.to_dict())[1] == ()
    assert loop.trials.current(request.session,route=original)[1] == (first,)
    assert loop.trials.get(first_trial)['state'] == 'active'
    assert len(calls.seen) == 6
