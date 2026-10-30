"""Current-head monitoring and stale-source retirement regressions; controlled models only."""
import json
from dataclasses import replace
from test_skill_closed_loop import setup_loop, adopt, PROPOSAL
from test_runtime_learning_queue import delivered
from test_channel_outbox import accepted
from ahc.application.channel_ingress import ChannelIngress
from ahc.domain.channel_ingress import ChannelInboundEvent, ChannelRoute
from ahc.infrastructure.runtime_inbox import RuntimeInbox
from ahc.infrastructure.scoped_skill_trials import pin_operation_catalog
from ahc.domain.skill_values import digest_json

def add_source(parts, name, route):
    repo,request,queue,clock,learning,loop,control,calls=parts
    current=repo.read_current(request.session.ref.virtual_session_id)
    event=ChannelInboundEvent(route,name,'when useful')
    ChannelIngress(RuntimeInbox(repo),route).submit(event,current.goal.digest)
    source=replace(request,operation_id=event.operation_id,input_text=event.input_text,goal_digest=current.goal.digest)
    assert accepted(repo,current,source).result['accepted']
    delivered(repo,source,route)
    return source

def test_seventeenth_current_physical_chat_is_in_monitor_set(tmp_path, repository):
    parts=setup_loop(tmp_path,repository)
    parts[4].experiments.configure(enabled=True,daily_limit=64)
    first=adopt(parts)
    repo,request,queue,clock,learning,loop,control,calls=parts
    ids=[first]
    for i in range(1,17):
        route=ChannelRoute('telegram','account','chat-'+str(i),'user',request.session)
        add_source(parts,'source-'+str(i),route)
        proposal=json.loads(json.dumps(PROPOSAL))
        proposal['proposal']['body']='Return twice the integer as JSON. Variant '+str(i)
        calls.outputs.extend([json.dumps(proposal),'21','42'])
        ids.append(adopt(parts))
    active=[row['trial_id'] for row in loop.trials.active(request.session)]
    print('ACTIVE_MONITOR',json.dumps({'adopted':len(ids),'monitored':len(active),'latest_monitored':ids[-1] in active,'latest_state':loop.trials.get(ids[-1])['state']}))
    assert ids[-1] in active

def test_retirement_requires_current_effective_digest(tmp_path, repository):
    from test_skill_graph_convergence import _activate_with_report
    from test_skill_markdown_resources import modern
    support=modern(skill_name='old-helper',agent_allowlist=())
    _activate_with_report(repository,support,'helper')
    parts=setup_loop(tmp_path,repository)
    repo,request,queue,clock,learning,loop,control,calls=parts
    route=queue.route_for_operation(request.session,request.operation_id)
    pin_operation_catalog(repository,request.session,request.operation_id,route=route)
    # A newer effective override exists before this old-source proposal is enqueued.
    newer=replace(support,body=support.body+'\nNew scoped behavior.',body_digest=None,revision_digest=None)
    repository.register_revision(newer)
    from ahc.infrastructure.scoped_skill_trials import context_id,context_value,encoded
    repository._mutate(lambda db:db.execute('INSERT INTO learning_scoped_heads VALUES(?,?,?,?)',
        (context_id(request.session,route),encoded(context_value(request.session,route)),1,encoded({'old-helper':newer.revision_digest}))))
    loop.config['suites']['arithmetic']['retire']={'old-helper':support.revision_digest}
    loop.policy_digest=digest_json(loop.config)
    first=control.step()
    assert first['closed_loop']['state']=='pending'
    result=control.step()
    names=[(r.skill_name,r.revision_digest) for r in loop.trials.current(request.session,route=route)[1]]
    print('STALE_RETIRE',json.dumps({'state':result['state'],'owner_allowed':support.revision_digest,'current_before':newer.revision_digest,'current_after':names}))
    assert any(name=='old-helper' and digest==newer.revision_digest for name,digest in names)


def test_superseded_trial_is_history_and_does_not_occupy_the_monitor_set(tmp_path, repository):
    parts=setup_loop(tmp_path,repository)
    first=adopt(parts)
    repo,request,queue,clock,learning,loop,control,calls=parts
    route=ChannelRoute('telegram','account','channel','user',request.session)
    add_source(parts,'replacement-source',route)
    proposal=json.loads(json.dumps(PROPOSAL))
    proposal['proposal']['body']='Multiply the input integer by two. Return JSON.'
    calls.outputs.extend([json.dumps(proposal),'21','42'])
    second=adopt(parts)
    assert [row['trial_id'] for row in loop.trials.active(request.session)] == [second]
    assert loop.trials.get(first)['state'] == 'active'  # immutable historical custody retained
    assert loop.trials.get(second)['state'] == 'active'
