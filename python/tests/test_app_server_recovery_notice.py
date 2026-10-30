"""Exact-route failure presentation and subsequent context keep durable uncertainty."""

import json
from dataclasses import replace

import pytest
from test_channel_outbox import state
from test_runtime_conversation_context import _submit_current

from ahc.application.channel_ingress import ChannelIngress
from ahc.application.runtime_service import RuntimeService
from ahc.domain.agent_backend import AgentTurnObservation
from ahc.domain.artifacts import ArtifactManifest
from ahc.domain.channel_ingress import ChannelInboundEvent
from ahc.domain.skill_values import canonical_json
from ahc.domain.workspace import WorkspaceIdentity
from ahc.infrastructure.artifact_repository import ArtifactRepository
from ahc.infrastructure.channel_outbox import ChannelOutbox
from ahc.infrastructure.runtime_channel_store import _previous_failure_output
from ahc.infrastructure.runtime_conversation_context import conversation_context
from ahc.infrastructure.runtime_inbox import RuntimeInbox


def interrupted(tmp_path, *, artifact_only=False, text="continue the work"):
    repo, snapshot, template, route = state(tmp_path)
    event = ChannelInboundEvent(route, "interrupted", text,
                                context={"message_id": "100"})
    ChannelIngress(RuntimeInbox(repo), route).submit(event, snapshot.goal.digest)
    request = replace(template, operation_id=event.operation_id, input_text=event.input_text)
    diagnostic = {
        "category": "backend-failed", "phase": "decode", "last_method": "error",
        "code": "backend-nonretryable-error", "remaining_ms": 3500000, "frames": 24,
        "cleanup": "not-observed", "backend_failure": {
            "source": "notification", "kind": "routing-unauthorized", "http_status": 401,
        },
    }
    if artifact_only:
        producer = repo.reserve_workspace_generation(snapshot, template.workspace_scope)
        manifest = ArtifactRepository(repo).publish(producer, b"confirmed saved work", "text/plain")
        work = {"workspace": None, "checkpoint": None, "artifacts": [manifest.to_dict()]}
    else:
        work = {"workspace": WorkspaceIdentity(
            request.session, request.workspace_scope, 1, 47).to_dict(),
            "checkpoint": None, "artifacts": []}

    class Backend:
        def invoke(self, value):
            return AgentTurnObservation(
                value, "unknown", "fixture", 1, 1, "thread", "turn", unresolved_effects=True,
                backend_diagnostic=diagnostic, confirmed_work=work,
            )

    result = RuntimeService(repo, Backend()).execute(snapshot, request)
    return repo, request, route, result


def test_failure_notice_links_current_input_and_describes_version(tmp_path):
    repo, request, route, result = interrupted(tmp_path)
    notice = ChannelOutbox(repo).read(route)[0]["payload"]
    assert notice["reply_to_message_id"] == "100"
    assert "授權／工作區路由" in notice["output_text"]
    assert "工作區版本：47" in notice["output_text"]
    assert "第 47 步" not in notice["output_text"]
    assert result.result["disposition"] == "unknown" and not result.result["accepted"]


def test_next_turn_receives_cause_and_recovery_facts_not_a_successful_answer(tmp_path):
    repo, _, route, _ = interrupted(tmp_path)
    current = _submit_current(repo, route)
    history = conversation_context(repo, route.session, current, include_incomplete=True)
    entry = next(x for x in history if x["user"] == "continue the work")
    assert "assistant" not in entry and entry["status"] == "unknown"
    assert entry["backend_diagnostic"]["backend_failure"]["kind"] == "routing-unauthorized"
    assert entry["recovery"]["workspace"]["revision"] == 47
    assert entry["unresolved_effects"]


def test_artifact_only_confirmed_work_survives_next_turn_without_inventing_checkpoint(tmp_path):
    repo, request, route, result = interrupted(tmp_path, artifact_only=True)
    artifact = result.result["confirmed_work"]["artifacts"][0]
    assert ArtifactRepository(repo).read(
        request.session, ArtifactManifest.from_dict(artifact).artifact_id) is not None
    current = _submit_current(repo, route)
    entry = conversation_context(repo, route.session, current, include_incomplete=True)[0]
    assert entry["recovery"] == {"workspace": None, "checkpoint": None, "artifact_count": 1}
    assert entry["status"] == "unknown" and "assistant" not in entry


@pytest.mark.parametrize("budget", [512, 800, 1200])
@pytest.mark.parametrize("text", ["continue the work", "continue the work " * 200],
                         ids=["short", "long"])
@pytest.mark.parametrize("artifact_only", [False, True], ids=["workspace", "artifact"])
def test_recovery_metadata_respects_history_budget_without_losing_uncertainty(
    tmp_path, budget, text, artifact_only,
):
    repo, _, route, _ = interrupted(tmp_path, artifact_only=artifact_only, text=text)
    current = _submit_current(repo, route)
    history = conversation_context(repo, route.session, current,
                                   include_incomplete=True, max_chars=budget)
    assert sum(len(json.dumps(entry, ensure_ascii=False)) for entry in history) <= budget
    assert history and history[0]["user"] and text.startswith(history[0]["user"])
    assert history[0]["status"] == "unknown" and history[0]["unresolved_effects"]
    assert history[0]["recovery"]["artifact_count"] == int(artifact_only)


@pytest.mark.parametrize("legacy", [False, True])
def test_predecessor_notice_bytes_remain_readable_without_rewriting(tmp_path, legacy):
    repo, request, route, result = interrupted(tmp_path)
    previous = _previous_failure_output(result.result, legacy=legacy)
    encoded = canonical_json(previous).decode()
    repo._mutate(lambda c: c.execute(
        "UPDATE runtime_channel_outbox SET payload_json=? WHERE operation_id=?",
        (encoded, request.operation_id)))
    records = ChannelOutbox(repo).read(route)
    assert records[0]["payload"] == previous and records[0]["state"] == "pending"
    with repo._read() as c:
        assert c.execute("SELECT payload_json FROM runtime_channel_outbox WHERE operation_id=?",
                         (request.operation_id,)).fetchone()[0] == encoded


def test_predecessor_contract_invalid_notice_remains_sent_and_readable(tmp_path):
    from ahc.application.channel_delivery import ChannelDelivery
    repo, snapshot, template, route = state(tmp_path)
    event = ChannelInboundEvent(route, 'protocol-failure', 'continue preserved work', context={'message_id':'100'})
    ChannelIngress(RuntimeInbox(repo), route).submit(event, snapshot.goal.digest)
    request = replace(template, operation_id=event.operation_id, input_text=event.input_text)
    diagnostic = {'category':'protocol','phase':'decode','last_method':'item/commandExecution/outputDelta',
                  'code':'contract-invalid','remaining_ms':2880278,'frames':4182,'cleanup':'not-observed'}
    work = {'workspace':WorkspaceIdentity(request.session,request.workspace_scope,1,645).to_dict(),
            'checkpoint':None,'artifacts':[]}
    class Backend:
        def invoke(self, bound):
            return AgentTurnObservation(bound,'unknown','fixture',1,1,'thread','turn',
                unresolved_effects=True,backend_diagnostic=diagnostic,confirmed_work=work)
    turn = RuntimeService(repo,Backend()).execute(snapshot,request)
    class Transport:
        calls=0
        def send(self,*args):
            self.calls+=1
            return 'predecessor-notice-receipt'
    transport=Transport()
    ChannelDelivery(ChannelOutbox(repo),route,transport).deliver(request.operation_id,'delivery-owner')
    row=ChannelOutbox(repo).read(route)[0]
    old={**row['payload'],'output_text':row['payload']['output_text'].replace(
        '\n中斷原因：Codex 回報的通訊契約核對失敗；操作結果仍待確認。','')}
    encoded=canonical_json(old).decode()
    repo._mutate(lambda db:db.execute('UPDATE runtime_channel_outbox SET payload_json=? WHERE operation_id=?',
                                     (encoded,request.operation_id)))
    read=ChannelOutbox(repo).read(route)[0]
    assert read['state']=='sent' and read['payload']==old and transport.calls==1
    assert not turn.result['accepted'] and turn.result['unresolved_effects']
    with repo._read() as db:
        assert db.execute('SELECT payload_json FROM runtime_channel_outbox WHERE operation_id=?',(request.operation_id,)).fetchone()[0]==encoded
