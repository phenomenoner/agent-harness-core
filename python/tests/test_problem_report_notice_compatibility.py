"""A new fallback presentation must read original notices without rewriting them."""
import pytest
from test_workspace_tools import setup

from ahc.application.channel_delivery import ChannelDelivery
from ahc.application.channel_ingress import ChannelIngress
from ahc.application.runtime_service import RuntimeService
from ahc.domain.agent_backend import AgentTurnObservation, AgentTurnRequest
from ahc.domain.channel_ingress import ChannelInboundEvent, ChannelRoute
from ahc.domain.skill_values import canonical_json
from ahc.infrastructure.channel_outbox import ChannelOutbox
from ahc.infrastructure.runtime_inbox import RuntimeInbox
from ahc.infrastructure.runtime_repository import RuntimeRepository
from ahc.infrastructure.working_repository import WorkingSetCorrupt

OLD_NOTICE = (
    "⚠ 任務未完成\n這回合沒有取得可確認的最終回覆，不能視為任務已完成。"
    "\n部分操作可能已執行，結果尚未確認；不會自動重跑。"
    "\n任務脈絡已保留；可用 /status 查看狀態，或要求先核對目前進度。"
)


@pytest.mark.parametrize('platform', ['telegram', 'discord'])
@pytest.mark.parametrize('state', ['pending', 'claimed', 'sent', 'unknown'])
def test_original_notice_reopens_with_identical_payload_and_delivery_state(tmp_path, platform, state):
    repo, workspace, _ = setup(tmp_path)
    snapshot = repo.read_current('session')
    route = ChannelRoute(platform, 'account', 'channel', 'user', snapshot.session)
    event = ChannelInboundEvent(route, 'previous-failure', 'prior work')
    ChannelIngress(RuntimeInbox(repo), route).submit(event, snapshot.goal.digest)
    request = AgentTurnRequest(snapshot.session, event.operation_id, snapshot.goal.digest,
        event.input_text, (), '0' * 64, '1' * 64, 'work', 100)

    class Backend:
        def invoke(self, bound):
            return AgentTurnObservation(bound, 'unknown', 'fixture', 1, 1,
                                        'thread', 'turn', unresolved_effects=True)

    class Destination:
        def send(self, *args):
            if state == 'unknown':
                raise RuntimeError('lost acknowledgment')
            return 'old-ack'

    try:
        RuntimeService(repo, Backend()).execute(snapshot, request)
        outbox = ChannelOutbox(repo)
        if state == 'claimed':
            outbox.claim(route, request.operation_id, 'claim')
        elif state in {'sent', 'unknown'}:
            try:
                ChannelDelivery(outbox, route, Destination()).deliver(request.operation_id, 'claim')
            except RuntimeError:
                assert state == 'unknown'
        # A fixture of an already persisted predecessor notice, never a live edit.
        old_payload = canonical_json({'output_text': OLD_NOTICE}).decode()
        repo._mutate(lambda db: db.execute(
            'UPDATE runtime_channel_outbox SET payload_json=? WHERE operation_id=?',
            (old_payload, request.operation_id)))
        reopened = RuntimeRepository(repo.root)
        records = ChannelOutbox(reopened).read(route)
        assert records[0]['state'] == state
        assert records[0]['payload'] == {'output_text': OLD_NOTICE}
        with reopened._read() as db:
            assert db.execute('SELECT payload_json FROM runtime_channel_outbox WHERE operation_id=?',
                              (request.operation_id,)).fetchone()[0] == old_payload
        # Compatibility is limited to recognized parent-bound notices.
        repo._mutate(lambda db: db.execute(
            'UPDATE runtime_channel_outbox SET payload_json=? WHERE operation_id=?',
            (canonical_json({'output_text': 'unbound replacement'}).decode(), request.operation_id)))
        with pytest.raises(WorkingSetCorrupt):
            ChannelOutbox(reopened).read(route)
    finally:
        workspace.close()
