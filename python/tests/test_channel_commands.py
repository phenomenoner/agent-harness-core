import time
from dataclasses import replace
from types import SimpleNamespace

import pytest
from test_runtime_core import Backend, setup

from ahc.application.channel_delivery import ChannelDelivery
from ahc.application.channel_ingress import ChannelIngress
from ahc.application.foreground_runtime import ForegroundRuntime
from ahc.application.runtime_service import RuntimeService
from ahc.cli.foreground_commands import compose_commands
from ahc.domain.channel_ingress import ChannelInboundEvent, ChannelRoute
from ahc.domain.sessions import ContractError
from ahc.infrastructure.channel_outbox import ChannelOutbox
from ahc.infrastructure.runtime_inbox import RuntimeInbox
from ahc.infrastructure.runtime_repository import RuntimeRepository


def command_setup(tmp_path, text, platform='telegram'):
    repo, current, request = setup(tmp_path)
    route = ChannelRoute(platform, 'account', 'channel', 'user', current.session)
    event = ChannelInboundEvent(route, 'event', text)
    ChannelIngress(RuntimeInbox(repo), route).submit(event, current.goal.digest)
    config = SimpleNamespace(
        session=current.session,
        backend={'model': 'gpt-5.6-luna', 'reasoning_effort': 'max'},
        effective_policy=SimpleNamespace(approval_mode='manual'),
        to_dict=lambda: {'channels': [{'route': route.to_dict(), 'enabled': True}],
                         'private': 'never-output-secret'},
    )
    request = replace(request, input_text=text, operation_id=event.operation_id,
                      deadline=time.monotonic()+30, capabilities=())
    return repo, current, request, route, config


def test_local_command_is_durable_without_backend_and_replay_does_not_repeat(tmp_path):
    repo, current, request = setup(tmp_path)
    backend = Backend()
    calls = []

    def reply(bound):
        calls.append(bound.operation_id)
        return 'Model: configured-luna'

    service = RuntimeService(repo, backend, local_reply=reply)
    result = service.execute(current, replace(request, input_text='/model'))
    assert result.result['accepted'] is True
    assert result.result['execution_kind'] == 'local-command'
    assert result.result['admissions'] == result.result['accepted_backend_turns'] == 0
    assert backend.calls == 0
    assert service.execute(current, replace(request, input_text='/model')) == result
    assert len(calls) == 1


@pytest.mark.parametrize('platform', ['telegram', 'discord'])
@pytest.mark.parametrize('text,expected', [
    ('/model','gpt-5.6-luna'), ('/think','max'), ('/reasoning','max'),
    ('/status','generation'), ('/help','/model'), ('/commands','/model'),
    ('/start','/model'), ('/new','尚未支援'), ('/stop','尚未支援'),
    ('/model another','未修改'), ('/model@otherbot','不帶 @'), ('/invented','尚未支援'),
])
def test_commands_use_durable_channel_delivery_and_never_create_backend(
        tmp_path, platform, text, expected):
    repo, current, request, route, config = command_setup(tmp_path, text, platform)
    def forbidden(*args, **kwargs):
        pytest.fail('command launched a model or typing worker')
    runtime = ForegroundRuntime(repo, current.session, lambda *_: request, forbidden,
                                local_reply=compose_commands(config, repo),
                                channel_typing=forbidden)
    runtime.start()
    try:
        result = runtime.step()
        assert expected in result.result['output_text']
        assert 'never-output-secret' not in result.result['output_text']
        assert result.result['accepted_backend_turns'] == 0
    finally:
        runtime.close()
    reopened = RuntimeRepository(repo.root)
    deliveries = []
    class Transport:
        def send(self, *args):
            deliveries.append(args)
            return 'receipt'
    delivery = ChannelDelivery(ChannelOutbox(reopened), route, Transport())
    delivery.deliver(request.operation_id, 'send-once')
    with pytest.raises(ContractError, match='not pending'):
        delivery.deliver(request.operation_id, 'retry')
    assert len(deliveries) == 1
    assert ChannelOutbox(reopened).read(route)[0]['state'] == 'sent'


def test_ordinary_chat_still_calls_backend(tmp_path):
    repo, current, request, _, config = command_setup(tmp_path, 'hello')
    backend = Backend()
    result = RuntimeService(repo, backend, local_reply=compose_commands(config, repo)).execute(
        current, request)
    assert backend.calls == 1 and result.result['output_text'] == 'answer'


def test_disabled_route_and_expired_command_fail_without_provider(tmp_path):
    repo, current, request, _, config = command_setup(tmp_path, '/model')
    config.to_dict = lambda: {'channels': []}
    backend = Backend()
    result = RuntimeService(repo, backend, local_reply=compose_commands(config, repo)).execute(
        current, request)
    assert backend.calls == 0 and result.result['accepted'] is False


def test_local_cli_slash_is_not_intercepted(tmp_path):
    repo, current, request = setup(tmp_path)
    config = SimpleNamespace(to_dict=lambda: {'channels': []})
    request = replace(request, input_text='/model', deadline=time.monotonic()+30)
    assert compose_commands(config, repo)(request) is None
