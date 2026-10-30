import pytest
from test_child_dispatch import state

from ahc.application.child_dispatch import ChildDispatchService
from ahc.domain.sessions import ContractError


@pytest.mark.parametrize('committed', [False, True])
def test_lost_completion_reply_reads_durable_truth_without_rerun(tmp_path, monkeypatch, committed):
    _, _, _, children, envelope = state(tmp_path)
    complete = children.complete
    calls = []

    class Port:
        def run(self, *args, **kwargs):
            calls.append('run')
            return 'answer'

        def close(self):
            calls.append('close')

    def lose_reply(*args):
        if committed:
            complete(*args)
        raise OSError('completion reply lost')

    monkeypatch.setattr(children, 'complete', lose_reply)
    service = ChildDispatchService(children, lambda _: Port())
    result = service.dispatch(envelope, 'token')
    assert result['state'] == ('terminal' if committed else 'unknown')
    assert result['result'] == ({'output_text': 'answer'} if committed else None)
    assert calls == ['run', 'close']
    assert service.dispatch(envelope, 'another-token') == result
    assert calls == ['run', 'close']


@pytest.mark.parametrize('mismatch', ['token', 'output', 'envelope'])
def test_completion_readback_rejects_foreign_claim_or_result(tmp_path, mismatch):
    from dataclasses import replace

    _, _, _, children, envelope = state(tmp_path)
    children.admit(envelope)
    children.claim(envelope.parent, envelope.child_id, 'token')
    children.complete(envelope.parent, envelope.child_id, 'token', 'answer')
    selected = replace(envelope, task_text='different') if mismatch == 'envelope' else envelope
    with pytest.raises(ContractError):
        children.reconcile_completion(selected, 'other' if mismatch == 'token' else 'token',
                                      'other' if mismatch == 'output' else 'answer')
    assert children.read(envelope.parent)[0]['result'] == {'output_text': 'answer'}


def test_unavailable_readback_is_not_guessed_or_retried(tmp_path, monkeypatch):
    _, _, _, children, envelope = state(tmp_path)
    calls = []

    class Port:
        def run(self, *args, **kwargs):
            calls.append('run')
            return 'answer'

        def close(self):
            calls.append('close')

    def unavailable(*args):
        calls.append('readback')
        raise OSError('database unavailable')

    monkeypatch.setattr(children, 'complete', lambda *args: (_ for _ in ()).throw(OSError()))
    monkeypatch.setattr(children, 'reconcile_completion', unavailable)
    service = ChildDispatchService(children, lambda _: Port())
    with pytest.raises(OSError):
        service.dispatch(envelope, 'token')
    assert calls == ['run', 'close', 'readback']
    assert children.read(envelope.parent)[0]['state'] == 'claimed'
    with pytest.raises(ContractError, match='no replay'):
        service.dispatch(envelope, 'token')
    assert calls == ['run', 'close', 'readback']
