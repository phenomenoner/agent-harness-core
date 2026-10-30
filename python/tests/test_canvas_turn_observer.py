import json
import os
import time
from dataclasses import replace
from pathlib import Path

import pytest
from skill_test_support import goal
from test_foreground_config_v2 import valid_config
from test_foreground_runtime import setup

from ahc.application.canvas_turn_observer import CanvasTurnObserver
from ahc.application.runtime_service import RuntimeService
from ahc.cli.foreground_canvas import compose_canvas
from ahc.cli.foreground_config_v2 import parse_foreground_config_v2
from ahc.domain.agent_backend import AgentTurnObservation, AgentTurnRequest
from ahc.domain.sessions import ContractError, VirtualSessionIdentity
from ahc.domain.working_sets import WorkingSetSnapshot
from ahc.infrastructure.runtime_repository import RuntimeRepository


def seed(store, session, operation, *, disposition='completed', text='symbolic-canvas'):
    current = store.read_current(session.ref.virtual_session_id)
    request = AgentTurnRequest(session, operation, current.goal.digest, text, (),
        'a' * 64, 'b' * 64, 'work', time.monotonic() + 30)

    class Backend:
        def invoke(self, bound):
            return AgentTurnObservation(bound, disposition, 'fixture', 1,
                1 if disposition == 'completed' else 0,
                output_text='PRIVATE OUTPUT', unresolved_effects=disposition == 'unknown')

    return request, RuntimeService(store, Backend()).execute(current, request)


def repository(tmp_path):
    store = RuntimeRepository.initialize(tmp_path / 'runtime')
    session = VirtualSessionIdentity.initial('session', 'main', 'route')
    store.create(WorkingSetSnapshot.initial(session, goal()))
    return store, session


def test_real_foreground_two_turns_replay_and_next_turn(tmp_path):
    runtime, requests, invoked, request = setup(tmp_path)
    runtime.canvas = CanvasTurnObserver(runtime.store, runtime.session)
    runtime.start()
    try:
        one = replace(request, input_text='symbolic-canvas PRIVATE INPUT')
        two = replace(one, operation_id='second')
        requests.extend([one, two, two, replace(request, operation_id='routine')])
        runtime.step()
        assert runtime._canvas_status['status'] == 'below-threshold'
        second = runtime.step()
        status = runtime.channel_status()['symbolic_canvas']
        assert status['status'] == 'written' and status['turns'] == 2
        path = Path(status['json_path'])
        original, modified = path.read_bytes(), path.stat().st_mtime_ns
        assert runtime.step() == second
        assert runtime._canvas_status['status'] == 'reused'
        assert path.read_bytes() == original and path.stat().st_mtime_ns == modified
        assert runtime.step().result['accepted']
        assert runtime._canvas_status['status'] == 'routine-skipped'
        assert invoked == ['op', 'second', 'routine']
        document = json.loads(original)
        assert document['mermaid'] == Path(status['mermaid_path']).read_text()
        assert len(document['nodes']) == 2 and len(document['edges']) == 1
        assert not document['goal_completion_verified']
        assert b'PRIVATE' not in original and b'done' not in original
    finally:
        runtime.close()
    reopened = CanvasTurnObserver(RuntimeRepository(runtime.store.root), runtime.session)
    assert reopened.observe(two, second)['status'] == 'reused'
    assert path.stat().st_mtime_ns == modified


def test_qualified_failure_keeps_unknown_and_does_not_capture_text(tmp_path):
    store, session = repository(tmp_path)
    observer = CanvasTurnObserver(store, session, min_turns=1)
    request, turn = seed(store, session, 'unknown', disposition='unknown', text='PRIVATE INPUT')
    status = observer.observe(request, turn)
    assert status['status'] == 'written'
    document = json.loads(Path(status['json_path']).read_bytes())
    assert document['nodes'][0]['state'] == 'unknown'
    assert document['runtime_references'][0]['unresolved_effects'] is True
    assert 'PRIVATE' not in json.dumps(document)
    assert store.read_turn(session, 'unknown') == turn


def test_history_is_scoped_bounded_and_anchored_before_later_turns(tmp_path):
    store, session = repository(tmp_path)
    foreign = VirtualSessionIdentity.initial('foreign', 'main', 'other-route')
    store.create(WorkingSetSnapshot.initial(foreign, goal()))
    for i in range(20):
        seed(store, session, 'op' + str(i))
        seed(store, foreign, 'foreign' + str(i))
    assert [x.operation_id for x in store.recent_terminal_turns(session, 'op19')] == [
        'op' + str(i) for i in range(4, 20)]
    assert [x.operation_id for x in store.recent_terminal_turns(session, 'op1')] == ['op0', 'op1']
    for wrong in (foreign, session.successor(), replace(session, agent_id='other')):
        with pytest.raises(ContractError, match='owner'):
            store.recent_terminal_turns(wrong, 'op19')
    for limit in (True, 0, 17):
        with pytest.raises(ContractError):
            store.recent_terminal_turns(session, 'op19', limit=limit)


def test_pair_partial_write_recovery_and_conflict_preservation(tmp_path, monkeypatch):
    store, session = repository(tmp_path)
    request, turn = seed(store, session, 'one')
    observer = CanvasTurnObserver(store, session, min_turns=1)
    original = Path.open

    def fail_mermaid(path, mode='r', *args, **kwargs):
        if path.suffix == '.mmd' and mode == 'xb':
            raise OSError('PRIVATE DISK ERROR')
        return original(path, mode, *args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(Path, 'open', fail_mermaid)
        assert observer.observe(request, turn)['status'] == 'artifact-unavailable'
    files = list((store.root / 'symbolic-canvas-auto').rglob('*.json'))
    assert len(files) == 1
    before = files[0].read_bytes()
    assert observer.observe(request, turn)['status'] == 'written'
    assert files[0].read_bytes() == before
    files[0].write_text('conflicting bytes')
    assert observer.observe(request, turn)['status'] == 'artifact-unavailable'
    assert files[0].read_text() == 'conflicting bytes'


@pytest.mark.skipif(os.name == 'nt', reason='native symlink behavior qualified on Linux')
@pytest.mark.parametrize('linked', ['directory', 'output'])
def test_output_links_do_not_write_outside_root(tmp_path, linked):
    store, session = repository(tmp_path)
    request, turn = seed(store, session, 'one')
    observer = CanvasTurnObserver(store, session, min_turns=1)
    outside = tmp_path / 'outside'
    outside.mkdir()
    if linked == 'directory':
        (store.root / 'symbolic-canvas-auto').symlink_to(outside, target_is_directory=True)
    else:
        status = observer.observe(request, turn)
        path = Path(status['mermaid_path'])
        path.unlink()  # This test owns the isolated disposable output.
        path.symlink_to(outside / 'uncreated')
    assert observer.observe(request, turn)['status'] == 'artifact-unavailable'
    assert not list(outside.iterdir())


def test_forged_or_foreign_observation_cannot_create_output(tmp_path):
    store, session = repository(tmp_path)
    request, turn = seed(store, session, 'one')
    observer = CanvasTurnObserver(store, session, min_turns=1)
    assert observer.observe(request, replace(turn, status='submitted'))['status'] == 'artifact-unavailable'
    assert observer.observe(replace(request, session=session.successor()), turn)['status'] == 'artifact-unavailable'
    assert not (store.root / 'symbolic-canvas-auto').exists()


def test_hook_exception_cannot_replace_answer_or_block_next_turn(tmp_path):
    runtime, requests, invoked, request = setup(tmp_path)

    class Broken:
        def observe(self, request, result):
            assert runtime._backend is None  # Cleanup happens before observation.
            raise RuntimeError('PRIVATE')

    runtime.canvas = Broken()
    runtime.start()
    try:
        requests.extend([request, replace(request, operation_id='next')])
        assert runtime.step().result['accepted']
        assert runtime._canvas_status == {'status': 'observer-unavailable'}
        assert runtime.step().result['accepted'] and invoked == ['op', 'next']
    finally:
        runtime.close()


def test_config_opt_in_preview_digest_disable_and_standby(tmp_path):
    raw = valid_config()
    absent = parse_foreground_config_v2(raw)
    assert compose_canvas(absent, None) is None
    raw['symbolic_canvas'] = dict(enabled=True, min_turns=2, trigger_mode='qualified')
    config = parse_foreground_config_v2(raw)
    assert config.preview()['symbolic_canvas'] == raw['symbolic_canvas']
    assert config.configuration_digest != absent.configuration_digest
    assert compose_canvas(config, None) is None  # Entire backend is disabled.
    raw['backend']['enabled'] = True
    config = parse_foreground_config_v2(raw)
    observer = compose_canvas(config, None)
    assert observer.session == config.session and observer.min_turns == 2
    raw['symbolic_canvas']['enabled'] = False
    assert compose_canvas(parse_foreground_config_v2(raw), None) is None
    assert not list(tmp_path.iterdir())


@pytest.mark.parametrize('value', [None, {}, {'enabled': True, 'min_turns': True, 'trigger_mode': 'all'},
    {'enabled': True, 'min_turns': 17, 'trigger_mode': 'all'},
    {'enabled': True, 'min_turns': 1, 'trigger_mode': 'unknown'},
    {'enabled': 1, 'min_turns': 1, 'trigger_mode': 'all'}])
def test_invalid_config_is_rejected(value):
    raw = valid_config()
    raw['symbolic_canvas'] = value
    with pytest.raises(ContractError):
        parse_foreground_config_v2(raw)


def test_quota_and_routine_skip_preserve_answer(tmp_path, monkeypatch):
    store, session = repository(tmp_path)
    request, turn = seed(store, session, 'one', text='long ordinary input ' * 100)
    observer = CanvasTurnObserver(store, session, min_turns=1)
    assert observer.observe(request, turn)['status'] == 'routine-skipped'
    assert not (store.root / 'symbolic-canvas-auto').exists()
    observer.trigger_mode = 'all'
    monkeypatch.setattr('ahc.application.canvas_turn_observer.MAX_FILES', 1)
    assert observer.observe(request, turn)['status'] == 'quota-reached'
    assert not list((store.root / 'symbolic-canvas-auto').rglob('*.json'))
    assert store.read_turn(session, 'one') == turn
