"""The observer must see a correlated process result before steering."""

import importlib.util
from pathlib import Path

spec = importlib.util.spec_from_file_location(
    "steering_acceptance", Path(__file__).parents[1] / "tools/steering_acceptance.py"
)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


def base():
    return [
        {"type": "session_meta", "payload": {"id": "thread-test"}},
        {"type": "event_msg", "payload": {"type": "user_message", "message": "test-NONCE"}},
    ]


def call(call_id="exec-1", raw='text(await tools.exec_command({cmd:"sleep 45"}));'):
    return {
        "type": "response_item",
        "payload": {"type": "custom_tool_call", "call_id": call_id, "name": "exec", "input": raw},
    }


def output(text, call_id="exec-1"):
    return {
        "type": "response_item",
        "timestamp": "now",
        "payload": {"type": "custom_tool_call_output", "call_id": call_id, "output": text},
    }


def inspect(events):
    return module.terminal_progress(events, "NONCE", "sleep 45")


def test_admission_or_prose_is_not_running():
    assert not inspect(base())["ready"]
    assert not inspect(base() + [output("session_id: 42")])["ready"]


def test_call_without_output_is_not_running():
    assert not inspect(base() + [call()])["ready"]


def test_foreign_output_is_not_running():
    assert not inspect(base() + [call(), output("session_id: 42", "foreign")])["ready"]


def test_marked_correlated_process_is_running():
    value = inspect(base() + [call(), output('{"session_id": 42, "exit_code": null}')])
    assert value["ready"] and value["session_id"] == "42" and value["thread"] == "thread-test"


def test_text_process_output_is_running():
    assert inspect(base() + [call(), output("Process running with session ID 42")])["ready"]


def test_completed_blocking_command_is_not_running():
    assert not inspect(base() + [call(), output('{"exit_code": 0}')])["ready"]


def test_terminal_after_session_is_not_running():
    events = base() + [
        call(),
        output("session_id: 42"),
        {"type": "event_msg", "payload": {"type": "turn_aborted"}},
    ]
    assert not inspect(events)["ready"]


def test_wait_requires_session_and_correlated_exit():
    events = base() + [
        call(),
        output("session_id: 42"),
        call("wait-1", "text(await tools.write_stdin({session_id:42}));"),
        output("Process exited with exit code 0", "wait-1"),
    ]
    result = inspect(events)
    assert result["wait_called"] and result["exit_zero"] and not result["ready"]


def test_other_task_nonce_never_arms():
    assert not module.terminal_progress(
        base() + [call(), output("session_id: 42")], "DIFFERENT", "sleep 45"
    )["ready"]


def test_native_user_message_item_arms():
    events = [
        base()[0],
        {
            "type": "response_item",
            "payload": {
                "type": "message",
                "role": "user",
                "content": [{"type": "input_text", "text": "test-NONCE"}],
            },
        },
    ]
    assert inspect(events + [call(), output("session_id: 42")])["ready"]


def test_assistant_message_cannot_arm():
    events = [
        base()[0],
        {
            "type": "response_item",
            "payload": {
                "type": "message",
                "role": "assistant",
                "content": [{"type": "output_text", "text": "test-NONCE"}],
            },
        },
    ]
    assert not inspect(events + [call(), output("session_id: 42")])["ready"]


def test_other_session_wait_does_not_count():
    events = base() + [
        call(),
        output("session_id: 42"),
        call("wait-1", "text(await tools.write_stdin({session_id:142}));"),
    ]
    assert not inspect(events)["wait_called"]


def test_restarted_command_does_not_arm():
    events = base() + [
        call(),
        output("session_id: 42"),
        call("exec-2"),
        output("session_id: 43", "exec-2"),
    ]
    assert not inspect(events)["ready"]


def cell_wait(cell="1"):
    return {
        "type": "response_item",
        "payload": {
            "type": "function_call",
            "name": "wait",
            "call_id": "cell-wait",
            "arguments": '{"cell_id":"' + cell + '"}',
        },
    }


def composed_events():
    return base() + [
        call(
            raw='const r=await tools.exec_command({cmd:"sleep 45"}); '
            "text(r); text(await tools.write_stdin({session_id:r.session_id}));"
        ),
        output('Script running with cell ID 1; {"session_id":42}; {"session_id":42}'),
    ]


def test_composed_wait_cell_retains_original_process_evidence():
    result = inspect(composed_events() + [cell_wait(), output('{"exit_code":0}', "cell-wait")])
    assert result["wait_called"] and result["exit_zero"] and result["exec_calls"] == 1


def test_foreign_cell_cannot_complete_original_process():
    result = inspect(
        composed_events() + [cell_wait("foreign"), output('{"exit_code":0}', "cell-wait")]
    )
    assert result["wait_called"] and not result["exit_zero"]
