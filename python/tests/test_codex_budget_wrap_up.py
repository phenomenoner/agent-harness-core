"""Graceful turn-budget wrap-up stays inside the admitted hard deadline."""

from dataclasses import replace

import pytest
from test_codex_client import Peer, setup
from test_foreground_config_native import native_config
from test_codex_phase_budgets import Clock

from ahc.adapters.codex_client import CodexProtocolClient
from ahc.adapters.codex_steering import turn_steer_message
from ahc.adapters.codex_transport import CodexTransportError
from ahc.cli.foreground_config_native import parse_foreground_config_native
from ahc.domain.sessions import ContractError


REQUEST_ID = "ahc-budget-wrap-up"


class BudgetPeer(Peer):
    """Scripted peer with idle callbacks and a manually advanced monotonic clock."""

    def __init__(
        self,
        clock,
        advances,
        *,
        idle=True,
        acknowledgement=None,
        fail_warning_writes=0,
        cancelled=None,
        late_acknowledgement=None,
    ):
        super().__init__()
        self.clock = clock
        self.advances = iter(advances)
        self.idle = idle
        self.acknowledgement = acknowledgement
        self.fail_warning_writes = fail_warning_writes
        self.cancelled = cancelled
        self.late_acknowledgement = late_acknowledgement
        self.read_deadlines = []
        self.accepted_when_returning = []
        self.warnings_when_returning = []
        self.warning_times = []
        self.warning_attempts = 0
        self.acknowledgement_enqueued = False
        self.late_acknowledgement_returned = False
        self.late_acknowledgement_after_terminal = False
        self.late_acknowledgement_time = None
        self.client = None

    def send(self, message, *, deadline):
        super().send(message, deadline=deadline)
        if message.get("id") == REQUEST_ID:
            self.warning_attempts += 1
            self.warning_times.append(self.clock())
            if self.fail_warning_writes:
                self.fail_warning_writes -= 1
                raise OSError("indeterminate wrap-up write")

    def receive(self, *, deadline, on_idle=None):
        self.read_deadlines.append(deadline)
        self.clock.value += next(self.advances, 0)
        if self.clock() >= deadline:
            raise CodexTransportError("deadline", transport_error_type="deadline")
        if on_idle is not None and self.idle:
            on_idle()
        if self.cancelled is not None and self.cancelled():
            raise CodexTransportError("cancelled")
        if (self.acknowledgement is not None and self.warning_attempts
                and not self.acknowledgement_enqueued
                and self.late_acknowledgement is None):
            self.messages.insert(0, self.acknowledgement)
            self.acknowledgement_enqueued = True
        self.accepted_when_returning.append(
            None if self.client is None else self.client.accepted
        )
        self.warnings_when_returning.append(self.warning_attempts)
        message = super().receive(deadline=deadline)
        if (self.late_acknowledgement is not None and self.warning_attempts
                and self.client is not None and self.client._native_terminal_received
                and not self.late_acknowledgement_returned):
            self.messages.insert(0, message)
            self.late_acknowledgement_returned = True
            self.late_acknowledgement_after_terminal = True
            self.late_acknowledgement_time = self.clock()
            return self.late_acknowledgement
        return message


def wrap_client(peer, clock, *, deadline=100, timeout=10, cancelled=None, **options):
    _, _, template = setup()
    client = CodexProtocolClient(
        peer,
        cwd="workspace",
        model="fixture",
        clock=clock,
        cancelled=(lambda: False) if cancelled is None else cancelled,
        wrap_up_timeout=timeout,
        **options,
    )
    request = replace(
        template,
        deadline=deadline,
        configuration_digest=client.configuration_digest,
    )
    peer.client = client
    return client, request


def warning_messages(peer):
    return [message for message in peer.sent if message.get("id") == REQUEST_ID]


def assert_exact_warning(peer):
    message, = warning_messages(peer)
    text = message["params"]["input"][0]["text"]
    assert message == turn_steer_message(REQUEST_ID, "thread", "turn", text)
    return text


def assert_completed(result):
    assert result.disposition == "completed"
    assert result.output_text == "answer"


@pytest.mark.parametrize("advances", [(0, 0, 0, 90), (0, 0, 90, 0)])
def test_idle_warning_is_exact_once_and_only_after_turn_admission(advances):
    clock = Clock()
    peer = BudgetPeer(clock, advances, idle=True)
    client, request = wrap_client(peer, clock)

    result = client.run_admitted(request)

    assert_completed(result)
    text = assert_exact_warning(peer)
    assert "approximately 10 seconds remain" in text
    assert peer.warning_times == [90]
    assert peer.accepted_when_returning[2] == 0
    assert peer.warnings_when_returning[2] == 0
    assert peer.accepted_when_returning[3] == 1
    assert peer.warnings_when_returning[3] == 1
    assert sum(message.get("method") == "turn/start" for message in peer.sent) == 1
    assert peer.read_deadlines[-1] == request.deadline == 100


def test_busy_advisory_frames_still_deliver_one_warning_to_the_same_turn():
    clock = Clock()
    peer = BudgetPeer(clock, [0, 0, 0, 90, 0, 0], idle=False)
    peer.messages[3:3] = [
        {"method": "future/information", "params": {}},
        {"method": "future/information", "params": {}},
    ]
    client, request = wrap_client(peer, clock)

    result = client.run_admitted(request)

    assert_completed(result)
    text = assert_exact_warning(peer)
    assert "approximately 10 seconds remain" in text
    assert peer.warning_times == [90]
    assert peer.warnings_when_returning[3] == 0
    assert peer.warnings_when_returning[4:] == [1, 1]
    assert sum(message.get("method") == "turn/start" for message in peer.sent) == 1
    assert peer.read_deadlines[-1] == request.deadline == 100


@pytest.mark.parametrize(
    ("acknowledgement", "status"),
    [
        pytest.param(None, "sent", id="missing"),
        pytest.param(
            {"id": REQUEST_ID, "error": {"code": -32000, "message": "stale"}},
            "rejected",
            id="rejected",
        ),
        pytest.param(
            {"id": REQUEST_ID, "result": {"turnId": "other"}},
            "unverified",
            id="malformed-or-wrong-turn",
        ),
    ],
)
def test_terminal_completion_survives_missing_or_failed_warning_ack(acknowledgement, status):
    clock = Clock()
    peer = BudgetPeer(clock, [0, 0, 0, 90], acknowledgement=acknowledgement)
    client, request = wrap_client(peer, clock)

    result = client.run_admitted(request)

    assert_completed(result)
    assert_exact_warning(peer)
    assert client._wrap_up.status == status
    assert peer.warning_attempts == 1
    assert sum(message.get("method") == "turn/start" for message in peer.sent) == 1
    assert peer.read_deadlines[-1] == request.deadline == 100


def test_short_task_scales_reserve_to_ten_percent_of_remaining_time():
    clock = Clock()
    peer = BudgetPeer(clock, [0, 0, 0, 17, 1], idle=True)
    peer.messages.insert(3, {"method": "future/information", "params": {}})
    client, request = wrap_client(peer, clock, deadline=20, timeout=180)

    result = client.run_admitted(request)

    assert_completed(result)
    text = assert_exact_warning(peer)
    assert "approximately 2 seconds remain" in text
    assert peer.warning_times == [18]
    assert peer.warnings_when_returning[3] == 0
    assert peer.warnings_when_returning[4] == 1
    assert peer.read_deadlines[-1] == request.deadline == 20


def test_hard_deadline_without_terminal_stays_unknown_and_unextended():
    clock = Clock()
    peer = BudgetPeer(clock, [0, 0, 0, 100], idle=True)
    client, request = wrap_client(peer, clock)

    result = client.run_admitted(request)

    assert result.disposition == "unknown"
    assert result.output_text == ""
    assert warning_messages(peer) == []
    assert sum(message.get("method") == "turn/start" for message in peer.sent) == 1
    assert peer.read_deadlines[-1] == request.deadline == 100


def test_terminal_before_notice_window_completes_without_warning():
    clock = Clock()
    peer = BudgetPeer(clock, [0, 0, 0, 89], idle=True)
    client, request = wrap_client(peer, clock)

    result = client.run_admitted(request)

    assert_completed(result)
    assert warning_messages(peer) == []
    assert peer.read_deadlines[-1] == request.deadline == 100


def test_cancelled_idle_does_not_send_a_wrap_up_warning():
    clock = Clock()
    cancelled = lambda: clock() >= 90
    peer = BudgetPeer(clock, [0, 0, 0, 90], idle=True, cancelled=cancelled)
    client, request = wrap_client(peer, clock, cancelled=cancelled)

    result = client.run_admitted(request)

    assert result.disposition == "unknown"
    assert result.output_text == ""
    assert warning_messages(peer) == []
    assert sum(message.get("method") == "turn/start" for message in peer.sent) == 1


def test_failed_warning_write_is_not_retried_on_later_busy_frames():
    clock = Clock()
    peer = BudgetPeer(
        clock,
        [0, 0, 0, 90, 0, 0],
        idle=False,
        fail_warning_writes=1,
    )
    peer.messages[3:3] = [
        {"method": "future/information", "params": {}},
        {"method": "future/information", "params": {}},
    ]
    client, request = wrap_client(peer, clock)

    result = client.run_admitted(request)

    assert_completed(result)
    assert_exact_warning(peer)
    assert client._wrap_up.status == "write-unknown"
    assert peer.warning_attempts == 1
    assert sum(message.get("method") == "turn/start" for message in peer.sent) == 1


def test_late_warning_ack_is_consumed_during_exact_terminal_readback():
    clock = Clock()
    peer = BudgetPeer(
        clock,
        [0, 0, 0, 90, 9, 2, 0],
        idle=True,
        late_acknowledgement={"id": REQUEST_ID, "result": {"turnId": "turn"}},
    )
    full = peer.messages[-1]["params"]["turn"]
    peer.messages.insert(3, {"method": "future/information", "params": {}})
    peer.messages[-1] = {
        "method": "turn/completed",
        "params": {
            "threadId": "thread",
            "turn": {**full, "items": [], "itemsView": "summary"},
        },
    }
    peer.messages.append({"id": 5, "result": {"data": [full]}})
    client, request = wrap_client(peer, clock, settlement_timeout=60)

    result = client.run_admitted(request)

    assert_completed(result)
    assert_exact_warning(peer)
    assert client._wrap_up.status == "accepted"
    assert peer.late_acknowledgement_after_terminal
    assert peer.late_acknowledgement_time > request.deadline
    assert peer.warning_attempts == 1
    assert sum(message.get("method") == "turn/start" for message in peer.sent) == 1
    assert peer.read_deadlines[-1] == 159


def test_wrap_up_is_opt_in_and_changes_the_client_configuration_binding():
    peer, client, request = setup()
    result = client.run_admitted(request)
    enabled = CodexProtocolClient(
        Peer(), cwd="workspace", model="fixture", clock=lambda: 0, wrap_up_timeout=10
    )

    assert_completed(result)
    assert warning_messages(peer) == []
    assert client._wrap_up is None
    assert enabled.configuration_digest != client.configuration_digest


def test_native_config_can_opt_in_while_default_stays_unset():
    value = native_config()
    assert parse_foreground_config_native(value).backend.get("wrap_up_timeout") is None

    value["backend"]["wrap_up_timeout"] = 180
    parsed = parse_foreground_config_native(value)

    assert parsed.backend["wrap_up_timeout"] == 180
    assert parsed.to_dict()["backend"]["wrap_up_timeout"] == 180


@pytest.mark.parametrize("value", [True, 0, -1, float("nan"), float("inf"), "10"])
def test_invalid_wrap_up_timeout_is_rejected(value):
    with pytest.raises(ContractError, match="wrap_up_timeout"):
        CodexProtocolClient(Peer(), cwd="workspace", model="fixture", wrap_up_timeout=value)
