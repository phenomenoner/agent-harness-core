"""One-shot exact-turn budget advice; the original deadline remains authoritative."""

from .codex_steering import (
    accepted_turn_steer_response,
    rejected_turn_steer_response,
    turn_steer_message,
)


class BudgetWrapUp:
    """Ephemeral advice for one admitted turn; never submit or replay model work."""

    REQUEST_ID = "ahc-budget-wrap-up"
    INSTRUCTIONS = (
        "AHC reserves a final portion of this turn's existing time limit for graceful wrap-up. "
        "When the host sends the budget wrap-up notice, stop expanding the task, safely finish "
        "necessary in-flight checks, save confirmed progress and unresolved effects if time permits, "
        "and return your final reply promptly. Report completed and unfinished work accurately. "
        "A final reply can be a handover; do not claim the task is complete without evidence."
    )

    def __init__(self, deadline, timeout, started_at):
        reserve = min(timeout, max(0, deadline - started_at) / 10)
        self.notice_at = deadline - reserve
        self.attempted = False
        self.status = "pending"
        self.thread = self.turn = None

    def poll(self, client):
        if (self.attempted or client.accepted != 1 or not client.effects_open
                or client.thread is None or client.turn is None
                or client.clock() < self.notice_at):
            return
        # Mark before writing: an indeterminate write is never a resend permission.
        self.attempted = True
        self.thread, self.turn = client.thread, client.turn
        self.status = "write-unknown"
        remaining = max(0, client.request.effective_deadline - client.clock())
        text = (
            f"AHC budget wrap-up notice: approximately {remaining:.0f} seconds remain "
            "before the existing hard time limit. Begin graceful wrap-up now. "
            "Do not start new workstreams or long-running tools. Safely finish only necessary "
            "in-flight verification, checkpoint confirmed progress and exact unresolved effects "
            "with available tools if time permits, then send your final reply promptly. "
            "State completed work, unfinished work, and the next continuation step accurately."
        )
        try:
            client._send(turn_steer_message(self.REQUEST_ID, self.thread, self.turn, text))
        except Exception:
            # This advice cannot invalidate a genuine independently observed final.
            # A broken transport will still fail through its normal receive path.
            return
        self.status = "sent"

    def consume_response(self, client, message):
        if (not self.attempted or "method" in message
                or message.get("id") != self.REQUEST_ID):
            return False
        client._pending_control_deadlines.pop(self.REQUEST_ID, None)
        if self.status in {"accepted", "rejected", "unverified"}:
            return True
        if accepted_turn_steer_response(message, self.REQUEST_ID, self.turn):
            self.status = "accepted"
        elif rejected_turn_steer_response(message, self.REQUEST_ID):
            self.status = "rejected"
        else:
            self.status = "unverified"
        return True
