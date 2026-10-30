"""Non-sleeping Gateway pump with reconnect bursts and cancellable cooldowns."""

import random
import time

from ahc.adapters.discord_gateway_state import DiscordGatewayState
from ahc.adapters.discord_websocket import DiscordGatewayClosed, DiscordWebSocket
from ahc.domain.sessions import ContractError


class DiscordGatewayDriver:
    def __init__(self, account_id, token, intents, admit, *, connect=DiscordWebSocket,
                 clock=time.monotonic, jitter=random.random, connection_limit=2,
                 resume=None, checkpoint=None, identify_admission=None):
        if (not all(callable(v) for v in (admit, connect, clock, jitter))
                or type(connection_limit) is not int or connection_limit < 1):
            raise ContractError("Discord driver binding invalid")
        self._persistence_failure = None
        checked_checkpoint = (self._durable_callback(checkpoint, "session-checkpoint-unavailable")
                              if callable(checkpoint) else checkpoint)
        self.protocol = DiscordGatewayState(account_id, token, intents,
                                             resume=resume, checkpoint=checked_checkpoint)
        self._admit = self._durable_callback(admit, "dispatch-persistence-unavailable")
        self._connect, self._clock, self._jitter = connect, clock, jitter
        self._socket, self._heartbeat_due = None, None
        self._handshake_due = None
        self._next_connect, self._attempts, self._limit = 0.0, 0, connection_limit
        self._total_attempts = 0
        self._ready_since = None
        self._pause_reason = None
        self.state = "new"
        self._close_code = None
        if identify_admission is not None and not callable(identify_admission):
            raise ContractError("Discord Identify admission invalid")
        self._identify_admission = identify_admission
        self._identify_blocked = False

    def _durable_callback(self, callback, reason):
        def checked(frame):
            try:
                return callback(frame)
            except Exception:
                # Protocol validation deliberately hides raw exceptions. Retain
                # the fixed failure category before that projection so a lost
                # or replaced checkpoint cannot enter transport-only recovery.
                self._persistence_failure = reason
                raise
        return checked

    def status(self):
        return {"state": self.state, "connection_attempts": self._attempts,
                "total_connection_attempts": self._total_attempts,
                "connection_limit": self._limit, "sequence": self.protocol.sequence,
                **({"close_code": self._close_code} if self._close_code is not None else {}),
                **({"reason": self._pause_reason} if self._pause_reason else {})}

    def _disconnect(self):
        if self._socket is not None:
            disconnect = getattr(self._socket, "disconnect", self._socket.close)
            disconnect()
            self._socket = None
        self._heartbeat_due = None
        self._ready_since = None
        # A transient outage must not permanently disable a long-lived receiver.
        # The configured count bounds one burst; a full burst cools down for a
        # minute to prevent rapid repeated sockets. Fatal protocol/authorization
        # decisions below still pause without sending another Identify.
        self._next_connect = self._clock() + (60 if self._attempts >= self._limit else 5)
        self.state = "reconnecting"
        self._pause_reason = None

    def _send(self, frame, *, cancelled=lambda: False):
        if frame is None:
            return
        if frame.get("action") == "reconnect":
            self._disconnect()
        else:
            if frame.get("op") == 2 and self._identify_admission is not None:
                try:
                    allowed = self._identify_admission() is True
                except Exception:
                    allowed = False
                if not allowed:
                    self._identify_blocked = True
                    self._disconnect()
                    self.state = "paused"
                    self._pause_reason = "identify-admission-unavailable"
                    return
                if cancelled():
                    self._disconnect()
                    self.state = "paused"
                    self._pause_reason = "cancelled"
                    return
            self._socket.send(frame)

    def step(self, *, cancelled=lambda: False):
        if self.state in {"closed", "paused"} or cancelled():
            return
        try:
            if self._socket is None:
                if self._clock() < self._next_connect:
                    return
                if self._attempts >= self._limit:
                    self._attempts = 0
                self._attempts += 1
                self._total_attempts += 1
                if self.protocol.state in {"failed", "reconnect"}:
                    self.protocol.reconnect()
                self._socket = self._connect(self.protocol.resume_url or "wss://gateway.discord.gg")
                self._handshake_due = self._clock() + 10
                self.state = "connecting"
            for _ in range(16):
                if cancelled():
                    return
                frame = self._socket.receive(timeout=0)
                if frame is None:
                    break
                awaiting_ack = self.protocol.awaiting_ack
                response = self.protocol.receive(frame, self._admit)
                if self.protocol.state == "ready":
                    if self._ready_since is None:
                        self._ready_since = self._clock()
                    # READY alone can flap. Only a solicited heartbeat ACK after
                    # a full minute of ready state restores the bounded budget.
                    if (frame["op"] == 11 and awaiting_ack
                            and self._clock() - self._ready_since >= 60):
                        self._attempts = 0
                if frame["op"] == 10:
                    jitter = self._jitter()
                    if type(jitter) not in (int, float) or not 0 <= jitter <= 1:
                        raise ContractError("Discord heartbeat jitter invalid")
                    self._heartbeat_due = self._clock() + self.protocol.interval_ms / 1000 * jitter
                self._send(response, cancelled=cancelled)
                if self._socket is None:
                    return
            if self._heartbeat_due is not None and self._clock() >= self._heartbeat_due:
                self._send(self.protocol.heartbeat())
                if self._socket is not None:
                    self._heartbeat_due = self._clock() + self.protocol.interval_ms / 1000
            if self._socket is not None:
                self.state = self.protocol.state
                if self.state != "ready" and self._clock() >= self._handshake_due:
                    raise ContractError("Discord handshake deadline exceeded")
        except DiscordGatewayClosed as error:
            self._close_code = error.code
            self.protocol.state = "failed"
            pause = error.code in {4004, 4010, 4011, 4012, 4013, 4014}
            reason = "non-reconnectable-close" if pause else None
            if error.code in {1000, 1001, 4007, 4009}:
                try:
                    self.protocol.invalidate_session()
                except Exception:
                    pause = True
                    reason = "session-invalidation-unavailable"
            self._disconnect()
            if pause:
                self.state = "paused"
                self._pause_reason = reason
        except Exception:
            self.protocol.state = "failed"
            self._disconnect()
            if self._persistence_failure is not None:
                self.state = "paused"
                self._pause_reason = self._persistence_failure

    def close(self):
        if self._socket is not None:
            self._socket.close()
            self._socket = None
        self.state = "closed"
