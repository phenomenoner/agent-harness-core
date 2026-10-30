"""Gateway protocol state; a transport owns sockets, timing, budgets and durable storage."""

from urllib.parse import urlsplit

from ahc.adapters.channel_events import _snowflake
from ahc.domain.sessions import ContractError


class DiscordGatewayState:
    def __init__(self, account_id, token, intents, *, resume=None, checkpoint=None):
        try:
            _snowflake(account_id)
        except ValueError:
            raise ContractError("Discord account invalid") from None
        if (type(token) is not str or not 1 <= len(token) <= 512
                or any(not (c.isascii() and (c.isalnum() or c in ":._-")) for c in token)
                or type(intents) is not int or not 0 <= intents < 2**53):
            raise ContractError("Discord Gateway binding invalid")
        self.account_id, self._token, self._intents = account_id, token, intents
        self.state, self.sequence = "hello", None
        self.session_id, self.resume_url = None, None
        self.interval_ms, self.awaiting_ack = None, False
        if checkpoint is not None and not callable(checkpoint):
            raise ContractError("Discord checkpoint callback invalid")
        self._checkpoint = checkpoint
        if resume is not None:
            if type(resume) is not dict or set(resume) != {"session_id", "resume_url", "sequence"}:
                raise ContractError("Discord resume snapshot invalid")
            # Reuse READY validation without a callback or any transport effect.
            probe = DiscordGatewayState(account_id, token, intents)
            probe.state = "identifying"
            probe.receive({"op": 0, "s": resume["sequence"], "t": "READY", "d": {
                "user": {"id": account_id, "bot": True}, "session_id": resume["session_id"],
                "resume_gateway_url": resume["resume_url"]}}, lambda frame: None)
            self.session_id, self.resume_url, self.sequence = (
                probe.session_id, probe.resume_url, probe.sequence)

    def reconnect(self):
        if self.state not in {"reconnect", "failed"}:
            raise ContractError("Discord reconnect is not requested")
        self.state, self.awaiting_ack = "hello", False

    def invalidate_session(self):
        if self._checkpoint is not None:
            self._checkpoint({"op": 9, "d": False})
        self.session_id = self.resume_url = self.sequence = None

    def heartbeat(self, *, requested=False):
        if self.state not in {"identifying", "resuming", "ready"}:
            raise ContractError("Discord heartbeat state invalid")
        if self.awaiting_ack and not requested:
            self.state = "reconnect"
            return {"action": "reconnect"}
        self.awaiting_ack = True
        return {"op": 1, "d": self.sequence}

    def receive(self, frame, admit):
        """admit must persist a dispatch before returning; failure retains the prior sequence."""
        try:
            return self._receive(frame, admit)
        except Exception:
            self.state = "failed"
            raise ContractError("Discord Gateway event unverified") from None

    def _receive(self, frame, admit):
        if type(frame) is not dict or type(frame.get("op")) is not int or not callable(admit):
            raise ValueError()
        op = frame["op"]
        if op == 10:
            interval = frame["d"]["heartbeat_interval"]
            if self.state != "hello" or type(interval) is not int or not 1 <= interval <= 300000:
                raise ValueError()
            self.interval_ms = interval
            if self.session_id is not None:
                self.state = "resuming"
                return {"op": 6, "d": {"token": self._token, "session_id": self.session_id,
                                        "seq": self.sequence}}
            self.state = "identifying"
            return {"op": 2, "d": {"token": self._token, "intents": self._intents,
                    "properties": {"os": "windows", "browser": "ahc", "device": "ahc"}}}
        if self.state not in {"identifying", "resuming", "ready"}:
            raise ValueError()
        if op == 11:
            self.awaiting_ack = False
            return None
        if op == 1:
            return self.heartbeat(requested=True)
        if op in (7, 9):
            if op == 9:
                if type(frame.get("d")) is not bool:
                    raise ValueError()
                if not frame["d"]:
                    self.invalidate_session()
            self.state = "reconnect"
            return {"action": "reconnect"}
        if op != 0:
            raise ValueError()
        sequence, event = frame["s"], frame["t"]
        if (type(sequence) is not int or not 0 <= sequence < 2**63
                or (self.sequence is not None and sequence <= self.sequence)):
            raise ValueError()
        if event == "READY":
            data = frame["d"]
            if (self.state != "identifying" or data["user"]["id"] != self.account_id
                    or data["user"].get("bot") is not True):
                raise ValueError()
            session, url = data["session_id"], data["resume_gateway_url"]
            if type(session) is not str or not 1 <= len(session) <= 256 or type(url) is not str:
                raise ValueError()
            parsed = urlsplit(url)
            if (parsed.scheme != "wss" or not parsed.hostname
                    or not (parsed.hostname == "gateway.discord.gg"
                            or parsed.hostname.endswith(".discord.gg"))
                    or parsed.username or parsed.password or parsed.port
                    or parsed.path not in ("", "/") or parsed.query or parsed.fragment):
                raise ValueError()
            if self._checkpoint is not None:
                self._checkpoint(frame)
            self.session_id, self.resume_url, self.state = session, url, "ready"
        elif event == "RESUMED":
            if self.state != "resuming":
                raise ValueError()
            if self._checkpoint is not None:
                self._checkpoint(frame)
            self.state = "ready"
        elif self.state not in {"ready", "resuming"}:
            raise ValueError()
        else:
            admit(frame)
        self.sequence = sequence
        return None
