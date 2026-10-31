"""Bounded completed answer evidence; tool payloads and deltas are never retained."""

from ahc.domain.sessions import ContractError, _identifier

MAX_FINAL_ITEMS = 128
MAX_FINAL_BYTES = 4 * 1048576


class FinalMessages:
    def __init__(self, thread, turn):
        self.thread, self.turn = thread, turn
        self.items = []
        self.identities = set()
        self.bytes = 0

    def observe(self, event):
        if event.get("method") != "item/completed":
            return
        params = event.get("params")
        if (type(params) is not dict or params.get("threadId") != self.thread
                or params.get("turnId") != self.turn):
            raise ContractError("Codex completed message binding invalid")
        self.add(params.get("item"))

    def add(self, item):
        if (type(item) is not dict or item.get("type") != "agentMessage"
                or item.get("phase") not in (None, "final_answer")
                or item.get("delivery") == "async" or item.get("questions")):
            return
        identity = _identifier(item.get("id"), "Codex item")
        text = item.get("text")
        if (type(text) is not str
                or any(0xD800 <= ord(char) <= 0xDFFF for char in text)):
            raise ContractError("Codex completed message encoding invalid")
        size = len(text.encode("utf-8"))
        if (identity in self.identities or len(self.items) >= MAX_FINAL_ITEMS
                or self.bytes + size > MAX_FINAL_BYTES):
            raise ContractError("Codex completed message duplicate or budget exceeded")
        self.identities.add(identity)
        self.bytes += size
        self.items.append({"type": "agentMessage", "id": identity,
                           "phase": "final_answer", "text": text})
