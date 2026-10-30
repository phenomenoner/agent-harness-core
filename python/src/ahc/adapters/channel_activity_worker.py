"""One bounded activity card, independent of the platform typing refresh thread."""

import threading
import time

from ahc.domain.channel_presentation import message_heading
from ahc.domain.sessions import ContractError


class ChannelActivityWorker:
    def __init__(self, typing, transport, can_send, reserve, record, operation_id,
                 *, clock=time.monotonic, interval=5, update_limit=180,
                 can_finalize=None, stop_requested=lambda: False, edit_retry_delays=(5, 15)):
        if (type(edit_retry_delays) is not tuple or len(edit_retry_delays) > 2
                or any(type(delay) is not int or not 0 <= delay <= 300 for delay in edit_retry_delays)):
            raise ContractError("activity retry budget invalid")
        self.edit_retry_delays, self._edit_retries = edit_retry_delays, 0
        self._last_edit_error = None
        self.typing, self.transport, self.can_send = typing, transport, can_send
        self.reserve, self.record, self.operation_id = reserve, record, operation_id
        self.clock, self.interval, self.update_limit = clock, interval, update_limit
        self.can_finalize = can_finalize or can_send
        self.stop_requested = stop_requested
        self._stop, self._lock = threading.Event(), threading.Lock()
        self._thread = None
        self._started = None
        self._phase, self._commentary = "準備中", ""
        self._comments = []
        self._initial_text = None
        self._accepted = False
        self._state, self._updates = "new", 0

    def status(self):
        with self._lock:
            return {"state": self._state, "updates": self._updates,
                    "update_limit": self.update_limit,
                    "edit_retries": self._edit_retries,
                    "last_edit_error": self._last_edit_error,
                    "elapsed_seconds": (0 if self._started is None else
                                        max(0, int(self.clock() - self._started))),
                    "typing": self.typing.status() if self.typing else None}

    def observe(self, phase, commentary=None):
        if phase not in {"準備中", "模型處理中", "工具執行中", "整理回覆中", "等待核准"}:
            return
        with self._lock:
            self._phase = phase
            if (type(commentary) is str and commentary.strip()
                    and not any(0xD800 <= ord(c) <= 0xDFFF for c in commentary)):
                self._commentary = commentary[:800]
                preview = " ".join(commentary.split())[:300]
                if not self._comments or self._comments[-1] != preview:
                    self._comments = (self._comments + [preview])[-3:]

    def finish(self, accepted):
        self._accepted = accepted is True

    def _text(self, *, final=False):
        elapsed = max(0, int(self.clock() - self._started))
        with self._lock:
            stopped = self.stop_requested()
            label = (("⏸ 已提出停止" if stopped else
                      "✅ 回覆已產生（任務結果見下方）" if self._accepted else "⚠ 任務未完成")
                     if final else "⏳ Working")
            text = (f"{message_heading('progress')}\n\n"
                    f"**{label}** · `{elapsed // 60:02d}:{elapsed % 60:02d}`")
            if not final:
                text += f"\n{self._phase}"
                if self._phase == "等待核准":
                    text += " · 請操作核准卡，或用 /approve、/deny"
            if stopped:
                text += "\n保留任務脈絡；未確認的執行結果仍待對帳。"
            if self._comments:
                text += "\n\n**中途說明**\n" + "\n".join(
                    "› " + value.replace("\n", "\n  ") for value in self._comments)
            if not final:
                text += "\n\n/stop 停止本次工作 · /status 查看狀態"
            return text

    def start(self):
        if self._started is not None:
            raise ContractError("activity worker already consumed")
        self._started = self.clock()
        if self.typing:
            self.typing.start()
        try:
            self._initial_text = self._text()
            reserved = self.reserve(self._initial_text)
        except Exception:
            reserved = False  # Optional UI cannot suppress the actual turn.
        if not reserved:
            self._state = "unavailable"
            return
        self._state = "running"
        self._thread = threading.Thread(target=self._run, name="ahc-activity", daemon=True)
        self._thread.start()

    def _run(self):
        try:
            receipt = None
            try:
                if self.can_send() and not self._stop.is_set():
                    receipt = self.transport.create(self._initial_text, self.operation_id)
            finally:
                self.record(receipt)  # Unknown create is recorded once, never replayed.
            if receipt is None:
                self._state = "unavailable"
                return
            while not self._stop.wait(self.interval):
                if not self.can_send() or self._updates >= self.update_limit - 1:
                    break
                if not self._edit(self._text()):
                    self._state = "unavailable"
                    return
            # Stopping work revokes ongoing activity, not the one exact-card closing
            # presentation. Replacement still revokes that edit through can_finalize.
            if self.can_finalize() and self._updates < self.update_limit:
                text = (self._text(final=True) if self._stop.is_set() or self.stop_requested() else
                        self._text() + "\n狀態更新已暫停")
                if not self._edit(text, final=True):
                    self._state = "unavailable"
                    return
            self._state = "closed"
        except Exception:
            self._state = "unavailable"  # No create fallback or blind edit retry.

    def _edit(self, text, *, final=False):
        # Only the acknowledged exact card is edited. Create is never replayed.
        # The retry budget belongs to this card's lifetime, not to each loop.
        while self._updates < self.update_limit - (0 if final else 1):
            self._updates += 1
            try:
                self.transport.update(text)
                return True
            except (TimeoutError, OSError) as error:
                self._last_edit_error = type(error).__name__
                if self._edit_retries >= len(self.edit_retry_delays) or self._stop.is_set():
                    return False
                delay = self.edit_retry_delays[self._edit_retries]
                self._edit_retries += 1
                if self._stop.wait(delay):
                    return False
                if not (self.can_finalize() if final else self.can_send()):
                    return False
            except Exception as error:
                self._last_edit_error = type(error).__name__
                return False  # Invalid ownership/response never receives a retry.
        return False

    def close(self):
        self._stop.set()
        if self.typing:
            self.typing.close()
        if self._thread:
            self._thread.join(15)
            if self._thread.is_alive():
                raise ContractError("activity worker shutdown unverified")
