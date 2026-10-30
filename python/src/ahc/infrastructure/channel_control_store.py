"""Atomic, exact-route operator commands and one-attempt response custody."""

import json
import re
import time

from ahc.domain.channel_commands import ALIASES, help_text
from ahc.domain.channel_ingress import ChannelRoute
from ahc.domain.channel_presentation import message_heading
from ahc.domain.sessions import ContractError, _identifier
from ahc.domain.skill_values import canonical_json, digest_json
from ahc.domain.turns import _input_text

from .channel_capabilities import handle_capabilities
from .channel_schedule_commands import handle_schedule_command
from .runtime_approval_store import RuntimeApprovalStore
from .runtime_channel_store import _exists, _route, _route_json
from .working_db import sqlite_is_busy
from .working_repository import WorkingSetConflict, WorkingWriterBusy


class ChannelSubmissionHeld(WorkingSetConflict):
    """A durable channel control fenced a selected submission before admission."""


def _read_probe(store, operation):
    """Run a read-only control probe, normalizing only transient SQLite busy reads."""
    try:
        with store._read() as connection:
            return operation(connection)
    except WorkingWriterBusy:
        raise
    except Exception as error:
        if sqlite_is_busy(error):
            raise WorkingWriterBusy("working continuity reader is busy") from error
        raise


def restart_pending(connection, session):
    if not _exists(connection, "channel_controls"):
        return False
    rows = connection.execute("SELECT route_json FROM channel_controls WHERE restart=1")
    return any(ChannelRoute.from_dict(json.loads(row[0])).session == session for row in rows)


class ChannelSteeringPort:
    """Durable, exact-turn steering driven only by the Codex frame decoder."""

    def __init__(self, controls, request):
        self.controls, self.request = controls, request
        self._outstanding = {}
        self._expected_turns = {}
        self._finished = {}

    def poll(self, client):
        """Claim and write at most one steer while the decoder is locally idle.

        This method deliberately does not call ``receive``.  A successful write
        remains ``sending`` until the same decoder consumes its exact reply.
        """
        if not getattr(client, 'effects_open', True):
            return
        thread, turn = getattr(client, "thread", None), getattr(client, "turn", None)
        if thread is None or turn is None:
            return
        _identifier(thread, "Codex steer thread")
        _identifier(turn, "Codex steer turn")
        if not self._flush_finished():
            return

        def claim(c):
            row = c.execute(
                "SELECT * FROM channel_steering WHERE target_operation_id=? "
                "AND state='pending' ORDER BY rowid LIMIT 1",
                (self.request.operation_id,),
            ).fetchone()
            if row is None:
                return None
            wire_id = "steer-" + digest_json(row["operation_id"])
            c.execute(
                "UPDATE channel_steering SET state='sending',thread_id=?,expected_turn_id=?,"
                "wire_id=? WHERE operation_id=? AND state='pending'",
                (thread, turn, wire_id, row["operation_id"]),
            )
            return dict(row), wire_id

        try:
            has_pending = _read_probe(
                self.controls.store,
                lambda c: c.execute(
                    "SELECT 1 FROM channel_steering WHERE target_operation_id=? "
                    "AND state='pending' LIMIT 1",
                    (self.request.operation_id,),
                ).fetchone() is not None,
            )
        except WorkingWriterBusy:
            # A transient read contention is deferred to the next idle callback.
            return
        if not has_pending:
            return
        try:
            claimed = self.controls.store._mutate(claim)
        except WorkingWriterBusy:
            # The next ordinary decoder idle callback can retry the durable claim.
            # No provider send has happened and the failed transaction rolled back.
            return
        if claimed is None:
            return
        row, wire_id = claimed
        self._outstanding[wire_id] = row["operation_id"]
        self._expected_turns[wire_id] = turn
        from ahc.adapters.codex_steering import turn_steer_message

        try:
            client._send(turn_steer_message(wire_id, thread, turn, row["input_text"]))
        except Exception:
            self._finish(wire_id, "unknown")
            raise

    def consume_response(self, message):
        """Consume only the correlated steer acknowledgement/rejection frame."""
        if type(message) is not dict:
            return False
        wire_id = message.get("id")
        if wire_id not in self._outstanding:
            return False
        if wire_id in self._finished:
            return True  # The first correlated observation owns this local commit.
        from ahc.adapters.codex_steering import (
            accepted_turn_steer_response,
            rejected_turn_steer_response,
        )

        if accepted_turn_steer_response(message, wire_id, self._expected_turns[wire_id]):
            self._finish(wire_id, "sent", "Codex 已確認收到引導。")
            return True
        if rejected_turn_steer_response(message, wire_id):
            # A correlated JSON-RPC error proves this request was not accepted.
            # The stored text stays recoverable for an explicit queue fallback;
            # it is never replayed by this transport path.
            self._finish(wire_id, "rejected", "Codex 未接受引導；將排入下一個工作。")
            return True
        self._finish(wire_id, "unknown")
        return True

    def reconcile_terminal(self):
        try:
            if self._flush_finished():
                self.controls.reconcile_steering_terminal(self.request.operation_id)
        except WorkingWriterBusy:
            # Preserve durable sending/pending custody for normal orphan recovery.
            # Advisory reconciliation must not replace the model's completed result.
            return

    def _finish(self, wire_id, state, notice=None):
        if wire_id not in self._outstanding:
            return
        self._finished.setdefault(wire_id, (state, notice))
        self._flush_finished()

    def _flush_finished(self):
        """One local commit attempt; never send or replay a wire request.

        At most one new claim is made per poll, and a deferred commit prevents
        another claim. The decoder's frame/deadline budget bounds this state.
        On process loss, the existing durable sending row becomes unknown.
        """
        if not self._finished:
            return True
        completed = dict(self._finished)
        def finish(c):
            for wire_id, (state, notice) in completed.items():
                operation_id = self._outstanding[wire_id]
                row = c.execute(
                    "SELECT * FROM channel_steering WHERE operation_id=? AND wire_id=? "
                    "AND expected_turn_id=? AND target_operation_id=? AND state='sending'",
                    (operation_id, wire_id, self._expected_turns[wire_id],
                     self.request.operation_id),
                ).fetchone()
                if row is None:
                    continue
                c.execute(
                    "UPDATE channel_steering SET state=? WHERE operation_id=? "
                    "AND wire_id=? AND state='sending'",
                    (state, operation_id, wire_id),
                )
                if notice is not None:
                    self.controls._steering_notice(c, row, state, notice)
        try:
            self.controls.store._mutate(finish)
        except WorkingWriterBusy:
            return False
        for wire_id in completed:
            self._outstanding.pop(wire_id)
            self._expected_turns.pop(wire_id)
            self._finished.pop(wire_id)
        return True


class ChannelControlStore:
    def __init__(self, store, config, *, cleanup_status=None):
        if cleanup_status is not None and not callable(cleanup_status):
            raise ContractError("channel cleanup status supplier invalid")
        self.store, self.config = store, config
        self.document = config.to_dict()
        self.options = self.document["channel_control"]
        self.child_requests = {}
        self._cleanup_status = cleanup_status
        self._admission_status = None
        self._background_status = None
        self.routes = {
            ChannelRoute.from_dict(r["route"])
            for r in self.document.get("channels", [])
            if r["enabled"]
        }

        def prepare(c):
            c.execute(
                "CREATE TABLE IF NOT EXISTS channel_controls (operation_id TEXT PRIMARY KEY,"
                "route_json TEXT NOT NULL,input_digest TEXT NOT NULL,reply TEXT NOT NULL,"
                "state TEXT NOT NULL,receipt TEXT,restart INTEGER NOT NULL DEFAULT 0)"
            )
            c.execute(
                "CREATE TABLE IF NOT EXISTS channel_preferences (route_json TEXT PRIMARY KEY,"
                "payload_json TEXT NOT NULL)"
            )
            c.execute("CREATE TABLE IF NOT EXISTS channel_cancel (operation_id TEXT PRIMARY KEY)")
            c.execute("CREATE TABLE IF NOT EXISTS channel_held (operation_id TEXT PRIMARY KEY)")
            c.execute(
                "CREATE TABLE IF NOT EXISTS channel_steering (operation_id TEXT PRIMARY KEY,"
                "route_json TEXT NOT NULL,target_operation_id TEXT NOT NULL,"
                "input_text TEXT NOT NULL,"
                "state TEXT NOT NULL,thread_id TEXT,expected_turn_id TEXT,wire_id TEXT,"
                "session_json TEXT,goal_digest TEXT,event_id TEXT)"
            )
            steering_columns = {row[1] for row in c.execute("PRAGMA table_info(channel_steering)")}
            for column in ("session_json", "goal_digest", "event_id"):
                if column not in steering_columns:
                    c.execute(f"ALTER TABLE channel_steering ADD COLUMN {column} TEXT")
            c.execute("UPDATE channel_controls SET state='unknown' WHERE state='sending'")
            c.execute("UPDATE channel_steering SET state='unknown' WHERE state='sending'")

        store._mutate(prepare)
        store._mutate(self._reconcile_steering_orphans)

    def bind_cleanup_status(self, supplier):
        if not callable(supplier):
            raise ContractError("channel cleanup status supplier invalid")
        self._cleanup_status = supplier

    def bind_admission_status(self, supplier):
        if not callable(supplier):
            raise ContractError("channel admission status supplier invalid")
        self._admission_status = supplier

    def bind_background_status(self, supplier):
        if not callable(supplier):
            raise ContractError("background status supplier invalid")
        self._background_status = supplier

    def admission_observation(self):
        if self._admission_status is None:
            return {"ready": True, "reason": "ready"}
        try:
            value = self._admission_status()
            if type(value) is dict and type(value.get("ready")) is bool:
                return value
        except Exception:
            pass
        return {"ready": False, "reason": "observation-unavailable"}

    @staticmethod
    def _steering_notice_id(operation_id, state):
        return "steering-" + digest_json({"operation": operation_id, "state": state})

    def _steering_notice(self, c, row, state, reply):
        c.execute(
            "INSERT OR IGNORE INTO channel_controls VALUES(?,?,?,?,'pending',NULL,0)",
            (
                self._steering_notice_id(row["operation_id"], state),
                row["route_json"],
                digest_json({"operation": row["operation_id"], "state": state}),
                reply,
            ),
        )

    def _queue_steering(self, c, row):
        """Promote a definitely unsubmitted steer to its original queued turn."""
        from ahc.domain.channel_ingress import ChannelInboundEvent
        from ahc.infrastructure.runtime_channel_store import bind_submission
        from ahc.infrastructure.runtime_inbox import _TABLE, RuntimeInbox

        if any(row[key] is None for key in ("session_json", "goal_digest", "event_id")):
            raise ContractError("channel steer fallback identity unavailable")
        try:
            route = ChannelRoute.from_dict(json.loads(row["route_json"]))
            session = route.session
            if canonical_json(session.to_dict()).decode() != row["session_json"]:
                raise ValueError()
        except (ValueError, TypeError, KeyError, json.JSONDecodeError, ContractError):
            raise ContractError("channel steer fallback route invalid") from None
        current = self.store._load_current(c, session.ref.virtual_session_id)
        if (
            current.session != session
            or current.goal.digest != row["goal_digest"]
            or current.status != "active"
        ):
            raise ContractError("channel steer fallback owner is not current")
        payload = RuntimeInbox._payload(
            session, row["operation_id"], row["goal_digest"], row["input_text"]
        )
        encoded, fingerprint = canonical_json(payload).decode(), digest_json(payload)
        c.execute(_TABLE)
        existing = c.execute(
            "SELECT payload_digest FROM runtime_submissions WHERE operation_id=?",
            (row["operation_id"],),
        ).fetchone()
        if existing is not None:
            if existing["payload_digest"] != fingerprint:
                raise ContractError("channel steer fallback operation changed")
            return
        c.execute(
            "INSERT INTO runtime_submissions VALUES(?,?,?,?,?)",
            (
                row["operation_id"],
                row["session_json"],
                row["goal_digest"],
                encoded,
                fingerprint,
            ),
        )
        bind_submission(
            c,
            ChannelInboundEvent(route, row["event_id"], row["input_text"]),
            existing=False,
        )

    def _target_is_terminal(self, c, target_operation_id):
        if not target_operation_id or not _exists(c, "runtime_turns"):
            return False
        row = c.execute(
            "SELECT status FROM runtime_turns WHERE operation_id=?", (target_operation_id,)
        ).fetchone()
        return row is not None and row["status"] == "terminal"

    def _reconcile_steering_rows(self, c, *, target_operation_id=None, stored_terminal=False):
        if not _exists(c, "channel_steering"):
            return
        query = (
            "SELECT * FROM channel_steering "
            "WHERE state IN ('pending','sending','rejected','unknown')"
        )
        args = ()
        if target_operation_id is not None:
            query += " AND target_operation_id=?"
            args = (target_operation_id,)
        for row in c.execute(query, args).fetchall():
            row = dict(row)
            state = row["state"]
            if stored_terminal and not self._target_is_terminal(c, row["target_operation_id"]):
                continue
            if state == "sending":
                c.execute(
                    "UPDATE channel_steering SET state='unknown' "
                    "WHERE operation_id=? AND state='sending'",
                    (row["operation_id"],),
                )
                self._steering_notice(
                    c, row, "unknown", "引導結果未確認；內容已保留，不會自動重送。"
                )
                continue
            if state == "unknown":
                self._steering_notice(
                    c, row, "unknown", "引導結果未確認；內容已保留，不會自動重送。"
                )
                continue
            if not row["target_operation_id"]:
                continue
            try:
                self._queue_steering(c, row)
            except ContractError:
                c.execute("UPDATE channel_steering SET state='held' WHERE operation_id=?",
                          (row["operation_id"],))
                self._steering_notice(c, row, "held",
                                      "引導尚未執行；原工作歸屬已變更，內容保留暫停。")
                continue
            c.execute(
                "UPDATE channel_steering SET state='queued' WHERE operation_id=? AND state=?",
                (row["operation_id"], state),
            )
            self._steering_notice(
                c, row, "queued", "目前回合已結束；內容已排入下一個工作。"
            )

    def _reconcile_steering_orphans(self, c):
        self._reconcile_steering_rows(c, stored_terminal=True)

    def reconcile_steering_terminal(self, operation_id):
        _identifier(operation_id, "channel steer target operation")
        if not _read_probe(self.store, lambda c: c.execute(
            "SELECT 1 FROM channel_steering WHERE target_operation_id=? "
            "AND state IN ('pending','sending','rejected','unknown') LIMIT 1",
            (operation_id,),
        ).fetchone() is not None):
            return
        self.store._mutate(
            lambda c: self._reconcile_steering_rows(c, target_operation_id=operation_id)
        )

    def _cleanup_observation(self):
        if self._cleanup_status is None:
            return "觀測不可用"
        try:
            value = self._cleanup_status()
        except Exception:
            return "觀測不可用"
        if (
            type(value) is not dict
            or type(value.get("automatic_attempts")) is not int
            or value.get("automatic_attempts") not in (0, 1, 2)
            or value.get("automatic_attempt_limit") != 2
            or value.get("observation_scope") != "current-runtime"
        ):
            return "觀測不可用"
        state = value.get("state")
        if state == "clear":
            return "無待處理資源"
        if state == "pending":
            return f"待清理（自動 {value['automatic_attempts']}/2）"
        if state == "exhausted":
            return "自動清理已停止；需明確生命週期清理"
        return "觀測不可用"

    def _preferences(self, c, route):
        row = c.execute(
            "SELECT payload_json FROM channel_preferences WHERE route_json=?", (_route_json(route),)
        ).fetchone()
        default = {
            "model": self.config.backend["model"],
            "effort": self.config.backend.get("reasoning_effort"),
            "fast": self.config.backend.get("service_tier") == "priority",
            "approval": self.config.effective_policy.approval_mode,
            "previous_approval": "manual",
            "messages": "queue",
            "history_after": 0,
            "revision": 0,
        }
        return default if row is None else json.loads(row[0])

    def submit(self, c, event, fingerprint, *, session=None, goal_digest=None):
        if event.route not in self.routes:
            raise ContractError("channel control route is not enabled")
        route_json = _route_json(event.route)
        old = c.execute(
            "SELECT * FROM channel_controls WHERE operation_id=?", (event.operation_id,)
        ).fetchone()
        if old:
            if old["route_json"] != route_json or old["input_digest"] != fingerprint:
                raise ContractError("channel command identity changed")
            row = c.execute(
                "SELECT input_text FROM channel_steering WHERE operation_id=?",
                (event.operation_id,),
            ).fetchone()
            result = {"operation_id": event.operation_id, "submission_digest": fingerprint}
            if row is not None and old["reply"].startswith("目前沒有執行中的工作"):
                result["queue_input_text"] = row["input_text"]
            return result
        preferences = self._preferences(c, event.route)
        context = event.context or {}
        if context.get("notice"):
            return self._record_control(c, event, fingerprint, preferences,
                                        context["notice"], False)
        from ahc.domain.channel_context import needs_queued_context

        # A text-only steer cannot carry the operation-bound quote or imported images.
        # Keep the complete message together and explicitly queue it instead.
        if needs_queued_context(context) and (
                not event.input_text.strip().startswith("/")
                or context.get("forwarded") is True
                or event.input_text.strip().lower().startswith("/steer ")):
            text = event.input_text
            if text.strip().lower().startswith("/steer ") and context.get("forwarded") is not True:
                text = text.strip().partition(" ")[2]
            if self._active(c, event.route) is not None:
                return self._record_control(
                    c, event, fingerprint, preferences,
                    "這則訊息含引用或附件，已完整排入下一個工作；不會拆成文字引導。", False
                ) | {"queue_input_text": text}
            return {"queue_input_text": text}
        raw = event.input_text.strip()
        if not raw.startswith("/"):
            return self._ordinary(c, event, fingerprint, preferences)
        for helper in (handle_schedule_command, handle_capabilities):
            reply = helper(self, c, event.route, raw)
            if reply is not None:
                return self._record_control(c, event, fingerprint, preferences, reply, False)
        command, separator, remainder = raw.partition(" ")
        command = command.lower()
        if command == "/steer":
            return self._steer(c, event, fingerprint, preferences, remainder if separator else "")
        args = remainder.split() if separator else []
        if not re.fullmatch(r"/[a-z][a-z0-9_]*", command) or len(args) > 1:
            reply, restart = "指令格式錯誤。請輸入 /help。", False
        else:
            reply, restart = self._command(
                c, event.route, preferences, command, args[0] if args else None
            )
        return self._record_control(c, event, fingerprint, preferences, reply, restart)

    def _record_control(self, c, event, fingerprint, preferences, reply, restart):
        c.execute(
            "INSERT INTO channel_preferences VALUES(?,?) ON CONFLICT(route_json) "
            "DO UPDATE SET payload_json=excluded.payload_json",
            (_route_json(event.route), canonical_json(preferences).decode()),
        )
        c.execute(
            "INSERT INTO channel_controls VALUES(?,?,?,?,'pending',NULL,?)",
            (event.operation_id, _route_json(event.route), fingerprint, reply, int(restart)),
        )
        return {"operation_id": event.operation_id, "submission_digest": fingerprint}

    def _ordinary(self, c, event, fingerprint, preferences):
        active = self._active(c, event.route)
        from .channel_presentation import operation_kind

        if active is not None and operation_kind(c, active) == "scheduled":
            return self._record_control(
                c, event, fingerprint, preferences,
                message_heading("reminder") +
                "\n\n定時任務正在執行；你的訊息已保留給下一個對話回合。", False
            ) | {"queue_input_text": event.input_text}
        if preferences.get("messages", "queue") == "steer" and active is not None:
            self._insert_steering(c, event, active, event.input_text)
            return self._record_control(
                c, event, fingerprint, preferences, "已收到引導要求，等待 Codex 確認。", False
            )
        if active is not None:
            return self._record_control(
                c, event, fingerprint, preferences, "已排入下一個工作。", False
            ) | {"queue_input_text": event.input_text}
        # Preserve idle queue delivery ordering.  No control reply is needed
        # before a normal idle message begins its next turn.
        return {"queue_input_text": event.input_text}

    def _steer(self, c, event, fingerprint, preferences, text):
        try:
            _input_text(text, "channel steer text")
        except ContractError:
            return self._record_control(
                c, event, fingerprint, preferences, "請在 /steer 後提供內容；未執行。", False
            )
        active = self._active(c, event.route)
        self._insert_steering(c, event, active or "", text, state="pending" if active else "queued")
        if active is None:
            return self._record_control(
                c,
                event,
                fingerprint,
                preferences,
                "目前沒有執行中的工作；已排入下一個工作。",
                False,
            ) | {"queue_input_text": text}
        return self._record_control(
            c, event, fingerprint, preferences, "已收到引導要求，等待 Codex 確認。", False
        )

    def _insert_steering(self, c, event, target_operation_id, text, *, state="pending"):
        _input_text(text, "channel steer text")
        if state not in ("pending", "queued"):
            raise ContractError("channel steer state invalid")
        existing = c.execute(
            "SELECT * FROM channel_steering WHERE operation_id=?", (event.operation_id,)
        ).fetchone()
        if existing is not None:
            if (
                existing["route_json"] != _route_json(event.route)
                or existing["target_operation_id"] != target_operation_id
                or existing["input_text"] != text
            ):
                raise ContractError("channel steer identity changed")
            return
        current = self.store._load_current(c, event.route.session.ref.virtual_session_id)
        if current.session != event.route.session or current.status != "active":
            raise ContractError("channel steer route is not current and active")
        c.execute(
            "INSERT INTO channel_steering VALUES(?,?,?,?, ?,NULL,NULL,NULL,?,?,?)",
            (
                event.operation_id,
                _route_json(event.route),
                target_operation_id,
                text,
                state,
                canonical_json(event.route.session.to_dict()).decode(),
                current.goal.digest,
                event.event_id,
            ),
        )

    def steering_port(self, request):
        from ahc.domain.agent_backend import AgentTurnRequest

        if type(request) is not AgentTurnRequest:
            raise ContractError("channel steering request invalid")
        return ChannelSteeringPort(self, request)

    def _active(self, c, route):
        current = self.store._load_current(c, route.session.ref.virtual_session_id)
        if current.active_turn_id is None:
            return None
        row = c.execute(
            "SELECT * FROM runtime_channel_routes WHERE operation_id=?", (current.active_turn_id,)
        ).fetchone()
        return current.active_turn_id if row and _route(row) == route else None

    def _command(self, c, route, p, command, arg):
        command = "/" + ALIASES.get(command.lstrip("/"), command.lstrip("/"))
        notice = ""
        models = self.options["models"]
        if command in ("/help", "/commands", "/start") and arg is None:
            return help_text(), False
        if command == "/model":
            if arg is None:
                return f"模型：{p['model']} / {p['effort']}\n可選：" + ", ".join(models), False
            model = self.options["aliases"].get(arg, arg)
            if model not in models:
                return "模型不在設定檔允許清單中；未變更。", False
            if p["effort"] and p["effort"] not in models[model]["efforts"]:
                return "目前推理強度不受此模型支援，請先 /think 切換；未變更。", False
            if p["fast"] and not models[model]["priority"]:
                return "此模型不支援 priority，請先 /fast off；未變更。", False
            p["model"] = model
        elif command in ("/think", "/reasoning"):
            if arg is None:
                return (
                    f"推理：{p['effort']}\n可選：" + ", ".join(models[p["model"]]["efforts"])
                ), False
            if arg not in models[p["model"]]["efforts"]:
                return "此模型不支援該推理強度；未變更。", False
            p["effort"] = arg
        elif command == "/fast":
            if arg is None:
                return f"Priority：{'on' if p['fast'] else 'off'}；使用 /fast on 或 off。", False
            if arg not in ("on", "off") or (arg == "on" and not models[p["model"]]["priority"]):
                return "Priority 參數錯誤或此模型不支援；未變更。", False
            p["fast"] = arg == "on"
        elif command == "/messages":
            if arg is None:
                return (
                    f"訊息模式：{p.get('messages', 'queue')}；"
                    "使用 /messages queue 或 steer。",
                    False,
                )
            if arg not in ("queue", "steer"):
                return "訊息模式僅支援 queue 或 steer；未變更。", False
            p["messages"] = arg
        elif command in ("/approvals", "/approval", "/yolo"):
            if command == "/yolo":
                if arg not in (None, "on", "off"):
                    return "使用 /yolo on 或 off。", False
                turn_on = p["approval"] != "yolo" if arg is None else arg == "on"
                if turn_on and p["approval"] != "yolo":
                    p["previous_approval"] = p["approval"]
                arg = "yolo" if turn_on else p["previous_approval"]
            if arg is None:
                return (
                    f"核准：{p['approval']}\nmanual：要求核准；"
                    "auto-approve：Codex 原生自動審查，"
                    "AHC 非唯讀工具仍詢問；always-approve：自動同意；yolo：不詢問。"
                    "檔案／網路權限另由 sandbox 設定。"
                ), False
            if arg not in ("manual", "auto-approve", "always-approve", "yolo"):
                return "不支援此核准模式；未變更。", False
            if arg == "yolo" and p["approval"] != "yolo":
                p["previous_approval"] = p["approval"]
            p["approval"] = arg
            if command == "/yolo":
                notice = ("YOLO 已開啟。\n" if arg == "yolo" else
                          f"YOLO 已關閉，已恢復核准模式：{arg}。\n")
        elif command in ("/approve", "/deny"):
            return self._approval(c, route, arg, command == "/approve"), False
        elif command == "/stop" and arg is None:
            active = self._active(c, route)
            if active:
                c.execute("INSERT OR IGNORE INTO channel_cancel VALUES(?)", (active,))
            return (
                "已提出取消；執行結果仍待對帳。" if active else "本聊天室沒有執行中的工作。"
            ), False
        elif command == "/new" and arg is None:
            if self._active(c, route):
                return "目前仍有工作執行中；請等結束或先 /stop，再 /new。", False
            c.execute("UPDATE channel_steering SET state='held' WHERE route_json=? "
                      "AND state IN ('pending','rejected')", (_route_json(route),))
            p["history_after"] = c.execute(
                "SELECT COALESCE(MAX(rowid),0) FROM runtime_submissions"
            ).fetchone()[0]
            from .telegram_album_inbox import hold_albums

            hold_albums(c, route)
            if _exists(c, "runtime_channel_file_inbox"):
                c.execute(
                    "INSERT OR IGNORE INTO channel_held SELECT f.operation_id FROM "
                    "runtime_channel_file_inbox f WHERE f.route_json=? AND NOT EXISTS "
                    "(SELECT 1 FROM runtime_turns t WHERE t.operation_id=f.operation_id)",
                    (_route_json(route),),
                )
            c.execute(
                "INSERT OR IGNORE INTO channel_held SELECT s.operation_id FROM "
                "runtime_submissions s JOIN runtime_channel_routes r USING(operation_id) "
                "WHERE r.route_json=? AND NOT EXISTS (SELECT 1 FROM runtime_turns t "
                "WHERE t.operation_id=s.operation_id)"
                + (" AND NOT EXISTS (SELECT 1 FROM runtime_schedule_occurrences o "
                   "WHERE o.operation_id=s.operation_id)"
                   if _exists(c, "runtime_schedule_occurrences") else ""),
                (_route_json(route),),
            )
            p["revision"] += 1
            return "已開啟新的對話脈絡；舊待辦已保留不重播，長期記憶與 Python 變數保留。", False
        elif command == "/restart" and arg is None:
            if restart_pending(c, route.session):
                return "此 AHC 服務已排定重啟，等待當前工作結束及回覆送出。", False
            return "已排定此 AHC 服務重啟；先等待當前工作結束，不重啟 WSL。", True
        elif command == "/status" and arg is None:
            active = self._active(c, route)
            admission = self.admission_observation()
            readiness = "可接受工作" if admission["ready"] else "暫時無法開始新工作；訊息保留"
            background = ""
            if self._background_status is not None:
                observed = self._background_status()
                background = (f"背景排程：執行 {observed.get('running', 0)}"
                    f" · 等待 {observed.get('pending', 0)}"
                    f" · 待核對 {observed.get('unresolved', 0)}\n")
            return (
                f"AHC · {route.session.agent_id}\n模型：{p['model']} / {p['effort']}\n"
                f"Priority：{p['fast']} · 核准：{p['approval']}\n"
                f"執行中：{active or '無'}\n清理：{self._cleanup_observation()}\n"
                f"可用性：{readiness}\n"
                + background +
                f"對話：{route.session.ref.virtual_session_id}"
                f" · generation {route.session.ref.generation}\n"
                f"設定版本：{p['revision']}；下一個工作生效。\n/stop 保留脈絡；/new 才重設。"
            ), False
        else:
            return "未知指令或參數；未執行，也未交給模型。請輸入 /help。", False
        p["revision"] += 1
        return (
            notice +
            f"已更新本聊天室，下一個工作生效：{p['model']} / {p['effort']}\n"
            f"Priority：{p['fast']} · 核准：{p['approval']}"
        ), False

    def _approval(self, c, route, arg, approve):
        active = self._active(c, route)
        if (
            active is not None
            and c.execute("SELECT 1 FROM channel_cancel WHERE operation_id=?", (active,)).fetchone()
        ):
            return "此工作已提出停止，核准項目已失效；未執行。"
        approvals = RuntimeApprovalStore(self.store)
        rows = (
            c.execute(
                "SELECT a.* FROM runtime_approvals a JOIN runtime_channel_routes r "
                "USING(operation_id) WHERE r.route_json=? AND a.state='pending'",
                (_route_json(route),),
            ).fetchall()
            if approvals._exists(c)
            else []
        )
        candidates = [
            r
            for r in rows
            if (arg is None or r["request_digest"] == arg)
            and approvals._decode(r).expires_at > time.time()
            and r["operation_id"] == self._active(c, route)
        ]
        children = []
        for digest, (pending, parent, child_store) in list(self.child_requests.items()):
            if pending.expires_at <= time.time():
                self.child_requests.pop(digest, None)
                continue
            if (
                (arg is None or digest == arg)
                and pending.session == route.session
                and parent == self._active(c, route)
            ):
                try:
                    child_store._current(c)
                    row, _ = child_store._read(c, route.session, digest)
                except ContractError:
                    self.child_requests.pop(digest, None)
                    continue
                if row["state"] == "pending":
                    children.append((pending, child_store))
        if len(candidates) + len(children) != 1:
            return "沒有唯一且仍有效的待核准操作；請帶上完整核准 ID。"
        request, responder = (
            children[0] if children else (approvals._decode(candidates[0]), approvals)
        )
        responder.respond(
            route.session, request.digest, "approve-once" if approve else "deny", _transaction=c
        )
        return "已核准這一次操作。" if approve else "已拒絕這一次操作。"

    def preferences_for(self, operation_id):
        with self.store._read() as c:
            row = c.execute(
                "SELECT * FROM runtime_channel_routes WHERE operation_id=?", (operation_id,)
            ).fetchone()
            return None if row is None else self._preferences(c, _route(row))

    def cancelled(self, operation_id):
        with self.store._read() as c:
            return (
                c.execute(
                    "SELECT 1 FROM channel_cancel WHERE operation_id=?", (operation_id,)
                ).fetchone()
                is not None
            )

    def restart_ready(self):
        def change(c):
            row = c.execute(
                "SELECT operation_id FROM channel_controls WHERE restart=1 "
                "AND state IN ('sent','unknown') ORDER BY rowid LIMIT 1"
            ).fetchone()
            if row:
                c.execute("UPDATE channel_controls SET restart=2 WHERE restart=1")
            return row is not None

        if not _read_probe(
            self.store,
            lambda c: c.execute(
                "SELECT 1 FROM channel_controls WHERE restart=1 "
                "AND state IN ('sent','unknown') LIMIT 1"
            ).fetchone() is not None,
        ):
            return False
        if self._background_status is not None:
            observed = getattr(self, "prepare_background_restart", self._background_status)()
            if (observed.get("running", 0) or observed.get("delivery_pending", 0)
                    or observed.get("state") in {"observation-unavailable", "close-unverified"}):
                return False
        return self.store._mutate(change)

    def notify_approval(self, request, *, parent_operation=None, child_store=None):
        def write(c):
            row = c.execute(
                "SELECT * FROM runtime_channel_routes WHERE operation_id=?",
                (parent_operation or request.operation_id,),
            ).fetchone()
            if not row or _route(row) not in self.routes:
                return
            if child_store is not None:
                if child_store.store is not self.store or parent_operation is None:
                    raise ContractError("child notification store mismatch")
                child_store._current(c)
                self.child_requests = {
                    k: v for k, v in self.child_requests.items() if v[0].expires_at > time.time()
                }
                if len(self.child_requests) >= 128:
                    raise ContractError("child channel approval notification budget exceeded")
                self.child_requests[request.digest] = (request, parent_operation, child_store)
            from ahc.adapters.channel_control_ui import approval_text

            text = approval_text(request)
            c.execute(
                "INSERT OR IGNORE INTO channel_controls VALUES(?,?,?,?,'pending',NULL,0)",
                ("approval-" + request.digest, row["route_json"], request.digest, text),
            )

        self.store._mutate(write)

    def deliver_one(self, transports):
        if self._admission_status is not None:
            from .channel_availability import queue_unavailable_notices
            queue_unavailable_notices(self, self.admission_observation())
        def claim(c):
            self._reconcile_steering_orphans(c)
            row = c.execute(
                "SELECT * FROM channel_controls WHERE state='pending' ORDER BY rowid LIMIT 1"
            ).fetchone()
            if row:
                route = ChannelRoute.from_dict(json.loads(row["route_json"]))
                if route not in transports:
                    raise ContractError("control delivery route unavailable")
                c.execute(
                    "UPDATE channel_controls SET state='sending' WHERE operation_id=?",
                    (row["operation_id"],),
                )
                return dict(row), route

        claimed = self.store._mutate(claim)
        if claimed is None:
            return
        row, route = claimed
        state, receipt = "unknown", None
        try:
            transport = transports[route]
            delivery_id = "control-" + digest_json(row["operation_id"])
            if row["operation_id"] == "approval-" + row["input_digest"] and hasattr(
                transport, "send_approval"
            ):
                receipt = transport.send_approval(
                    route, {"output_text": row["reply"]}, delivery_id, row["input_digest"]
                )
            else:
                receipt = transport.send(route, {"output_text": row["reply"]}, delivery_id)
            state = "sent"
        except Exception:
            pass  # The provider may have accepted it. Never replay an uncertain send.
        self.store._mutate(
            lambda c: c.execute(
                "UPDATE channel_controls SET state=?,receipt=? "
                "WHERE operation_id=? AND state='sending'",
                (state, receipt, row["operation_id"]),
            )
        )
