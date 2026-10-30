"""Route-bound outbox rows written only within existing runtime transactions."""

import json

from ahc.domain.channel_ingress import ChannelInboundEvent, ChannelRoute, channel_operation_id
from ahc.domain.runtime_turn import (
    CLEANUP_NOTICE,
    RUNTIME_RESULT_SCHEMA_VERSION,
    answer_seal_projection,
)
from ahc.domain.sessions import ContractError
from ahc.domain.skill_values import canonical_json
from ahc.infrastructure.working_repository import WorkingSetCorrupt


def _exists(connection, name):
    return (
        connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)
        ).fetchone()
        is not None
    )


def _route_json(route):
    if type(route) is not ChannelRoute:
        raise ContractError("channel route invalid")
    return canonical_json(route.to_dict()).decode()


def _route(row):
    try:
        route = ChannelRoute.from_dict(json.loads(row["route_json"]))
        if (
            row["route_json"] != _route_json(route)
            or channel_operation_id(route, row["event_id"]) != row["operation_id"]
        ):
            raise ValueError()
        return route
    except (KeyError, TypeError, ValueError, json.JSONDecodeError, ContractError):
        raise WorkingSetCorrupt("channel route binding corrupted") from None


def bind_submission(connection, event, *, existing):
    """Bind a newly persisted inbox event; never attach a route retrospectively."""
    if type(event) is not ChannelInboundEvent or type(existing) is not bool:
        raise ContractError("channel submission binding invalid")
    if not _exists(connection, "runtime_channel_routes"):
        raise ContractError("channel route storage unavailable")
    operation_id, encoded = event.operation_id, _route_json(event.route)
    row = connection.execute(
        "SELECT * FROM runtime_channel_routes WHERE operation_id=?", (operation_id,)
    ).fetchone()
    if existing and row is None:
        raise ContractError("existing submission has no channel route")
    if row is not None:
        route = _route(row)
        if route != event.route or row["event_id"] != event.event_id:
            raise ContractError("channel operation already bound to another route")
        return
    if existing:
        raise ContractError("existing submission has no channel route")
    connection.execute(
        "INSERT INTO runtime_channel_routes VALUES(?,?,?)", (operation_id, encoded, event.event_id)
    )


def _output(result):
    try:
        output = result["output_text"]
    except (KeyError, TypeError):
        raise WorkingSetCorrupt("channel final result corrupted") from None
    if (
        type(result) is not dict
        or not (result.get("accepted") is True or result.get("report_accepted") is True)
        or type(output) is not str
        or any(0xD800 <= ord(char) <= 0xDFFF for char in output)
    ):
        raise WorkingSetCorrupt("channel final result corrupted")
    schema_version = result.get("result_schema_version")
    if schema_version is not None and schema_version != RUNTIME_RESULT_SCHEMA_VERSION:
        raise WorkingSetCorrupt("channel final result schema unsupported")
    if schema_version == RUNTIME_RESULT_SCHEMA_VERSION:
        completion = result.get("backend_completion")
        if (
            (result.get("report_accepted") is not True and (
                result.get("disposition") != "completed"
                or result.get("unresolved_effects") is not False))
            or type(completion) is not dict
            or set(completion) != {"thread", "turn"}
        ):
            raise WorkingSetCorrupt("channel final result is not deliverable")
        if result.get("report_accepted") is True:
            try:
                answer_seal_projection(result)
            except ContractError:
                raise WorkingSetCorrupt("channel problem report is not deliverable") from None
        if result.get("cleanup_pending") is True:
            output += ("\n\n補充：最終回報已保存，資源清理仍在處理。"
                       if result.get("report_accepted") is True else CLEANUP_NOTICE)
    from ahc.domain.channel_reply import reply_message_id

    reply = result.get("reply_to_message_id")
    return {"output_text": output, **({"reply_to_message_id": reply_message_id(reply)}
                                     if reply is not None else {})}


def _previous_failure_output(result, *, legacy=False):
    """Project a terminal failure notice, never an assistant answer or raw diagnostic."""
    if (type(result) is not dict or result.get("accepted") is not False
            or result.get("disposition") not in {"unknown", "not-submitted", "unsupported"}
            or result.get("output_text") != ""
            or type(result.get("unresolved_effects")) is not bool
            or result.get("result_schema_version") not in {None, RUNTIME_RESULT_SCHEMA_VERSION}
            or result.get("artifacts")):
        raise WorkingSetCorrupt("channel failure result corrupted")
    text = ("⚠ 任務未完成\n這回合沒有取得可確認的最終回覆，不能視為任務已完成。"
            if legacy else "這次未能取得模型的最終回報，因此先停止這回合。"
            "任務進度與操作紀錄已保留。")
    diagnostic = result.get("backend_diagnostic", {})
    if (type(diagnostic) is dict and diagnostic.get("category") == "workspace"
            and diagnostic.get("code") == "workspace-effects-unresolved"):
        from ahc.domain.workspace import workspace_failure_projection

        failure = workspace_failure_projection(diagnostic.get("workspace_failure"))
        label = {"send": "工作區請求傳送", "receive": "工作區結果接收",
                 "result-validation": "工作區回報核對", "result-save": "工作區結果保存",
                 "execute": "工作區程式執行"}.get(failure.get("phase"))
        if label:
            text += "\n中斷環節：" + label + "。"
            if failure["phase"] == "result-save":
                text += "程式已回報，但狀態保存未確認；不會重跑程式。"
        else:
            text += "\n中斷環節：工作區工具執行；這一步的結果尚未確認。"
        cause = {"writer-busy": "狀態庫暫時忙碌", "owner-conflict": "任務歸屬已變更",
                 "eof": "工作區通道提前關閉", "deadline": "等待工作區回報逾時",
                 "malformed-json": "工作區回報無法解讀", "protocol-frame": "工作區回報無法解讀",
                 "reader-io": "讀取工作區回報失敗", "io": "工作區讀寫失敗"}.get(failure.get("code"))
        if cause:
            text += "\n原因：" + cause + "。"
    if result["unresolved_effects"] or result["disposition"] == "unknown":
        text += "\n部分操作可能已執行，結果尚未確認；不會自動重跑。"
    else:
        text += "\n後端未完成這回合，沒有可交付的結果。"
    # Describe durable observations separately from the unresolved task/effect.
    # No model text, filenames or diagnostic payload are promoted to an answer.
    confirmed = result.get("confirmed_work")
    if type(confirmed) is dict:
        workspace = confirmed.get("workspace")
        if type(workspace) is dict and type(workspace.get("revision")) is int:
            text += f"\n已確認回報的工作區步驟：第 {workspace['revision']} 步。"
        if type(confirmed.get("checkpoint")) is dict:
            text += "\n已確認保存任務檢查點，可據此核對並接續。"
        artifacts = confirmed.get("artifacts")
        if type(artifacts) is list and artifacts:
            text += f"\n已確認保存 {len(artifacts)} 份本地附件；尚不代表已送達。"
    text += ("\n任務脈絡已保留；可用 /status 查看狀態，或要求先核對目前進度。"
              if legacy else "\n建議下一步：先核對已保存的進度與原操作結果，再接續未完成部分；"
              "可用 /status 查看狀態。")
    return {"output_text": text}


def _failure_output(result, *, legacy=False, provider_details=True, protocol_details=True,
                    transport_details=True):
    """Current presentation; predecessor wording remains independently readable."""
    if legacy:
        return _previous_failure_output(result, legacy=True)
    payload = _previous_failure_output(result)
    text = "⚠ 任務未完成\n" + payload["output_text"]
    confirmed = result.get("confirmed_work")
    workspace = confirmed.get("workspace") if type(confirmed) is dict else None
    if type(workspace) is dict and type(workspace.get("revision")) is int:
        revision = workspace["revision"]
        text = text.replace(f"已確認回報的工作區步驟：第 {revision} 步。",
                            f"已保存工作區版本：{revision}；版本號不代表完成的任務步數。")
    from ahc.domain.backend_diagnostics import diagnostic_projection

    diagnostic = diagnostic_projection(result.get("backend_diagnostic"))
    diagnostic = diagnostic.get("backend_diagnostic", {})
    cause = {
        "contract-invalid": "Codex 回報的通訊契約核對失敗；操作結果仍待確認。",
        "native-thread-unbound": "原生子代理事件尚未正確綁定。",
        "backend-startup-timeout": "Codex 啟動等待逾時。",
        "control-request-timeout": "Codex 控制請求等待逾時。",
        "terminal-settlement-timeout": "最終結果核對等待逾時。",
        "deadline": "本回合執行預算已到。",
    }.get(diagnostic.get("code"))
    if not protocol_details and diagnostic.get("code") == "contract-invalid":
        cause = None  # Exact predecessor presentation, never a new task result.
    transport_cause = None
    if transport_details and diagnostic.get("category") == "transport":
        transport_cause = {
            "protocol-frame": "Codex 通訊資料格式或大小核對失敗；操作結果仍待確認。",
            "malformed-json": "Codex 通訊資料無法解讀；操作結果仍待確認。",
            "invalid-encoding": "Codex 通訊資料編碼無法解讀；操作結果仍待確認。",
            "eof": "Codex 通訊通道提前關閉，未取得最終回報。",
            "reader-io": "讀取 Codex 通訊資料失敗，未取得最終回報。",
        }.get(diagnostic.get("code"))
        reason = diagnostic.get("transport_failure", {}).get("reason")
        frame_fault = diagnostic.get("code") in {"protocol-frame", "cleanup-unverified"}
        if frame_fault and reason == "frame-size-limit":
            transport_cause = "Codex 單筆通訊資料超過接收大小上限，本回合已停止；操作結果仍待確認。"
        elif frame_fault and reason == "unterminated-frame":
            transport_cause = "Codex 通訊資料在完整接收前中斷；操作結果仍待確認。"
        if transport_cause and diagnostic.get("code") == "cleanup-unverified":
            transport_cause += "資源清理亦未確認。"
    failure = diagnostic.get("backend_failure", {})
    provider_notice = None
    if failure.get("kind") in {"unauthorized", "routing-unauthorized"}:
        cause = "Codex 後端授權／工作區路由失敗；需核對登入與路由狀態。"
    elif failure.get("kind") in {
            "auth-refresh-required", "attestation-required", "legacy-approval"}:
        cause = ("Codex 要求目前未授權或未支援的後端互動；"
                 "未提供憑證、證明或核准。")
    elif failure.get("kind") in {"overloaded", "capacity", "rate-limit", "usage-limit"}:
        cause = "Codex 後端容量或用量限制，未取得最終回報。"
        if provider_details:
            provider_notice = {
                "overloaded": (
                    "模型供應商暫時過載，選用的模型當時服務繁忙，本回合因此中斷。",
                    "稍後可請我先核對已保存的進度與原操作結果，再接續；或改用其他模型。"),
                "capacity": (
                    "模型供應商暫時無可用容量，選用的模型當時無法處理本回合。",
                    "稍後可請我先核對已保存的進度與原操作結果，再接續；或改用其他模型。"),
                "rate-limit": (
                    "模型供應商限制請求頻率，本回合暫時無法繼續。",
                    "請稍候，再請我先核對已保存的進度與原操作結果，再接續。"),
                "usage-limit": (
                    "模型供應商回報用量額度已達上限，本回合無法繼續。",
                    "請先核對模型供應商帳號的用量／額度，恢復可用後再請我核對進度並接續。"),
            }[failure["kind"]]
    elif failure.get("kind") == "context-window":
        cause = "Codex 脈絡容量已到，需先保留進度再整理脈絡。"
    if transport_cause:
        text = text.replace(
            "⚠ 任務未完成\n", "⚠ 任務未完成\n中斷原因：" + transport_cause + "\n", 1)
    elif provider_notice:
        cause, suggestion = provider_notice
        text = text.replace("⚠ 任務未完成\n", "⚠ 任務未完成\n中斷原因：" + cause + "\n", 1)
        text = text.replace(
            "建議下一步：先核對已保存的進度與原操作結果，再接續未完成部分；",
            "建議下一步：" + suggestion)
    elif cause:
        text += "\n中斷原因：" + cause
    if failure.get("tool_activity") == "not-observed":
        text = text.replace("部分操作可能已執行，結果尚未確認；不會自動重跑。",
                            "已收事件中尚未觀察到工具呼叫；結果仍未確認，不會自動重跑。")
    recovery = result.get("backend_recovery")
    if type(recovery) is dict:
        attempts = recovery.get("attempts")
        if type(attempts) is int and 1 <= attempts <= 2:
            text += f"\n已在冷卻後嘗試安全接續 {attempts} 次，仍未取得最終回報。"
        if recovery.get("state") == "blocked":
            text += "\n仍有工具或子代理狀態待核對，未自動重跑。"
        elif recovery.get("state") == "cancelled":
            text += "\n冷卻接續已依停止要求取消。"
        elif recovery.get("state") == "deadline":
            text += "\n剩餘執行時間不足以進行下一次冷卻接續。"
    return {"output_text": text}


def _failure_payload(connection, route, operation_id, result, *, provider_details=True,
                     protocol_details=True, transport_details=True):
    payload = _failure_output(result, provider_details=provider_details,
                              protocol_details=protocol_details,
                              transport_details=transport_details)
    from .channel_context_store import context_for_operation

    try:
        context = context_for_operation(connection, route.session, operation_id)
        if context and context.get("message_id") is not None:
            row = connection.execute(
                "SELECT route_json FROM runtime_channel_context WHERE operation_id=?",
                (operation_id,)).fetchone()
            if row["route_json"] == _route_json(route):
                from ahc.domain.channel_reply import reply_message_id

                payload["reply_to_message_id"] = reply_message_id(context["message_id"])
    except (WorkingSetCorrupt, ContractError):
        pass  # Optional reply metadata cannot suppress an exact-route failure notice.
    return payload


def _failure_payload_matches(connection, route, operation_id, result, payload):
    # Never rewrite acknowledged predecessor notices or their delivery state.
    diagnostic = result.get("backend_diagnostic")
    if (type(diagnostic) is dict and diagnostic.get("category") == "transport"
            and payload in (
                _failure_payload(connection, route, operation_id, result, transport_details=False),
                _failure_output(result, transport_details=False))):
        return True
    if (type(diagnostic) is dict and diagnostic.get("category") == "protocol"
            and diagnostic.get("code") == "contract-invalid" and payload in (
                _failure_payload(connection, route, operation_id, result, protocol_details=False),
                _failure_output(result, protocol_details=False))):
        return True
    return payload in (
        _failure_payload(connection, route, operation_id, result),
        _failure_payload(connection, route, operation_id, result, provider_details=False),
        _failure_output(result, provider_details=False),
        _failure_output(result), _previous_failure_output(result),
        _previous_failure_output(result, legacy=True),
    )


def _matching_seal(connection, turn, result):
    schema_version = result.get("result_schema_version")
    if schema_version is None:
        return True  # Existing terminal records retain their legacy decoding path.
    if schema_version != RUNTIME_RESULT_SCHEMA_VERSION:
        return False
    seal = connection.execute(
        "SELECT * FROM runtime_answer_seals WHERE operation_id=?", (turn["operation_id"],)
    ).fetchone()
    try:
        return (
            seal is not None
            and seal["request_digest"] == turn["request_digest"]
            and seal["session_json"] == turn["session_json"]
            and canonical_json(answer_seal_projection(json.loads(seal["result_json"])))
            == canonical_json(answer_seal_projection(result))
        )
    except (ContractError, ValueError, TypeError):
        return False


def enqueue_final(connection, turn_row, result):
    """Create one answer or failure notice after the matching terminal intent exists."""
    if not _exists(connection, "runtime_channel_routes"):
        return
    if type(result) is not dict:
        raise WorkingSetCorrupt("channel final result corrupted")
    operation_id = turn_row["operation_id"]
    route_row = connection.execute(
        "SELECT * FROM runtime_channel_routes WHERE operation_id=?", (operation_id,)
    ).fetchone()
    if route_row is None:
        return
    if not _exists(connection, "runtime_channel_outbox"):
        raise WorkingSetCorrupt("channel outbox storage unavailable")
    route = _route(route_row)
    turn = connection.execute(
        "SELECT * FROM runtime_turns WHERE operation_id=?", (operation_id,)
    ).fetchone()
    intent = connection.execute(
        "SELECT * FROM runtime_intents WHERE operation_id=?", (operation_id,)
    ).fetchone()
    if (
        turn is None
        or intent is None
        or turn["status"] != "terminal"
        or turn["session_json"] != canonical_json(route.session.to_dict()).decode()
        or intent["session_json"] != turn["session_json"]
        or intent["payload_json"] != canonical_json(result).decode()
    ):
        raise WorkingSetCorrupt("channel terminal binding corrupted")
    accepted = result.get("accepted") is True
    reported = result.get("report_accepted") is True
    if accepted or reported:
        if (intent["kind"] != ("mailbox" if accepted else "parked")
                or not _matching_seal(connection, turn, result)):
            raise WorkingSetCorrupt("channel final answer seal corrupted")
        if result.get("reply_to_message_id") is not None:
            from .channel_reply_catalog import validate_reply

            validate_reply(connection, route.session, operation_id,
                           result["reply_to_message_id"])
        payload = _output(result)
    else:
        if result.get("disposition") not in {"unknown", "not-submitted", "unsupported"}:
            return  # Legacy noncanonical failures retain their existing no-notice behavior.
        if intent["kind"] != "parked":
            raise WorkingSetCorrupt("channel failure intent corrupted")
        payload = _failure_payload(connection, route, operation_id, result)
    encoded = canonical_json(payload).decode()
    row = connection.execute(
        "SELECT * FROM runtime_channel_outbox WHERE operation_id=?", (operation_id,)
    ).fetchone()
    if row is None:
        from .runtime_result_artifacts import enqueue_attachments

        connection.execute(
            "INSERT INTO runtime_channel_outbox VALUES(?,?,?,'pending',NULL,NULL)",
            (operation_id, route_row["route_json"], encoded),
        )
        if accepted:
            enqueue_attachments(connection, operation_id, route, result)
    elif row["route_json"] != route_row["route_json"] or row["payload_json"] != encoded:
        raise WorkingSetCorrupt("channel outbox binding corrupted")
