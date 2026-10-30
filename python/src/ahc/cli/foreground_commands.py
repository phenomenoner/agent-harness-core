"""Read-only chat commands answered from bound configuration, without a model."""

import re
import time

from ahc.domain.channel_ingress import ChannelRoute
from ahc.domain.sessions import ContractError
from ahc.infrastructure.runtime_channel_store import _route

_HELP = (
    "AHC 指令（直接由 runtime 回覆，不呼叫模型）\n"
    "/model — 目前設定的模型\n"
    "/think 或 /reasoning — 推理強度\n"
    "/status — 此服務設定與對話狀態\n"
    "/help 或 /commands — 指令說明\n"
    "模型／推理切換、/new、/stop、/restart、/approve、/deny 尚未支援；"
    "不會當作一般聊天執行。Discord 目前使用文字指令，非原生指令選單。"
)


def compose_commands(config, store, *, cancelled=lambda: False):
    document = config.to_dict()
    routes = {ChannelRoute.from_dict(row['route'])
              for row in document.get('channels', []) if row['enabled']}

    def reply(request):
        text = request.input_text.strip()
        if not text.startswith('/'):
            return None
        if cancelled() or time.monotonic() >= request.effective_deadline:
            raise ContractError('local command cancelled or expired')
        with store._read() as connection:
            row = connection.execute(
                'SELECT * FROM runtime_channel_routes WHERE operation_id=?',
                (request.operation_id,),
            ).fetchone()
            if row is None:
                return None  # Local CLI and scheduled work keep their existing semantics.
            route = _route(row)
            scheduled = (connection.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' "
                "AND name='runtime_schedule_occurrences'").fetchone() is not None
                and connection.execute(
                    "SELECT 1 FROM runtime_schedule_occurrences WHERE operation_id=?",
                    (request.operation_id,),).fetchone() is not None)
            queued_steer = (connection.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' "
                "AND name='channel_steering'").fetchone() is not None
                and connection.execute(
                    "SELECT 1 FROM channel_steering WHERE operation_id=? AND state='queued'",
                    (request.operation_id,),).fetchone() is not None)
        if route.session != request.session or request.session != config.session:
            raise ContractError('local command session mismatch')
        if route not in routes:
            raise ContractError('local command route is not enabled')
        if scheduled or queued_steer:
            return None  # Durable task text, including a leading slash, is not a new command.
        pieces = text.split(maxsplit=1)
        command = pieces[0].lower()
        if not re.fullmatch(r'/[a-z][a-z0-9_]*', command):
            return '無法識別此指令格式。請使用不帶 @名稱 的 /help 查看支援項目。'
        if len(pieces) > 1:
            return '此版本只支援無參數的查詢指令；未修改任何設定。請輸入 /help。'
        if command in ('/help', '/commands', '/start'):
            return _HELP
        model = config.backend['model']
        effort = config.backend.get('reasoning_effort') or 'backend default（未明確指定）'
        if command == '/model':
            return (f'目前設定模型：{model}\n推理強度：{effort}\n'
                    '來源：AHC 啟動設定；此查詢未呼叫模型。')
        if command in ('/think', '/reasoning'):
            return f'目前設定推理強度：{effort}\n此查詢未呼叫模型。'
        if command == '/status':
            context = document.get('agent_context', {})
            return (
                f'AHC 服務正在處理此查詢\nAgent：{config.session.agent_id}\n'
                f'模型設定：{model} / {effort}\n'
                f'核准模式：{config.effective_policy.approval_mode}\n'
                f'對話 generation：{config.session.ref.generation}\n'
                f'自動記憶召回：{"已設定" if context.get("recall") else "未設定"}\n'
                '以上為目前服務設定；不代表 provider 健康檢查，也未取消其他工作。'
            )
        return '此版本尚未支援這個指令；未執行任何操作，也未交給模型。請輸入 /help。'

    return reply
