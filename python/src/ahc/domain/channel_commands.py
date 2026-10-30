"""Shared command discovery for typed controls and platform-native menus."""

COMMANDS = (
    ("help", "指令與操作說明", None),
    ("status", "目前工作與對話狀態", None),
    ("capabilities", "目前可用工具、Windows 路徑與記憶範圍", None),
    ("messages", "工作中訊息採排隊或即時引導", "queue|steer"),
    ("steer", "將補充內容送入當前工作；無工作時排隊", "內容"),
    ("cron", "建立、列出、暫停或刪除本聊天室排程", "list|add|pause|resume|delete"),
    ("model", "查詢或切換模型", "模型名稱"),
    ("think", "查詢或切換推理強度", "強度"),
    ("fast", "Priority 服務，可能增加費用", "on|off"),
    ("approvals", "查詢或切換核准模式", "manual|auto-approve|always-approve|yolo"),
    ("yolo", "切換不詢問模式；不帶參數會反向切換", "on|off"),
    ("approve", "僅核准一次操作", "完整 ID"),
    ("deny", "拒絕一次操作", "完整 ID"),
    ("stop", "停止當前工作，保留對話脈絡", None),
    ("new", "清除短期脈絡，保留記憶與 Python 變數", None),
    ("restart", "排空工作後重啟此 AHC 服務", None),
)
ALIASES = {"commands": "help", "start": "help", "models": "model",
           "reasoning": "think", "approval": "approvals"}


def help_text():
    lines = ["**AHC 指令** · 由 runtime 直接處理"]
    for name, description, argument in COMMANDS:
        hint = f" [{argument}]" if argument else ""
        lines.append(f"/{name}{hint} — {description}")
    lines.append("\n設定保存在本聊天室，下一個工作生效。/stop 不清除脈絡；/new 才重設。")
    lines.append("/yolo 不帶參數會反向切換；on 明確開啟，off 關閉並恢復先前核准模式。")
    lines.append("/new 保留模型、推理強度與核准設定；不會關閉 YOLO。")
    lines.append("/messages queue|steer 設定工作中訊息模式；/steer 內容 明確引導當前工作。")
    lines.append("/cron add every 10m 任務；/cron add daily 09:00 Asia/Taipei 任務；/cron list。")
    lines.append("核准卡可點選一次核准／拒絕，也可輸入完整 ID。YOLO 不變更檔案／網路權限。")
    return "\n".join(lines)


def discord_commands():
    return [{"name": name, "description": description, "type": 1,
             **({"options": [{"name": "value", "description": argument, "type": 3,
                               "required": False,
                               "max_length": 4000 if name in {"steer", "cron"} else 128}]}
                if argument else {})}
            for name, description, argument in COMMANDS]
