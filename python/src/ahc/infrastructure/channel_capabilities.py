"""Read-only, model-free chat capability overview from the current composition."""


def handle_capabilities(controls, connection, route, text):
    if text.strip() != "/capabilities":
        return None
    doc = controls.document
    rows = doc.get("host_paths", [])
    paths = "; ".join(f"{r['host_root']} → {r['backend_root']}" for r in rows)
    memory = doc.get("memory", {})
    native = doc.get("native_minion", {}).get("profile", {}).get("enabled", False)
    limits = doc.get("child_limits", {"max_children": 16, "max_concurrency": 3})
    context = doc.get("operating_context", {})
    memory_state = (memory.get("kind", "未設定") if memory.get("enabled") else "未啟用")
    mcp = "已配置" if doc.get("mcp") else "未配置"
    skills = context.get("skills_coverage", "已配置" if doc.get("skill_root") else "未配置")
    return "\n".join([
        "AHC 能力與操作入口（目前設定，不呼叫模型）",
        f"Windows 路徑：{paths or '未配置'}；實際存取沿用核准／sandbox 設定。",
        "訊息：/messages queue|steer；/steer 內容；/stop 保留脈絡。",
        "排程：/cron list；/cron add every 10m 內容；"
        "/cron add daily 09:00 Asia/Taipei 內容。",
        f"子任務：每回合最多 {limits['max_children']}，並行上限 {limits['max_concurrency']}；"
        f"native minion：{'已配置' if native else '未配置'}。",
        f"記憶：{memory_state}；{context.get('memory_coverage', '覆蓋範圍未提供')}。",
        f"Skills：{skills}。",
        f"MCP：{mcp}。宣告能力不代表每項操作都已完成實際驗測。",
    ])
