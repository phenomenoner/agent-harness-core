"""Native compaction policy and payload-free observations for one bound thread."""

import json
from datetime import UTC, datetime
from pathlib import Path

from ahc.domain.sessions import ContractError


def context_policy(value):
    if value is None:
        return None
    if (type(value) is not dict or set(value) != {"soft_limit_percent", "headroom_tokens"}
            or type(value["soft_limit_percent"]) is not int
            or not 50 <= value["soft_limit_percent"] <= 90
            or type(value["headroom_tokens"]) is not int
            or not 1024 <= value["headroom_tokens"] <= 131072):
        raise ContractError("backend context policy invalid")
    return dict(value)


def catalog_window(home, model, *, now=None):
    """A recent exact-model catalog is a startup hint, never an entitlement probe."""
    try:
        path = Path(home) / "models_cache.json"
        if path.stat().st_size > 2 * 1024 * 1024:
            return None
        data = json.loads(path.read_text(encoding="utf-8"))
        fetched = datetime.fromisoformat(data["fetched_at"].replace("Z", "+00:00"))
        age = ((now or datetime.now(UTC)) - fetched).total_seconds()
        if not 0 <= age <= 86400:
            return None
        matches = [m for m in data["models"] if m.get("slug") == model]
        if len(matches) != 1:
            return None
        raw = matches[0].get("context_window")
        percent = matches[0].get("effective_context_window_percent")
        if (type(raw) is not int or not 4096 <= raw <= 100_000_000
                or type(percent) is not int or not 1 <= percent <= 100):
            return None
        return raw * percent // 100
    except (OSError, ValueError, TypeError, KeyError):
        return None


def compact_limit(policy, window):
    if policy is None or window is None:
        return None
    if type(window) is not int or window < 4096:
        raise ContractError("backend context window invalid")
    limit = min(window * policy["soft_limit_percent"] // 100,
                window - policy["headroom_tokens"])
    return limit if limit >= 1024 else None


def observe_compaction(client, message):
    """Observe exact parent item boundaries without accepting a turn or tool."""
    params = message.get("params")
    if (type(params) is not dict or params.get("threadId") != client.thread
            or params.get("turnId") != client.turn or client.turn is None):
        return
    item = params.get("item")
    if type(item) is not dict or item.get("type") != "contextCompaction":
        return
    identity = item.get("id")
    if type(identity) is not str or not identity or len(identity) > 4096:
        return
    if message.get("method") == "item/started":
        observed = client.token_observation
        client._compaction_pending = (identity,
            None if observed is None else observed.last.total_tokens)
    elif (message.get("method") == "item/completed"
          and getattr(client, "_compaction_pending", None) is not None
          and client._compaction_pending[0] == identity):
        client._compaction_count = getattr(client, "_compaction_count", 0) + 1
        client._compaction_before = client._compaction_pending[1]
        client._compaction_after = None
        client._compaction_pending = None
