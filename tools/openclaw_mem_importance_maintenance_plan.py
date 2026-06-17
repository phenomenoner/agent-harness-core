#!/usr/bin/env python3
"""Review-only memory importance maintenance planner for OpenClaw."""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import sqlite3
import sys
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError


try:
    TPE = ZoneInfo("Asia/Taipei")
except ZoneInfoNotFoundError:
    TPE = dt.timezone(dt.timedelta(hours=8), name="Asia/Taipei")
BLOCKED_PREFIX = "BLOCKED memory-importance-plan"


def one(conn: sqlite3.Connection, sql: str, params: tuple[Any, ...] = ()) -> int:
    row = conn.execute(sql, params).fetchone()
    return int(row[0] or 0) if row else 0


def pct(done: int, total: int) -> float:
    return round((done / total) * 100.0, 2) if total else 0.0


def query_count(conn: sqlite3.Connection, table: str, where: str, params: tuple[Any, ...] = ()) -> int:
    return one(conn, f"select count(*) from {table} where {where}", params)


def lane(description: str, count: int, role: str = "review", notify_threshold: int = 50) -> dict[str, Any]:
    return {
        "description": description,
        "count": count,
        "role": role,
        "notifyThreshold": notify_threshold,
        "notifyBucket": count // max(notify_threshold, 1),
    }


def build_plan(conn: sqlite3.Connection, db_path: Path, contract_path: Path) -> dict[str, Any]:
    generated_at = dt.datetime.now(TPE).isoformat()
    obs_total = one(conn, "select count(*) from observations")
    obs_embedded = one(conn, "select count(distinct observation_id) from observation_embeddings")
    obs_embedded_en = one(conn, "select count(distinct observation_id) from observation_embeddings_en")
    ep_total = one(conn, "select count(*) from episodic_events")
    ep_embedded = one(conn, "select count(distinct event_row_id) from episodic_event_embeddings")
    docs_total = one(conn, "select count(*) from docs_chunks")
    docs_embedded = one(conn, "select count(distinct chunk_rowid) from docs_embeddings")

    candidates = {
        "protect_must_remember": lane(
            "Records mentioning must_remember should be protected from automatic demotion.",
            query_count(
                conn,
                "observations",
                "coalesce(summary, '') || ' ' || coalesce(detail_json, '') like '%must_remember%'",
            ),
            role="protect",
            notify_threshold=1,
        ),
        "redacted_episode_review": lane(
            "Redacted episodes should stay low-priority unless surrounding context gives durable value.",
            query_count(conn, "episodic_events", "redacted = 1"),
        ),
        "unknown_session_episode_review": lane(
            "Episodes without a stable session id are candidates for contextual or stale treatment.",
            query_count(
                conn,
                "episodic_events",
                "session_id is null or session_id = '' or session_id = 'unknown'",
            ),
        ),
        "tool_noise_observation_review": lane(
            "Tool/status observations without must_remember, checkpoint, decision, or receipt markers are noise candidates.",
            query_count(
                conn,
                "observations",
                """(kind = 'tool' or coalesce(tool_name, '') <> '')
                and lower(coalesce(summary, '') || ' ' || coalesce(detail_json, '')) not like '%must_remember%'
                and lower(coalesce(summary, '') || ' ' || coalesce(detail_json, '')) not like '%checkpoint%'
                and lower(coalesce(summary, '') || ' ' || coalesce(detail_json, '')) not like '%decision%'
                and lower(coalesce(summary, '') || ' ' || coalesce(detail_json, '')) not like '%receipt%'""",
            ),
        ),
        "redacted_observation_review": lane(
            "Redacted observations should not be promoted from content alone.",
            query_count(
                conn,
                "observations",
                "coalesce(summary, '') || ' ' || coalesce(detail_json, '') like '%[REDACTED%'",
            ),
        ),
        "unembedded_observation_backlog": lane(
            "Remaining unembedded observations are backlog, not an automatic rebuild mandate.",
            max((obs_total - obs_embedded), 0),
        ),
        "unembedded_episode_backlog": lane(
            "Remaining unembedded episodic events are backlog, not an automatic rebuild mandate.",
            max((ep_total - ep_embedded), 0),
        ),
        "unembedded_docs_backlog": lane(
            "Remaining unembedded docs chunks are backlog, not an automatic rebuild mandate.",
            max((docs_total - docs_embedded), 0),
        ),
    }

    coverage = {
        "observations": {
            "total": obs_total,
            "embedded": obs_embedded,
            "embedded_en": obs_embedded_en,
            "coverage_pct": pct(obs_embedded, obs_total),
        },
        "episodic_events": {
            "total": ep_total,
            "embedded": ep_embedded,
            "coverage_pct": pct(ep_embedded, ep_total),
        },
        "docs_chunks": {
            "total": docs_total,
            "embedded": docs_embedded,
            "coverage_pct": pct(docs_embedded, docs_total),
        },
    }

    digest_src = json.dumps({"coverage": coverage, "candidates": candidates}, sort_keys=True).encode("utf-8")
    notify_digest_src = json.dumps(
        {
            name: {
                "role": item["role"],
                "notifyBucket": item["notifyBucket"],
                "notifyThreshold": item["notifyThreshold"],
            }
            for name, item in candidates.items()
            if item["role"] == "review"
        },
        sort_keys=True,
    ).encode("utf-8")
    return {
        "schema": "openclaw.mem.importance_maintenance.plan.v1",
        "generatedAt": generated_at,
        "timezone": "Asia/Taipei",
        "mode": "review_only",
        "db": str(db_path),
        "contract": str(contract_path),
        "digest": hashlib.sha256(digest_src).hexdigest(),
        "notificationDigest": hashlib.sha256(notify_digest_src).hexdigest(),
        "coverage": coverage,
        "candidates": candidates,
        "recommendedNextActions": [
            "Keep weekly job plan-only until candidate quality is reviewed across multiple receipts.",
            "Do not demote must_remember records without explicit replacement-source evidence.",
            "Use candidate counts to decide whether a future bounded apply mode is worth implementing.",
        ],
        "mutationPerformed": False,
    }


def write_summary(plan: dict[str, Any], path: Path) -> None:
    cov = plan["coverage"]
    candidates = plan["candidates"]
    lines = [
        "# OpenClaw Memory Importance Maintenance Summary",
        "",
        f"Generated: `{plan['generatedAt']}`",
        f"Mode: `{plan['mode']}`",
        f"Digest: `{plan['digest']}`",
        "",
        "## Coverage",
        "",
    ]
    for name, item in cov.items():
        if "embedded_en" in item:
            lines.append(
                f"- {name}: {item['embedded']} / {item['total']} embedded ({item['coverage_pct']}%); "
                f"{item['embedded_en']} English embeddings"
            )
        else:
            lines.append(f"- {name}: {item['embedded']} / {item['total']} embedded ({item['coverage_pct']}%)")
    lines.extend(["", "## Candidate Lanes", ""])
    for name, item in candidates.items():
        lines.append(f"- {name}: {item['count']} - {item['description']}")
    lines.extend(
        [
            "",
            "## Boundary",
            "",
            "- Review-only run.",
            "- No SQLite mutation performed.",
            "- No deletion or automatic demotion performed.",
            "",
        ]
    )
    path.write_text("\n".join(lines), encoding="utf-8")


def actionable_lanes(plan: dict[str, Any]) -> list[tuple[str, int]]:
    lanes = []
    for name, item in plan["candidates"].items():
        count = int(item["count"])
        if item.get("role") == "review" and count >= int(item.get("notifyThreshold", 1)):
            lanes.append((name, count))
    return lanes


def build_notification(plan: dict[str, Any], summary_path: Path, notify_state_path: Path) -> tuple[str | None, dict[str, Any] | None]:
    previous_digest = None
    if notify_state_path.exists():
        try:
            previous_digest = json.loads(notify_state_path.read_text(encoding="utf-8")).get("lastNotifiedNotificationDigest")
        except json.JSONDecodeError:
            previous_digest = None

    if previous_digest == plan["notificationDigest"]:
        return None, None

    lanes = actionable_lanes(plan)
    if not lanes:
        return None, None

    count = sum(count for _, count in lanes)
    lanes = sorted(lanes, key=lambda item: item[1], reverse=True)[:4]
    lane_text = ", ".join(f"{name}={count}" for name, count in lanes)
    state = {
        "lastNotifiedAt": dt.datetime.now(TPE).isoformat(),
        "lastNotifiedDigest": plan["digest"],
        "lastNotifiedNotificationDigest": plan["notificationDigest"],
        "actionableCount": count,
        "summary": str(summary_path),
    }
    message = (
        "memory-importance actionable findings\n"
        f"summary={summary_path}\n"
        f"digest={plan['notificationDigest']}\n"
        f"total_candidate_signals={count}\n"
        f"top_lanes={lane_text}\n"
        "next_action=review summary.md and decide whether to keep plan-only or approve bounded apply mode"
    )
    return message, state


def run() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", required=True, type=Path)
    parser.add_argument("--out-dir", required=True, type=Path)
    parser.add_argument("--contract", required=True, type=Path)
    parser.add_argument("--print-summary", action="store_true")
    parser.add_argument("--notify-on-actionable", action="store_true")
    args = parser.parse_args()

    if not args.db.exists():
        print(f"{BLOCKED_PREFIX} reason=missing_db path={args.db}")
        return 1
    if not args.contract.exists():
        print(f"{BLOCKED_PREFIX} reason=missing_contract path={args.contract}")
        return 1

    day = dt.datetime.now(TPE).strftime("%Y-%m-%d")
    run_dir = args.out_dir / day
    run_dir.mkdir(parents=True, exist_ok=True)

    uri = f"file:{args.db.as_posix()}?mode=ro"
    with sqlite3.connect(uri, uri=True) as conn:
        conn.execute("pragma query_only = on")
        plan = build_plan(conn, args.db, args.contract)

    plan_path = run_dir / "plan.json"
    summary_path = run_dir / "summary.md"
    plan["artifacts"] = {"plan": str(plan_path), "summary": str(summary_path)}
    plan_path.write_text(json.dumps(plan, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    write_summary(plan, summary_path)

    if args.notify_on_actionable:
        notification, notify_state = build_notification(plan, summary_path, args.out_dir / "last-notified.json")
        if notification:
            print(notification)
            if notify_state is not None:
                try:
                    (args.out_dir / "last-notified.json").write_text(
                        json.dumps(notify_state, ensure_ascii=False, indent=2) + "\n",
                        encoding="utf-8",
                    )
                except OSError as exc:
                    # The actionable notification has already been emitted; do not mix
                    # a BLOCKED line into stdout and confuse the cron reply contract.
                    print(
                        f"{BLOCKED_PREFIX} post_notify_state_write_failed={type(exc).__name__}",
                        file=sys.stderr,
                    )
            return 0

    if args.print_summary:
        print(f"memory-importance-plan ok plan={plan_path} summary={summary_path} digest={plan['digest']}")
    else:
        print("NO_REPLY")
    return 0


def main() -> int:
    try:
        return run()
    except BrokenPipeError:
        return 1
    except (OSError, sqlite3.Error) as exc:
        print(f"{BLOCKED_PREFIX} reason={type(exc).__name__} detail={str(exc).replace(chr(10), ' ')[:300]}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
