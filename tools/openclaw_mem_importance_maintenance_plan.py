#!/usr/bin/env python3
"""Review-only memory importance maintenance planner for OpenClaw."""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import sqlite3
from pathlib import Path
from typing import Any


def one(conn: sqlite3.Connection, sql: str, params: tuple[Any, ...] = ()) -> int:
    row = conn.execute(sql, params).fetchone()
    return int(row[0] or 0) if row else 0


def pct(done: int, total: int) -> float:
    return round((done / total) * 100.0, 2) if total else 0.0


def query_count(conn: sqlite3.Connection, table: str, where: str, params: tuple[Any, ...] = ()) -> int:
    return one(conn, f"select count(*) from {table} where {where}", params)


def build_plan(conn: sqlite3.Connection, db_path: Path, contract_path: Path) -> dict[str, Any]:
    generated_at = dt.datetime.now(dt.timezone.utc).isoformat()
    obs_total = one(conn, "select count(*) from observations")
    obs_embedded = one(conn, "select count(distinct observation_id) from observation_embeddings")
    obs_embedded_en = one(conn, "select count(distinct observation_id) from observation_embeddings_en")
    ep_total = one(conn, "select count(*) from episodic_events")
    ep_embedded = one(conn, "select count(distinct event_row_id) from episodic_event_embeddings")
    docs_total = one(conn, "select count(*) from docs_chunks")
    docs_embedded = one(conn, "select count(distinct chunk_rowid) from docs_embeddings")

    candidates = {
        "protect_must_remember": {
            "description": "Records mentioning must_remember should be protected from automatic demotion.",
            "count": query_count(
                conn,
                "observations",
                "coalesce(summary, '') || ' ' || coalesce(detail_json, '') like '%must_remember%'",
            ),
        },
        "redacted_episode_review": {
            "description": "Redacted episodes should stay low-priority unless surrounding context gives durable value.",
            "count": query_count(conn, "episodic_events", "redacted = 1"),
        },
        "unknown_session_episode_review": {
            "description": "Episodes without a stable session id are candidates for contextual or stale treatment.",
            "count": query_count(
                conn,
                "episodic_events",
                "session_id is null or session_id = '' or session_id = 'unknown'",
            ),
        },
        "tool_noise_observation_review": {
            "description": "Tool/status observations are candidates for ignore unless they carry receipts or decisions.",
            "count": query_count(
                conn,
                "observations",
                "kind = 'tool' or coalesce(tool_name, '') <> ''",
            ),
        },
        "redacted_observation_review": {
            "description": "Redacted observations should not be promoted from content alone.",
            "count": query_count(
                conn,
                "observations",
                "coalesce(summary, '') || ' ' || coalesce(detail_json, '') like '%[REDACTED%'",
            ),
        },
        "unembedded_contextual_backlog": {
            "description": "Remaining unembedded content is backlog, not an automatic rebuild mandate.",
            "count": max((obs_total - obs_embedded), 0)
            + max((ep_total - ep_embedded), 0)
            + max((docs_total - docs_embedded), 0),
        },
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
    return {
        "schema": "openclaw.mem.importance_maintenance.plan.v1",
        "generatedAt": generated_at,
        "mode": "review_only",
        "db": str(db_path),
        "contract": str(contract_path),
        "digest": hashlib.sha256(digest_src).hexdigest(),
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


def actionable_count(plan: dict[str, Any]) -> int:
    return sum(int(item["count"]) for item in plan["candidates"].values())


def build_notification(plan: dict[str, Any], summary_path: Path, notify_state_path: Path) -> str | None:
    previous_digest = None
    if notify_state_path.exists():
        try:
            previous_digest = json.loads(notify_state_path.read_text(encoding="utf-8")).get("lastNotifiedDigest")
        except json.JSONDecodeError:
            previous_digest = None

    if previous_digest == plan["digest"]:
        return None

    count = actionable_count(plan)
    if count <= 0:
        return None

    notify_state_path.write_text(
        json.dumps(
            {
                "lastNotifiedAt": dt.datetime.now(dt.timezone.utc).isoformat(),
                "lastNotifiedDigest": plan["digest"],
                "actionableCount": count,
                "summary": str(summary_path),
            },
            ensure_ascii=False,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )

    lanes = sorted(
        ((name, int(item["count"])) for name, item in plan["candidates"].items()),
        key=lambda item: item[1],
        reverse=True,
    )[:4]
    lane_text = ", ".join(f"{name}={count}" for name, count in lanes)
    return (
        "memory-importance actionable findings\n"
        f"summary={summary_path}\n"
        f"digest={plan['digest']}\n"
        f"total_candidate_signals={count}\n"
        f"top_lanes={lane_text}\n"
        "next_action=review summary.md and decide whether to keep plan-only or approve bounded apply mode"
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", required=True, type=Path)
    parser.add_argument("--out-dir", required=True, type=Path)
    parser.add_argument("--contract", required=True, type=Path)
    parser.add_argument("--print-summary", action="store_true")
    parser.add_argument("--notify-on-actionable", action="store_true")
    args = parser.parse_args()

    if not args.db.exists():
        raise SystemExit(f"BLOCKED memory-importance-plan missing_db={args.db}")
    if not args.contract.exists():
        raise SystemExit(f"BLOCKED memory-importance-plan missing_contract={args.contract}")

    day = dt.datetime.now().strftime("%Y-%m-%d")
    run_dir = args.out_dir / day
    run_dir.mkdir(parents=True, exist_ok=True)

    uri = f"file:{args.db.as_posix()}?mode=ro"
    with sqlite3.connect(uri, uri=True) as conn:
        plan = build_plan(conn, args.db, args.contract)

    plan_path = run_dir / "plan.json"
    summary_path = run_dir / "summary.md"
    plan["artifacts"] = {"plan": str(plan_path), "summary": str(summary_path)}
    plan_path.write_text(json.dumps(plan, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    write_summary(plan, summary_path)

    if args.notify_on_actionable:
        notification = build_notification(plan, summary_path, args.out_dir / "last-notified.json")
        if notification:
            print(notification)
            return 0

    if args.print_summary:
        print(f"memory-importance-plan ok plan={plan_path} summary={summary_path} digest={plan['digest']}")
    else:
        print("NO_REPLY")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
