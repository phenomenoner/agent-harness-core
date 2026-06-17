# Checkpoint - round6-3 openclaw-mem backfill

Created: 2026-06-17 11:25 Asia/Taipei

## Status

Checkpoint saved for the round6-3 `openclaw-mem` recovery/backfill line.

This checkpoint records the current line state only. No live runtime topology, gateway process, auth, model routing, scheduler, external post, or permission setting was changed by this checkpoint.

## Current Battle Picture

Round6-3 assessed new-home `openclaw-mem` recovery and then completed the first, second, and third embedding backfill batches using the existing `text-embedding-3-small` lane.

Current operational conclusion:

- Ordinary recall-assisted chat remains usable.
- `openclaw-mem` is still not 100% old-home / mem-engine parity.
- First-batch embedding coverage is now materially better for recent/must observations and operator docs.
- Second-batch episodic embedding coverage is now materially better for recent high-value main-agent conversation/ops events.
- Third-batch historical observation coverage is now materially better for non-ignore high-signal observations after noise demotion.
- Remaining lanes are intentionally deferred/noisy; do not blindly all-backfill redacted, unknown-session, tool-spam, or historical wrapper rows.

## Durable Artifacts

Assessment:

- `.debug/round6-3/openclaw-mem-capability-recovery-assessment-2026-06-17.md`

Claude second-brain receipts:

- `.debug/round6-3/claude-second-brain-openclaw-mem-gap-review-brief.md`
- `.debug/round6-3/claude-second-brain-openclaw-mem-gap-review-loop.md`

Backfill helper and receipts:

- `.debug/round6-3/openclaw_mem_first_batch_backfill.py`
- `.debug/round6-3/openclaw_mem_second_batch_episode_backfill.py`
- `.debug/round6-3/openclaw_mem_third_batch_observation_backfill.py`
- `.debug/round6-3/embedding-backfill-20260617/openclaw-mem.before-first-batch.sqlite`
- `.debug/round6-3/embedding-backfill-20260617/openclaw-mem.before-second-batch.sqlite`
- `.debug/round6-3/embedding-backfill-20260617/openclaw-mem.before-third-batch.sqlite`
- `.debug/round6-3/embedding-backfill-20260617/smoke-receipt-2.json`
- `.debug/round6-3/embedding-backfill-20260617/first-batch-receipt.json`
- `.debug/round6-3/embedding-backfill-20260617/second-batch-dry-run-receipt.json`
- `.debug/round6-3/embedding-backfill-20260617/second-batch-smoke-receipt.json`
- `.debug/round6-3/embedding-backfill-20260617/second-batch-receipt.json`
- `.debug/round6-3/embedding-backfill-20260617/second-batch-cursor-state.json`
- `.debug/round6-3/embedding-backfill-20260617/third-batch-dry-run-receipt.json`
- `.debug/round6-3/embedding-backfill-20260617/third-batch-smoke-receipt.json`
- `.debug/round6-3/embedding-backfill-20260617/third-batch-receipt.json`
- `.debug/round6-3/embedding-backfill-20260617/third-batch-cursor-state.json`

## Completed

First-batch embedding backfill completed with:

- Model: `text-embedding-3-small`
- Base URL: `https://api.openai.com/v1`
- Cutoff: `2026-04-18T00:00:00Z`
- Scope:
  - recent 30-60 day observations
  - `must_remember` observations
  - docs/operator-canon related chunks

Backfilled:

- Observations: `19056` in the full run, plus `20` smoke rows before it
- Operator docs: `333` in the full run, plus `5` smoke rows before it

Remaining first-batch candidates after run:

- recent/must observations: `0`
- operator docs: `0`

Second-batch episodic embedding backfill completed with:

- Model: `text-embedding-3-small`
- Base URL: `https://api.openai.com/v1`
- Cutoff: `2026-04-18T00:00:00Z`
- Scope:
  - recent 60-day episodic events
  - `agent_id='main'`
  - `session_id <> 'unknown'`
  - non-redacted conversation/ops rows
  - excludes tool call/result rows, heartbeat polls, subagent wrappers, marker-only replies, and obvious shell/apply/stdout/stderr tool noise

Backfilled:

- Episodic events: `100` smoke rows, then `33843` full-run rows
- Total second-batch inserted: `33943`

Remaining second-batch candidates after run:

- high-value recent main-agent episodic candidates: `0`

Third-batch historical observation embedding backfill completed with:

- Model: `text-embedding-3-small`
- Base URL: `https://api.openai.com/v1`
- Scope:
  - historical observations remaining after the first batch
  - non-`ignore` rows only
  - excludes high-volume tool spam surfaces: `edit`, `process`, `write`, `apply_patch`, session/subagent/update-plan wrappers
  - excludes yielded/status wrapper summaries
  - minimum summary length: `40`

Backfilled:

- Observations: `100` smoke rows, then `2908` full-run rows
- Total third-batch inserted: `3008`

Remaining third-batch candidates after run:

- non-ignore historical observations after noise demotion: `0`

Coverage after run:

| Lane | Before | After |
|---|---:|---:|
| observations | `6 / 63744` | `22090 / 63744` |
| docs chunks | `10190 / 92444` | `10528 / 92444` |
| episodic events | `24 / 214114` | `33967 / 214114` |

Verification performed:

- `openclaw-mem status --json` saw observation embeddings `19082`.
- `agent-harness.exe memory-service-status ... --json` saw observation coverage `2993` bps and docs coverage `1138` bps.
- `agent-harness.exe memory-service-recall ... --query "openclaw-mem memory engine recovery"` returned hits via `sqlite-vector+service-writeback`.
- Direct `openclaw-mem docs search` worked with vector hits after applying the harness memory env bridge.
- `agent-harness.exe memory-service-status ... --json` saw episodic coverage `1586` bps after the second batch.
- Direct `openclaw-mem episodes search "model allocation cron restored" --mode hybrid --global --json` returned `vector_status=ok` after applying the harness memory env bridge.
- Episodic hash integrity check saw `33967` checked rows and `0` bad `search_text_hash` rows.
- `agent-harness.exe memory-service-status ... --json` saw observation coverage `3465` bps after the third batch.
- Direct `openclaw-mem hybrid "graph capture markdown memory recall" --json` returned text+vector hits after applying the harness memory env bridge.

## Remaining Gates

Hard recovery gaps still open:

- snapshot-adapter ownership is still hard-pinned
- mem-engine canary lacks promotion/shadow traffic
- ContextPack schema negotiation is still missing
- direct memory MCP tools are still absent
- Qdrant remains snapshot-only, not native recall
- legacy hook / routeAuto / autoCapture parity is partial
- graph topology source is still missing

Backfill next gates:

- Second-batch high-value episodic subset is complete and receipt-backed.
- Third-batch high-signal historical observation subset is complete and receipt-backed.
- Deferred lanes still need governed selectors before any future embedding: redacted rows, unknown-session duplicates, tool call/result rows, wrapper noise, `ignore` observations, and other historical low-signal data.

## Worktree Note

At checkpoint time, `git status --short` showed:

```text
 M AGENTS.md
 M README.md
 M docs/harness-agent-validation-next-session-20260616.md
?? docs/checkpoints/
```

The checkpoint did not revert or modify unrelated tracked dirty files. The `.debug/round6-3` artifacts are ignored debug artifacts and do not show in ordinary `git status --short`.

## Closure Statement

The round6-3 first, second, and third embedding backfill batches are complete and receipt-backed. The system is better for recent/must observation recall, operator-canon doc recall, high-value recent episodic recall, and non-ignore historical observation recall, but full `openclaw-mem` parity remains gated by ownership, MCP/ContextPack contracts, Qdrant/native service behavior, graph freshness, and governed treatment of deferred noisy/historical memory lanes.
