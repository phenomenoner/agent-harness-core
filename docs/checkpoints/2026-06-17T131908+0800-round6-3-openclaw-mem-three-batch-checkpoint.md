# Checkpoint - round6-3 openclaw-mem three-batch backfill

Created: 2026-06-17 13:19 Asia/Taipei

## Status

Checkpoint saved for the round6-3 `openclaw-mem` embedding recovery line after all three planned backfill batches completed.

This checkpoint records the line state only. No live runtime topology, gateway process, auth, model routing, scheduler, external post, permission setting, or Rust implementation was changed by this checkpoint.

## Current Battle Picture

The planned staged backfill is complete:

1. First batch: recent/must observations + docs/operator canon.
2. Second batch: recent high-value main-agent episodic conversation/ops rows.
3. Third batch: historical observations after noise demotion.

The remaining embedding lanes are intentionally deferred/noisy, not an automatic fourth batch:

- `ignore` observations
- tool-spam and wrapper observations
- redacted episodes
- unknown-session / duplicate episode rows
- raw tool call/result episodes
- older historical low-signal rows

Those lanes should only be embedded later with a governed selector and quality receipt.

## Coverage Receipt

Latest `agent-harness.exe memory-service-status --json` after the third batch:

| Lane | Rows | Embedded | Coverage |
|---|---:|---:|---:|
| observations | `63744` | `22090` | `3465` bps |
| episodic events | `214114` | `33967` | `1586` bps |
| docs chunks | `92444` | `10528` | `1138` bps |

The active memory service mode is still `snapshot-adapter`; `openclaw-mem-engine` remains `available-not-promoted`. Qdrant edge remains preserved snapshot evidence, not active native Qdrant recall.

## Backfill Artifacts

Assessment:

- `.debug/round6-3/openclaw-mem-capability-recovery-assessment-2026-06-17.md`

Checkpoint updated in-place:

- `docs/checkpoints/2026-06-17T112500+0800-round6-3-openclaw-mem-backfill-checkpoint.md`

Helpers:

- `.debug/round6-3/openclaw_mem_first_batch_backfill.py`
- `.debug/round6-3/openclaw_mem_second_batch_episode_backfill.py`
- `.debug/round6-3/openclaw_mem_third_batch_observation_backfill.py`

Backups:

- `.debug/round6-3/embedding-backfill-20260617/openclaw-mem.before-first-batch.sqlite`
- `.debug/round6-3/embedding-backfill-20260617/openclaw-mem.before-second-batch.sqlite`
- `.debug/round6-3/embedding-backfill-20260617/openclaw-mem.before-third-batch.sqlite`

Receipts:

- `.debug/round6-3/embedding-backfill-20260617/first-batch-receipt.json`
- `.debug/round6-3/embedding-backfill-20260617/second-batch-receipt.json`
- `.debug/round6-3/embedding-backfill-20260617/third-batch-receipt.json`
- `.debug/round6-3/embedding-backfill-20260617/second-batch-cursor-state.json`
- `.debug/round6-3/embedding-backfill-20260617/third-batch-cursor-state.json`

## Completed

First batch:

- Model: `text-embedding-3-small`
- Observations inserted: `19056` full run plus `20` smoke rows
- Operator docs inserted: `333` full run plus `5` smoke rows
- Remaining first-batch candidates: `0`

Second batch:

- Model: `text-embedding-3-small`
- Episodic events inserted: `33843` full run plus `100` smoke rows
- Total second-batch inserted: `33943`
- Remaining second-batch candidates: `0`
- Hash integrity: `33967` checked episodic embedding rows, `0` bad `search_text_hash` rows

Third batch:

- Model: `text-embedding-3-small`
- Observations inserted: `2908` full run plus `100` smoke rows
- Total third-batch inserted: `3008`
- Remaining third-batch candidates: `0`

## Verification

Verified after batch completion:

- `agent-harness.exe memory-service-status --json` reports status `ready` for the snapshot adapter and fresh coverage counts.
- Direct `openclaw-mem episodes search "model allocation cron restored" --mode hybrid --global --json` returned `vector_status=ok` after applying the harness memory env bridge.
- Direct `openclaw-mem hybrid "graph capture markdown memory recall" --json` returned text+vector hits after applying the harness memory env bridge.
- The assessment and earlier checkpoint were updated with the third-batch state.

## Remaining Gates

Embedding recovery is materially improved, but full `openclaw-mem` parity is still not recovered.

Hard gates still open:

- snapshot-adapter ownership remains hard-pinned
- mem-engine canary lacks promotion/shadow traffic
- ContextPack schema negotiation is still missing
- direct memory MCP tools are still absent
- Qdrant remains snapshot-only, not native recall
- graph topology source is still missing
- legacy hook / routeAuto / autoCapture parity remains partial
- direct naked CLI still needs env bridge for vector search

## Worktree Note

At checkpoint time, `git status --short` showed unrelated tracked Rust/docs changes and untracked checkpoint/media files. This checkpoint did not revert or modify those unrelated changes.

Relevant status excerpt:

```text
 M AGENTS.md
 M README.md
 M crates/agent-harness-cli/Cargo.toml
 M crates/agent-harness-cli/src/main.rs
 M crates/agent-harness-core/src/channel_ingress.rs
 M crates/agent-harness-core/src/channel_pipeline.rs
 M crates/agent-harness-core/src/channel_runtime.rs
 M crates/agent-harness-core/src/channel_state.rs
 M crates/agent-harness-core/src/codex_runtime.rs
 M crates/agent-harness-core/src/lib.rs
 M crates/agent-harness-core/src/prompt.rs
 M crates/agent-harness-core/src/runtime_pipeline.rs
 M crates/agent-harness-core/src/runtime_queue.rs
 M crates/agent-harness-core/src/runtime_worker.rs
 M crates/agent-harness-core/src/turns.rs
 M crates/agent-harness-core/src/workers.rs
 M docs/harness-agent-validation-next-session-20260616.md
?? crates/agent-harness-cli/src/telegram_media.rs
?? crates/agent-harness-core/src/media.rs
?? docs/checkpoints/
```

## Closure Statement

The three planned embedding backfill batches are complete and receipt-backed. This improves practical recall coverage for high-value recent, episodic, docs/operator, and de-noised historical memory. The remaining work is no longer simple backfill; it is product/runtime parity work plus governed handling of deferred noisy memory lanes.
