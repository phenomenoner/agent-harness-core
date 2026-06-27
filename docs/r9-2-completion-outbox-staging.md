# R9-2 Completion/Outbox Staging

Date: 2026-06-27
Status: staged and validated; live cutover not performed.

## Summary

This pass fixes two related runtime terminalization gaps:

- Codex runtime now accepts a real final answer followed by `turn/completed` with `items=[]` and `itemsView=notLoaded` instead of misclassifying it as stale compact-only completion.
- Runtime pipeline now treats "Codex completion already recorded" as only a transcript/completion fact, not as proof that a channel reply was enqueued.

Runtime final replies now carry source queue/completion correlation. Duplicate final outbox writes are suppressed with an execution-scoped lock, `channel-final-outbox-receipt.json`, and a source-correlated scan of `state/channels/outbox.jsonl`.

## Validation

Staging validation completed with:

- Focused completion/outbox regression tests: passed.
- `cargo test -p agent-harness-core run_codex_runtime_ --target-dir target\staging-completion-outbox-r9-2-core -- --test-threads=1`: `20 passed; 0 failed`.
- `cargo test -p agent-harness-core run_runtime_queue_once_ --target-dir target\staging-completion-outbox-r9-2-core -- --test-threads=1`: `9 passed; 0 failed`.
- `cargo test -p agent-harness-core --target-dir target\staging-completion-outbox-r9-2-core -- --test-threads=1`: `440 passed; 0 failed` plus `5 passed; 0 failed` integration tests.
- `cargo fmt --all -- --check`: passed.
- `cargo check --workspace --target-dir target\staging-completion-outbox-r9-2-check`: passed.
- `cargo build -p agent-harness-cli --target-dir target\staging-completion-outbox-r9-2-build`: passed.
- `target\debug\agent-harness.exe public-hygiene --root .public-export\agent-harness-core`: passed with `forbiddenHits=[]`.
- `git diff --check`: no whitespace errors; only LF/CRLF warnings.

Staged candidate binary:

- Path: `target\staging-completion-outbox-r9-2-build\debug\agent-harness.exe`
- SHA-256: `4C3C0C4B25801D2E0BA58BCB1D689330EA3A66F0C88BC9085BBCBF8FE57F0451`

## Review Notes

The requested `claude -p` review loops were attempted with scoped prompts, but the local Claude CLI returned:

```text
Failed to authenticate. API Error: 401 Invalid authentication credentials
```

Fallback subagent reviews were used for the action-plan critique. Their P0/P1 findings drove the source-correlated outbox scan, marker validation, and duplicate side-effect gating. A fallback evidence-review subagent timed out and was closed before returning findings.

Local detailed evidence remains under:

- `.debug/round9-2/completion-outbox/action-plan-2026-06-27.md`
- `.debug/round9-2/completion-outbox/staging-evidence-and-cutover-pointer-2026-06-27.md`

## Live Cutover Pointer

Do not cut over from an active chat session without operator approval.

When approved:

1. Confirm idle/readiness:
   - `target\debug\agent-harness.exe worker-status --target-home .agent-harness`
   - `target\debug\agent-harness.exe channel-outbox-plan --target-home .agent-harness --limit 20`
   - `target\debug\agent-harness.exe healthz --target-home .agent-harness --require-writable-state`
2. Record an `ops-cutover-request` using the staged candidate binary above.
3. Have the operator mint a live-control token.
4. Stop live loops through the approved cutover path, back up stopped state, replace `target\debug\agent-harness.exe`, restart supervised loops, then run post-cutover health/status/outbox/worker checks.

Rollback:

- Preserve the current `target\debug\agent-harness.exe` as `target\debug\agent-harness.pre-r9-2-completion-outbox-<timestamp>.exe` before replacement.
- Restore that backup and restart through the same live-control path if post-cutover checks fail.
