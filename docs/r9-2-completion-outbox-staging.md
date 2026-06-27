# R9-2 Completion/Outbox Staging

Date: 2026-06-27
Status: staged, validated, and live cutover performed.

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

The requested `claude -p` review loops were attempted with scoped prompts, but the local Claude CLI was unavailable in this environment. Fallback subagent reviews were used for the action-plan critique. Their P0/P1 findings drove the source-correlated outbox scan, marker validation, and duplicate side-effect gating. A fallback evidence-review subagent timed out and was closed before returning findings.

Detailed operator evidence was kept in ignored local receipts and is not part of the public repository export.

## Live Cutover

Live cutover was performed on 2026-06-27 after operator pre-authorization.

- Ticket: `cutover-1782554603192`
- Live binary: `target\debug\agent-harness.exe`
- Live SHA-256: `4CF3582F3866003589CD6ADEDB5193A958C83F7E4FC084C9FCC0F514B7FB0D06`
- Retained candidate: `target\debug\agent-harness.r9-2-completion-outbox-candidate-20260627-175531.exe`
- Previous live backup: `target\debug\agent-harness.pre-r9-2-completion-outbox-20260627-180314.exe`
- Stopped-state backup label: `pre-r9-2-completion-outbox-cutover-stopped-20260627-180314`

The original staged candidate path was no longer present at cutover time. A stable candidate was rebuilt from the same tree after verifying no diff against `origin/main`; the rebuilt debug binary was retained and used for cutover.

Post-cutover validation:

- `healthz --target-home .agent-harness --require-writable-state`: `ready=true`, `live=true`, `readinessReady=true`, eight live loops and eight supervisor services.
- `status --target-home .agent-harness --json`: readiness `passed=59`, `warnings=1`, `failed=0`.
- `worker-status --target-home .agent-harness`: `pending=0`, `leased=0`, `running=0`, downstream runtime open items `0`, active cron runs `0`.
- `channel-outbox-plan --target-home .agent-harness --limit 140`: Telegram/Discord pending `0`; remaining pending headers are historical local/native-cron items.
- `supervisor-reconcile --all --dry-run`: `launchCommands=0`, `stale=0`, `running=8`.
- `ops-cutover-receipt`: `status=ready`.

During post-cutover verification, Discord gateway had a fresh loop heartbeat but stale supervisor service metadata from the spawn boundary. The live state repair removed the stale `lastHeartbeatAtMs` from `.agent-harness\state\supervisor\services\discord-gateway-loop.json` so supervisor inventory uses the fresh heartbeat; backup and receipt are retained in ignored local operator evidence.

## Follow-Up Channel Runtime Hotpatch

On 2026-06-27, live Discord DM testing found a follow-up runtime gate failure unrelated to the R9-2 completion/outbox fixes. Discord ingress and delivery receipts were present, but the latest Discord DM turns failed terminally at Codex runtime preflight because Windows resolved `codex` to an extensionless npm shim while a spawnable `codex.cmd` sibling was available.

The staged hotpatch normalizes extensionless Windows Codex shims to spawnable siblings during both runtime planning and preflight. It also fixes supervisor inventory readback so a fresh loop heartbeat wins over stale supervisor service metadata. Telegram was checked on the same live state; recent Telegram main turns passed preflight, while older Telegram dead letters were timeout/protocol failures rather than this executable resolver issue.

Staged candidate:

- Path: `target\staging-live-channel-hotpatch-20260627-build\debug\agent-harness.exe`
- SHA-256: `BADBD93BD18788DF3FB7A435CD0B8CF374F12C12F0FE9CF06FD3BCFC27E0E48D`

Staged validation:

- New regression tests first failed, then passed after the fix:
  - `preflight_codex_runtime_uses_spawnable_sibling_for_extensionless_windows_shim`
  - `inventory_prefers_fresh_loop_heartbeat_over_stale_service_state`
- `cargo fmt --all -- --check`: passed.
- `cargo check --workspace`: passed.
- `cargo test -p agent-harness-core`: `442` unit tests and `5` integration tests passed.
- Staging binary `supervisor-reconcile --all` against live state: `launchCommands=[]`, `missing=[]`, `stale=[]`.
- Staging binary `codex-preflight` against the latest failed Discord plan: `Receipt: Ready`, `codex-executable` resolved to `D:\Users\user\AppData\Roaming\npm\codex.CMD`.

Live cutover for this follow-up is pending until the source commit is pushed.

Rollback:

- Restore `target\debug\agent-harness.pre-r9-2-completion-outbox-20260627-180314.exe` to `target\debug\agent-harness.exe` and restart through the same live-control path if rollback is needed.
