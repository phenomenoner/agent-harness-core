# Round6-2 Issues Optimization Implementation - 2026-06-17

## Scope

Inputs reviewed:

1. `.debug/round6-2/progress-action-stream-and-function-self-check-review.md`
2. `.debug/round6-2/openclaw-mem-new-home-capability-assessment-2026-06-17.md`

Workflow note: implementation started before the later explicit `superpowers` request. After Superpowers activation, the remaining work followed its review/verification discipline where practical, with fresh focused verification before completion claims.

## Implemented Changes

### 1. Progress Body/Action Stream Restoration

Files:

- `crates/agent-harness-core/src/progress.rs`
- `docs/agent-harness-operations-handbook.md`

Implemented:

- Restored user-visible progress body/action rendering for safe operation trajectory events:
  - `SkillView`
  - `Todo`
  - `Terminal`
  - `SearchFiles`
  - `ExecuteCode`
  - `ToolCall`
- Kept the status panel compact and generic, such as `Working - running tools`.
- Preserved the assistant narration boundary:
  - `Current step:` only uses `AgentProgressKind::AssistantNarration`.
  - Runtime/tool operation previews are not relabeled as assistant narration.
- Excluded privacy-sensitive or non-operation events from the body/action stream by default:
  - `Runtime`
  - `AssistantStream`
  - `AssistantNarration`
  - `ReadFile`
  - `MemoryRecall`
  - `Delivery`
- Preserved existing preview safety:
  - `sanitize_progress_preview()`
  - `quote_safe_preview()`
  - configured preview caps
  - sensitive preview redaction
- Added a terminal cursor guard in `plan_agent_progress_delivery()`:
  - once a terminal progress state has been recorded, late operation events do not create a new body message for already-terminal work.
  - body/status terminal closure is lane-aware, so a terminal body delivery does not suppress a retryable status failure, and a terminal status delivery does not suppress a retryable body failure that already existed at the terminal event line.
- Kept body/status cursor lanes independent:
  - body and status have separate provider message ids, hashes, event lines, and rate-limit checks.
- Kept permission/provider hardening:
  - `SkippedDenied` advances cursors.
  - `SkippedPermanent` advances cursors.
  - terminal delivery remains monotonic after late events.

Tests added/updated:

- `renders_safe_operation_action_stream_separate_from_status_by_default`
- `action_stream_excludes_private_or_non_operation_events_by_default`
- `assistant_narration_renders_as_current_step_only`
- `delivery_plan_uses_send_then_edit_and_rate_limits`
- `skipped_denied_progress_delivery_advances_cursor`
- `skipped_permanent_progress_delivery_advances_cursor`
- `terminal_progress_state_is_monotonic_after_late_events`
- `terminal_cursor_does_not_suppress_unrecorded_lane_retry`
- secret redaction coverage for `sk-*`, GitHub token, Bearer, and `--api-key` style previews

Design decision:

- No new `response.progressActionStreamMode` config was added in this slice. The reviewed config search found no existing key, and the issue document marked the config gate as optional. The selected product behavior restores CK's expected safe operation stream by default.

### 2. Function Self-Check Guide Fixes

File:

- `docs/agent-harness-function-self-check-guide.md`

Implemented:

- Replaced invalid multi-filter Cargo examples with one filter per command.
- Used verified current module paths:
  - `runtime_queue::tests`
  - `workers::tests`
  - `cron::tests`
  - `cron_scheduler::tests`
  - `progress::tests`
  - `runtime_pipeline::tests`
- Added a note that `cargo test` accepts one test filter before `--`.
- Split `channel-identity-check` guidance into:
  - negative fail-closed smoke for an unbound synthetic chat id
  - positive bound smoke with a synthetic staging registry at `.tmp\function-check\config\channel-identity-bindings.json`
- Added a warning that `.tmp` staging examples are synthetic and not permission to quote ignored private state, live channel IDs, tokens, or `.agent-harness` payload contents into docs.

### 3. OpenClawMem New-Home Harness-Layer Optimizations

Files:

- `crates/agent-harness-core/src/memory.rs`
- `crates/agent-harness-cli/src/main.rs`
- `tools/openclaw-mem-env.ps1`
- `docs/agent-harness-operations-handbook.md`

Implemented:

- Extended `MemoryCredentialBridgeReport` with redacted direct-CLI bridge metadata:
  - `directCliEnvBridgeRequired`
  - `directCliEnvMappings`
  - `directCliNote`
- Reported the mapping names needed for naked direct CLI runs without printing secret values:
  - `AGENT_HARNESS_MEMORY_EMBEDDING_API_KEY` -> `OPENAI_API_KEY`
  - `AGENT_HARNESS_MEMORY_EMBEDDING_BASE_URL` -> `OPENAI_BASE_URL`
  - `AGENT_HARNESS_MEMORY_EMBEDDING_BASE_URL` -> `OPENAI_API_BASE`
  - `AGENT_HARNESS_MEMORY_EMBEDDING_MODEL` -> `AGENT_HARNESS_MEMORY_EMBEDDING_MODEL`
- Added a warning to `memory-service-status`/read-path reports explaining that naked `openclaw-mem` CLI invocations do not automatically inherit harness memory credentials.
- Updated non-JSON `memory-service-status` output to show direct CLI bridge target env names.
- Added `tools/openclaw-mem-env.ps1`:
  - reads `<harness-home>\secrets\memory-credentials.env`
  - supports process-env fallback for harness memory embedding env names
  - expands `${ENV_NAME}` API key references
  - parses JSON-style quoted env values
  - maps harness memory credential env names into OpenAI-compatible child-process env names
  - sets Windows UTF-8 env (`PYTHONUTF8=1`, `PYTHONIOENCODING=utf-8`)
  - invokes `openclaw-mem` or a provided command without printing the secret values
- Added `tools/openclaw-mem-env.tests.ps1`:
  - verifies positional `pack --db ...` arguments are passed to `openclaw-mem`, not bound as the wrapper command
  - verifies `${ENV_NAME}` expansion
  - verifies quoted model/base URL parsing
  - verifies process env fallback when the credentials file is missing
- Extended `memory-read-path-smoke` with scope/trust smoke fields:
  - `scopeTrustSmokeOk`
  - `scopeTrustSmokeFindings`
- Added per-agent service-writeback isolation coverage so private agent writeback recall does not cross into another agent's recall path.
- Updated the handbook to distinguish:
  - harness-mediated memory read path is bridged
  - naked direct CLI still needs the wrapper/env bridge
  - standalone `openclaw-mem-channel-a` BOM handling remains an external `openclaw-mem` package fix

Tests added:

- `openclaw_mem_service_status_reports_direct_cli_env_bridge_without_secret`
- `openclaw_mem_service_recall_keeps_agent_writeback_private`

Parser/syntax check:

- `tools/openclaw-mem-env.ps1` parsed successfully through PowerShell AST parser.
- `tools/openclaw-mem-env.tests.ps1` passed with fake `openclaw-mem.cmd` and dummy credentials.

## Not Directly Completed In This Repo Slice

These blockers remain real, but they require external `openclaw-mem` engine/source changes, live promotion, durable data generation, or high-cost data backfill. The harness now reports or documents them instead of pretending they are solved.

### B1. mem-engine promotion

Status:

- Not promoted in this repo slice.
- The harness still reports `activeSlotOwner=snapshot-adapter`.
- `memory_mem_engine_canary()` remains report-only.

Reason:

- Promotion requires a compatible external `openclaw-mem-engine` contract, canary lane, recall/store/forget/propose/pack parity receipts, rollback plan, and operator-approved live cutover.

### B2. graph readiness red

Status:

- Not flipped to green in this repo slice.
- The harness continues to require durable Windows topology at `state/memory/graph/topology-extract-full.json`.

Reason:

- Generating topology and refreshing graph data is a live/data operation, not a safe code-only patch.

### B3. sparse embedding coverage

Status:

- Not backfilled in this repo slice.
- Coverage remains reported through `memory-service-status`.

Reason:

- Backfill requires API spend, batching policy, failure receipts, and operator scheduling.

### B5. standalone Channel A BOM failure

Status:

- Not patched in this repo.
- Harness read-path smoke accepts BOM/no-BOM via its lenient parser.
- Standalone `openclaw-mem-channel-a` BOM handling requires an external `openclaw_mem/channel_a.py` fix, likely `utf-8-sig`.

Reason:

- `channel_a.py` is not tracked in this repository, and the handbook says not to patch `openclaw-mem` internals from this harness repo.

### B6. full direct pack trust/scope enforcement

Status:

- Harness reports conservative scope/trust policy and per-agent writeback rules.
- Harness read-path smoke now records scope/trust smoke status.
- Unit coverage proves service-writeback recall stays per-agent and does not expose another agent's private writeback.
- Full `openclaw-mem pack` trust policy enforcement still depends on the external CLI/engine path.

Reason:

- The harness can prove its adapter surface; it cannot guarantee all external pack paths enforce the same policy until that contract exists and is tested.

## Verification So Far

Completed:

```powershell
cargo fmt --all
cargo test -p agent-harness-core progress::tests --target-dir target\staging-test-progress-action-stream -- --test-threads=1
cargo test -p agent-harness-core memory::tests --target-dir target\staging-test-memory-bridge -- --test-threads=1
cargo fmt --all --check
cargo test -p agent-harness-cli --target-dir target\staging-test-round6-2-cli
cargo check --workspace --target-dir target\staging-check-round6-2
cargo build -p agent-harness-cli --target-dir target\staging-build-round6-2
.\target\staging-build-round6-2\debug\agent-harness.exe public-hygiene --root .\.public-export\agent-harness-core
git diff --check
.\tools\openclaw-mem-env.tests.ps1
```

Observed:

- Progress focused tests: `16 passed; 0 failed`.
- Memory focused tests: `14 passed; 0 failed`.
- CLI tests: `25 passed; 0 failed`.
- Workspace check: passed.
- Staging CLI build: passed.
- Public hygiene: `passed=true`, `forbiddenHits=[]`.
- Diff hygiene: no whitespace errors; Windows line-ending warnings only.
- PowerShell wrapper smoke: `openclaw-mem-env-tests-ok`.

Pending before cutover/finish:

```powershell
final review pass
commit/push
operator-approved live cutover and post-cutover status/health validation
```

## Live Cutover Note

This changes live progress UX and CLI/status memory metadata. It should follow normal staged validation and the operator cutover path. Do not hot-patch or restart live `.agent-harness` from an active channel session before the intentional cutover step.
