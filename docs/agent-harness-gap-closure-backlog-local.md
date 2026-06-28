# Agent Harness Gap Closure Backlog (Local/Staging)

Status: active local backlog
Started: 2026-06-28
Scope: close repo-local implementation, tests, and documentation gaps for memory ownership, long task resilience, and multi-agent isolation. Live cutover is intentionally out of scope and remains an operator handoff.

## Operating Constraints

- Use staging target directories for builds and tests.
- Do not stop, restart, replace, or mutate the live `.agent-harness` gateway.
- Keep topology, invariants, release checklist, and operator self-check docs aligned whenever expected behavior changes.
- Run Claude second-brain review loops for planning-heavy or high-risk slices with scoped context only.
- Preserve existing unrelated working-tree changes; do not revert user/operator edits.

## Gap Clusters

### G1 Memory Ownership

Promotion labels:
- `openclaw-mem-full-parity-gap`
- `per-agent-memory-recall-compartment-gap`

Design target:
- Active memory ownership is explicit in receipts and status.
- Non-main/public agents receive only their own memory plus explicitly approved public/global sources.
- Global imported/private `main` memory is excluded for public/non-main agents unless a policy explicitly allows it.
- Fallback read paths remain visibly read-only and cannot be mistaken for full openclaw-mem parity.

Backlog:
- [x] G1.1 Add or update tests proving non-main/public recall policy excludes private/global imported memories by default.
- [x] G1.2 Add filtered-count and source-scope/trust telemetry where recall policy filters memory context.
- [ ] G1.3 Ensure status/read-path smoke surfaces distinguish per-agent recall compartment readiness from openclaw-mem full parity.
- [x] G1.4 Update topology, invariants, release checklist, and operator docs with the new gate evidence.

Evidence to collect:
- Focused memory policy tests. Current local evidence: `cargo test -p agent-harness-core non_main_memory_prompt_context_excludes_global_imported_snapshot_by_default --target-dir target\staging-round10-gap-closure-memory-green -- --test-threads=1` passed on 2026-06-28 after a red run caught global imported memory leakage.
- Status or smoke fixture showing `globalImportedSnapshotAllowed=false` or equivalent filtered policy for a non-main/public agent. Current local evidence: `memory::tests::non_main_memory_prompt_context_excludes_global_imported_snapshot_by_default` asserts Xiaoxiaoli prompt/service recall receipts contain `globalImportedSnapshotAllowed=false`, source scope `agent-service-recall`, and no `main-private` text.
- Documentation refs updated with exact test names.

### G2 Long Task Resilience

Promotion labels:
- `virtual-session-continuity-gap`
- `progress-delivery-volume-gap`
- `progress-final-surface-gap`
- `tool-use-timeout-recovery-gap`

Design target:
- Long tasks survive compaction/rollover through a stable virtual session and fresh concrete sessions.
- Progress panels are bounded and terminally convergent.
- Final outbox payloads contain final answers or terminal notifications only, never diagnostic progress streams.
- Tool-use idle timeout recovery is bounded, traceable, and does not lose queue/final-outbox control.

Backlog:
- [ ] G2.1 Add an end-to-end forced-rollover scenario covering fresh session key, same virtual session, working-set prompt injection, guarded unsafe-state handling, and final delivery trace.
- [ ] G2.2 Add Telegram/Discord replay fixtures for progress edit caps, repeated text-hash suppression, terminal convergence, final-outbox presence, and no post-terminal edit churn.
- [ ] G2.3 Extend progress-final-surface replay coverage to compact-before-turn fallback and fresh-thread recovery paths.
- [ ] G2.4 Add per-tool timeout/cancellation trace fixtures or document the remaining supervisor parity boundary if not implementable in this slice.
- [ ] G2.5 Add a regression for Claude/second-brain review as a long-running external tool/review loop: review output or timeout must not be mistaken for final workflow closure, and the parent task must keep a resumable queue/control receipt. Timeout recovery is locally covered by `codex_runtime::tests::run_codex_runtime_recovers_tool_use_idle_timeout_with_fresh_thread`; review-only recovery output is locally covered by `codex_runtime::tests::run_codex_runtime_keeps_external_review_recovery_as_evidence_not_final`. Broader runtime-pipeline resumability/outbox replay remains open.
- [x] G2.6 Add a `/new` task-boundary regression: Telegram DM `/new` must start a new concrete session and a new virtual/task boundary so context packing and memory recall cannot make a fresh request look like a continuation of the prior task.

Evidence to collect:
- Focused runtime pipeline/progress/codex runtime tests.
- Replay fixture receipts under ignored debug/state paths only when needed.
- Documentation refs updated with exact test names and remaining boundaries.
- Current local evidence: `prompt::tests::prompt_bundle_new_command_boundary_skips_prior_task_memory_context` failed before the fix by injecting `old-social-image-continuation` from imported memory after `/new`, then passed with `cargo test -p agent-harness-core prompt_bundle_new_command_boundary_skips_prior_task_memory_context --target-dir target\staging-round10-gap-closure-new-boundary-green -- --test-threads=1`.
- Current partial G2.5 evidence: `codex_runtime::tests::run_codex_runtime_recovers_tool_use_idle_timeout_with_fresh_thread` uses a fake `commandExecution` command `claude -p review prompt` and passed with `cargo test -p agent-harness-core run_codex_runtime_recovers_tool_use_idle_timeout_with_fresh_thread --target-dir target\staging-round10-gap-closure-external-review-green -- --test-threads=1`. `codex_runtime::tests::run_codex_runtime_keeps_external_review_recovery_as_evidence_not_final` passed with `cargo test -p agent-harness-core run_codex_runtime_keeps_external_review_recovery_as_evidence_not_final --target-dir target\staging-round10-gap-closure-external-review-green -- --test-threads=1`.
- Current T3 context-preflight evidence: `codex_runtime::tests::run_codex_runtime_preflight_compacts_high_usage_bound_thread_without_model_window` failed first with `context_recovery=None` for a 136602-token replay and then passed with `cargo test -p agent-harness-core run_codex_runtime_preflight_compacts_high_usage_bound_thread_without_model_window --target-dir target\staging-round10-context-preflight-green -- --test-threads=1`, proving high bound-thread usage forces compact before the next turn even without `modelContextWindow`.

### G3 Multi-Agent Isolation

Promotion labels:
- `multi-agent-full-matrix-gap`
- `scenario-matrix-coverage-gap`

Design target:
- Any configured agent can own workspace/prompt source, channel state, provider/model settings, memory namespace, context history, runtime lane, worker/subagent leases, final outbox, and delivery trace while sharing harness infrastructure.
- Same-agent stale-session suppression still works.
- A non-main agent sharing the same platform/channel/user is not suppressed by another agent's active session state.

Backlog:
- [ ] G3.1 Add a reusable configured-agent scenario matrix fixture or command pack.
- [ ] G3.2 Cover channel command state, provider/model override, memory/context history, worker lease/subagent lifecycle, final outbox, and delivery trace in the matrix.
- [ ] G3.3 Add or update docs so agentId remains a routing boundary across channel state, runtime queue/pipeline, prompt assembly, outbox/delivery, memory, and supervisor/cutover behavior.
- [ ] G3.4 Keep Xiaoxiaoli/non-main regressions as named evidence in release docs.

Evidence to collect:
- Focused agent-boundary tests with exact test names.
- Matrix output or documented command pack.
- Updated topology contract impact matrix.

## Current Slice

Selected slice: focused prompt/memory boundary hardening plus documentation alignment.

Candidate first slice:
1. Memory recall compartment policy test and telemetry. Status: local green for non-main prompt/service fallback compartment.
2. `/new` fresh-task prompt/memory boundary. Status: local green for prompt assembly; broader virtual-session rollover replay remains open.
3. External review loop resilience. Status: local replay covered; timeout recovery and review-only evidence capture are green in `codex_runtime`, and `runtime_pipeline::tests::run_runtime_queue_once_keeps_external_review_evidence_resumable_without_final_outbox` proves review-only evidence keeps the parent queue retryable without writing a final outbox. Live/soak evidence remains open before promoting G2.5 to full parity.
4. Context preflight high-usage guard. Status: local T3 replay green for high prior bound-thread usage without `modelContextWindow`; broader Telegram/Discord final-outbox replay remains open.
5. Configured-agent matrix fixture scaffold.

Sidecar status:
- Long-task resilience sidecar returned a read-only review. Use as advisory only.
- Memory and multi-agent sidecar status readback returned `not_found` after turn abort; do not treat them as evidence.

Selection rule:
- Prefer the smallest slice that closes a documented promotion sub-gate with a deterministic local test.

## Validation Checklist

- [ ] `cargo fmt --all -- --check`
- [ ] Focused tests for changed modules under a staging target dir.
- [ ] Broader `cargo test -p agent-harness-core` or justified narrower equivalent.
- [ ] Documentation and schema/invariant checks relevant to changed files.
- [ ] Public hygiene if public docs or exported surfaces changed.
- [ ] Claude second-brain review for the scoped plan/diff.
- [ ] Cleanup of task-scoped staging/debug artifacts that are not retained as evidence.

## Cutover Handoff

Live cutover is not part of this backlog execution. Before handoff, record:
- candidate branch/commit,
- validation commands and results,
- retained candidate build or absence thereof,
- expected live config changes, if any,
- rollback notes,
- artifacts intentionally retained.
