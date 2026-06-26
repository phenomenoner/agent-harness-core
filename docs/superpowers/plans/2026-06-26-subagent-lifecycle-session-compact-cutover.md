# Round9-1 Subagent Lifecycle, Session Isolation, and Compact Cutover Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Apply the Round9-1 follow-up fixes for subagent lifecycle auth visibility, Codex compact event boundaries, Xiaoxiaoli agent session-state isolation, unattended Codex Windows sandbox config, and guarded live cutover.

**Architecture:** Keep the existing live harness topology intact until the intentional cutover. Fix protocol/session behavior in the core Rust modules with regression tests first, preserve already-staged lifecycle changes, then build a staging binary and apply it through `ops-cutover-*` with config backup and post-cutover receipts.

**Tech Stack:** Rust workspace (`agent-harness-core`, `agent-harness-cli`), PowerShell operator commands, existing `.agent-harness` live home, existing `.debug/round9-1/subagent-lifecycle` matrix and receipt paths.

---

### Task 1: Preserve Existing Subagent Lifecycle Cutover Scope

**Files:**
- Verify only: `crates/agent-harness-core/src/subagent_lifecycle.rs`
- Verify only: `crates/agent-harness-core/src/workers.rs`
- Read: `.debug/round9-1/subagent-lifecycle/external-agent-apply-live-cutover-instructions.md`

- [ ] **Step 1: Confirm staged lifecycle diff**

Run:

```powershell
git diff -- crates\agent-harness-core\src\subagent_lifecycle.rs crates\agent-harness-core\src\workers.rs
```

Expected: `authVisibility` reports `verified` when provider and auth lane are both present, and worker tests assert `auth_visibility == "verified"`.

- [ ] **Step 2: Keep unrelated files untouched**

Run:

```powershell
git status --short
```

Expected: existing unrelated `?? .external/` remains untracked and is not added to this change.

### Task 2: Codex Official Compact Event Boundary Regression

**Files:**
- Modify: `crates/agent-harness-core/src/codex_runtime.rs`

- [ ] **Step 1: Write failing regression test**

Add a test near `run_codex_runtime_preflight_compacts_existing_thread_before_turn`:

```rust
#[test]
fn run_codex_runtime_preflight_drains_compact_turn_completed_before_user_turn() {
    let root = temp_root("run_codex_runtime_preflight_drains_compact_turn_completed_before_user_turn");
    let source = write_codex_runtime_source(&root);
    let harness_home = root.join(".agent-harness");
    fs::create_dir_all(&harness_home).unwrap();
    fs::write(
        harness_home.join(HARNESS_CONFIG_FILE_NAME),
        r#"{"codexContext":{"modelContextWindow":1000,"compactAtActiveContextRatio":0.5,"modelAutoCompactTokenLimit":900}}"#,
    )
    .unwrap();
    enqueue_and_prepare(&source, &harness_home);
    let plan_report = plan_codex_runtime(CodexRuntimePlanOptions {
        harness_home: harness_home.clone(),
        execution_dir: None,
        codex_executable: Some(std::env::current_exe().unwrap()),
    })
    .unwrap();
    let plan = plan_report.plan.as_ref().unwrap();
    let plan_file = plan_report.plan_file.as_ref().unwrap();
    replace_env_requirements(plan_file, serde_json::json!([]));
    replace_invocation_thread_id(plan_file, Some("thread-existing"));
    fs::write(
        harness_home
            .join("state")
            .join("runtime-queue")
            .join("codex-runtime-run-receipts.jsonl"),
        format!(
            "{}\n",
            serde_json::json!({
                "codexBindingFile": plan.outputs.codex_binding_file.to_string_lossy(),
                "usage": { "inputTokens": 920, "outputTokens": 10, "totalTokens": 930, "source": "test" }
            })
        ),
    )
    .unwrap();
    let (executable, arguments, events_file) =
        compact_turn_completed_then_reply_app_server_command(&root);
    replace_invocation(plan_file, executable, arguments);

    let report = run_codex_runtime(CodexRuntimeRunOptions {
        harness_home: harness_home.clone(),
        execution_dir: None,
        plan_file: None,
        timeout_ms: 10_000,
        idle_timeout_ms: 10_000,
        progress_context: None,
    })
    .unwrap();

    assert_eq!(report.receipt.status, CodexRuntimeRunStatus::Completed);
    let transcript =
        fs::read_to_string(report.completion.as_ref().unwrap().transcript_file.as_ref().unwrap())
            .unwrap();
    assert!(transcript.contains("Reply after compact turn boundary."));
    assert!(!transcript.contains("(no assistant text captured"));
    let events = fs::read_to_string(events_file).unwrap();
    assert!(events.contains("thread/compact/start"));
    assert!(events.contains("turn/start"));
}
```

- [ ] **Step 2: Verify RED**

Run:

```powershell
cargo test -p agent-harness-core run_codex_runtime_preflight_drains_compact_turn_completed_before_user_turn --target-dir target\staging-round9-1-session-compact-red -- --test-threads=1
```

Expected before implementation: FAIL because the stale compact `turn/completed` is treated as the user turn and no assistant final text is captured.

- [ ] **Step 3: Implement minimal protocol fix**

Update `wait_for_context_compaction_completed()` so `item/completed` for `contextCompaction` records completion but waits for the compact `turn/completed` or `thread/compacted` event before returning. Update `wait_for_turn_completed()` so a compact-only `turn/completed` with `items=[]` and no assistant text is ignored as a stale boundary instead of completing the user turn. Update `finish_codex_runtime_run()` so `Completed` with empty assistant text becomes `ProtocolError` with no transcript placeholder.

- [ ] **Step 4: Verify GREEN**

Run the same filtered test. Expected: PASS.

### Task 3: Xiaoxiaoli Agent Session-State Isolation Regression

**Files:**
- Modify: `crates/agent-harness-core/src/turns.rs`
- Modify: `crates/agent-harness-core/src/channel_state.rs`
- Modify: `crates/agent-harness-core/src/prompt.rs`

- [ ] **Step 1: Write failing `build_turn_plan` regression**

Add a test in `turns.rs`:

```rust
#[test]
fn turn_plan_ignores_channel_state_session_for_different_agent() {
    let root = temp_root("turn_plan_ignores_channel_state_session_for_different_agent");
    let source = write_turn_source(&root);
    let harness_home = root.join(".agent-harness");
    write_channel_state(
        &harness_home,
        r#"{
          "schema": "agent-harness.channel-session-state.v1",
          "platform": "telegram",
          "channelId": "dm",
          "userId": "user",
          "activeSessionKey": "telegram:dm:user:main:session-1",
          "agentId": "main",
          "provider": "openai",
          "model": "gpt-5",
          "modelOverrideProvider": "openai",
          "modelOverrideModel": "gpt-5",
          "updatedAtMs": 1000
        }"#,
    );
    let registry = load_agent_registry(&source).unwrap();
    let skills = build_source_skill_index(&source).unwrap();

    let plan = build_turn_plan(
        &source,
        &registry,
        &skills,
        TurnPlanInput {
            harness_home: Some(harness_home),
            platform: "telegram".to_string(),
            channel_id: "dm".to_string(),
            user_id: "user".to_string(),
            text: "hello".to_string(),
            inbound_context: None,
            inbound_media_artifacts: Vec::new(),
            requested_agent_id: Some("other".to_string()),
            session_hint: None,
            skill_limit: 3,
        },
    )
    .unwrap();

    assert_eq!(plan.agent.as_ref().unwrap().id, "other");
    assert_eq!(plan.session_key, "telegram:dm:user:other");
    assert_eq!(plan.model_policy.model.as_deref(), Some("gpt-5.4"));
    assert!(plan.channel_state.is_none());
}
```

- [ ] **Step 2: Write failing OperationPlan visibility regression**

Add a test in `prompt.rs` that creates an open OperationPlan for `agent_id="main"` and `session_key="telegram:dm:user:main:session-1"`, assembles a prompt for an `other`/Xiaoxiaoli-style turn, and asserts the OperationPlan section does not include the main plan id/item id.

- [ ] **Step 3: Verify RED**

Run:

```powershell
cargo test -p agent-harness-core turn_plan_ignores_channel_state_session_for_different_agent --target-dir target\staging-round9-1-session-compact-red -- --test-threads=1
cargo test -p agent-harness-core prompt_bundle_hides_main_operation_plan_from_other_agent --target-dir target\staging-round9-1-session-compact-red -- --test-threads=1
```

Expected before implementation: first test FAILS by reusing `main` session/model state; second test FAILS by showing fallback OperationPlan context to the non-main agent.

- [ ] **Step 4: Implement minimal isolation fix**

In `turns.rs`, load channel state only when `state.agent_id` and the embedded agent segment in `activeSessionKey` are compatible with the selected agent. If not compatible, ignore it for session key, model overrides, thinking overrides, skill query text, and prompt `channel_state`. Keep explicit `session_hint` precedence unchanged.

In `channel_state.rs`, when applying `StartNewSession`, derive `state.agent_id` from the new session key so `/new` for `xiaoxiaoli` persists the selected agent segment rather than carrying a previous `main` identity.

In `prompt.rs`, only use fallback OperationPlan snapshots for `main`/unspecified agent turns. Non-main turns should see matching plans only by same session key or same agent id.

- [ ] **Step 5: Verify GREEN**

Run the same filtered tests. Expected: PASS.

### Task 4: Documentation, Config Patch, and Validation Matrix

**Files:**
- Modify: `.debug/round9-1/subagent-lifecycle/status.md`
- Modify live during cutover only: `.agent-harness/harness-config.json`
- Modify if stale: `docs/agent-harness-operations-handbook.md`

- [ ] **Step 1: Apply live config backup and patch during cutover window**

Back up `.agent-harness/harness-config.json` under `.debug/round9-1/subagent-lifecycle/cutover-backups/<stamp>/`, then replace legacy `"codexSandbox": "elevated"` with `"codexSandboxMode": "disabled"` while preserving `"codexSandboxPolicy": "dangerFullAccess"` and `"codexApprovalPolicy": "accept"`.

- [ ] **Step 2: Run staged verification**

Run:

```powershell
cargo fmt --all -- --check
cargo check --workspace --target-dir target\staging-subagent-lifecycle-cutover-check
cargo test -p agent-harness-core --target-dir target\staging-subagent-lifecycle-cutover-core -- --test-threads=1
cargo test -p agent-harness-cli --target-dir target\staging-subagent-lifecycle-cutover-cli -- --test-threads=1
& D:\Warehouse\Rust-OpenClaw-Core\.debug\round9-1\subagent-lifecycle\run-matrix.ps1 -TargetDir target\staging-subagent-lifecycle-cutover-matrix
cargo build -p agent-harness-cli --bin agent-harness --target-dir target\staging-subagent-lifecycle-live-build
git diff --check
```

Expected: all commands pass; matrix reports `allOk=true; G1=true; G2=true; G3=true; G4=true; G5=true`.

### Task 5: Guarded Live Cutover, Receipts, Commit, Push

**Files:**
- Modify: `.debug/round9-1/subagent-lifecycle/status.md`
- Modify: `docs/agent-harness-operations-handbook.md`

- [ ] **Step 1: Request, approve, apply cutover**

Use `ops-cutover-request`, `ops-cutover-approve`, `ops-cutover-apply`, then `.\harness.ps1 gateway restart --live-control-token <token>` exactly through the handbook/operator flow. Do not use ad hoc process killing unless the handbook recovery path requires it.

- [ ] **Step 2: Run post-cutover checks**

Run:

```powershell
.\target\debug\agent-harness.exe healthz --harness-home .\.agent-harness --require-writable-state
.\target\debug\agent-harness.exe status --harness-home .\.agent-harness --json
.\target\debug\agent-harness.exe worker-status --harness-home .\.agent-harness
.\target\debug\agent-harness.exe codex-plan --harness-home .\.agent-harness
Select-String -LiteralPath .\.agent-harness\codex-home\config.toml -Pattern "\[windows\]|sandbox ="
& .\.debug\round9-1\subagent-lifecycle\run-matrix.ps1 -TargetDir target\staging-subagent-lifecycle-postcutover-matrix
```

Expected: health ready/live, readiness failed=0, no pending/leased/running retryable worker work, no `[windows]` or `sandbox = "elevated"` in generated Codex config, post-cutover matrix all green.

- [ ] **Step 3: Record receipts and update live validation docs**

Append `status.md` with ticket id, token holder, candidate hash, previous binary backup/hash, config backup path, post-cutover receipt paths, Codex config readback, and sandbox log tail summary. Update handbook Current Live Validation with the new source commit, binary hash, backup label, validation commands, and post-cutover state.

- [ ] **Step 4: Commit and push**

Run final status review, stage only intended files, commit with an accurate message, and push the current branch.

---

---

## Execution Evidence - 2026-06-26 Pre-Cutover

- Implementation completed for compact boundary draining, empty assistant output rejection, denied-approval safety-notice-only rejection, per-agent channel session isolation, `/new` agent recording/routing reset, non-main OperationPlan filtering, and lifecycle auth visibility `verified` receipts.
- Reviewer follow-up completed: a static reviewer found that harness safety notices could make an empty Codex final look non-empty; `run_codex_runtime_rejects_completed_turn_with_only_harness_notice` now covers and prevents that path.
- Repo-local Codex CLI/backend updated to `0.142.2`, the latest stable release on `openai/codex`; `0.143.0-alpha.*` remains pre-release.
- Fresh validation passed after the reviewer follow-up: focused regressions, full core/CLI suites, Python gateway helper tests, Round9-1 matrix `20260626-195937-matrix.json` with `allOk=true`, staged candidate build SHA-256 `84D0A2418127449B089C8D0158A1B4AC6A0DB8280FCB5A473BD3D5C4551E1F2D`, public hygiene `forbiddenHits=[]`, and `git diff --check`.
- Live cutover was intentionally not applied during the pre-cutover pass: approval was required for the live config change that disables Codex Windows sandbox mode and enables unattended approval acceptance.

## Execution Evidence - 2026-06-26 Live Cutover

- Operator approval was received for the live config patch and restart.
- Live config backup: `.debug\round9-1\subagent-lifecycle\cutover-backups\20260626-202742\harness-config.json.pre-round9-1-session-compact-codex-0.142.2-cutover`.
- Live security block after patch: `codexApprovalPolicy=accept`, `codexSandboxMode=disabled`, `codexSandboxPolicy=dangerFullAccess`; legacy `codexSandbox` was removed. `config-validate --harness-home .\.agent-harness` returned `status=valid`.
- Guarded cutover ticket: `cutover-1782477023513`; request/apply receipts were appended under `.agent-harness\state\cutover\`.
- Previous live binary backup: `target\debug\agent-harness.pre-round9-1-session-compact-20260626-203023.exe`, SHA-256 `946D0AC01F6DF266D2D356A5A8342B3D87238E9E7D56C8C653CDEC079BACA908`.
- Canonical live binary after copy: `target\debug\agent-harness.exe`, SHA-256 `84D0A2418127449B089C8D0158A1B4AC6A0DB8280FCB5A473BD3D5C4551E1F2D`, size `21623296`.
- Post-restart repair: the generated scheduled-task start path initially restored seven direct runners, leaving stale `telegram-loop-xiaoxiaoli`; a scoped `supervisor-reconcile --desired-services-json ... --apply` launched only `telegram-loop-xiaoxiaoli` and verified supervisor/child command lines with `--agent xiaoxiaoli` and `--telegram-account xiaoxiaoli`.
- Generated Codex config refresh: `codex-plan` rewrote both `.agent-harness\codex-home\config.toml` and `.agent-harness\codex-home-providers\openrouter\config.toml`; neither contains `[windows]` or `sandbox =`.
- Sandbox log tail check: no new post-cutover `codex-windows-sandbox-setup.exe` refresh was observed; the latest matching tail entries were pre-cutover lines around 16:21.
- Post-cutover validation:
  - `healthz --harness-home .\.agent-harness --require-writable-state`: `ready=true`, `live=true`, `readinessReady=true`, eight live non-stale loop heartbeats.
  - `status --harness-home .\.agent-harness --json`: readiness `passed=58`, `warnings=2`, `failed=0`.
  - `worker-status --harness-home .\.agent-harness`: `pending=0`, `leased=0`, `running=0`, `failedRetryable=0`, `failedTerminal=5`, `runtimeOpenItems=0`, `activeCronRuns=0`.
  - Post-cutover matrix receipt `.debug\round9-1\subagent-lifecycle\receipts\20260626-204739-matrix.json`: `allOk=true`, gates G1-G5 true.
  - `ops-cutover-receipt --harness-home .\.agent-harness`: `status=ready`, readiness `passed=58`, `warnings=2`, `failed=0`.
