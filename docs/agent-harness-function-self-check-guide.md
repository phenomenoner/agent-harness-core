# Agent Harness Function Self-Check Guide

Date: 2026-06-16

This guide is for coding agents working on `agent-harness-core` when they add, change, or expose a Rust function, CLI command, adapter behavior, receipt schema, or supervisor/runtime path. Use it before claiming the function is valid. The point is not to find a command that passes; the point is to prove the function's intended contract against current code, tests, docs, and runtime-safe smoke evidence.

## Ground Rules

- Start from the current worktree and the current operational handbook; do not rely on memory of earlier sessions.
- Treat validity as unproven until every intended behavior has direct evidence.
- Keep validation scoped to the function's blast radius, but do not shrink the contract to fit the tests you already have.
- Use staging target directories for builds, tests, and smoke runs unless the task explicitly reaches an approved live cutover.
- Do not print secrets, bot tokens, OAuth files, raw provider credentials, raw channel IDs, or private state payloads.
- Do not quote contents from ignored local state such as `.agent-harness`, `.debug`, `.tmp`, or `.review` into tracked docs.
- If a check cannot be proved locally, mark it `unverified` with the exact missing evidence instead of calling it pass.

## Function Contract Card

Write this card in your notes or PR summary before testing:

```text
Function or command:
Changed files:
Caller(s):
Input contract:
Output contract:
State written:
Receipts or logs:
Failure modes:
Retry/idempotency expectation:
Security/trust boundary:
Concurrency or ordering expectation:
Docs/help/skill surfaces:
Minimum proof required:
```

The card should be specific enough that another agent can reproduce the evidence without reading the whole diff.

## Evidence Ladder

Use the strongest evidence that matches the contract:

1. Static proof: type signatures, ownership boundaries, schema fields, parser branches, and redaction paths match the intended behavior.
2. Unit tests: local branch behavior is covered, including negative and edge cases.
3. Integration tests: the function works with the surrounding queue/config/runtime/state layer.
4. CLI smoke: the user-facing command returns the expected machine-readable or human-readable output.
5. State/receipt inspection: the expected files are written, redacted, idempotent, and parseable.
6. Non-live operational smoke: staging harness homes and staging target dirs prove the path without disturbing the live gateway.
7. Live evidence: only after explicit cutover approval, live `healthz`, `status`, receipts, and loop/process state prove the deployed behavior.

Do not use a lower rung as proof for a higher-risk claim. For example, a unit test for a formatter does not prove Telegram delivery, and `cargo check` does not prove JSONL cursor advancement.

## Core Checklist

Use this for every new or changed function:

- Contract: the function name, parameters, return type, errors, and side effects match the intended caller contract.
- Ownership: the function lives in the right crate/module and does not bypass existing helpers for config, state paths, JSONL append, locking, redaction, or receipt writing.
- Inputs: missing, empty, malformed, relative, duplicated, and unsupported inputs are handled deliberately.
- Outputs: JSON fields, enum strings, CLI text, and receipt schemas are stable and match local naming conventions.
- Errors: expected failures return useful status/reason values and do not panic unless the surrounding code already treats that condition as impossible.
- Idempotency: retrying the function does not duplicate durable work unless the contract says it should.
- Concurrency: Windows file locking, JSONL append ordering, runtime leases, worker leases, and scheduler watermarks are preserved where relevant.
- Security: secrets are never logged, serialized into receipts, copied into public exports, or exposed through help text.
- Trust boundary: untrusted channel/model/tool input remains bounded, escaped, redacted, or classified before it can affect shell, filesystem, provider, or live-control behavior.
- Compatibility: existing import snapshots, legacy files, and previous receipt schemas remain readable when upgrade compatibility is expected.
- Documentation: CLI help, README, operations handbook, feature docs, skills, and schema/invariant docs are updated only where the behavior actually changed.

## Feature-Family Checks

### Pure Rust Helpers

- Add unit tests near the module that owns the helper.
- Cover the normal case, empty input, malformed input, boundary values, and any Windows path normalization behavior.
- Prefer structured parsers and typed enums over string matching when the surrounding code has a structured option.
- Verify the helper is not duplicating an existing local utility.

Suggested evidence:

```powershell
cargo test -p agent-harness-core module_name::tests --target-dir target\staging-test-function-check -- --test-threads=1
cargo check -p agent-harness-core --target-dir target\staging-check-function-check
```

Use one real Cargo test filter per command. Replace placeholders such as `module_name::tests` with a filter that exists in the current source tree; `cargo test` does not accept multiple independent filters before `--`.

### CLI Commands And Flags

- Parser accepts the documented flags and rejects unknown flags with a useful error.
- Required flags have explicit missing-value errors.
- Human output and `--json` output are both intentional if both surfaces exist.
- CLI help lists the command and every public flag once.
- Any command that writes state has an idempotent receipt path and never writes outside the requested harness home or output directory.

Suggested evidence:

```powershell
cargo test -p agent-harness-cli cli_module::tests --target-dir target\staging-test-function-check -- --test-threads=1
cargo run -p agent-harness-cli --target-dir target\staging-run-function-check -- <command> --help
```

### State And Receipt Writers

- Paths are rooted under the explicit harness home, source home, output dir, or staging dir.
- JSON and JSONL are parseable after the write.
- Repeated runs either update the same durable record safely or append one clear receipt per attempt.
- Receipts include enough status/reason/context for operators to debug without exposing secrets.
- Large ledgers are sampled or tailed where the existing code expects bounded reads.
- Windows sharing violations are retried or surfaced as retryable/degraded according to the existing module pattern.

Suggested evidence:

```powershell
.\target\staging-build-function-check\debug\agent-harness.exe <command> --harness-home .\.tmp\function-check --json
.\target\staging-build-function-check\debug\agent-harness.exe status --harness-home .\.tmp\function-check --json
```

### Runtime Queue, Worker, And Scheduler Paths

- Queue ids, worker job ids, cron run ids, runtime class, origin, session key, and agent id are preserved through the path.
- Terminal statuses close the right unit of work; retryable statuses remain claimable according to the retry/backoff policy.
- Same-session interactive turns remain serialized where the runtime/session policy requires it.
- Cron work stays in the cron worker/runtime lane and does not open interactive channel work.
- Worker jobs do not execute shell scripts outside configured allowed roots.
- Scheduler lint errors block scheduler enablement; warnings are explained.

Suggested evidence:

```powershell
cargo test -p agent-harness-core runtime_queue::tests --target-dir target\staging-test-function-check-runtime-queue -- --test-threads=1
cargo test -p agent-harness-core workers::tests --target-dir target\staging-test-function-check-workers -- --test-threads=1
cargo test -p agent-harness-core cron::tests --target-dir target\staging-test-function-check-cron -- --test-threads=1
cargo test -p agent-harness-core cron_scheduler::tests --target-dir target\staging-test-function-check-cron-scheduler -- --test-threads=1
.\target\staging-build-function-check\debug\agent-harness.exe worker-status --harness-home .\.tmp\function-check
```

### Channel, Progress, And Delivery Paths

- Ingress permission and channel identity checks fail closed.
- Account id, thread id, reply reference, delivery intent, and platform-specific limits are preserved from structured context, not model text.
- Provider errors are classified into retryable, terminal, or cursor-advancing skip according to the provider contract.
- User-visible progress output does not expose unsanitized tool payloads, raw command output, provider internals, secret values, or sensitive IDs; any operation previews shown in the body/action stream must be capped and redacted.
- Final replies and progress panels use different formatting policies when the product surface requires it.
- Attachments are structured as attachments, not leaked as raw model directives.

Suggested evidence:

```powershell
cargo test -p agent-harness-core progress::tests --target-dir target\staging-test-function-check-progress -- --test-threads=1
cargo test -p agent-harness-core runtime_pipeline::tests --target-dir target\staging-test-function-check-runtime-pipeline -- --test-threads=1
.\target\staging-build-function-check\debug\agent-harness.exe channel-identity-check --harness-home .\.tmp\function-check --platform telegram --account-id default --chat-id smoke --agent main
```

The `channel-identity-check --chat-id smoke` example is a fail-closed negative smoke unless a matching staging binding exists. A positive smoke must create a synthetic staging registry first, for example:

```powershell
New-Item -ItemType Directory -Force .\.tmp\function-check\config
@'
{
  "schema": "agent-harness.channel-identity-registry.v1",
  "bindings": [
    {
      "platform": "telegram",
      "accountId": "default",
      "chatId": "bound-smoke",
      "agentId": "main",
      "enabled": true
    }
  ]
}
'@ | Set-Content -Encoding utf8NoBOM .\.tmp\function-check\config\channel-identity-bindings.json
.\target\staging-build-function-check\debug\agent-harness.exe channel-identity-check --harness-home .\.tmp\function-check --platform telegram --account-id default --chat-id bound-smoke --agent main
```

Use synthetic staging paths such as `.\.tmp\function-check` for examples. They are not an instruction to quote ignored private state, live channel IDs, tokens, or local `.agent-harness` payload contents into docs.

### Memory, Plugin, And Sidecar Paths

- Memory adapters report configured mode honestly; do not imply a live remote service exists when only snapshots are available.
- Recall/store/propose operations preserve per-agent scope and review/approval policy.
- Credential checks report presence, source, length, or redacted metadata only.
- Sidecar script paths resolve to the intended repository or harness path, not to the current shell directory by accident.
- JSON-RPC request and response shapes are stable and include method/status evidence.

Suggested evidence:

```powershell
cargo test -p agent-harness-core memory::tests --target-dir target\staging-test-function-check -- --test-threads=1
.\target\staging-build-function-check\debug\agent-harness.exe memory-read-path-smoke --harness-home .\.tmp\function-check --query smoke --json
.\target\staging-build-function-check\debug\agent-harness.exe plugin-sidecar-call --harness-home .\.tmp\function-check --method sidecar.status --params-json "{}"
```

### Live-Control, Supervisor, And Cutover Paths

- Live gateway control must be protected by the cutover token flow when targeting the live harness.
- `start`, `stop`, `restart`, binary replacement, process termination, and scheduled-task operations must not be hidden inside unrelated commands.
- Generated supervisor scripts use absolute paths, contain no raw tokens, and preserve the intended loop topology.
- Non-live staging is complete before live cutover.
- Post-cutover validation uses `healthz`, `status`, worker status, cron status, direct runner/process state, and an `ops-cutover-receipt`.

Suggested non-live evidence:

```powershell
.\target\staging-build-function-check\debug\agent-harness.exe supervisor-plan --harness-home .\.tmp\function-check --source-home .\.tmp\function-check --workspace .\.tmp\function-check --harness-cli .\target\staging-build-function-check\debug\agent-harness.exe --codex-exe C:\path\to\codex.cmd --agent main
.\target\staging-build-function-check\debug\agent-harness.exe public-hygiene --root .\.public-export\agent-harness-core
```

## Test Selection Rules

- If the function only changes a pure helper, run the focused tests and `cargo check` for the owning crate.
- If the function changes CLI parsing, run the CLI tests and one CLI smoke for the changed command.
- If the function writes durable state, inspect the generated JSON/JSONL and rerun the command to prove idempotency or deliberate append behavior.
- If the function affects runtime/channel/worker/scheduler behavior, run the focused module tests plus the full owning crate tests.
- If the function changes shared contracts, run both `agent-harness-core` and `agent-harness-cli` tests.
- If the function changes public behavior, run formatting, workspace check, docs update, public hygiene, and `git diff --check`.

Common command set for a behavior-changing change:

```powershell
cargo fmt --all --check
cargo check --workspace --target-dir target\staging-check-function-check
cargo test -p agent-harness-core --target-dir target\staging-test-function-check-core -- --test-threads=1
cargo test -p agent-harness-cli --target-dir target\staging-test-function-check-cli -- --test-threads=1
cargo build -p agent-harness-cli --target-dir target\staging-build-function-check
.\target\staging-build-function-check\debug\agent-harness.exe public-hygiene --root .\.public-export\agent-harness-core
git diff --check
```

Line-ending warnings from Git on Windows are not function failures. Whitespace errors reported by `git diff --check` are failures.

## Self-Check Prompt For An Agent

Use this prompt when asking an agent to validate its own new function before handoff:

```text
[AGENT_HARNESS_FUNCTION_SELF_CHECK]
function=<module::function or cli-command>
change_scope=<one sentence>
mode=read-only-validation-first

Validate the new or changed Agent Harness function without relying on prior intent.

1. State the function contract: inputs, outputs, state writes, receipts, failure modes, security/trust boundary, and callers.
2. Identify the exact evidence needed to prove each part of the contract.
3. Inspect the current code paths that implement the function and the current tests/docs that claim it.
4. Run the narrowest tests that actually cover the contract, then broaden if the function touches shared runtime/channel/worker/memory/supervisor behavior.
5. If a CLI or state path exists, run a non-live smoke against a staging harness home or staging output directory.
6. Report PASS/WARN/FAIL per contract item. Mark missing evidence as WARN or FAIL; do not infer pass.
7. Do not print secrets or raw private state. Use receipt paths, schema names, status fields, and redacted summaries.

Return:
- Function:
- Contract:
- Evidence checked:
- Tests/smokes run:
- Findings:
- Residual risk:
- Required follow-up:
```

## Report Format

Use this at handoff:

```text
Function validity self-check

Function:
Scope:
Verdict: PASS | WARN | FAIL

Evidence:
- Static/code:
- Tests:
- CLI/state smoke:
- Docs/help:
- Security/trust:
- Runtime/live impact:

Residual risk:
- ...

Follow-up:
- ...
```

`PASS` means every contract item has direct evidence. `WARN` means the function is usable but has bounded missing evidence or an operator-known limitation. `FAIL` means the function should not be merged, shipped, or cut over.

## Common False Passes

- A command compiles but its help text, docs, or parser branch does not expose the new flag.
- A unit test proves formatting but not provider delivery or cursor advancement.
- A smoke writes a receipt, but the receipt omits the field operators need to debug failures.
- A staging run uses the live harness home by accident.
- A new JSON field is written but not read by the status/readiness path that operators use.
- A function works on a clean temp directory but fails with existing upgraded state.
- A live cutover is declared complete before verifying process/loop topology and post-cutover receipts.
- A doc says a behavior is active, but the builtin harness skill or CLI help still describes the old behavior.
