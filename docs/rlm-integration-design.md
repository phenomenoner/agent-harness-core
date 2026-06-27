# RLM Integration Design

Date: 2026-06-26

Status: Design note. Not implemented.

References:

- Recursive Language Models paper: https://arxiv.org/abs/2512.24601
- Trampoline AI `predict-rlm`: https://github.com/Trampoline-AI/predict-rlm
- `predict-rlm` license: MIT, with copyright and license notice retention required by the upstream license.

## Summary

Recursive Language Models (RLMs) are useful for bounded, evidence-heavy reasoning tasks inside a longer agent workflow. They should not replace Agent Harness runtime/session/subagent orchestration. The recommended boundary is:

- Agent Harness owns lifecycle, queueing, leases, watchdogs, receipts, human steering, context rollover, permissions, and side effects.
- RLM owns bounded reasoning over large evidence sets, structured extraction, recursive sub-LM calls, and artifact-local analysis.

The practical direction is to add an Agent Harness-specific RLM adapter/module, not to make `predict-rlm` a hard dependency of `agent-harness-core`. A later Rust-native RLM control loop may be worthwhile, but it should start from a harness-owned interface and trace schema rather than from a full runtime rewrite.

## Background

The RLM paper describes a model that can recursively invoke model calls while using external context and programmatic control flow instead of relying only on one fixed prompt. The `predict-rlm` project implements this idea as a Python/DSPy runtime:

- The root LM writes Python actions in a REPL-like environment.
- Intermediate variables and files persist between iterations.
- The model can call `predict(...)` with DSPy signatures for typed sub-LM work.
- The loop continues until the generated code calls `SUBMIT`.
- Traces capture generated code, observations, tool calls, predict calls, timings, usage, and errors.
- Backends include JSPI/Deno/Pyodide and native/sandboxed supervisor paths.

This is a good fit for tasks where the hard part is reading, filtering, chunking, cross-checking, and summarizing a large amount of evidence. It is not a complete replacement for a durable long-task control plane.

## Best-Fit Scenarios

Use an RLM engine inside a harness worker/subagent when the subtask is evidence-heavy and bounded:

- Large paper, spec, design note, or issue-thread analysis.
- CI/log/failure triage where the relevant evidence spans long logs and many files.
- Codebase impact analysis before changing shared runtime, schema, queue, or lifecycle behavior.
- Artifact QA for PDF, spreadsheet, DOCX, benchmark, report, or generated asset outputs.
- Context rollover support, where a worker turns a long transcript, diffs, test logs, and subagent outputs into structured working-set memory.

Avoid using RLM as the direct owner for live operational side effects:

- Gateway stop/start/restart or cutover.
- Auth, permission, credential, or channel identity changes.
- Destructive filesystem operations.
- Public pushes, external messages, purchases, trades, or spend.
- Any task that requires durable approval, receipt, or operator-controlled rollback.

## Comparison With Agent Harness Virtual Sessions

Agent Harness virtual-session behavior is an operational continuity mechanism. It combines session keys, session-lane serialization, continuation metadata, working-set memory, context rollover records, queue items, receipts, and worker/subagent lifecycle state.

This is stronger than RLM for long-running operational work because it provides:

- Durable queue and worker state.
- Same-session FIFO protection.
- Leases and concurrency limits.
- Idempotency keys.
- Watchdog jobs and master wakeups.
- Context compact/retry/checkpoint/new-thread fallback.
- Human-channel progress and final delivery boundaries.
- Recovery receipts for terminal, timeout, failed, canceled, skipped, and dead-letter states.

RLM is stronger than a normal virtual-session prompt for bounded deep analysis because it provides:

- Programmatic access to large evidence.
- Persistent local variables/files within the reasoning run.
- Typed recursive sub-LM calls.
- Structured trace of reasoning steps and subcalls.
- Better map/reduce ergonomics for large documents, logs, and code surfaces.

The two systems are complementary. Agent Harness should be the outer control plane. RLM can be an inner reasoning engine used by selected worker jobs.

## Integration Options

### Option A: Loose Plugin Or Sidecar

Call `predict-rlm` from a worker as an external Python sidecar.

Advantages:

- Fastest way to validate usefulness.
- Reuses upstream REPL, DSPy signatures, skills, file handling, and traces.
- Keeps Python/Deno/Pyodide dependencies outside the Rust core.

Disadvantages:

- Timeout, budget, artifact paths, trace merging, and failure classification can become ad hoc if the boundary is not defined first.
- Operational semantics differ from harness-native worker jobs.
- The Python runtime still owns parts of execution state during the run.

This is the recommended prototype path.

### Option B: Agent Harness-Specific Adapter Module

Add a dedicated adapter crate or module such as `agent-harness-rlm` or `rlm_worker`. The adapter owns conversion between harness worker jobs and a concrete RLM backend.

Responsibilities:

- Build a controlled `RlmJobSpec`.
- Prepare a staged workspace and allowed input artifacts.
- Start the selected backend process.
- Enforce wall-clock timeout and budget limits.
- Collect result JSON, trace JSON, usage, stderr/stdout summaries, and output artifacts.
- Convert backend failures into harness failure classes.
- Write durable receipts through the worker/job lifecycle.

Advantages:

- Preserves harness ownership of lifecycle and side effects.
- Keeps backend dependencies optional.
- Makes it possible to swap `predict-rlm` for a Rust-native control loop later.

Disadvantages:

- Requires a stable schema before broad use.
- Still needs careful policy around tool access, package install, file sync, and network.

This is the recommended production boundary.

### Option C: Rust-Native RLM Control Loop

Implement the RLM orchestration semantics in Rust while keeping execution backends pluggable.

Rust should own:

- Iteration loop.
- Budget and recursion limits.
- Subcall scheduling.
- Trace schema.
- Artifact registration.
- State persistence.
- Permission policy.
- Failure classification.

The first Rust-native version should not try to reimplement every sandbox backend. It can call a constrained Python subprocess, Deno/Pyodide runner, WASI runner, or harness-approved tool runner behind a backend trait.

Advantages:

- Best fit with worker leases, receipts, queue state, and context rollover.
- Lower orchestration overhead and fewer cross-language lifecycle surprises.
- Easier durable iteration/subcall persistence.
- Easier policy enforcement for local files, tools, and network access.

Disadvantages:

- Full parity with `predict-rlm` would be expensive.
- The hard part is safe model-written code execution, not the Rust loop itself.
- LLM latency dominates many workloads, so raw speed gains may be modest.

This is a later-phase option after the adapter proves value.

### Option D: Direct Core Dependency

Make `predict-rlm` a required dependency of `agent-harness-core`.

This is not recommended. It would couple the Rust control plane to Python, DSPy, Deno/Pyodide or SBX backend assumptions, and upstream runtime semantics. It would also make core builds, tests, release hygiene, and live operations more fragile.

## Recommended Architecture

Add a harness-owned interface and keep backends replaceable.

Conceptual types:

```text
RlmJobSpec
  job_id
  parent_worker_job_id
  task
  input_artifacts
  allowed_tools
  output_schema
  max_iterations
  max_subcalls
  max_depth
  max_wall_time_ms
  max_tokens
  sandbox_policy
  network_policy
  artifact_policy

RlmResult
  status
  structured_output
  trace_ref
  artifact_refs
  usage
  failure_class
  stderr_summary
  stdout_summary

RlmTrace
  iterations[]
  subcalls[]
  tool_calls[]
  artifacts[]
  usage_events[]
  failure_events[]
```

The worker store remains the source of truth. An RLM run is a worker job attempt, not an independent long-running agent outside the harness. If an RLM job needs to fan out, the fan-out should either be internal typed subcalls with strict budget limits or explicit child worker jobs created by the harness.

## Failure And Recovery Policy

RLM failures should be normalized into harness-visible classes:

- `invalid_request`: schema, policy, or input artifact error before execution.
- `budget_exhausted`: max iterations, subcalls, depth, wall time, or token budget reached.
- `backend_unavailable`: Python/Deno/SBX/backend startup failed.
- `sandbox_fatal`: backend crashed or lost unrecoverable state.
- `tool_policy_denied`: requested tool/file/network operation was not allowed.
- `output_invalid`: final output failed schema validation.
- `model_error`: upstream model call failed after retries.
- `partial_success`: usable result exists but some evidence or artifact processing failed.

The adapter should emit compact operator summaries and keep full traces/artifacts behind paths. It should not print secrets, raw `.env` contents, provider tokens, or unrelated workspace data.

## Security And Trust Boundary

Model-written code is untrusted. The adapter must default to least privilege:

- Stage input artifacts into a task workspace.
- Expose only declared files and tools.
- Deny network unless explicitly allowed by job policy.
- Deny package installation unless explicitly allowed by backend policy.
- Keep output sync explicit and path-validated.
- Record every host tool call in the trace.
- Apply the same live-harness safety rules as other worker jobs.

RLM may recommend live operations, but it must not execute live gateway control directly.

## Implementation Phases

### Phase 1: Prototype Adapter

- Add a small sidecar invocation path around upstream `predict-rlm`.
- Run only on staged artifacts and fake/offline fixtures.
- Capture result, trace, stdout/stderr summaries, and artifact paths.
- Add one or two evidence-heavy smoke tasks, such as log triage and docs synthesis.

Exit criteria:

- The RLM output is better than a normal subagent prompt on at least one real evidence-heavy task.
- Failures are visible and classified.
- No live gateway, credential, or destructive operation is reachable through the prototype.

### Phase 2: Harness-Native Schemas

- Define `RlmJobSpec`, `RlmResult`, and `RlmTrace` schemas.
- Add receipt paths and worker job payload conventions.
- Add budget fields and deterministic failure classification.
- Add tests for policy-denied, timeout, invalid-output, and partial-success cases.

Exit criteria:

- The worker system can treat RLM jobs like other durable jobs.
- The same job spec can target the upstream adapter or a fake test backend.

### Phase 3: Rust Control Loop

- Implement the iteration loop, budget accounting, trace recording, and subcall scheduling in Rust.
- Keep execution backends behind a trait.
- Start with a fake backend and one constrained external execution backend.

Exit criteria:

- RLM iteration and subcall state can be persisted through harness receipts.
- A failed or timed-out backend does not corrupt the worker queue.
- The old adapter can remain as a compatibility backend.

### Phase 4: Production Hardening

- Add staged soak tasks.
- Add public hygiene checks for trace redaction.
- Add operator docs and CLI status surfaces.
- Decide whether to keep `predict-rlm` as an optional backend, retire it, or retain it only for comparison.

## Open Questions

- Which first evidence-heavy task should be the benchmark: CI log triage, docs synthesis, codebase impact analysis, or context rollover compression?
- Should RLM jobs be a new worker kind such as `rlm_reasoning`, or a subtype of an existing `llm_subagent` job?
- Should RLM subcalls be internal-only, or should selected subcalls become explicit child worker jobs for stronger durability?
- What artifact retention policy should apply to full traces, generated code, and sandbox workspaces?
- Which backends are acceptable for live-adjacent staged use: Python subprocess, Deno/Pyodide, SBX, WASI, or fake-only until a security review?

## Decision

Use `predict-rlm` as a reference implementation and optional prototype backend. Build the harness contract first. If the prototype proves value, implement a harness-native RLM adapter/module. A Rust-native RLM control loop should be considered after the schema and operational boundary are stable.

Do not add `predict-rlm` as a hard dependency of `agent-harness-core`, and do not let RLM own long-task lifecycle, permissions, live operations, or recovery.
