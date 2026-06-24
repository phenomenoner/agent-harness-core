# Project Agent Instructions

## Start Here

- For a new working session, read [docs/agent-harness-operations-handbook.md](docs/agent-harness-operations-handbook.md) first: it holds the live topology, current live validation, the full command walkthrough, and the documentation map. The root `README.md` is the public-facing overview, not the operational source of truth.

## Default Superpowers For Development

- For programming or software-development tasks, default to enabling and following [$superpowers](C:\Users\user\.agents\skills\superpowers\SKILL.md) before implementation unless the user explicitly says not to use it.
- Treat this repo-level AGENTS section, or a user `[$superpowers]` mention, as the explicit project invocation for this repository if the standalone Superpowers skill text says it should only be used when explicitly invoked.
- Treat Superpowers as the baseline development workflow for planning, implementation discipline, verification, and completion checks. If the skill file is unavailable in a session, state that briefly and continue with the closest matching local process.

## Command Approval Discipline

- Prefer an already reviewed external PowerShell command path with `sandbox_permissions: "require_escalated"` for shell work in this repository. Treat the local sandbox Windows logon/session failure (`CreateProcessAsUserW failed: 1312`) as a confirmed local limitation, not an intermittent issue to repeatedly retry.
- Do not submit three or more parallel escalated shell commands for automatic review.
<!-- Temporarily disabled by operator experiment:
- When a command needs `sandbox_permissions: "require_escalated"`, run one escalated command at a time unless the user explicitly asks for parallel execution.
-->
- Prefer a single focused command that gathers the needed context over several simultaneous reviewed commands.
- If an automatic approval review times out, retry at most once as a single command, then continue with a safer local alternative or ask the user for direction.
- Always set a reasonable shell `timeout_ms` for commands that need automatic approval review when the tool supports it; this limits command runtime, not the reviewer's wait time.

## Live Harness Safety

- Do not stop, restart, or replace the live `.agent-harness` gateway unless the task explicitly calls for cutover or live operation.
- Use staging target directories for tests and builds until the cutover step is intentionally reached.

## Ops Keyword

- When the user says `戰定` or asks to run the `戰定流程`, treat it as an ops keyword for carrying the current plan through the full operational workflow: implement the planned changes to completion, make the relevant tests pass, update and clean up documentation and skills, run public hygiene, push the finished work, and then perform the intentional live cutover to enable it.
- During this workflow, "update and clean up documentation and skills" means correcting or removing stale, contradictory, or no-longer-applicable guidance instead of only appending new notes.
- The final live cutover is an explicit part of `戰定`/`戰定流程`; until that step is reached, continue to use staging target directories and avoid disturbing the live `.agent-harness` gateway.

## Sub-Agent Delegation Preference

- When sub-agent tooling is available and the task is inside CK's authorized delegation envelope, use sub-agents by default to accelerate bounded sidecar tasks such as read-only codebase inspection, plan/diff review, documentation gap checks, and test matrix review, unless the user explicitly asks not to use sub-agents.
- CK's authorized delegation envelope includes requests that explicitly mention sub-agents, delegation, parallel work, reviewer loops, smoke checks, long-running ops work, or workflows naturally split across bounded sidecar inspection/verification tasks. It excludes tiny single-answer replies, destructive/live-control operations, auth or permission changes, external posts/messages, purchases/trades/spend, and any task where CK explicitly says not to delegate.
- For implementation work that can be split into disjoint code ownership, prefer delegating bounded coding subtasks to `gpt-5.3-codex-spark` worker sub-agents, with `gpt-5.4-mini` as the fallback when Spark is unavailable, spawn is rejected, or a narrower retry is appropriate. Use Codex-authenticated worker lanes only; if provider/auth routing is not visible in the sub-agent receipt, record Codex-auth status as unverified rather than assuming it.
- Every sub-agent assignment must have a bounded scope and an expected output. When waiting for a sub-agent, always use an explicit `timeout_ms` instead of an unbounded wait.
- If a sub-agent wait times out, inspect whether the result is needed on the critical path. If it is not critical, continue locally; if it is critical, retry at most once with a shorter, clearer prompt and timeout.
- Close completed, timed-out, irrelevant, or invalid-dispatch sub-agents promptly so stalled side work does not consume worker capacity or hide the actual blocker.
- Keep live gateway control, destructive shell actions, final cutover, and any operation that can interrupt the active communication channel on the main agent path.
- For sub-agents that edit code, assign disjoint file ownership, tell them they are not alone in the codebase, and require them to work with existing changes instead of reverting unrelated edits.
- Always include the intended root path in sub-agent prompts. For this repo use `D:\Warehouse\Rust-OpenClaw-Core`; for active harness-home operations use `D:\Warehouse\Rust-OpenClaw-Core\.agent-harness`. Do not rely on inherited cwd, which may still be the legacy compatibility root `D:\Warehouse\Research\OpenClaw_WSL`.
