# Project Agent Instructions

## Start Here

- For a new working session, read [docs/agent-harness-operations-handbook.md](docs/agent-harness-operations-handbook.md) first: it holds the live topology, current live validation, the full command walkthrough, and the documentation map. The root `README.md` is the public-facing overview, not the operational source of truth.

## Command Approval Discipline

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
