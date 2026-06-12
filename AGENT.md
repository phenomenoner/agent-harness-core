# Project Agent Instructions

## Command Approval Discipline

- Do not submit three or more parallel escalated shell commands for automatic review.
- When a command needs `sandbox_permissions: "require_escalated"`, run one escalated command at a time unless the user explicitly asks for parallel execution.
- Prefer a single focused command that gathers the needed context over several simultaneous reviewed commands.
- If an automatic approval review times out, retry at most once as a single command, then continue with a safer local alternative or ask the user for direction.

## Live Harness Safety

- Do not stop, restart, or replace the live `.agent-harness` gateway unless the task explicitly calls for cutover or live operation.
- Use staging target directories for tests and builds until the cutover step is intentionally reached.
