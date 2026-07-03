# Project Agent Instructions

## Start Here

- For a new local working session, read `docs/.private/agent-harness-operations-handbook.md` first when it exists: it holds the live topology, current live validation, the full command walkthrough, and the private documentation map. The root `README.md` is the public-facing overview, not the operational source of truth.
- Treat the handbook as task-scoped repo-development orientation, not ambient prompt material. Read only the sections needed for the current repo task, keep excerpts bounded, and do not keep the handbook resident for ordinary channel/chat turns or active harness-home operations that do not need repo-development context.
- For programming, project-understanding, design, implementation, verification, completion, documentation cleanup, and handoff/cutover work, use `docs/.private/openspec-superpowers-agency-agents-workflow-guide.md` as the default SOP. It is the private canonical workflow for OpenSpec/Superpowers planning, automated review, completion gates, test synthesis, public/private hygiene, cleanup, self-improvement capture, project-understanding context packs, and battle-set operations.

## Documentation Language

- Write technical documents in English by default, including design notes, implementation plans, technical proposals, runbooks, backlog documents, and review artifacts, unless the user explicitly asks for another language.

## Default Development SOP

- Treat `docs/.private/openspec-superpowers-agency-agents-workflow-guide.md` as the default programming and software-development workflow for this repository. Use it before falling back to ad hoc planning or implementation.
- For unfamiliar code, stale docs, unclear topology, or cross-module changes, start with the SOP's project-understanding intake: build or refresh the private codebase map, docs inventory, impact map, and context pack before proposing or implementing.
- For behavior-changing work, use the SOP's OpenSpec/Superpowers path: proposal or impact context, design review, task plan, TDD implementation, automated spec/code review, verification gates, archive/handoff, and learning capture as applicable.
- For small local changes, scale the SOP down rather than skipping it: still classify blast radius, run fresh verification, respect public/private hygiene, and report cleanup.
- If a workflow rule is duplicated between this `AGENTS.md` and the private SOP, this file states the high-priority project constraint and the SOP supplies the detailed procedure. If they conflict, follow the stricter safety, privacy, verification, or live-ops rule and record the mismatch as a self-improvement learning.

## Topology And Change Gates

- Before behavior-changing edits, review [docs/agent-harness-topology-contract.md](docs/agent-harness-topology-contract.md) for identity axes, component ownership, and the required impact matrix. Treat `agentId` as a routing boundary across channel state, session freshness, prompt assembly, runtime, outbox, delivery, and memory.
- If a change touches channel identity/state/ingress, runtime queue/pipeline, prompt assembly, final outbox/delivery, memory, or supervisor/cutover behavior, update the topology contract, [docs/invariants.md](docs/invariants.md), the local release checklist at `docs/.private/release-checklist.md` when present, and the relevant local operator/self-check doc when their expectations change.
- For channel/runtime/session changes, include the agent-boundary scenario pack: same-agent stale-session suppression still works, and a non-main agent sharing the same platform/channel/user is not suppressed by another agent's active session state.
- Treat platform/channel as a task-continuity boundary when reconnecting prior work. Before claiming a "previous session" state, verify receipts by exact `platform`, `accountId`, `channelId`, `userId`, `agentId`, and `sessionKey`; do not substitute Telegram DM evidence for Discord DM work, or vice versa, even when artifacts, topics, or user intent look related.
- First-citizen ops rule: when CK says "previous session" / "last session" / `上個 session` without narrowing it, interpret that as the whole active virtual session for the current platform/channel/user/agent lane, not an arbitrary concrete child session or unrelated repo session. Only narrow to a child/continuation session when CK explicitly names a child session, queue id, continuation index, or artifact. Discord and Telegram remain separate first-class lanes; never inspect or rely on the other channel's session unless CK explicitly asks for cross-channel evidence.
- For gaps where design intent is broader than current implementation, use the topology contract's Expected Vs Actual Gaps table and Promotion Gate column as the source of missing regressions to add before claiming parity. Current examples include progress delivery volume, progress/final-surface separation, openclaw-mem full parity, multi-agent full matrix, virtual-session continuity, repo code graph support, and scenario-matrix coverage.
- For implementation-completeness and test-case synthesis, use the completion gate in `docs/.private/openspec-superpowers-agency-agents-workflow-guide.md` as the promoted default. The older `.debug/test-synthesis-and-completeness-sop-2026-06-28.md` is historical/local reference material only when needed for details not yet moved into the SOP.
- Keep the private canonical feature/module topology described by the SOP up to date when changes affect module relationships, invariants, design-vs-implementation gaps, or test coverage expectations.

## Public / Private Repo Surface

- Treat `README.md`, `CHANGELOG.md`, `SECURITY.md`, `DOC-GUIDELINES.md`, root `AGENTS.md`, and public `docs/` / `tools/` entries as GitHub-facing. They must explain architecture, configuration, usage, or public project status without exposing local ops receipts, private handoffs, debug evidence, channel/user identifiers, machine-specific runbooks, generated local graph/session caches, or private topology evidence.
- Keep non-public documents under ignored `docs/.private/`. This includes live operations handbooks, release/checkpoint handoffs, cutover evidence, validation scratch notes, Superpowers/OpenSpec plans, project-understanding artifacts, self-improvement learnings, private topology maps, and owner/operator-only runbooks.
- Keep non-public tools under ignored `tools/.private/`. This includes local environment wrappers, one-off maintenance scripts, private evidence collectors, and tools that only make sense for the owner machine.
- Keep `.debug/` local-only. If a file under `.debug/` was ever tracked, remove it from the index while preserving the local copy when it is still useful.
- When adding new docs or tools, choose the public location only if a new user or contributor benefits from reading or running it from GitHub. Otherwise put it directly in the matching `.private` folder.
- Before public remote push or PR creation, run the SOP's public/private hygiene gate. Private docs may be committed only locally or to a private destination; do not push them to a public remote.

## Default Superpowers For Development

- For programming or software-development tasks, default to enabling and following [$superpowers](C:\Users\user\.agents\skills\superpowers\SKILL.md) through the private SOP before implementation unless the user explicitly says not to use it.
- Treat this repo-level AGENTS section, or a user `[$superpowers]` mention, as the explicit project invocation for this repository if the standalone Superpowers skill text says it should only be used when explicitly invoked.
- Treat Superpowers as the baseline development workflow for planning, implementation discipline, verification, and completion checks, with OpenSpec/project-understanding/self-improvement steps supplied by the private SOP. If the skill file is unavailable in a session, state that briefly and continue with the closest matching local process.

## Command Approval Discipline

- Prefer an already reviewed external PowerShell command path with `sandbox_permissions: "require_escalated"` for shell work in this repository. Treat the local sandbox Windows logon/session failure (`CreateProcessAsUserW failed: 1312`) as a confirmed local limitation, not an intermittent issue to repeatedly retry.
- Do not submit three or more parallel escalated shell commands for automatic review.
<!-- Temporarily disabled by operator experiment:
- When a command needs `sandbox_permissions: "require_escalated"`, run one escalated command at a time unless the user explicitly asks for parallel execution.
-->
- Prefer a single focused command that gathers the needed context over several simultaneous reviewed commands.
- If an automatic approval review times out, retry at most once as a single command, then continue with a safer local alternative or ask the user for direction.
- Always set a reasonable shell `timeout_ms` for commands that need automatic approval review when the tool supports it; this limits command runtime, not the reviewer's wait time.

## External Review Tools

- CK authorizes `claude -p` as a whitelisted external review mechanism in this repository when the user explicitly requests Claude review, review loops, or `claude -p`. It may receive the scoped technical document, plan, diff, or code excerpt needed for that requested review.
- Do not include secrets, credentials, private tokens, raw `.env` contents, or unrelated workspace data in `claude -p` prompts. Keep prompts scoped to the review target and record the review outcome in the relevant debug/review artifact when applicable.

## Live Harness Safety

- Do not stop, restart, or replace the live `.agent-harness` gateway unless the task explicitly calls for cutover or live operation.
- Use staging target directories for tests and builds until the cutover step is intentionally reached.

## Post-Task Cleanup

- Before finishing a repo task, follow the cleanup gate in `docs/.private/openspec-superpowers-agency-agents-workflow-guide.md`: remove intermediate validation artifacts that are no longer needed, especially task-scoped `target\staging-*`, `target\staging-test-*`, `target\staging-check-*`, `target\staging-build-*`, `target\tmp`, local graph/index scratch files, and throwaway debug outputs created for the current task.
- Do not automatically remove live or rollback material: keep `target\debug\agent-harness.exe`, documented `target\debug\agent-harness.pre-*.exe` rollback binaries, current cutover candidate builds, artifacts referenced by the operations handbook, and evidence that is still needed for audit, review, or reproduction.
- If an artifact might still be useful but is not needed in the active workspace, archive it outside the repo with a manifest/checksum before deleting the workspace copy; prefer `E:\Warehouse_Rust-OpenClaw-Core_target\` for archived `target` material.
- Include cleanup in the final verification checklist: report what was removed, what was archived, and what was intentionally retained.

## Ops Keyword

- When the user says `戰定` or asks to run the `戰定流程`, run the private SOP's Battle-Set Mode: carry the approved plan through implementation, tests, completion gate, test synthesis, docs/topology/self-improvement updates, public/private hygiene, cleanup, public-safe push when authorized, and the selected cutover path.
- During this workflow, "update and clean up documentation and skills" means correcting or removing stale, contradictory, or no-longer-applicable guidance instead of only appending new notes.
- If the `戰定` request does not explicitly say handoff-only or no-cutover, treat it as authorization to perform the intentional live cutover after all required gates pass. If the user asks for handoff-only/manual cutover, generate the private handoff document and stop before live-control actions. Until the cutover step is reached, continue to use staging target directories and avoid disturbing the live `.agent-harness` gateway.

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
