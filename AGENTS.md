# Project Agent Instructions

## Scope And Authority

- These instructions govern the source/development repository. `.agent-harness/` is excluded from ordinary source work.
- `.agent-harness/` is the deployed live `agent-harness-core` instance and runtime home. It is operationally related to this source repository, but it is not the source-editing tree or default working directory.
- Do not read, search, modify, execute, validate, or control anything under `.agent-harness/` unless the user explicitly scopes a deployment, live-operations, cutover, repair, or post-cutover verification task to it.
- Use the user request, this file, current code/tests, and the relevant public contracts as active authority. `README.md` is a public overview, not an operations runbook.
- Read private documents only when a specific task needs them. A document marked `RETIRED` is historical context only and must not be invoked as a workflow or rule source.
- Write technical documents in English unless the user asks for another language.

## Deployment Boundary

- Develop and verify changes in the source repository and staging locations. Mutate the live `.agent-harness/` deployment only during an explicitly authorized cutover or live-operations task, using the applicable current runbook and rollback evidence.
- Treat the deployed path as a compatibility boundary. Moving `.agent-harness/` is a separate migration project, not routine cleanup; it requires a path-consumer inventory, compatibility or redirect plan, staged validation, intentional cutover, and rollback plan.

## Development Gates

- Use the smallest workflow that safely fits the change. No planning framework, reviewer loop, TDD ceremony, or delegation pattern is automatic.
- Preserve unrelated user changes in the working tree. Do not rewrite or revert files outside the task.
- Before behavior-changing work, review `docs/agent-harness-topology-contract.md` and the relevant rows in `docs/invariants.md`. Update those documents, the applicable tests, and `docs/.private/release-checklist.md` when the change alters their contracts or expectations.
- Treat `agentId` and the exact `platform`, `accountId`, `channelId`, `userId`, and `sessionKey` tuple as routing and continuity boundaries. Do not substitute evidence from another agent or channel.
- For software changes, apply `completeness-and-test-synthesis` before a completion claim: classify blast radius, map touched invariants, run fresh verification at the required tier, and report gaps. Cross-cutting identity, routing, shared-state, memory, delivery, supervisor, or security changes normally require scenario/replay evidence (T3) unless the user accepts a lower-tier gap.

## Public And Private Surfaces

- Treat root policy/project files and public `docs/` and `tools/` entries as GitHub-facing. Keep secrets, local receipts, channel/user identifiers, machine-specific runbooks, private handoffs, graph/session caches, and debug evidence out of them.
- Keep operator-only documents under ignored `docs/.private/`, local-only tools under `tools/.private/`, and scratch evidence under `.debug/`.
- Before a public push or PR, inspect the actual public diff for private paths, identifiers, secrets, receipts, and accidental local artifacts. Private material must not be pushed to a public remote.

## Delegation And External Review

- Follow the global dispatch policy. Direct execution is the default for small or coupled work; delegate only bounded, genuinely independent work with exclusive artifact ownership. Final synthesis and repository-wide verification stay with the main agent.
- Worker briefs must name this repository root and must explicitly forbid `.agent-harness/` unless the user placed it in scope.
- Use `claude -p` only when the user explicitly requests Claude review. Send only the scoped artifact and never secrets, credentials, raw `.env` data, or unrelated workspace content.

## Cleanup

- Remove only task-created temporary outputs that are no longer needed. Preserve user artifacts, rollback/candidate material, and evidence still required for review or reproduction.
- Report material cleanup, archives, and intentional retention in the final handoff. Do not manufacture cleanup work when the task created no temporary artifacts.

## Retired Rules And Skills

`RETIRED` means: do not auto-load, invoke, cite as authority, or let the item control execution. Retain it only for historical lookup unless the user explicitly reactivates it.

- `docs/.private/openspec-superpowers-agency-agents-workflow-guide.md` as a default or binding SOP.
- Automatic OpenSpec, Superpowers, Agency Agents, TDD, multi-reviewer, or archive/self-improvement workflow chains.
- All `superpowers:*` skills and the Superpowers bootstrap/plugin workflow. The Codex-global plugin is disabled separately.
- Automatic loading of the operations handbook for ordinary source work. It remains applicable only when an explicitly scoped deployment, live-operations, cutover, repair, or post-cutover task needs it.
- Treating `.agent-harness/` as the source-editing tree or default workspace; it is the live deployment/runtime target.
- `戰定` / Battle-Set as implicit authorization for full workflow execution, public push, live control, or cutover. Those actions now require explicit task-level authorization.
- The `CreateProcessAsUserW failed: 1312` sandbox-escalation workaround and its automatic approval/retry rules; use the active tool permission policy instead.
- Default sub-agent fan-out, fixed worker-model prescriptions, and duplicated repo-level dispatch templates.
