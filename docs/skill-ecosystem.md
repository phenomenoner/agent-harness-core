# Skill Ecosystem

Agent Harness Core treats skills as runtime state, not static notes. A skill is a directory with `SKILL.md` plus optional `references/`, `templates/`, `scripts/`, and `assets/` support folders.

## Current Foundation And Adaptive Direction

The current source contract provides scoped discovery, deterministic eligibility and selection receipts, bounded prompt delivery, proposal-mediated mutation, guarded apply, reversible lifecycle operations, and checksum-locked packs. Those mechanisms are the auditable foundation for a broader product direction: **Adaptive Skill Intelligence**.

That direction treats a skill as procedural memory that can improve through verified experience. It adds task-local competence roles, minimal sufficient disclosure, stable virtual-session revision manifests, contextual beliefs about applicability, reliability, contribution, and cost, independent cost-aware evaluation, relationship-first consolidation, and progressively evidenced autonomous promotion and rollback.

The distinction is intentional. Current sections below describe implemented source behavior. [Adaptive Skill Intelligence](adaptive-skill-intelligence.md) describes the target architecture and measurement contract; the [web essay](https://phenomenoner.github.io/agent-harness-core/essay/adaptive-skill-intelligence/) presents the same direction for a broader audience. Neither is a claim that outcome-linked autonomous evolution is fully implemented or enabled.

| Current foundation | Adaptive direction |
|---|---|
| Exact eligibility, lexical/FTS scoring, selection receipts, and bounded delivery | Role-first retrieval with calibrated abstention and adaptive minimal disclosure |
| Selection, usage, proposal, apply, lifecycle, and pack records | Joined task effects and outcomes with attribution and contextual uncertainty |
| Proposal-mediated trace plus guarded autonomous synthesis/apply by default | Per-task evidence, periodic reflection, independent evaluation, and progressively broader autonomous mutation |
| Reversible archive/restore and checksum validation | Versioned topology, conflict resolution, consolidation, promotion, and exact downgrade |

## Discovery And Selection

Skill discovery reads workspace, managed, project-agent, imported, bundled, agent-created, and pack namespaces. Turn planning resolves the selected agent before building the runtime index: `main` keeps the source home/workspace roots, while a non-main agent uses its own agent directory plus configured or `agents/<agent>/workspace` root. Shared harness/imported namespaces remain available after eligibility filtering. Category folders are supported one level below each root, for example `skills/trading/neoapi-orders/SKILL.md`. Dot directories, including `.archive/`, are excluded from the live index.

The matcher records deterministic selection receipts at `state/learning/skill-selection-receipts.jsonl`. Matcher v4 uses tokenizer `mixed-v1`, which preserves ASCII token behavior and adds CJK bigram matching for Chinese, Japanese, and Korean skill triggers. Score receipts can include id, title, description, declared trigger, body keyword, tag, category, SQLite FTS5, and bounded usage-prior components. Usage-prior scoring is computed from events whose `agentId` matches the selected agent; the global snapshot remains a diagnostic aggregate and is not a cross-agent scoring pool.

Selection eligibility is resolved before lexical or FTS scoring. A non-empty `agents` frontmatter list is a hard allowlist for both automatic and explicit selection; a missing `agents` field means the shared skill remains eligible to all agents. `lifecycle: retired` and `lifecycle: retired_historical` block both automatic and explicit selection. `disable-model-invocation: true` blocks automatic selection but still permits an explicit user invocation, while `user-invocable: false` blocks only explicit user invocation. A skill with both controls is not selected through either path.

Prompt assembly includes a stable skill catalog with id, description, category, tags, delivery mode, score, checksum, and selection reasons. Full bodies are injected only for selected `injected-body` or explicit invocation-envelope entries. Catalog entries can be retrieved on a later turn with `$<skill-id>`.

## Learning And Quality Gates

Skill mutation remains proposal-mediated for traceability, but the proposal is not a mandatory human approval queue. Create, patch, replace, and archive proposals are recorded under `state/learning/skill-proposals.jsonl`; guarded autonomous and operator-triggered apply both use checksum validation, a per-target lock, backup creation, approved-root checks, and receipts before writing.

`skill-synthesize` and the runtime self-improvement hook default to guarded autonomous review/apply for agent-created skills. A complex successful turn that had no selected skill can enqueue a `skill_synthesis` worker job with explicit current-policy fields, persist the proposal and decision trace, and apply only when every lint, guard, checksum, target-lock, backup, and approved-root gate passes. Unsafe candidates are blocked or quarantined. `--propose-only` and `learning.skillSynthesis.mode = "propose-only"` remain explicit non-mutating modes; legacy worker payloads without policy fields also remain non-mutating for upgrade safety.

Quality gates are additive:

- `skill-lint` checks authoring quality and blocks apply on error-severity findings when enabled.
- `skill-guard` scans agent-created and pack content for dangerous structure or prompt/security patterns before prompt exposure.
- `skill-doctor` aggregates index consistency, lint, guard, lifecycle, pack-lock, selection, and learning activity into a health summary.

`status --json` exposes the doctor summary under `skills`. `healthz` exposes the same summary under `skills` and treats doctor errors as readiness blockers for autonomous skill apply on mutation-controlled surfaces (`agent-created` and `pack`). Legacy imported and builtin findings remain visible as warnings unless they are promoted to a mutation-controlled surface.

## Lifecycle And Packs

The curator lifecycle tracks active, stale, archived, and pinned skills in `state/skills/skill-lifecycle.json`. Archive is a reversible directory move into `.archive/`; it is not deletion.

Idle-gated curation fences on current open runtime/cron work, unreconciled runtime leases, and pending, leased, running, or retryable-failed worker jobs. Append-only historical queue totals, including terminal and durably parked rows, remain visible diagnostics but do not permanently block autonomous maintenance.

Skill packs use checksum manifests and an install lockfile. Import verifies manifest hashes fail-closed, applies guard policy by trust level, and installs under `skills/packs/<packId>/`. Removal is lockfile-driven and validates every path before deletion.

## Public Compatibility

The schema registry documents every public receipt and state file used by the skill ecosystem. Additive fields remain v1-compatible. Breaking changes require a new schema version, a reader for the prior version, and migration evidence.

## Shortest Upgrade Path

The next product slice is deliberately narrow:

1. freeze a versioned skill card and deterministic catalog digest;
2. inventory and rebaseline the existing library without changing serving;
3. run a role-first matcher in shadow mode against a reviewed replay corpus;
4. join routing and delivery to exact virtual-session outcomes and cost;
5. activate one reversible exact cohort only when the comparison shows value;
6. close one selected-skill improvement loop through held-out replay, canary,
   promotion, re-selection, and exact restore.

This order establishes attribution before optimization and rollback before broader autonomy.
Queue-store migration, warm process pools, native MCP, cross-platform packaging, broad knowledge
graphs, and unattended structural consolidation are not prerequisites unless current measurements
show that they block this slice. The [roadmap](agent-harness-core-roadmap-backlog.md) records the
exit conditions and the continuous stability rails.
