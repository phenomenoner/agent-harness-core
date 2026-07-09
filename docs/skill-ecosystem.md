# Skill Ecosystem

Agent Harness Core treats skills as runtime state, not static notes. A skill is a directory with `SKILL.md` plus optional `references/`, `templates/`, `scripts/`, and `assets/` support folders.

## Discovery And Selection

Skill discovery reads workspace, managed, project-agent, imported, bundled, agent-created, and pack namespaces. Category folders are supported one level below each root, for example `skills/trading/neoapi-orders/SKILL.md`. Dot directories, including `.archive/`, are excluded from the live index.

The matcher records deterministic selection receipts at `state/learning/skill-selection-receipts.jsonl`. Matcher v3 uses tokenizer `mixed-v1`, which preserves ASCII token behavior and adds CJK bigram matching for Chinese, Japanese, and Korean skill triggers. Score receipts can include id, title, description, declared trigger, body keyword, tag, category, SQLite FTS5, and bounded usage-prior components.

Prompt assembly includes a stable skill catalog with id, description, category, tags, delivery mode, score, checksum, and selection reasons. Full bodies are injected only for selected `injected-body` or explicit invocation-envelope entries. Catalog entries can be retrieved on a later turn with `$<skill-id>`.

## Learning And Quality Gates

Skill mutation remains proposal-mediated, but autonomous review/apply is a first-class path. Create, patch, replace, and archive proposals are recorded under `state/learning/skill-proposals.jsonl`; operator-approved and autonomously approved apply both use checksum validation, a per-target lock, backup creation, approved-root checks, and receipts before writing.

`skill-synthesize` defaults to autonomous apply. The runtime self-improvement hook can enqueue `skill_synthesis` worker jobs for complex successful turns that had no selected skill, and those jobs synthesize an agent-created skill proposal before running the same autonomous lint/guard/apply pipeline. `--propose-only` and `learning.skillSynthesis.mode = "propose-only"` are opt-out controls for recording proposals without applying them.

Quality gates are additive:

- `skill-lint` checks authoring quality and blocks apply on error-severity findings when enabled.
- `skill-guard` scans agent-created and pack content for dangerous structure or prompt/security patterns before prompt exposure.
- `skill-doctor` aggregates index consistency, lint, guard, lifecycle, pack-lock, selection, and learning activity into a health summary.

`status --json` exposes the doctor summary under `skills`. `healthz` exposes the same summary under `skills` and treats doctor errors as readiness blockers for autonomous skill apply on mutation-controlled surfaces (`agent-created` and `pack`). Legacy imported and builtin findings remain visible as warnings unless they are promoted to a mutation-controlled surface.

## Lifecycle And Packs

The curator lifecycle tracks active, stale, archived, and pinned skills in `state/skills/skill-lifecycle.json`. Archive is a reversible directory move into `.archive/`; it is not deletion.

Skill packs use checksum manifests and an install lockfile. Import verifies manifest hashes fail-closed, applies guard policy by trust level, and installs under `skills/packs/<packId>/`. Removal is lockfile-driven and validates every path before deletion.

## Public Compatibility

The schema registry documents every public receipt and state file used by the skill ecosystem. Additive fields remain v1-compatible. Breaking changes require a new schema version, a reader for the prior version, and migration evidence.
