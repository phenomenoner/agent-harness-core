# Documentation Writing Guidelines

How to write and maintain documentation in this repository. These rules apply to humans and coding agents alike. When an existing doc conflicts with these guidelines, fix the doc or fix the guideline — do not let them drift apart silently.

## Document Inventory And Roles

| Document | Role | Audience |
|---|---|---|
| [README.md](README.md) | Public-facing overview: positioning, architecture, quick start, FAQ. | Visitors, contributors, search and AI answer engines. |
| [AGENTS.md](AGENTS.md) | Behavior rules for coding agents working in this repo. Keep it short and imperative. | Coding agents. |
| [CHANGELOG.md](CHANGELOG.md) | What changed, with verification evidence. | Operator, future sessions. |
| [SECURITY.md](SECURITY.md) | Reporting process, security posture, release gates. | Everyone. |
| `docs/*.md` | Public architecture, configuration, contract, and usage references. | Visitors and contributors. |
| `docs/.private/*` | Local-only live operations, release gates, handoffs, debug evidence, checkpoints, and owner/operator runbooks. | Local operator and agent working sessions. |
| `tools/*` | Public helper tools that are useful from a GitHub checkout. | Visitors and contributors. |
| `tools/.private/*` | Local-only wrappers, maintenance scripts, evidence collectors, and owner-machine helpers. | Local operator and agent working sessions. |

## Pick The Right Home

- A public claim, positioning statement, or onboarding step belongs in `README.md`.
- Public topology, command behavior, and configuration references belong in public `docs/`. Live state, local cutover notes, private release gates, and operational procedures belong in `docs/.private/`.
- Design rationale and trade-off analysis belong in a `docs/` tech note (prefix harness-specific files with `agent-harness-`).
- A rule agents must follow belongs in `AGENTS.md`.
- Every fact gets exactly one home. Other documents link to it; they do not restate it. Duplicated facts rot independently.

## Public Documents (README) — Style Rules

The README is optimized for human skimming and for generative/search engines (GEO):

- Open with an extractable definition: the first body paragraph states plainly what the project is ("Agent Harness Core is a …"). An answer engine should be able to quote it verbatim.
- Use question-style headings where natural, and keep an FAQ whose answers are self-contained — each Q&A pair must make sense quoted in isolation.
- Lead with benefits, then mechanics. Tables for enumerable facts, mermaid for architecture, prose for everything else.
- Every claim must be verifiable in this repository (test counts, dependency lists, feature behavior). No aspirational claims; pre-release status stays stated until releases exist.
- Weave keywords naturally (self-hosted, Rust, agent runtime, Telegram, Discord, LLM orchestration). Never keyword-stuff.
- Keep command examples minimal and link the operations handbook for the full walkthrough.

## Internal Documents — Style Rules

- Dense and factual beats polished. State evidence inline: the commands run, test counts, receipt file names, and exact paths that prove a claim.
- Use absolute dates (`2026-06-12`), never relative ones ("today", "last week"). Sessions read these months later.
- Use exact repo-relative paths for real files and `C:\path\to\...` placeholders for user-specific locations.
- Status snapshots must name the verification commands that produced them, and prefer pointing at live sources (`status --json`, `.\harness.ps1 gateway status`) over restating values that will go stale.
- When a dated snapshot is superseded, update it or delete it — do not append a second snapshot below the first.

## Hygiene — Hard Rules

- Never include secrets, bot tokens, API keys, real user/chat/channel/guild IDs, or any content from `.agent-harness/secrets`.
- Nothing from `.agent-harness`, `.review`, `.debug`, or `.tmp` may be quoted into tracked documents.
- All tracked documents are public: they must pass `agent-harness public-hygiene`. Write everything as if it is already on GitHub — because it is.
- `docs/.private/`, `tools/.private/`, `.debug/`, and `.external/` are local-only and ignored. Do not force-add from those paths unless the file has been deliberately promoted and sanitized into a public path.
- Redact by default. Receipts-style summaries (names, lengths, statuses) are fine; raw values are not.

## Formatting Conventions

- One `#` H1 per file; Title Case for H2/H3 headings.
- Fenced code blocks always carry a language tag (`powershell`, `json`, `mermaid`).
- Links between repo documents are relative markdown links, never absolute GitHub URLs.
- No hard line wrapping; one paragraph per line.
- LF in the repository (Git handles working-copy CRLF on Windows).

## Update Discipline

- A behavior change updates the relevant public tech note or local-private runbook and adds a `CHANGELOG.md` entry in the same commit when the public surface changes.
- A new public document gets added to the README "Documentation" table when it is useful to visitors or contributors. Local-only documents stay under `docs/.private/` and are not linked from public docs.
- README changes only when the public story changes — new capability families, status changes, positioning. Internal churn does not touch it.
- GitHub metadata (About description, topics) is part of the docs surface: keep it consistent with the README definition when positioning changes.
