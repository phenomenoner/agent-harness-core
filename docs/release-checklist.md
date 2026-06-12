# Agent Harness Release Checklist

Date: 2026-06-12

This checklist is mirrored by `agent-harness release-checklist`. It is intentionally review-oriented: passing it should leave evidence that maps to the external review dimensions.

## Required Before Any Live Cutover

- `cargo fmt --all`
- `cargo test --workspace --target-dir target\staging-test-workspace`
- `agent-harness config-validate --target-home <staging-home>`
- `agent-harness healthz --target-home <staging-home> --require-writable-state`
- `agent-harness metrics --target-home <staging-home>`
- `agent-harness schema-registry`
- `agent-harness invariants`
- `agent-harness public-hygiene --root <public-export-root>`
- Changelog entry updated.
- Schema registry updated for every new receipt/state schema.
- Docs/skills/help stale-guidance review completed: update or remove outdated, misleading, or ambiguous instructions in public docs, generated CLI help, builtin harness skills, feature parity HTML/Markdown, and operational runbooks. Do not satisfy this gate by only adding new notes while leaving old contradictory guidance in place.
- Rollback note recorded for the deployment candidate.
- Staging trace sample captured for normal, canceled, timeout, and dead-letter paths.

## Required Before Production-Complete Roadmap Claims

- Seven-day queue shadow summary with zero unexplained divergence before P2 cutover.
- WinSW/service-wrapper restart, ordered shutdown, and reboot recovery receipts before unattended-operation claims.
- Telegram, Discord, and OpenRouter smoke receipts before live channel/provider claims.
- Live or fake-only canary deploy receipt for the exact candidate binary before cutover.
- Vault migration, restore, and rotation receipts before long-lived plaintext secret stores are retired.
- Network-backed dependency advisory audit when an advisory database tool is available.
- Sanitized scenario replay corpus and restore drill receipt before P7 quality/maturity claims.

## Last Verified Staging Evidence

- 2026-06-12: `cargo fmt --all` passed.
- 2026-06-12: `cargo test -p agent-harness-cli --target-dir target\staging-test-cli` passed, 16 tests.
- 2026-06-12: `cargo test --workspace --target-dir target\staging-test-workspace` passed, 16 CLI tests, 207 core tests, 0 doc-tests.
- 2026-06-12: `cargo tree --workspace --duplicates` reported duplicate `webpki-roots` through `ureq`/TLS only.
- 2026-06-12: Provider-isolation hotfix added stale-guidance review to the release checklist and updated the builtin harness ops skill so OpenRouter routes use provider-specific Codex homes instead of the shared Codex/OAuth home.
