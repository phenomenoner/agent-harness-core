# Security Policy

## Supported Scope

This repository is currently pre-release. Security fixes apply to the active `main` branch and any explicitly tagged release once releases begin.

## Reporting

Do not file public issues with secrets, exploit details, tokens, private channel ids, or private harness state. Report privately to the repository owner/operator with:

- affected commit or release,
- reproduction steps,
- whether secrets, prompts, channel messages, or local command execution are involved,
- relevant sanitized receipts or trace ids.

## Current Security Posture

- Long-lived secrets should move to the repo-local encrypted vault implemented by `agent-harness vault-put` / `agent-harness vault-get`.
- `vault-get` reports presence and byte length only; it does not print decrypted secret values.
- Plugin and MCP real invocation must stay behind explicit allow-lists, timeouts, per-agent/channel permissions, and receipts.
- Shell execution must be restricted to canonical allowed roots and should add hash pinning/env scrubbing before broader live use.
- Public exports must pass `agent-harness public-hygiene` and must not include `.agent-harness`, `.review`, `.debug`, or secret paths.

## Release Gates

Before a public release, run:

- `cargo fmt --all`
- `cargo test --workspace`
- dependency advisory audit when the advisory DB/tool is available
- `agent-harness schema-registry`
- `agent-harness invariants`
- `agent-harness public-hygiene --root <public-export-root>`
