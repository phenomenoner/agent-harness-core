# Agent Harness Trust Boundaries

Date: 2026-07-21

This document supports P6.5 and the external review security dimension. It names the boundaries that must fail closed or fail open safely before live enablement.

## Boundary Rules

| Boundary | Trust level | Default behavior | Receipts/evidence |
|---|---|---|---|
| Channel ingress text | Untrusted | Treat as user content only; commands are parsed by explicit allow-list. Injection markers are scan findings, not instructions. | Channel receive receipts, `security-scan`, adversarial fixtures. |
| Prompt files and imported skills | Semi-trusted local project data | May be read into prompts and may receive learning proposals. Patches/archive require backup and receipts. | Prompt bundle receipts, learning proposal receipts. |
| Bundled skills | Trusted distribution content, mutable by operator policy | May be patched or archived only with receipts and backups. | Harness skill sync receipts, learning proposal receipts. |
| Runtime queue and worker store | Trusted local state | Terminal states are immutable; retry creates new ids. | Queue receipts, WorkerStore receipts, trace output. |
| Codex app-server/provider process | External execution boundary | Preflight credentials/config, bounded max/idle timeout, cancellation markers, transcript/trajectory receipts. | Codex runtime receipts and preflight receipts. |
| Shell lane | High-risk local execution | Canonicalize path under allowed roots; future hardening should add command hash pinning and environment scrubbing. | `security-scan`, deterministic shell audit receipts. |
| Plugin/MCP tools | Semi-trusted external/tool boundary | Explicit allow-list, timeout, per-agent/channel permission, and receipts before real invocation. Tool descriptions are pinned/hash-checked where possible. | `mcp-request`, `tool-pin-check`, plugin sidecar receipts. |
| Connector approval actions | Untrusted provider callback boundary | Provider payloads contain only public action identifiers. Resolve protected approval authority server-side, bind it to the exact lane, user, concrete session, effect generation, action, and parameter digest, and reject expired, replayed, ambiguous, or mismatched actions before any remote effect. | Typed inbound-action evidence, approval disposition receipts, external-effect transitions. |
| Memory service / openclaw-mem | Local adapter or external service boundary | ContextPack validation and fail-open degradation. Invalid memory packs must not block normal turns. The active owner remains `snapshot-adapter` until a compatible local in-process or remote endpoint probe, lease heartbeat, shadow parity, trust/scope, rollback proof, and operator promotion gates all pass. | ContextPack fixtures, memory proposal/writeback receipts, owner state, shadow parity receipts, promotion/rollback receipts, trace samples. |
| Secret vault | Trusted encrypted local store | Store long-lived secrets in repo-local encrypted vault. Never print decrypted secret values in CLI output. | Vault put/get receipts or summaries, migration/rotation receipts. |
| Public export | Public release boundary | No `.agent-harness`, `.review`, `.debug`, or secret paths. | `public-hygiene` report. |

## Current Staging Implementation

- `scan_security_boundaries` detects prompt boundary markers and shell path escapes.
- `parse_context_pack` validates bounded memory payloads without requiring the external service to be live.
- `state/memory/owner.json` records the active memory owner, lease id, heartbeat, rollback owner, promotion status, and last parity receipt. Missing state defaults to `snapshot-adapter`.
- `memory_owner` endpoint probes record whether a remote `openclaw-mem` endpoint advertises `openclaw-mem.remote-memory-service.v1`; the endpoint value is redacted in receipts.
- Shadow recall/store/capture receipts compare snapshot-adapter and mem-engine output digests while `mutatesActiveContext=false`; `memory-owner-local-prepare` records this parity for the local in-process adapter without creating a new memory store.
- Promotion stays blocked until endpoint probe, active lease, fresh heartbeat, rollback proof, trust/scope tests, recall parity, store/propose parity, and operator approval are all true.
- Crash recovery rolls an expired or stale `mem-engine` lease back to `snapshot-adapter` and appends a rollback receipt.
- `handle_mcp_request` implements initialize/list/call with allow-list rejection receipts.
- `tool_description_hash` and `tool-pin-check` support MCP/tool description pinning.
- `put_vault_secret` and `get_vault_secret` implement repo-local encrypted vault storage without printing secrets from `vault-get`.
- Telegram callbacks and Discord component interactions are normalized into a provider-neutral typed action. Public action identifiers are non-bearer references; protected state retains the capability and exact authority binding.
- Approval waits park without holding a worker lease. Decision/expiry reconciliation is monotonic, so only one terminal outcome can authorize or deny the exact effect generation.

## Remaining Hardening Gates

- Add environment scrubbing and command hash pinning to the actual shell job runner.
- Wire MCP/tool description pins into `enable-check` before enabling real invocation.
- Run a network-backed dependency advisory audit before public release.
- Capture live secret migration/rotation receipts before retiring old plaintext env-file stores.
