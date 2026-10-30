# AHC Technical Overview

Updated: 2026-10-30. The active runtime is `python/src/ahc`. The Rust crates remain a distinct historical implementation.

## Component Ownership

| Layer | Source location | Responsibility |
| --- | --- | --- |
| Domain | `python/src/ahc/domain` | Immutable session, request, result, policy, workspace and schedule contracts. |
| Application | `python/src/ahc/application` | Foreground execution, tool orchestration, acceptance, progress and delivery. |
| Infrastructure | `python/src/ahc/infrastructure` | SQLite state, inbox/outbox, schedules, channel controls and ownership. |
| Adapters | `python/src/ahc/adapters` | Codex protocol/bundles, channel transports, workspaces and integrations. |
| CLI composition | `python/src/ahc/cli` | Validated configuration, profiles, dependency binding and operator commands. |

AHC owns the virtual session and persistent workspace independently of a Codex thread. `AgentBackend` supplies an admitted turn. Backend output is validated and committed before final delivery. A verified model report may be accepted for delivery while the original task remains unresolved.

## Identity And Persistence

The exact agent, platform, account, channel, user and virtual-session generation define routing and continuity. Configured interfaces can share provider authentication without merging their conversation or state ownership. Foreign threads, routes and generations cannot substitute for the current owner.

SQLite stores submissions, admitted turns, answer seals, intents, channel routes, progress claims, final outboxes and schedule occurrences. An interruption does not authorize replay of a submitted operation. Sent receipts, unknown effects and incomplete work remain distinct. Checkpoints and restored generations retain their own provenance; source records differ from indexes and cached views.

## Scheduling And Interaction

`ForegroundRuntime` consumes conversation work. Channel-controlled trusted composition attaches a separate `BackgroundSchedule` owner, which dispatches due work into dedicated schedule sessions and workspaces in the same store. Foreground conversation work can continue during background execution. Compositions without that owner retain foreground idle schedule admission. `ScheduleTick` atomically records occurrence identity and inbox admission. Catch-up is bounded and chronological. Finishing another task makes execution available; it does not create the schedule's trigger.

`runtime_schedule_occurrences` determines scheduled presentation after restart and even after schedule removal. Task text, elapsed time and model commentary cannot select it. Ordinary user messages during scheduled work queue for a separate conversation turn instead of automatically steering the scheduled model. Explicit operator controls retain their existing contracts.

Background dispatch preserves occurrence and delivery ownership. Existing submitted legacy work is not automatically migrated or replayed.

## Presentation And Delivery

Interactive work can use typing and one bounded activity card. Composition and persisted progress reservation both suppress scheduled activity. Explicit `workspace.progress` calls return a suppressed observation without a transport effect or send claim.

After the outbox's existing route, parent, seal and current-owner checks, it derives a category for the claimed delivery. The transport adds a category prefix while stored model output, original outbox payloads, seals and acknowledged/unknown receipts remain unchanged. A category describes origin or a need for action, not task success.

The scheduled approval port returns unavailable before publishing an interactive request or waiting for chat. It grants no new authority. Existing automatically authorized effects remain governed by their admitted policy. An uncertain final send retains the existing unknown outcome and no-replay custody. Attachments retain separate ownership and delivery records.

## Backend, Tools And Memory

The [Linux archive manifest](../../python/src/ahc/adapters/codex_linux_backend.json) pins the backend version, archive digest, exact members, hashes and modes. Archive qualification differs from native file verification, process launch and provider readiness. AHC does not discover another agent's installation or use PATH as a backend fallback.

The adapter initializes app-server, starts or resumes the admitted thread, exposes admitted dynamic tools and validates correlated results. Vendor incoming frames are bounded at 16 MiB and outbound frames at 1 MiB. Queued raw frames have a 32 MiB ceiling; MCP incoming frames retain their separate 512 KiB limit. Sanitized failure metadata records fixed rejection reasons and observed byte counts without retaining wire payloads. Result persistence and failure notifications preserve UNKNOWN/no-replay custody and predecessor delivery receipts. A bounded reader queue applies backpressure; aggregate frame counts and bytes do not truncate valid long responses. Task deadlines remain enforced. Unsupported requests cannot fabricate user answers, consent, credentials or host authority. Native child discovery is separate from admitted child execution and accepted results.

Python/IPython execution, checkpoints, artifacts, memory, optional MCP calls and ordinary child work retain exact operation and owner binding. The memory journal owns original content; the configured MemPalace/Qdrant projection remains rebuildable. See [detailed architecture](PRODUCT-ARCHITECTURE.md), [task lifecycles](TASK-LIFECYCLES.md) and [failure policy](WORKFLOW-FAILURE-POLICY.md).

## Verification Boundaries

Focused controlled tests cover quiet progress, categories, approval refusal, restart provenance, one-shot/unknown delivery and the next interactive request. Mocked transports establish payloads and ordering, not Telegram/Discord client rendering.

Backend qualification uses actual upstream Linux bytes, a credential-free home, generated schemas and real app-server thread operations without inference. Authenticated model/tool execution, provider readiness, live selection and endurance require separate evidence. A source update is not automatically deployed.
