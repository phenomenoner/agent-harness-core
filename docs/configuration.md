# Configuration

Agent Harness Core reads most operator state from a harness home directory supplied with `--harness-home` or `--target-home`. Keep that directory outside git or under an ignored `.agent-harness/` path.

## Common Paths

- `harness-config.json`: harness runtime and security settings.
- `state/`: receipts, queues, logs, supervisor files, worker database, and channel state.
- `state/cron-runs/cron-runs.sqlite`: native cron LLM run admission, retry, quarantine, and operator control state.
- `secrets/`: optional local env files for channel/provider/memory credentials.
- `skills/`: bundled and imported skill indexes used by prompt assembly.

## Runtime Workspaces

Use `--source-home` for imported prompt files, registry, skills, and legacy context. Use `--runtime-workspace` only for the Codex working directory. Prompt assembly falls back to the imported source workspace when the runtime workspace does not contain prompt files.

## Supervisor Plans and Receipt Maintenance

`supervisor-plan` generates the Windows Task Scheduler start/stop bundle from the authoritative harness configuration. Do not hand-edit generated runner scripts: regenerate the bundle after changing supervised loop configuration, first using `supervisor-reconcile --all --dry-run` to review ownership.

An enabled secondary Telegram lane is declared under `supervisor.telegramLoops`. Each entry needs a distinct safe `serviceId` with a `telegram-loop-` prefix and an account selector; `agent` is optional:

```json
{
  "supervisor": {
    "telegramLoops": [
      {
        "enabled": true,
        "serviceId": "telegram-loop-secondary",
        "account": "secondary",
        "agent": "secondary"
      }
    ]
  }
}
```

When `supervisor-plan --include-ledger-maintenance` is used, the generated bundle retains every enabled configured Telegram loop and adds the isolated `ledger-maintenance-loop`. That owner performs bounded receipt/history retention after interactive work; ingress, progress, runtime completion, and final delivery only signal it and do not synchronously compact histories.

## Model Capability and Reasoning Control

v0.8.0 resolves reasoning against the exact effective provider/model route. Codex capability discovery supplies the model slug, default reasoning effort, and supported effort strings; the harness records a catalog revision and preserves the exact accepted effort through channel state, queue admission, and `turn/start`. Do not assume that two GPT-5.6 routes expose the same capabilities. For example, `gpt-5.6-sol` may use exact `max` only when the current exact route advertises `max`.

Enable discovery gradually in `harness-config.json`:

```json
{
  "orchestration": {
    "features": {
      "modelCatalogV2": {
        "mode": "authoritative",
        "enabledAgentIds": ["main", "reviewer"]
      }
    }
  }
}
```

`mode` accepts `off`, `shadow`, or `authoritative`. `enabledAgentIds` is optional; when present it is an exact per-agent rollout cohort. Shadow mode records the discovered result while legacy normalization remains authoritative. Authoritative mode accepts an effort only for a matching catalog route and fails closed when a stored route-valid policy cannot be revalidated.

The v0.8.0 reasoning surface follows these rules:

- `/think` and `/reasoning` are aliases for the same state and status output. They are not two independent knobs.
- `/think max` followed by `/reasoning low` results in `low`; reversing the order results in `max`. The last valid write in the same scope wins.
- `default` asks the exact route to use its advertised default.
- `max` is the highest currently known legal GPT-5.6 reasoning effort. It stays distinct and is not normalized down when advertised by the exact route.
- Exact `ultra` is removed from both cached model-catalog input and Codex app-server capability observations. It is rejected by commands, stored-policy recovery, child-policy validation, queue admission, and runtime wire validation; it is never advertised, configurable, or sent to Codex.
- Legacy `ultra-high` and `ultra_high` normalize to `xhigh`; they are only compatibility spellings. Capability lists canonicalize these aliases before duplicate handling, so an alias next to canonical `xhigh` does not create a second effort.
- Unknown future effort names remain open-ended only when the exact effective route advertises them. They are not inferred from another model or accepted from unverified configuration.

There is no public `ultra` effort or resource-policy configuration in v0.8.0. Do not add one to `harness-config.json`.

Agent model defaults remain OpenClaw-style source configuration in `openclaw.json`. Each configured agent may select a different model:

```json
{
  "agents": {
    "defaults": { "provider": "openai", "model": "gpt-5.6-sol" },
    "list": [
      { "id": "main", "model": "gpt-5.6-sol", "enabled": true },
      {
        "id": "reviewer",
        "model": "gpt-5.6-terra",
        "workspace": "agents/reviewer/workspace",
        "enabled": true
      }
    ]
  }
}
```

This example selects models; it does not hard-code a reasoning capability. Use the catalog-backed command or a validated child policy to choose an effort that the exact route advertises.

## Skill Ecosystem

`skills/` is indexed from operator-authored, imported, bundled, agent-created, and pack namespaces. Category subdirectories are supported one level below each root, and `.archive/` or other dot directories are excluded from discovery.

`harness-config.json` can configure the skill matcher, virtual-manifest observer, catalog, taxonomy, lint, guard, synthesis, nudges, and curator. Defaults keep serving unchanged and mutation paths proposal-only and checksum guarded:

```json
{
  "skills": {
    "matcher": {
      "ftsEnabled": true,
      "usagePriorEnabled": true,
      "shadowV2Enabled": false,
      "minScore": 0
    },
    "virtualManifest": {
      "observeEnabled": false
    },
    "catalog": {
      "enabled": true,
      "limit": 8
    },
    "taxonomy": {
      "categories": [
        "operations",
        "channels",
        "memory",
        "runtime",
        "trading",
        "research",
        "media",
        "development",
        "self-improvement",
        "general"
      ]
    },
    "guard": {
      "agentCreated": true,
      "packPolicy": "default"
    },
    "lint": {
      "enforceOnApply": true
    }
  },
  "learning": {
    "skillSynthesis": {
      "enabled": true,
      "mode": "propose-only",
      "dailyCap": 3,
      "minToolCalls": 5,
      "minAssistantChars": 600
    },
    "skillNudge": {
      "enabled": true,
      "turnInterval": 8
    },
    "memoryNudge": {
      "enabled": true,
      "turnInterval": 6
    },
    "curator": {
      "enabled": true,
      "mode": "propose",
      "intervalHours": 168,
      "staleAfterDays": 30,
      "archiveAfterDays": 90,
      "consolidate": true,
      "minClusterSize": 2,
      "includeNamespaces": ["agent-created"]
    }
  }
}
```

`skills.matcher.shadowV2Enabled` is an observability-only staging/C1 switch. When enabled, runtime preparation evaluates router v2 from the current inbound task and writes an exact account-aware `skill-routing.v2` receipt under the harness state directory. Active v4 skill selection, prompt bytes, usage priors, delivery, and learning remain authoritative and unchanged. If the runtime cannot prove the exact account-aware lane, it skips the receipt instead of falling back to a partial identity.

`skills.virtualManifest.observeEnabled` is a separate default-off F2 instrumentation switch. It requires an exact F1 route receipt and records a frozen exact-lane virtual manifest plus append-once delivery evidence. It cannot alter active skill IDs or prompt bytes. Concrete backend rollover rehydrates the frozen revision without a fresh routing link; `/new` closes the old manifest and starts without inherited task intent. Enabling F1 alone never creates manifest or delivery evidence.

`learning.skillSynthesis.mode = "propose-only"` is the default skill-learning posture: a complex successful turn that did not use a matching skill may enqueue a `skill_synthesis` worker job and persist a reviewable proposal, but it cannot mutate a skill. Explicit `"auto"`, `"apply"`, or `"dispatch-and-replace"` config supplies worker apply authorization; the worker still requires both `applyAuthorized=true` and `proposeOnly=false`, then runs lint/guard review and the checksum/backup apply path. The CLI likewise requires explicit `skill-synthesize --apply`.

Unknown taxonomy categories do not fail config validation; `skill-lint` reports category quality issues so operators can add categories without breaking older binaries. `skill-view` can print a selected skill body or support file and records a `viewed` usage event.

## Memory Bridge

When `mem-engine` is the active memory owner, the harness can route status,
recall, and approved store operations through the openclaw-mem subprocess bridge
instead of stale response files or the read-only migration fallback. The bridge
primary path requires `openclaw-mem >= 1.9.31`.

`harness-config.json` accepts either a full bridge command or a bridge binary:

```json
{
  "memory": {
    "openclawMemBridgeCommand": "openclaw-mem-bridge-dispatch .agent-harness",
    "openclawMemBridgeBin": "openclaw-mem-bridge-dispatch"
  }
}
```

`openclawMemBridgeCommand` takes precedence when both fields are present because
it can include required arguments such as the harness home. `openclawMemBridgeBin`
is the portable fallback when no extra arguments are needed. Read-only
`status`/`recall` calls use a bounded deadline and one retry; approved `store`
calls fail closed and are not retried, avoiding duplicate canonical writes.

## Service Executables

Windows service-mode execution should avoid unqualified PATH shims for long-lived
runtime and gateway processes.

For Codex app-server runtime plans, the harness prefers native vendor
`codex.exe` when it detects an npm `codex.cmd` shim with a matching packaged
binary; npm `codex.cmd` is only the fallback. Extensionless shims and Codex
Desktop MSIX resource paths are rejected by preflight.

For Node-backed gateway and plugin sidecar commands, the default Node resolution
order is:

```text
AGENT_HARNESS_NODE_EXE -> explicit Windows node.exe install path -> PATH node.exe -> node
```

Operators can set `AGENT_HARNESS_NODE_EXE` or pass `--node-exe` to pin a known
spawnable Node executable.

## Runtime Terminal State

`timeout` run-once receipts are terminal for the parent queue id. Runtime selection, status open-item counts, native typing context, and progress delivery all treat the parent turn as closed after a timeout. To retry work, enqueue a new turn or explicit retry id rather than reusing the old queue id.

Round10 adds one guarded exception before terminal timeout selection: when Codex runtime observes an active tool-use item and then hits the idle JSONL timeout, the harness stops the prior app-server/tool path, records `toolUseTimeout` in `agent-harness.codex-runtime-run.v1`, and opens one bounded fresh-thread recovery prompt so the model can decide whether to retry narrowly, use an alternative, or report a blocker. If that recovery output is only external review evidence, it is recorded as `agent-harness.external-review-evidence.v1` and does not close the parent workflow as a final reply. If recovery fails, the resulting timeout remains terminal for the parent queue id. This is an initial guard, not full per-tool supervisor parity.

Context preflight also applies an absolute high-usage guard through `codexContext.highContextUsageCompactTokenLimit` (default `120000`). This covers bound-thread incidents where prompt bytes are modest but prior recorded usage is already very high and no `modelContextWindow` ratio can be computed; the guard forces official compact before the next turn when an existing thread is bound.

Progress delivery state is terminal-monotonic. Once a parent queue id has delivered terminal runtime progress, later stray events for that same queue id must not downgrade the status panel back to non-terminal working state.

Long-running jobs or local services that intentionally outlive the chat turn should be represented as managed worker/background jobs with independent accepted, heartbeat, status, completion, and cancellation receipts.

## Worker Limits

`harness-config.json` can define worker dispatch limits:

```json
{
  "workerDispatch": {
    "globalConcurrencyLimit": 12,
    "groupConcurrencyLimit": 6,
    "channelConcurrencyLimit": 3,
    "laneConcurrencyLimits": {
      "cron": 3,
      "llm": 6,
      "shell": 6,
      "watchdog": 2,
      "maintenance": 2,
      "plugin": 2
    }
  }
}
```

Native LLM cron jobs use the `cron` worker lane before they hand off to the `cron` runtime class. This keeps scheduler-originated LLM work from filling the general `llm` worker lane.

`channelConcurrencyLimit` is a worker/fan-out cap. It does not allow the same ordinary main-agent DM/session to run multiple Codex turns at once; same-session ordering is enforced by `runtimeDispatch`.

The invariant is:

```text
global limit >= per-agent limit >= per-agent-per-channel limit
```

Invalid narrower-to-wider settings are capped at runtime with warnings.

## Multi-Agent Child Coordination

Child execution policy is a dispatch-time contract, not one global model setting. The master may assign every child an independent immutable provider, model, reasoning preference, resolved backend policy, catalog revision, tool/sandbox profile, timeout, attempt limit, budget, delegation limit, and result contract. Siblings can therefore use different GPT-5.6 family models and efforts without rewriting the master agent's defaults.

Every child terminal result is bound to an exact master owner containing the full lane, virtual session, parent/source queue identity, and optional operation-plan identity. The durable mailbox stores the actual terminal outcome only as a redacted summary of at most 4 KiB plus up to 16 opaque artifact references; raw prompts, raw provider events, credentials, and absolute paths are forbidden. Coordinator prompt context is built only from these validated envelope fields and structural child metadata, never from an arbitrary worker payload or raw transcript. An incomplete legacy owner remains auditable but is excluded from automatic resume.

Worker-to-runtime routing is not payload-authoritative. The harness derives `runtimeClass` and `origin` from `WorkerJobKind` (including the distinct coordinator-resume and native-cron paths), rejects a conflicting requested route, and reconciles terminal state only from `agent-harness.runtime-run-once.v1` receipts whose queue id, runtime class, and origin match that trusted route. Unknown schemas or mismatched provenance are ignored rather than terminalizing the worker.

The master owns continuation and final delivery:

1. Child success or failure is appended once to the exact-owner mailbox; child/external progress, final text, and errors do not create parent user-facing outbox rows.
2. While any queue owns an active lease in the exact master lane, the watchdog leaves results unclaimed and does not wake or interrupt the master. This includes newer same-lane work, not only the original parent queue id.
3. After confirmed lease release, sibling results coalesce into exactly one logical durable resume intent and one typed coordinator continuation; deterministic identities and idempotent admission provide the exactly-once resume effect across duplicate or restarted watchdog passes.
4. The continuation acknowledges mailbox rows only after it acquires its runtime lease.
5. Only the resumed master/coordinator may synthesize the user-facing final reply.

Before continuation, every expected child id must have valid exact-owner terminal evidence. Missing or invalid evidence is replaced atomically with a deterministic failed-omission envelope, so a partial batch cannot silently look complete. Duplicate watchdog passes, process restart, and expired claim recovery must reuse the same intent rather than enqueueing another continuation. The in-code invariant describes the enqueue side as at-most-once; durability and reclaim close the corresponding lost-resume path. There is no legacy direct-child-final compatibility mode.

## Runtime Dispatch Classes

Runtime queue items now carry `runtimeClass` and `origin` metadata. Channel ingress defaults to `interactive`, native LLM cron defaults to `cron`, worker-originated turns default to `worker`, and operational lanes may use `maintenance`.

`harness-config.json` can define class-specific runtime capacity independently from worker job leasing:

```json
{
  "runtimeDispatch": {
    "globalConcurrencyLimit": 12,
    "interactiveReserve": 1,
    "classes": {
      "interactive": {
        "maxActive": 12,
        "perAgentMaxActive": 6,
        "perChannelMaxActive": 3,
        "perSessionMaxActive": 1,
        "sessionFifo": true,
        "sameSessionMainAgentSerialization": true,
        "perJobMaxActive": 999999,
        "maxQueuedPerAgent": 999999
      },
      "cron": {
        "maxActive": 2,
        "perAgentMaxActive": 1,
        "perChannelMaxActive": 1,
        "perSessionMaxActive": 1,
        "sessionFifo": true,
        "perJobMaxActive": 1,
        "maxQueuedPerAgent": 4
      }
    }
  }
}
```

The runtime loop uses class-scoped lease files under `state/runtime-queue/classes/<class>/runtime-leases.json`. Legacy root `state/runtime-queue/runtime-leases.json` is still read during capacity checks so an upgrade does not ignore active pre-Round5 leases.

For ordinary channel-origin `interactive` main-agent items, the session lane key is `runtimeClass + agentId + platform + channelId + userId + sessionKey`. `perSessionMaxActive=1` and `sessionFifo=true` mean a second same-session message waits for the older same-lane item to reach terminal run status even when broader `perChannelMaxActive` capacity remains. Worker/subagent lanes can keep wider fan-out because `sameSessionMainAgentSerialization` is false outside the interactive main-agent lane by default.

## Codex Context Recovery

`codexContext` controls harness integration with Codex's official context compaction path:

```json
{
  "codexContext": {
    "enabled": true,
    "preferOfficialCompact": true,
    "autoCompactBeforeTurn": true,
    "retryOnceAfterCompact": true,
    "fallbackOnCompactFailure": "checkpoint-and-new-thread",
    "warnAtActiveContextRatio": 0.75,
    "compactAtActiveContextRatio": 0.85,
    "contextCapacityMaxAgeMs": 86400000,
    "manualRecoveryAllowed": true,
    "modelContextWindow": 128000,
    "modelAutoCompactTokenLimit": 100000,
    "modelAutoCompactTokenLimitScope": "total",
    "toolOutputTokenLimit": 12000
  }
}
```

`thread/tokenUsage/updated.params.tokenUsage.modelContextWindow` is retained with the exact binding, provider/model route, backend context generation, source, and observation time. A live capacity is usable only when every identity field matches and the observation is no older than `contextCapacityMaxAgeMs` (default 24 hours). Preflight records the effective capacity, source, freshness classification, observation time, and ratio. A configured `modelContextWindow` is the static route fallback; the `highContextUsageCompactTokenLimit` default of 120000 is an unknown/stale-capacity failsafe and is evaluated only when neither a fresh live capacity nor a static route capacity is available. Therefore the failsafe cannot mask a known model-window ratio.

The optional model/token keys are passed through to generated Codex TOML as `model_context_window`, `model_auto_compact_token_limit`, `model_auto_compact_token_limit_scope`, and `tool_output_token_limit`. `contextCapacityMaxAgeMs` is harness-only and is not passed to Codex. `compactPrompt` and `experimentalCompactPromptFile` are also accepted and passed through as `compact_prompt` and `experimental_compact_prompt_file`.

Each `codex-run` writes per-execution `codex-context-preflight.json` and appends `state/runtime-queue/codex-context-preflight-receipts.jsonl`. When a resumed thread is over the compact threshold, the harness calls official app-server `thread/compact/start` before `turn/start` and waits for the `contextCompaction` item to complete. Hard context-window failures are classified as `context-exhausted`; the harness retries once after official compact, then writes `codex-context-checkpoint.json` and `codex-context-rollover.json` and opens a fresh Codex thread when fallback is enabled.

## Codex Web Search

The harness always selects Codex built-in web search explicitly and independently of sandbox mode:

```json
{
  "codexWebSearch": {
    "defaultMode": "cached",
    "freshnessMode": "live",
    "sensitiveMode": "disabled",
    "requireCapability": true,
    "allowLive": true,
    "disabledLaneDigests": []
  }
}
```

Ordinary turns request `cached`. Explicit current/freshness intent such as browse/search/latest/today/verify-online requests `live` only when live is enabled and the exact lane is not denied. Sensitive, offline, and replay turns always request `disabled`; the validator accepts only `disabled` for `sensitiveMode`. `disabledLaneDigests` contains exact canonical lane digests and is never inferred from channel names, account aliases, sandbox, or filesystem permissions.

Before thread start/resume, the runtime reads same-connection `modelProvider/capabilities/read`. If `webSearch` is absent or false while capability enforcement is enabled, the effective mode becomes `disabled` and the developer instructions explicitly forbid claiming online verification. The effective mode is sent as top-level `web_search` on `thread/start` or `thread/resume`. A legacy thread or a thread whose effective mode, provider, or exact lane no longer matches is rolled over instead of silently retaining prior tool authority.

Per-execution decision and action receipts record requested/effective mode, capability generation, exact IDs, action, a query SHA-256, safe public domain when available, and citation counts. They never record raw queries or private/local URLs. Search output is untrusted runtime context and is not admitted into memory, skill-learning, or dream artifacts without their separate provenance/admission gates. Lanes that require a strict pre-query allowlist, deterministic payload, or exact query/cost cap must disable built-in live search and use a separately controlled harness tool.

## Cron Run Isolation

Native cron LLM turns use CronRunStore before worker enqueue:

- `sessionPolicy=one-shot` is the default and produces a scheduled-slot session key such as `cron:<agent>:<entry>:<scheduled_ms>`.
- `sessionPolicy=sticky` keeps continuity for that cron entry, but the scheduler forces the key into `cron:<agent>:<entry>:sticky:<suffix>` so it cannot collide with interactive session keys.
- Cron transcript and trajectory paths live under `agents/<agent>/cron-sessions/`, not the normal interactive `sessions/` directory.
- `cron-runs` lists admitted, worker-enqueued, runtime-enqueued, running, terminal, retry-pending, and quarantined cron runs.
- `cron-run-control --action retry --run-id <id>` resets the run to retry-pending, clears stale worker/runtime refs, and lets the scheduler clear the matching watermark and enqueue a new worker job with an attempt-specific idempotency key.
- `cron-run-control --action skip|quarantine|unquarantine` can stop or isolate bad cron work without blocking unrelated agents. Worker and runtime dispatch re-check CronRunStore controls and tombstone skipped runtime queue items instead of running stale work.

### Deterministic Cron Execution Policy

Deterministic entries may define `timeoutMs` and `maxAttempts` in `workspace/docs/ops/cron-canon.json`. Defaults remain `300000` milliseconds and `3` attempts for backward compatibility. Valid bounds are `1000..86400000` milliseconds and `1..10` attempts; invalid values fail scheduler lint and prevent enqueue.

`TZ` or `CRON_TZ` lines in a crontab apply to current-slot and restart catch-up evaluation. Standard minute, hour, day-of-month, month, and day-of-week fields are evaluated in that timezone. Worker payloads receive `AGENT_HARNESS_CRON_ENTRY_ID`, `AGENT_HARNESS_CRON_SCHEDULED_FOR_MS`, and, when configured, `AGENT_HARNESS_CRON_TIMEZONE`, so a local command can make one scheduled occurrence idempotent.

Only a file named exactly `crontab` or ending exactly in `.crontab` is loaded. Backup and temporary copies such as `.crontab.bak-*` and `.crontab.tmp` are ignored. Long-running or externally visible jobs should use an explicit catch-up policy that suppresses unsafe stale occurrences after restart.

## Response Formatting

`harness-config.json` can configure how intermediate Codex assistant narration and final channel reply tone are surfaced:

```json
{
  "response": {
    "assistantNarrationMode": "progress_panel",
    "assistantNarrationMaxChars": 1200,
    "assistantNarrationProgressMinUpdateMs": 2500,
    "assistantNarrationFinalPrefix": "Work log",
    "emojiAccentMode": "off",
    "emojiAccentAgentModes": {
      "main": "subtle",
      "ops": "off"
    },
    "emojiAccentChannelModes": {
      "telegram:12345": "subtle"
    },
    "progressDeliveryMode": "on",
    "progressDeliveryMaxNonterminalUpdatesPerLane": 6,
    "progressDeliveryMaxNonterminalBodyUpdatesPerQueue": 6,
    "progressDeliveryStatusHeartbeatAfterBodyCapMs": 300000,
    "progressDeliveryAgentModes": {
      "ops": "off"
    },
    "progressDeliveryChannelModes": {
      "telegram:group-alpha": "off"
    }
  }
}
```

Assistant narration modes:

- `progress_panel`: default. Store narration for audit, render it as the latest progress current step, and keep final channel replies final-answer only.
- `inline_preface`: include a compact work-log preface before the final answer.
- `off`: keep raw runtime artifacts but do not show narration in progress or final replies.

Emoji accent modes:

- `off`: default. Do not mechanically append an accent.
- `subtle`: opt-in. Append one small accent to successful `agent-reply` text only.

The accent policy is applied at the final `agent-reply` outbox boundary after a successful runtime turn when `subtle` is selected. It does not alter command replies, `/status`, error replies, progress/status messages, code-heavy replies, fenced code blocks, risk/security/status-style replies, or text that already ends with an emoji. Channel overrides win over agent overrides, and agent overrides win over the global default. Channel selectors can be `platform:channelId:userId`, `platform:channelId`, `channelId`, or `platform`.

Progress delivery modes:

- `on`: default. Deliver eligible progress panels normally.
- `off`: mute eligible progress panels while final `agent-reply` delivery remains unchanged.

`progressDeliveryMode` sets the global default, `progressDeliveryAgentModes` overrides by agent id, and `progressDeliveryChannelModes` overrides by channel selector. Channel selectors use the same progress event identity order as delivery: `platform:channelId:thread:threadId`, `platform:channelId`, `channelId:thread:threadId`, then `channelId`, so a group/topic-specific mute can be narrower than a whole channel mute. Channel overrides win over agent overrides, which win over the global setting. Supported off aliases are `off`, `none`, `hidden`, `disabled`, `disable`, `false`, `mute`, and `muted`; supported on aliases are `on`, `enabled`, `enable`, `true`, `progress_panel`, and `progress-panel`.

`progressDeliveryMaxNonterminalUpdatesPerLane` is the backward-compatible body/action lane cap. `progressDeliveryMaxNonterminalBodyUpdatesPerQueue` is the clearer alias for the same setting. The default is `6`; set it to `0` only for staging when provider-visible churn is acceptable. This cap applies to the event-history body/action lane, not the status/current-step lane.

`progressDeliveryStatusHeartbeatAfterBodyCapMs` controls low-frequency status/current-step heartbeat edits after the body/action lane reaches its cap. The default is `300000` milliseconds. Status heartbeats still obey text-hash dedupe and terminal `Done`/`Failed` convergence bypasses all non-terminal caps.

## Goal Autonomy

Goal transition and final-outbox gating are always observable, but autonomous child enqueue is independently activated. The safe default is observation-only:

```json
{
  "goalAutonomy": {
    "mode": "observe",
    "activeLaneDigests": [],
    "sliceHardTimeoutMs": 1800000,
    "sliceIdleTimeoutMs": 300000,
    "sliceDrainWindowMs": 180000,
    "wallClockBudgetMs": 172800000,
    "maxSlices": 96,
    "maxTotalTokens": 100000000,
    "maxNoProgressSlices": 6,
    "maxRecoverySlices": 12
  }
}
```

`mode` accepts `disabled`, `observe`, or `active`. Active mode is still fail-closed unless the current canonical FullLaneKey SHA-256 is explicitly listed in `activeLaneDigests`; a lane from another account, channel, user, agent, runtime class, root session, or concrete session is not a substitute. The validator requires every digest to be 64 hexadecimal characters and requires at least one digest when mode is `active`.

An active goal slice writes a unified transition before final-outbox selection. If continuation is authorized, the runtime commits a deterministic intent before enqueue, keeps `campaignSliceGeneration` separate from context-recovery `continuationIndex`, reconciles commit/enqueue interruptions on restart, and acknowledges the intent only after the child acquires the runtime lane lease. Candidate/foundation cutover must remain `observe`; activating one reviewed exact-lane C2-G cohort is a separate live operation requiring explicit approval and rollback evidence.

Goal slices are bounded to 30–45 minutes. `sliceIdleTimeoutMs` must be at least one minute and below the hard timeout. `sliceDrainWindowMs` is deliberately derived and must equal the smaller of 10% of the hard timeout or 180000 ms, matching the runtime deadline-drain guard. `wallClockBudgetMs` cannot exceed 48 hours. Slice-count, cumulative token-cost, repeated-progress-fingerprint, and recovery-count breakers are positive hard boundaries; reaching one stops continuation through the unified transition and emits at most one sanitized terminal notice. `goal-campaign-status --target-home <staging-home>` is read-only and reports the effective policy plus the latest append-only budget receipt for each campaign.

## Connector Approval Policy

Side-effecting MCP connector elicitations use `security.connectorApprovalPolicy`; this is separate
from Codex command/file approval and sandbox configuration. The default is `needs-user`.

```json
{
  "security": {
    "connectorApprovalPolicy": {
      "default": "needs-user",
      "rules": [
        {
          "connector": "github",
          "actions": ["create_issue"],
          "mode": "explicit-action-token"
        }
      ]
    }
  }
}
```

Valid modes are `deny`, `needs-user`, and `explicit-action-token`. Rules are matched by connector
and, when non-empty, action name. `deny` returns one protocol-valid denial and terminalizes that
effect generation. The two approval modes park the turn promptly, emit a distinct
`WaitingForApproval` progress state, and mint a short-lived capability bound to the exact
platform/account/channel/user/agent lane, action, parameter digest, and effect generation.
Protected live-control actions remain denied even if a rule would otherwise allow approval.

The provider-neutral channel commands are `/approve <ahx1_...>` and `/deny <ahx1_...>`. They are
command-only replies and never enqueue a new model request directly. A valid approval commits one
exact-lane continuation; repeating the same decision reuses the same child. Wrong-lane, expired,
opposite-decision, stopped, superseded, or digest-mismatched capabilities fail closed. Raw bearer
tokens are stored only in the protected latest-state snapshot and are excluded from generic
receipts, command effects, public serialization, and logs.

If a process stops after the initial `Requested` snapshot but before the approval disposition is
durable, restart re-applies the same connector policy under the stable effect id and creates one
protected approval capability or a denial before any remote submission.

Approved GitHub issue creation resumes with a stable opaque `agent-harness-effect` marker that the
connector action must preserve in the issue body. Recovery uses only an exact marker lookup bound
to the approved parameter digest: a complete query may prove absence, while a partial query remains
unprovable. Connectors with native idempotency support use the stable `ahx-<effect-id>` key and must
return readback evidence bound to the same connector, action, and parameter digest. Unsupported or
mismatched readback stays `ambiguous`; it never authorizes blind resubmission.

## Backend Authentication

Each backend provider uses a canonical harness-owned Codex home. The default OpenAI provider uses `<harness-home>/codex-home`; named providers use `<harness-home>/codex-home-providers/<provider>`. The runtime never falls back to `~/.codex`, `%USERPROFILE%/.codex`, a global `CODEX_HOME`, or global npm authentication.

New provider homes are created with current-operator-only permissions: Unix mode `0700`, or a protected Windows DACL containing one current-process-token SID allow ACE with full control and object/container inheritance. `backend-auth --action doctor` verifies this shape and cannot report ready when it drifts. Existing provider homes are not silently rewritten during ordinary source/runtime resolution; permission migration belongs to the explicitly approved stopped-state cutover.

`backendAuth.runtimeGateEnabled` is a boolean and defaults to `false`. When enabled in a staged candidate, an OpenAI turn without durable `ready` state is recorded as nonterminal `auth-deferred`: the queue is preserved, no Codex process or final/error outbox is produced, and retry budget is unchanged. A strictly newer ready generation appends one deterministic `retry-pending` wake before marking the continuation resumed; restart reconciliation uses the same event key and cannot duplicate that wake. Keep the gate disabled in live configuration until the auth T2/T3 gates and explicit live-cutover approval are complete.

Use the operator-only CLI surface with the deployment-owned canonical Codex executable:

- `backend-auth --action status` reads secret-free durable lifecycle state.
- `backend-auth --action probe` and `--action refresh` reconcile state through app-server `account/read` plus `model/list`; an account without model/capability evidence is not runtime-ready.
- `backend-auth --action login-browser` runs Codex's interactive browser login.
- `backend-auth --action login-device-code` runs the interactive device-code flow.
- `backend-auth --action login-api-key-stdin` reads the key only from operator stdin; never pass it as an argument or environment value.
- `backend-auth --action cancel` writes an exact operation-correlation cancel request; the owning operator process kills its child and reconciles `cancelled` without persisting the raw operation/login id.
- `backend-auth --action logout` removes credentials through the canonical Codex executable and reconciles with `account/read`.
- `backend-auth --action doctor` checks cold/pending/ready state, provider lease/cancel consistency, and same-generation account/capability receipt evidence without starting a login.

Pass `--provenance-receipt <candidate-receipt.json>` for candidate/auth operations. The receipt must match the canonical executable, exact 0.144.5 version, and ready provenance result; backend-auth receipts store only its SHA-256 reference, never a machine-local receipt path.

Login challenges and credentials are transient operator-process data. They must not be copied into harness configuration, logs, receipts, public diffs, Discord/Telegram messages, or runtime queue payloads. Source/staging implementation does not activate authentication in the live deployment.

## Prompt Files

Prompt files are resolved per configured agent. The main agent uses the source workspace. A non-main agent uses its configured `workspace`, or `agents/<agent-id>/workspace` by default; if that isolated workspace or agent directory is absent or has no prompt files, the harness does not silently borrow the main agent's files.

Agent ids used for worker session storage must be one safe path component: no absolute path, `.` / `..`, slash, backslash, colon, or control character. Worker transcript reconciliation also canonicalizes the expected `agents/<agent-id>/sessions` (or `cron-sessions`) directory and refuses symlink/path escapes. Use stable simple ids such as `main` or `reviewer`.

The eight canonical files are:

- `AGENTS.md`: workspace operating instructions.
- `SOUL.md`: persona and voice guidance.
- `TOOLS.md`: tool usage policy.
- `USER.md`: user preferences.
- `IDENTITY.md`: agent identity.
- `HEARTBEAT.md`: liveness or cadence guidance.
- `BOOTSTRAP.md`: startup context.
- `MEMORY.md`: agent-scoped durable memory guidance intended for prompt injection.

These eight files are static agent context. Dynamic memory recall is planned and assembled separately on each eligible turn; recalled records are not a ninth manifest file and do not change the static-file ledger.

`AGENT.md` is a fallback alias for `AGENTS.md`, and `BOOT.md` is a fallback alias for `BOOTSTRAP.md`. When canonical and alias files both exist, the canonical file wins and prompt assembly records a warning. Aliases are migration inputs, not additional prompt layers.

Every agent turn emits `agent-harness.agent-prompt-manifest.v1`. The manifest inventories all canonical names, content hashes, source paths, roles, and one of `included`, `reused`, or `removed`. A file removed after prior injection produces a tombstone; it is not silently remembered. A changed file is injected again. A changed Codex backend/thread generation also forces reinjection even when file bytes are unchanged.

Ledger reuse requires the exact `platform`, `accountId`, `channelId`, `userId`, `agentId`, `runtimeClass`, virtual-session root, and concrete `sessionKey`, plus the backend generation. Another agent, another exact lane, a `/new` root, or a new Codex backend generation cannot inherit the prior manifest entry. Exact-lane OperationPlan context also fails closed on a mismatched lane.

Skills are selected dynamically. Command-only turns may legitimately select zero skills; ordinary agent turns can inject relevant `SKILL.md` bodies.
