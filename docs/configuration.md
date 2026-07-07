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
    "manualRecoveryAllowed": true,
    "modelContextWindow": 128000,
    "modelAutoCompactTokenLimit": 100000,
    "modelAutoCompactTokenLimitScope": "total",
    "toolOutputTokenLimit": 12000
  }
}
```

The optional model/token keys are passed through to generated Codex TOML as `model_context_window`, `model_auto_compact_token_limit`, `model_auto_compact_token_limit_scope`, and `tool_output_token_limit`. `compactPrompt` and `experimentalCompactPromptFile` are also accepted and passed through as `compact_prompt` and `experimental_compact_prompt_file`.

Each `codex-run` writes per-execution `codex-context-preflight.json` and appends `state/runtime-queue/codex-context-preflight-receipts.jsonl`. When a resumed thread is over the compact threshold, the harness calls official app-server `thread/compact/start` before `turn/start` and waits for the `contextCompaction` item to complete. Hard context-window failures are classified as `context-exhausted`; the harness retries once after official compact, then writes `codex-context-checkpoint.json` and `codex-context-rollover.json` and opens a fresh Codex thread when fallback is enabled.

## Cron Run Isolation

Native cron LLM turns use CronRunStore before worker enqueue:

- `sessionPolicy=one-shot` is the default and produces a scheduled-slot session key such as `cron:<agent>:<entry>:<scheduled_ms>`.
- `sessionPolicy=sticky` keeps continuity for that cron entry, but the scheduler forces the key into `cron:<agent>:<entry>:sticky:<suffix>` so it cannot collide with interactive session keys.
- Cron transcript and trajectory paths live under `agents/<agent>/cron-sessions/`, not the normal interactive `sessions/` directory.
- `cron-runs` lists admitted, worker-enqueued, runtime-enqueued, running, terminal, retry-pending, and quarantined cron runs.
- `cron-run-control --action retry --run-id <id>` resets the run to retry-pending, clears stale worker/runtime refs, and lets the scheduler clear the matching watermark and enqueue a new worker job with an attempt-specific idempotency key.
- `cron-run-control --action skip|quarantine|unquarantine` can stop or isolate bad cron work without blocking unrelated agents. Worker and runtime dispatch re-check CronRunStore controls and tombstone skipped runtime queue items instead of running stale work.

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

## Prompt Files

Prompt bundle generation adds explicit role headers for known prompt files. Examples:

- `AGENTS.md`: workspace operating instructions.
- `SOUL.md`: persona and voice guidance.
- `TOOLS.md`: tool usage policy.
- `USER.md`: user preferences.
- `IDENTITY.md`: agent identity.
- `HEARTBEAT.md`: liveness or cadence guidance.
- `BOOTSTRAP.md`: startup context.

Skills are selected dynamically. Command-only turns may legitimately select zero skills; ordinary agent turns can inject relevant `SKILL.md` bodies.
