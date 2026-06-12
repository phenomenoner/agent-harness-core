# Configuration

Agent Harness Core reads most operator state from a harness home directory supplied with `--harness-home` or `--target-home`. Keep that directory outside git or under an ignored `.agent-harness/` path.

## Common Paths

- `harness-config.json`: harness runtime and security settings.
- `state/`: receipts, queues, logs, supervisor files, worker database, and channel state.
- `secrets/`: optional local env files for channel/provider/memory credentials.
- `skills/`: bundled and imported skill indexes used by prompt assembly.

## Runtime Workspaces

Use `--source-home` for imported prompt files, registry, skills, and legacy context. Use `--runtime-workspace` only for the Codex working directory. Prompt assembly falls back to the imported source workspace when the runtime workspace does not contain prompt files.

## Runtime Terminal State

`timeout` run-once receipts are terminal for the parent queue id. Runtime selection, status open-item counts, native typing context, and progress delivery all treat the parent turn as closed after a timeout. To retry work, enqueue a new turn or explicit retry id rather than reusing the old queue id.

Progress delivery state is terminal-monotonic. Once a parent queue id has delivered terminal runtime progress, later stray events for that same queue id must not downgrade the status panel back to non-terminal working state.

Long-running jobs or local services that intentionally outlive the chat turn should be represented as managed worker/background jobs with independent accepted, heartbeat, status, completion, and cancellation receipts.

## Worker Limits

`harness-config.json` can define worker dispatch limits:

```json
{
  "workerDispatch": {
    "globalConcurrencyLimit": 12,
    "groupConcurrencyLimit": 6,
    "channelConcurrencyLimit": 3
  }
}
```

The invariant is:

```text
global limit >= per-agent limit >= per-agent-per-channel limit
```

Invalid narrower-to-wider settings are capped at runtime with warnings.

## Response Formatting

`harness-config.json` can configure how intermediate Codex assistant narration and final channel reply tone are surfaced:

```json
{
  "response": {
    "assistantNarrationMode": "progress_panel",
    "assistantNarrationMaxChars": 500,
    "assistantNarrationProgressMinUpdateMs": 2500,
    "assistantNarrationFinalPrefix": "Work log",
    "emojiAccentMode": "off",
    "emojiAccentAgentModes": {
      "main": "subtle",
      "ops": "off"
    },
    "emojiAccentChannelModes": {
      "telegram:12345": "subtle"
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
