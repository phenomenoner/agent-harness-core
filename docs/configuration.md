# Configuration

Agent Harness Core reads most operator state from a harness home directory supplied with `--harness-home` or `--target-home`. Keep that directory outside git or under an ignored `.agent-harness/` path.

## Common Paths

- `harness-config.json`: harness runtime and security settings.
- `state/`: receipts, queues, logs, supervisor files, worker database, and channel state.
- `secrets/`: optional local env files for channel/provider/memory credentials.
- `skills/`: bundled and imported skill indexes used by prompt assembly.

## Runtime Workspaces

Use `--source-home` for imported prompt files, registry, skills, and legacy context. Use `--runtime-workspace` only for the Codex working directory. Prompt assembly falls back to the imported source workspace when the runtime workspace does not contain prompt files.

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

