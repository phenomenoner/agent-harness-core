# Agent Harness TG/Discord DM Self-Check Guide

Date: 2026-06-25

This guide is the operator handoff for asking the `main` agent to verify the live Telegram DM and Discord DM channel paths from inside Agent Harness. It is designed for a single normal-message turn in each DM channel, with the agent doing as much read-only verification as it can and returning artifact pointers for operator follow-up.

Current activation baseline:

- Harness home: `D:\Warehouse\Rust-OpenClaw-Core\.agent-harness`
- Active prompt/config authority: `D:\Warehouse\Rust-OpenClaw-Core\.agent-harness\workspace`, `.agent-harness\openclaw.json`, and `.agent-harness\harness-config.json`
- Harness CLI: `D:\Warehouse\Rust-OpenClaw-Core\target\debug\agent-harness.exe`
- Retired source snapshot archive: `D:\Warehouse\Rust-OpenClaw-Core\imports\openclaw-core-snapshot`
- Runtime workspace/Codex cwd: `D:\Warehouse\Rust-OpenClaw-Core\.agent-harness`
- Codex runtime executable: generated live runners should pass `--codex-exe D:\Warehouse\Rust-OpenClaw-Core\.tools\codex-cli\node_modules\@openai\codex-win32-x64\vendor\x86_64-pc-windows-msvc\bin\codex.exe`; an extensionless npm shim or Codex Desktop MSIX resource path is not a valid live app-server executable.
- Active agent: `main`
- Latest readiness target: `ready=true`, no failed checks, and no unexpected warnings. Exact pass counts may drift as new readiness checks are added.
- Latest channel outbox target: `pending=0`, `retryable=0`; any new test backlog should drain after delivery settles.
- Live loops currently expected in status after the current cutover: `runtime-loop`, `worker-loop`, `progress-delivery-loop`, `telegram-loop`, `telegram-loop-xiaoxiaoli`, `discord-outbox-loop`, `discord-gateway-loop`, and `cron-scheduler-loop`.
- Supervisor plan: canonical generated entries cover the always-on runtime, worker, progress, Telegram, Discord outbox/gateway, and cron loops; the preserved `telegram-loop-xiaoxiaoli` runner is started with the same pinned Codex executable. `runtime-loop` is a single process with bounded in-process runtime concurrency via `--runtime-concurrency 12`; in supervised infinite mode it should keep safe-mode restart enabled.
- Channel identity: when a binding registry is configured, the platform/account/channel tuple must resolve to `main` before the DM reaches the model.
- Same-session runtime ordering: ordinary channel-origin `main` turns are serialized per `agent + platform + channel + user + sessionKey`; `workerDispatch.channelConcurrencyLimit=3` is still a broader worker/fan-out cap and must not be interpreted as permission for the same DM session to run multiple main-agent Codex turns at once.
- Agent-boundary freshness: channel/session/runtime changes must preserve `agentId` as a routing boundary. A same-agent old session may be suppressed after `/new`; a completed turn for another agent on the same platform/channel/user must not be suppressed only because the shared channel state currently points at `main` or another agent. The full scenario pack is in `docs/agent-harness-topology-contract.md`.
- Codex context recovery: context preflight writes `state/runtime-queue/codex-context-preflight-receipts.jsonl` plus per-execution `codex-context-preflight.json`; hard context-window failures should surface as `context-exhausted` with official compact retry or checkpoint/fresh-thread recovery metadata instead of a generic failed-terminal reply.
- Runtime-loop liveness rule: missing/stale/error/stopped/stopping runtime-loop heartbeat is FAIL for live readiness. `safe-mode` is WARN/degraded because the loop is still alive but has reduced runtime concurrency to 1.
- Large-ledger rule: `status`, `healthz`, and readiness may report sampled log/outbox/runtime counts after tail-sampling large JSONL files. Treat sampled counts as an operational window and check the warning text before comparing historical totals.

## Safety Rules

- Use allowed-user DM channels only.
- Do not print tokens, env file contents, OAuth files, API keys, cookies, raw Discord/Telegram IDs, or raw gateway secrets.
- Prefer read-only CLI checks. Normal DM handling will naturally write runtime, transcript, outbox, delivery, and progress receipts.
- Do not run destructive commands, cleanup commands, broad recursive deletes, or credential export commands.
- Do not enqueue cron/subagent work during this check.
- If a check cannot be verified from inside the agent turn, report it as `operator-post-check-needed` instead of claiming pass.

## What One DM Turn Can Prove

A single normal DM sent to `main` can prove:

- The platform inbound adapter accepted the allowed DM.
- The message was routed to `main`.
- A runtime queue item was prepared and executed through Codex, and same-session queue ordering would keep a second ordinary DM message queued until the first reaches terminal status.
- Prompt assembly, imported memory context, model/session continuity, and transcript/trajectory recording were available to the turn.
- The agent can read or summarize status artifacts if shell/filesystem tools are available.
- The final response contains a channel-specific sentinel that the operator can use to verify delivery.

A single turn cannot fully self-prove final delivery of its own reply, because delivery receipts are written after the agent finishes. The agent should include artifact paths and the sentinel; the operator verifies delivery afterward.

## Operator Preflight

Run this before sending the DM prompts if you want an external baseline:

```powershell
Set-Location D:\Warehouse\Rust-OpenClaw-Core
.\target\debug\agent-harness.exe enable-check --harness-home .\.agent-harness
.\target\debug\agent-harness.exe healthz --target-home .\.agent-harness --require-writable-state
.\target\debug\agent-harness.exe status --harness-home .\.agent-harness --json
.\target\debug\agent-harness.exe worker-status --harness-home .\.agent-harness
.\target\debug\agent-harness.exe cron-scheduler-lint --harness-home .\.agent-harness --source-home .\.agent-harness --workspace .\.agent-harness\workspace --enable
```

Expected baseline:

- `enable-check`: `Ready: yes`, `failed=0`, and only understood warnings.
- `healthz`: `ready=true`, `live=true`, writable state when `--require-writable-state` is used, and no runtime-loop missing/stale/error/stopped/stopping state. `runtime-loop safe-mode` is degraded WARN, not a silent pass.
- `status`: `ready=true`, `runtime.openItems=0`, and `runtime.latestNonIdleRunOnce` should show the last real runtime event instead of an idle `no-work` tick or retryable `lease-busy`.
- `loops.heartbeats`: all active loop heartbeats present and fresh; current live scheduler deployments include `cron-scheduler-loop`.
- `cron-scheduler-lint`: `status=ok` or understood warnings before enabling scheduler changes; errors block scheduler cutover.
- `workers.totals.failedTerminal=0`
- channel outbox has no unexpected retryable backlog

## Telegram DM Prompt

Send this as a normal Telegram DM to the harness bot from an allowed user. Do not send it as a slash command.

```text
[AGENT_HARNESS_SELF_CHECK]
channel=telegram_dm
agent=main
correlation=tg-main-selfcheck-YYYYMMDD-HHMM
mode=read-only-self-check

請你作為 Agent Harness 的 main agent，自動確認目前 Telegram DM channel 的 live 功能狀態。

請執行或推論以下檢查：
1. 確認你正在處理 Telegram DM normal-message turn，並回報 agent/session/model/thinking 狀態；不要揭露 raw token 或敏感 ID。
2. 若你有 shell/filesystem tool access，請在 D:\Warehouse\Rust-OpenClaw-Core 執行 read-only checks：
   - .\target\debug\agent-harness.exe enable-check --harness-home .\.agent-harness
   - .\target\debug\agent-harness.exe status --harness-home .\.agent-harness --json
   - .\target\debug\agent-harness.exe worker-status --harness-home .\.agent-harness
3. 檢查或摘要：runtime queue 是否沒有 open backlog、status 是否有 latest non-idle runtime event、loop heartbeats 是否包含 runtime/worker/progress/telegram/discord-outbox/discord-gateway/cron-scheduler（若 scheduler 已啟用）、runtime-loop 是否是單 loop bounded concurrency 而不是多個 runtime-loop-N 互搶 queue、runtime-loop 是否沒有 missing/stale/error/stopped/stopping（safe-mode 只能算 WARN）、worker failedTerminal 是否為 0、worker config 是否是 global=12/per-agent=6/per-agent-channel=3、runtimeDispatch 是否對 interactive main-agent same-session turn 使用 perSessionMaxActive=1/sessionFifo=true、Codex context preflight/recovery receipts 是否存在或可解釋為本 turn 尚未觸發 recovery、memory hook/prompt-context 是否可用、plugin hooks/memory-slot receipts 是否存在。
4. 檢查 prompt context：Prompt files 不應是 0/7；目前 `.agent-harness/workspace` 若沒有 `BOOTSTRAP.md`，`6/7` 是可接受 baseline。每個 prompt file 應有明確作用說明並要求 agent 遵守，例如 AGENTS.md 是 workspace instructions、SOUL.md 是 persona/voice、TOOLS.md 是 tool policy、USER.md 是 user preferences、IDENTITY.md 是 identity、HEARTBEAT.md 是 cadence/liveness、BOOTSTRAP.md 是 startup context。Skills 是 dynamic task context；若本 turn 沒選到 skill，應說明這可能正常，而不是當成 prompt-file loading failure。
5. 確認本 turn 可作為 Telegram inbound allowed-user routing、main agent dispatch、Codex runtime、prompt/memory context、transcript/trajectory write 的 live smoke。
6. 回覆時必須包含 sentinel：TG-DM-SELF-CHECK:tg-main-selfcheck-YYYYMMDD-HHMM
7. 以 PASS/WARN/FAIL 報告。無法在本 turn 內自證的 delivery receipt 請標成 operator-post-check-needed，並給 operator 要看的 artifact paths。

請用以下格式回報：
- Overall:
- Channel:
- Runtime:
- Memory:
- Plugins:
- Workers:
- Delivery:
- Artifact pointers:
- Follow-up:
```

Telegram-specific pass criteria:

- Agent identifies the current check as Telegram DM.
- Agent returns the exact `TG-DM-SELF-CHECK:<correlation>` sentinel.
- No secret values are printed.
- Readiness/status summary is `ready=true` with no failed checks, or the agent explains any drift.
- Artifact pointers include at least:
- `state/runtime-queue/run-once-receipts.jsonl`
- `state/runtime-queue/codex-context-preflight-receipts.jsonl`
- `state/runtime-queue/codex-runtime-completion-receipts.jsonl`
  - `agents/main/sessions/`
  - `state/channels/outbox.jsonl`
  - `state/channels/delivery-receipts.jsonl`
  - `state/supervisor/loop-heartbeats/telegram-loop.json`

## Discord DM Prompt

Send this as a normal Discord DM to the harness bot from an allowed user. Do not send it as a slash command.

```text
[AGENT_HARNESS_SELF_CHECK]
channel=discord_dm
agent=main
correlation=discord-main-selfcheck-YYYYMMDD-HHMM
mode=read-only-self-check

請你作為 Agent Harness 的 main agent，自動確認目前 Discord DM channel 的 live 功能狀態。

請執行或推論以下檢查：
1. 確認你正在處理 Discord DM normal-message turn，並回報 agent/session/model/thinking 狀態；不要揭露 raw token 或敏感 ID。
2. 若你有 shell/filesystem tool access，請在 D:\Warehouse\Rust-OpenClaw-Core 執行 read-only checks：
   - .\target\debug\agent-harness.exe enable-check --harness-home .\.agent-harness
   - .\target\debug\agent-harness.exe status --harness-home .\.agent-harness --json
   - .\target\debug\agent-harness.exe worker-status --harness-home .\.agent-harness
3. 檢查或摘要：Discord gateway heartbeat 是否 live、discord-outbox-loop 是否 live、runtime queue 是否沒有 open backlog、status 是否有 latest non-idle runtime event、runtime-loop 是否是單 loop bounded concurrency 而不是多個 runtime-loop-N 互搶 queue、runtime-loop 是否沒有 missing/stale/error/stopped/stopping（safe-mode 只能算 WARN）、cron-scheduler-loop 是否在 scheduler 已啟用時 live、worker failedTerminal 是否為 0、worker config 是否是 global=12/per-agent=6/per-agent-channel=3、runtimeDispatch 是否對 interactive main-agent same-session turn 使用 perSessionMaxActive=1/sessionFifo=true、Codex context preflight/recovery receipts 是否存在或可解釋為本 turn 尚未觸發 recovery、memory hook/prompt-context 是否可用、plugin hooks/memory-slot receipts 是否存在。
4. 檢查 prompt context：Prompt files 不應是 0/7；目前 `.agent-harness/workspace` 若沒有 `BOOTSTRAP.md`，`6/7` 是可接受 baseline。每個 prompt file 應有明確作用說明並要求 agent 遵守，例如 AGENTS.md 是 workspace instructions、SOUL.md 是 persona/voice、TOOLS.md 是 tool policy、USER.md 是 user preferences、IDENTITY.md 是 identity、HEARTBEAT.md 是 cadence/liveness、BOOTSTRAP.md 是 startup context。Skills 是 dynamic task context；若本 turn 沒選到 skill，應說明這可能正常，而不是當成 prompt-file loading failure。
5. 確認本 turn 可作為 Discord DM inbound Gateway routing、main agent dispatch、Codex runtime、prompt/memory context、transcript/trajectory write 的 live smoke。
6. 回覆時必須包含 sentinel：DISCORD-DM-SELF-CHECK:discord-main-selfcheck-YYYYMMDD-HHMM
7. 以 PASS/WARN/FAIL 報告。無法在本 turn 內自證的 final REST delivery receipt 請標成 operator-post-check-needed，並給 operator 要看的 artifact paths。

請用以下格式回報：
- Overall:
- Channel:
- Runtime:
- Memory:
- Plugins:
- Workers:
- Delivery:
- Artifact pointers:
- Follow-up:
```

Discord-specific pass criteria:

- Agent identifies the current check as Discord DM.
- Agent returns the exact `DISCORD-DM-SELF-CHECK:<correlation>` sentinel.
- No secret values are printed.
- Readiness/status summary is `ready=true` with no failed checks, or the agent explains any drift.
- Artifact pointers include at least:
  - `state/channels/discord-gateway-events.jsonl`
  - `state/channels/discord-gateway-probe-receipts.jsonl`
- `state/runtime-queue/run-once-receipts.jsonl`
- `state/runtime-queue/codex-context-preflight-receipts.jsonl`
- `state/runtime-queue/codex-runtime-completion-receipts.jsonl`
  - `agents/main/sessions/`
  - `state/channels/outbox.jsonl`
  - `state/channels/delivery-receipts.jsonl`
  - `state/supervisor/loop-heartbeats/discord-gateway-loop.json`
  - `state/supervisor/loop-heartbeats/discord-outbox-loop.json`

## Operator Post-Check

After the agent replies in each channel, run:

```powershell
Set-Location D:\Warehouse\Rust-OpenClaw-Core
.\target\debug\agent-harness.exe status --harness-home .\.agent-harness --json
.\target\debug\agent-harness.exe enable-check --harness-home .\.agent-harness
.\target\debug\agent-harness.exe healthz --target-home .\.agent-harness --require-writable-state
.\target\debug\agent-harness.exe channel-outbox-plan --harness-home .\.agent-harness --platform telegram --limit 10
.\target\debug\agent-harness.exe channel-outbox-plan --harness-home .\.agent-harness --platform discord --limit 10
```

Pass criteria:

- Both DM replies are visible to the operator and contain the expected sentinel.
- `enable-check` and `healthz` remain `ready=true` with no failed checks.
- `status --json` shows all active loop heartbeats fresh, including `cron-scheduler-loop` when scheduler is enabled.
- `runtime.openItems=0`.
- `channels.outbox.all.pending=0` after delivery settles.
- `delivery-receipts.jsonl` has delivered entries for the new Telegram and Discord self-check replies.
- Native attachment delivery should work for structured outbox attachments: Telegram uses `sendPhoto`/`sendDocument`; Discord uses multipart upload. Plain `MEDIA:<path>` lines from assistant output should be converted into structured attachments by runtime before delivery.
- Final `agent-reply` payloads contain the final answer or terminal notification only. Progress-panel narration, repeated in-progress notes, and diagnostic working narration must remain in progress receipts/panels and must not be delivered as a normal Telegram or Discord reply.
- No new `workers.totals.failedTerminal` entries.
- No sensitive values appeared in the agent replies.

## Optional Command Smoke

The self-check prompts above are normal-message smoke tests. They do not prove slash-command parsing end to end because the agent cannot originate separate inbound slash commands from inside the same turn. If command smoke is needed, send these manually in each DM channel after the normal-message smoke:

```text
/status
/model
/think high
/new
/btw channel command smoke note
/steer keep the next reply concise
/stop
```

Expected command smoke:

- Commands return command replies without entering the model path where appropriate.
- `/think` and `/model` report or update channel state according to permissions.
- `/new`, `/btw`, and `/steer` write channel command state.
- `/stop` records intent and writes a runtime cancel marker for the active session; a running Codex app-server turn should record `canceled` and stop on the runtime poll loop.
- `state/channels/outbox.jsonl`, `state/channels/delivery-receipts.jsonl`, and `state/channels/<platform>/.../state.json` reflect the command interactions.

## Result Recording Template

Use this section when pasting results into a handoff note.

```markdown
## TG DM Self-Check Result

- Time:
- Correlation:
- Overall:
- Sentinel seen:
- Agent session:
- Delivery receipt:
- Transcript/trajectory:
- Warnings:

## Discord DM Self-Check Result

- Time:
- Correlation:
- Overall:
- Sentinel seen:
- Agent session:
- Delivery receipt:
- Transcript/trajectory:
- Warnings:
```
