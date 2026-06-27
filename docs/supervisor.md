# Windows Supervisor

`supervisor-plan` writes a Windows handoff bundle under:

```text
<harness-home>\state\supervisor\windows-scheduled-tasks
```

The bundle contains runner scripts plus install/start/stop/uninstall scripts. It does not register tasks automatically.

## Runtime Loop

`--runtime-workers <n>` maps to one `runtime-loop` process with:

```text
--runtime-concurrency <n>
```

One runtime loop owns queue inspection and dispatch. Per-item leases and worker limits prevent a long turn in one channel from blocking unrelated channel work when capacity is available.

When `supervisor-plan` is run without `--codex-exe`, it should discover a repo-local spawnable Codex CLI and pin that concrete path into generated runtime-capable scripts. On Windows, the preferred default is `.tools\codex-cli\node_modules\@openai\codex-win32-x64\vendor\x86_64-pc-windows-msvc\bin\codex.exe`, with `.tools\codex-cli\node_modules\.bin\codex.cmd` as fallback. Generated live runners should not depend on an extensionless npm shim or Codex Desktop MSIX resource path through `PATH`.

## Start Fallback

The generated `start-scheduled-tasks.ps1` first tries `Start-ScheduledTask`. If a task is not registered, it starts the generated runner script directly as a hidden PowerShell process. This makes local operator handoff work even before Task Scheduler registration succeeds.

When the script is invoked from a live agent session (`AGENT_HARNESS_LIVE_SESSION=1`), it validates `AGENT_HARNESS_LIVE_CONTROL_TOKEN` or `-LiveControlToken` through `ops-cutover-status` before mutating live scheduled tasks or clearing stop files. Local staging scripts are unaffected unless the live-session marker is present.

## Stop Files

The generated stop script creates stop files for each loop. Long-running loops check those files and stop gracefully after active work reaches a safe point. `ops-control stop` writes structured JSON stop files with `serviceId`, `reason`, `createdBy`, `createdAtMs`, and `persistent`; legacy plain-text stop files remain readable as a reason.

`status --json` reports each loop's stop-file path, presence, reason, and structured metadata, and `healthz` treats an active stop file as not live. A fresh heartbeat timestamp is not enough to prove a loop is healthy if the heartbeat is corrupt, references a missing process, or an active stop file remains in `state/supervisor/stop`.

## Observe-Only Service Registry

Loop heartbeat writers also write per-service records under:

```text
<harness-home>\state\supervisor\services
```

Each record uses `agent-harness.supervisor-service-state.v1` and includes `serviceId`, `serviceKind`, `generationId`, `pid`, `startedAtMs`, `lastHeartbeatAtMs`, `lastSuccessfulIterationAtMs`, `iteration`, `desiredState`, `actualState`, `servicePriority`, `deliveryLane`, and `restartDelayMs`. `status --json` reports these records under `loops.services`, while `healthz` reports them under `supervisorServices`.

This registry is observe-only for loops that still use the existing external runner model. It also records supervisor-owned children as they migrate, giving operators a single service-state surface during the transition.

`supervisor-run --service progress-delivery-loop` and `supervisor-run --service discord-outbox-loop` are the first supervisor-owned child paths. Generated progress delivery and Discord outbox runner scripts start this Rust wrapper instead of launching their child loops directly. The wrapper starts each child with a stable service generation id, waits for process exit, writes restart/backoff state into `state\supervisor\services\<service>.json`, and restarts after failures. Progress is marked as telemetry priority; Discord outbox is marked as final-delivery priority with a shorter restart delay. Runtime, worker, and ingress runners remain externally owned until their later migration phases.

## Runner Logs

Generated runners write all process streams directly to per-loop log files and rotate old logs with `Select-Object -Skip 20`. They no longer pipe long-running loop output through `Tee-Object`.

The generated runtime-loop runner writes `runtime-loop-runner-safe-mode.json` after process-level exits. That state includes `errorClass`, `logFile`, restart count, `restartAfterSeconds`, and `memoryGateDecision`; OOM or memory-pressure signatures are classified as `resource-exhausted`, write a temporary structured stop file for `progress-delivery-loop`, and use a longer bounded restart delay.

The generated stop and uninstall scripts use the same live-control guard as start. A live channel agent turn must not stop/uninstall its own gateway path; it should create an `ops-cutover-request` ticket and wait for operator approval.

## Log Retention

Generated runner scripts keep the newest 20 supervisor logs per component before writing a new log.

