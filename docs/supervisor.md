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

## Start Fallback

The generated `start-scheduled-tasks.ps1` first tries `Start-ScheduledTask`. If a task is not registered, it starts the generated runner script directly as a hidden PowerShell process. This makes local operator handoff work even before Task Scheduler registration succeeds.

When the script is invoked from a live agent session (`AGENT_HARNESS_LIVE_SESSION=1`), it validates `AGENT_HARNESS_LIVE_CONTROL_TOKEN` or `-LiveControlToken` through `ops-cutover-status` before mutating live scheduled tasks or clearing stop files. Local staging scripts are unaffected unless the live-session marker is present.

## Stop Files

The generated stop script creates stop files for each loop. Long-running loops check those files and stop gracefully after active work reaches a safe point.

`status --json` reports each loop's stop-file path, presence, and reason, and `healthz` treats an active stop file as not live. A fresh heartbeat timestamp is not enough to prove a loop is healthy if the heartbeat references a missing process or an active stop file remains in `state/supervisor/stop`.

The generated stop and uninstall scripts use the same live-control guard as start. A live channel agent turn must not stop/uninstall its own gateway path; it should create an `ops-cutover-request` ticket and wait for operator approval.

## Log Retention

Generated runner scripts keep the newest 20 supervisor logs per component before writing a new log.

