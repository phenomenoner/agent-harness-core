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

## Stop Files

The generated stop script creates stop files for each loop. Long-running loops check those files and stop gracefully after active work reaches a safe point.

## Log Retention

Generated runner scripts keep the newest 20 supervisor logs per component before writing a new log.

