# Native Linux foreground configuration

`ahc.foreground.config.v3` selects native Linux execution explicitly. The host does
not infer or convert a Windows configuration. `v2` retains its Windows/WSL contract
for compatibility and controlled migration; neither version changes session or route
identity automatically.

Native workspace kinds are `linux-python` and `linux-ipython`. They carry `cwd`,
`interpreter`, `execution_timeout`, `lifetime_seconds` and optional `lifetime_mode`.
There is no distribution, Windows launcher or user-switch field. The enclosing
non-root delegated systemd user service supplies execution ownership. Native backend
kind is `codex-linux-trusted`; its pinned bundle, explicit home, model, effort and
policy keep the existing trusted Codex protocol contract.

Runtime, skill, workspace/interpreter, backend/home and native memory locations use
normalized absolute POSIX paths outside `/mnt`; `/` is not an owned resource root.
Service generation applies the same rule to configuration and credential files.
These are core dependencies, distinct from explicit Windows file inputs. An accepted path is configuration,
not proof of its filesystem, permissions or deployed instance. Native skill storage
is a proper descendant of the service user's `~/AHC_workspace`; Windows retains
its existing workspace base. Migration must select and verify the actual locations.

### Explicit ordinary-user daily access

`execution_profile="user-full-access"` selects the native user's available
filesystem, network and programs for the ordinary Python/IPython workspace. It
requires `policy.sandbox_mode="danger-full-access"`, `workspace_granted=true`
and no `workspace.protection`. A sealed provider backend can remain configured
independently. It does not add a UID switch, root/sudo or drive mounting.
The WSL deployment separately enables fixed-drive automount and executable interop
by default. Full-access workspaces use the enclosing user's HOME/name, a native
PATH and standard Windows command paths for a declared C mapping. Optional local
WSL init-server discovery never makes Windows availability a startup prerequisite.
Protected and omitted profiles retain their existing empty-environment behavior.
See [execution access](EXECUTION-ACCESS.md) for instruction learning, isolated
evaluation and same-user agent boundaries. Omitted profiles retain existing behavior.

### Opt-in protected Python workspace

Native `linux-python` may explicitly include `workspace.protection`. It has exactly
`executable`, `sha256`, `readonly_roots`, `protected_roots` and `resource_limits`.
The executable is the pinned native namespace-isolation binary; its lowercase SHA-256
is verified by the existing boundary before launch. Root lists are nonempty, unique
normalized POSIX paths (at most 16 each); `runtime_root` must appear explicitly in
`protected_roots`. The only persistent writable mount is derived from `workspace.cwd`.
Resource limits explicitly contain `memory_bytes`, `pids`, `cpu_quota_us`,
`cpu_period_us` and `swap_bytes`, using the existing native kernel-limit contract.

Parsing does not inspect another host's filesystem. Actual foreground workspace
creation validates the canonical existing mount roots, alias/overlap constraints,
isolation executable and finite resource limits. The delegated native service must
use the existing resource-owner pre-start preparation for cpu/memory/pids controllers;
missing controllers are a failure, not an unbounded fallback. `linux-ipython` and
v2 reject this opt-in in this first slice. Existing configurations without protection
retain their old behavior and must not be relabeled protected.

This protects only the selected Python workspace route. It does not protect backend,
ordinary-child or MCP execution, and cannot enable automatic learning promotion;
proposal-only learning retains its disabled promotion authority until route-wide
and lifecycle acceptance are separately closed.

Snapshot memory locators and native-memory reference files use POSIX paths in v3.
`ahc.native-memory.bindings.v2` separately identifies POSIX memory database/provider
locators; v1 retains Windows locators. Agent/resource/scope, provider digest, store
policy and grants remain explicit. Changing a path does not grant additional access.

Foreground selection, preview, diagnostics and recovery use the explicit schema
dispatcher. Main and ordinary-child backend factories share runner selection, while
the native persistent workspace reuses the existing durable Python protocol and
checkpoint semantics. A runner may start a fresh generation only after the prior
generation is proven clean; deadline callbacks retain their original process owner.

Source/staging qualification includes actual native processes and repositories:
sequential foreground tasks, ordinary-child completion, Python/IPython error
continuation, checkpoint/new-process restore and subsequent tasks. A controlled
protocol peer is integration evidence, not pinned backend/model or live-channel
acceptance. Whole-product migration and live acceptance remain separate gates.

## Bounded Windows operator entrance

All actions use one named-distribution client with a host deadline, including
output collection. `-TimeoutSeconds` aliases the existing `-HealthTimeoutSeconds`
parameter (default 15, range 1–60). `status` now uses the same structured unit
observation as `health`, rather than unbounded human-readable `systemctl status`.
Successful `logs` retains text output. Failed log/control entry returns a structured
`ahc.operator-observation.v1` result and exit 2. A start/stop/restart whose result
cannot be observed reports `controlOutcome=unknown`; do not replay it automatically.
An acknowledged control is not receiver/backend readiness. No action retries,
restarts WSL or changes another distribution. Only explicitly selected Linux
start/stop/restart commands mutate the named service.

### Unit health observation

The source operator entry supports `-Action health` with the existing explicit
`-Distribution`, `-LinuxUser` and `-Unit` selectors. Keep `tools/ahc-linux.ps1` and
`tools/ahc-health.ps1` together when staging the operator tools. This change does
not update an existing installed operator copy.

Health runs one read-only `systemctl --user show` command. Its Windows-side deadline
is 15 seconds by default (`-HealthTimeoutSeconds`, range 1–60), including output
collection. A timed-out diagnostic client is terminated through its retained process
handle, with at most one additional second for exit. There is no retry, receiver
start, distro restart, provider request or recovery action. Invoking WSL can start
the selected distro and its already-enabled services; this is not a passive probe
of a deliberately stopped distro. Maintenance operators must account for that effect.

The JSON result uses `ahc.operator-health.v1`. `observation` separates an observed
unit from `observation-timeout`, `probe-failed` and `invalid-observation`. Raw errors
are suppressed. An exact-unit, active process produces `unitState: running`, while
receiver, backend and memory readiness remain `unknown`. Unit state alone cannot
establish those layers. Exit 0 means a valid unit observation, including an inactive
or failed unit; exit 2 means the observation failed. Neither exit code proves Agent
availability or successful channel delivery.

`sampledAt` is UTC; `validForSeconds` is 120. Consumers must discard expired samples
and samples invalidated by clock changes, returning unknown instead of carrying
forward an old running state. This command is a one-shot observation, not an
installed watchdog or sustained-availability acceptance. The existing interactive
status/logs and explicitly requested control actions retain their behavior; use
health for the bounded machine-readable observation.
