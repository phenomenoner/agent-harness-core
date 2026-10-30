# Linux native process ownership

Status: implementation contract for isolated qualification; no live migration.
Parent decision: [whole-product Linux standalone](PRODUCT-ARCHITECTURE.md).

## Outcome and mechanism

An explicitly owned native process can use local protocol pipes, create descendants,
exit or fail, and be cleaned before another process is admitted. Cleanup must not
signal a replacement PID or cgroup. A controller crash must leave its descendants
inside the enclosing service's cleanup boundary.

Do we really need this to make things happen? Yes: local backend, persistent
interpreter, MCP and supervisor execution require bounded descendant cleanup.
Is there a simpler and more direct way? Embed an ephemeral cgroup owner in the
existing managed-process API, under one delegated systemd user service. Deleting
children removes product behavior; manual cleanup does not support continued use;
process groups alone cannot retain descendants that call `setsid()`.

Selected mechanism: **EMBED + PLATFORM_PRIMITIVE**. No broker, new durable journal
or per-task systemd unit is introduced. Linux cgroup v2 delegation is described
by the [systemd delegation contract](https://systemd.io/CGROUP_DELEGATION/).
The parent service runs as a non-root user with `Delegate=yes`,
`KillMode=control-group` and `SendSIGKILL=yes`. Normal foreground operation may
run attached to that service; an arbitrary unmanaged shell is not a qualified
owner and must reject managed execution before starting a workload.

The trust model is D-037 personal trusted execution. This is not hostile same-UID
isolation: a program deliberately moving itself to a different delegated group or
starting an independent service is outside the containment claim. Such an independent
unit must never be represented as an ordinary descendant of this service.

## Start, effect and cleanup contract

1. Confirm the current process is directly inside a qualified delegated user service,
   with the current service invocation and exact cgroup path. Reject root and missing,
   unsupported or unobservable ownership before starting any workload.
2. Allocate a fresh UUID child cgroup. Keep open directory, kill and events handles;
   this kernel object, not a later path or PID lookup, is cleanup authority.
3. Start only a fixed inert Python bootstrap. It waits on a private gate and cannot
   execute the requested command until it has moved itself into the owned cgroup.
   EOF, a malformed gate, timeout or unavailable membership means no workload exec.
4. Read the bootstrap acknowledgement and its process identity, then release the
   gate once. Target argv, environment and cwd are explicit; no shell interpolation,
   Windows launcher or inherited private environment is involved. Exec failure or
   lost acknowledgement must not imply that an already released command had no effect.
5. Cleanup writes to the retained `cgroup.kill` descriptor at most once and waits on
   the retained `cgroup.events` descriptor for `populated 0`. Reap the direct child.
   Unavailable observation or timeout remains incomplete. A subsequent cleanup may
   observe completion but must not replay an indeterminate kill.
6. Success is cached. Resource handles remain owned until verified completion;
   unresolved cleanup cannot be relabelled successful by closing descriptors.
   Empty UUID group removal is only local bookkeeping; it grants no kill authority.

The destructive seam is final ownership observation -> cgroup kill. A retained
kernel file descriptor spans this interval, so replacement by another cgroup at
the same pathname cannot redirect the effect. No raw PID signal is used for
managed cleanup. The bootstrap moves its own PID, eliminating a parent-side
check-then-migrate PID reuse race. One non-reused UUID belongs to one owner;
normal code never adopts an existing group or discovers groups to kill by name.

## Verification and remaining integration

Focused Linux tests must cover command/stdio behavior, exact environment and argv,
descendants after `setsid`, parent exit before cleanup, one-shot kill and incomplete
observation, rejected unmanaged execution, failed pre-exec admission, stale
descriptor replacement, and a separate next process after cleanup. Parent-service
crash/stop readback must show zero residual descendants. These are local process
claims, not actual model dispatch or delivery acceptance.

The shared managed-process integration affects MCP stdio and optional supervised
services. Legacy Windows Job behavior remains compatible. The existing WSL backend
and workspace factories are not made native by this primitive alone. Native
configuration, durable backend/workspace custody, ordinary/native child composition,
recovery, dependency packaging and installed foreground scenarios require their
own evidence before whole-product readiness can be claimed.

Risk: wrong-target cleanup has high consequence and unknown frequency; fail closed
at missing owner/descriptor evidence. Empty bookkeeping-directory retention has low
consequence and is bounded by the enclosing service lifetime. No release, migration,
credential provisioning, receiver switch or live acceptance follows from these tests.
