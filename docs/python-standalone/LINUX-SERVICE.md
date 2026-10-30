# Native Linux installation and service ownership

Install Python 3.14 and the `ahc[linux]` wheel in an explicit Linux release directory.
The Linux extra includes IPython, Discord WebSocket support and MCP schema validation.
`uv.lock` records the complete dependency closure. Export with `uv export --frozen
--no-dev --extra linux --no-emit-project` and install the exported requirements with
hash checking before installing the separately verified AHC wheel. Never copy a
Windows virtual environment into Linux.

Keep release code, explicit config, authoritative state and user workspaces separate.
Service configuration, interpreter, workspace and credential paths must remain
Linux-owned; generation rejects `/mnt` paths. An explicit Windows file input does
not become a service dependency. The service removes `WSL_INTEROP` and `WSLENV`
and selects a Linux tool PATH.
Native foreground admission verifies a persistent Linux filesystem for the runtime
root; Windows-mounted, volatile, linked or unverified storage is rejected. This
does not convert data or establish a coherent migration snapshot.

An application TOML declares one or more `foreground_configs` entries matching its
profile registry. Each service selects one declared agent. If one agent has separate
channel stores, use separate application configurations and distinct unit names;
do not invent different agent identities to obtain process isolation.

`python/tools/prepare_linux_service.py` prepares a new unit file using explicit
interpreter, application config, agent, workspace and credential-file paths. It does
not install, enable or start the unit. The service runs as the current non-root user,
with `Delegate=yes`, `KillMode=control-group`, `SendSIGKILL=yes`, mode mask 0077 and
one bounded systemd restart policy: three starts per minute, three-second delay,
30-second stop deadline. A normal operator stop exits successfully and does not
trigger restart. A requested restart or abnormal exit may restart within that budget.

After migration qualification and single-owner receiver preflight, place or link
the reviewed unit under the user's systemd configuration, reload that user manager,
and enable/start only the selected unit. The exact service, installed executable,
config, InvocationID, cgroup and store owner must be read back. Do not run a second
restart wrapper around the same foreground owner.

Credential files must be explicitly provisioned for that owner and readable only by
the service user. They are references in generated units, never copied into public
metadata or routine evidence. An empty file is sufficient for isolated no-channel
verification; it cannot qualify authenticated operation.

On Linux, use `systemctl --user start|status|stop|restart <unit>` and `journalctl
--user --unit <unit>`. `tools/ahc-linux.ps1` supplies the corresponding optional Windows
entrance with explicit distribution, Linux user and unit arguments. It stores no AHC
state and does not participate in model IPC or per-task execution. WSL boot/login
integration is a separately configured entrance; the Linux application remains
operable without a Windows desktop client. Every Windows operator action has one
bounded wait. An unavailable observation reports unknown Linux state; a timed-out
control reports an unknown effect and must not be replayed automatically.

Interop loss is not guest loss. With native storage/network available, running
agents must continue channel reception, backend/workspace work and next-task
admission. This applies to all declared agents and both supported channels. WSL
instance lifetime is separately configured and qualified; enabled systemd units
and finite observation keepers do not establish indefinite guest lifetime. See
[the interop and lifetime boundaries](PRODUCT-ARCHITECTURE.md#interop-failure-containment).

Isolated service evidence includes real CLI/workbench startup, competing-owner
rejection, main-process crash, new-invocation restart, exact CLI status/stop and no
residual workspace cgroup or held custody. This does not establish host reboot,
overnight availability, migrated data or live-channel acceptance.
