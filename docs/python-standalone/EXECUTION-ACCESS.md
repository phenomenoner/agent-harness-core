# User-selected native execution access

The personal trusted-agent product can explicitly select ordinary-user access for
daily work. This is independent of channel, model effort and approval presentation.
The same contract applies to local CLI, Telegram, Discord and ordinary child work.

In native `ahc.foreground.config.v3`, `execution_profile="user-full-access"`
requires a granted `danger-full-access` policy and a Linux Python/IPython workspace
without `workspace.protection`. The persistent workspace then uses the enclosing
Linux user's filesystem, mounted drives, network and executable permissions.
AHC process custody, deadlines, exact session/workspace identity, checkpoint
continuity, result acceptance and unknown-effect handling still apply.

The profile neither changes the execution UID nor grants root/sudo, mounts a drive,
enables Windows interop or borrows another distribution. Core configuration,
runtime state, memory, skills, interpreters and backend artifacts remain Linux-owned.
An already mounted Windows path is an explicit task input; its absence or failure
affects the task using that path rather than unrelated Linux work.

The WSL deployment default enables fixed-drive automount, Windows executable
interop and Windows PATH append through the distribution's own configuration.
This is a deployment capability, separate from the foreground access grant.
Explicit host mappings describe available drive roots; required Linux core state
does not move to them. Do not create a required host-drive boot mount in `fstab`.

Full-access native workspaces receive the enclosing user's HOME/name and a native
program PATH, with standard Windows command directories for an explicitly mapped
C drive. They copy no channel/provider secrets or arbitrary parent environment.
When local socket/status metadata identifies a long-lived WSL init server,
the workspace may use that server for Windows calls. Missing, denied or stale
discovery leaves Linux workspace creation available and does not start a broker,
connect to Windows, guess an operator socket or borrow another guest.

Bound Windows operations below the surrounding workspace deadline. A known tool
failure may be reported before continuing Linux work; an unconfirmed Windows
effect remains unknown and must not be repeated. Linux cgroup cleanup does not
prove cleanup of a separately launched Windows process. Qualify positive Windows
execution and failed-operation/Linux continuation from each actual affected engine;
setting a flag or passing configuration tests alone is not runtime acceptance.

User-service deployment selects `NoNewPrivileges=yes` and verifies that its native
workspace children retain that flag. This prevents new exec-based privilege gains;
it does not claim containment against every privileged service a trusted user can
already contact. Configured instructions describe the selected daily access and
direct resource checks through `workspace.execute`, rather than treating the
provider backend's separate namespace as the workspace's filesystem view.

The selected provider backend may retain its sealed configuration/history boundary
while harness-owned Python tools access user resources. Backend-native tools and
MCP capabilities still follow their declared contracts; full filesystem access
does not invent an unavailable tool. Staged provider authentication remains
explicitly provisioned. Its containing path is visible to the trusted user's Python
workspace in this profile, so this profile does not claim hostile-code credential
isolation. Credential contents must not appear in logs or user-facing evidence.

Instruction-only reversible learning can use the owner's explicit trusted daily
grant. Protected governance skills, owner binding, existing tool declarations,
finite trial limits and the durable operator stop remain enforced by the learning
ports. This is authorization, not an OS containment claim for learned instructions.
Separately configured programmatic evaluations retain the existing offline,
ephemeral namespace, scratch limits and runtime/skill-store protection. Selecting
daily access does not configure or qualify that evaluator automatically.

Agent profiles remain independent. Memory, skill and channel APIs keep their exact
agent/route scope and never merge another agent's data automatically. Agents sharing
one unrestricted OS user are personal trusted agents; this mode does not promise
filesystem secrecy between them. An inactive profile is not activated by this grant.

Configurations without this explicit profile retain their prior behavior. Mixed
full-access/workspace-protection declarations and partial grants are rejected;
a failed protected launch never falls back into this profile. Windows `v2` is not
reinterpreted. Preview reports configuration only; a deployment must verify the
actual ordinary-user process, target-path access and both affected interface owners.
