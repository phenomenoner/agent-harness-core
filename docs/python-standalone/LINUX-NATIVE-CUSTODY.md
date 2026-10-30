# Native process custody and recovery

This extends the [native process mechanism](LINUX-NATIVE-PROCESS.md) into the
existing runtime repository. It does not introduce another database or broker.

Do we really need this to make things happen? Yes: losing an acknowledgement must
not erase an accepted task or permanently confuse a new controller with old processes.
Is there a simpler and more direct way? Reuse the existing pre-effect intent, owner
lease, workspace generation and custody transactions, with an explicit native identity.
Selected mechanism: **EMBED**. Existing Windows transient-unit records retain their
exact format; native cgroups are never represented as fictional systemd units.

A native identity binds machine identity, kernel boot, enclosing service name and
InvocationID, relative child cgroup path, device and inode. A creation attempt reserves
this exact empty cgroup before the existing transaction persists intent. Only then
may a runner release the workload gate. The same identity must be returned and
committed before stream attachment. A failed database write cannot authorize launch.

Missing creation acknowledgement retains the intent and unknown task outcome.
Reconciliation of the recorded attempt acquires the existing owner lease, validates
the full group identity and cleans only that exact generation. An absent group under
the verified enclosing owner can establish process absence, not business success.
An enclosing service-generation replacement permits retirement only after its exact
old child path is absent; another machine, owner service, mismatched inode or unavailable
observation remains blocked. No old workload or delivery is replayed.

Cleanup reopens a group only under the verified service scope, compares the retained
device/inode to the opened directory, and uses the same opened kernel handle for its
effect. Replacement between the comparison and kill cannot redirect that handle.
Same-UID hostile movement is outside D-037; supported owners never reuse UUID names.
The existing database owner/execution locks prevent competing supported controllers
from changing the same session/workspace custody while recovery or attachment proceeds.

Main backend, persistent workspace and ordinary children all consume the same native
runner contract and retain their existing distinct role/child bindings. A current
session, execution grant or child permit remains required independently of custody.
Native admission is not permission widening. Legacy records cannot be automatically
interpreted as native cleanup authority during migration.

Required evidence: legacy record readability, full-identity tamper rejection,
pre-effect persistence ordering, lost-ack cleanup without task replay, role separation,
foreign/stale owner refusal, parent crash/restart and a distinct successful next task.
Controlled peers qualify lifecycle seams; real provider/channel acceptance is separate.
