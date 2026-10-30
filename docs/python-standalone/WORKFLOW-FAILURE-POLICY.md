# Workflow failure policy

Status: accepted product decision, 2026-10-07. Canonical decision: D-040.

The accepted [task lifecycle and constraint register](TASK-LIFECYCLES.md) makes
this policy explicit for each task family. Every blocking restriction must map
to its lifecycle node and demonstrate practical necessity. Unmapped checks are
removal items. Useful task completion and the next/continuation task are the
minimum acceptance path.

The ordinary path is receive a message, admit its task, execute under the selected
permissions, accept the result, and deliver it to the original route. A missing
enhancement or an informational notification must not turn a useful answer into
a failed task. Each mandatory check must protect a named effect or data boundary.
This is a product-wide contract across supported interfaces, including the local
CLI, Telegram and Discord. A channel adapter may implement transport-specific
authentication, cursors, formatting and acknowledgements; it must not impose a
stricter gate on optional information than the shared task engine.

## Failure semantics

Under D-041, a local tool problem stops the affected operation, not the model.
Keep the original model turn active and return structured fault/confirmed-progress
feedback. If safe correction is unavailable, ask it to conclude with completed
progress, the problem, the reason execution stopped and a useful next step.
Unknown effects fence subsequent tools in that turn; the final report remains
deliverable with `report_accepted=true`, while the task is `accepted=false` and
`disposition=unknown`. Preserve actual uncertainty and resource custody. The
existing immutable seal and original-route outbox protect the report separately
from task success. A model/transport outage, explicit cancellation, invalid owner
or expired hard deadline uses the grounded fallback without another submission.

Persisted fallback notices keep the presentation used when they were created.
Reading an outbox validates the notice against recognized, deterministic renderings
of its bound terminal result; a wording update must not invalidate historical
sent or unknown deliveries. It must neither rewrite nor replay them. An arbitrary
replacement still fails the parent/result binding. Deployment readiness must
include a successful foreground cycle after initial receiver readiness, including
historical outbox reconciliation; initial startup publication alone is insufficient.

Use bounded **fail-open** for discarded information and optional presentation.
Skip the failed enhancement, preserve a sanitized indication, and continue the
independent task. Do not retry it within the same turn. The existing frame,
deadline, cancellation and owner cleanup budgets still apply.

Use **fail-soft** when a requested capability is unavailable: return an explicit
unavailable or incomplete result for that capability while allowing independent
work. Unavailable recall means unknown memory availability, not an empty memory
store. Unusable usage means unknown usage, not zero cost. Missing skill evidence
does not establish material skill use or a successful improvement.

Use **fail-closed at the affected effect** for unproven authorization, foreign
identity, compromised state integrity, unowned process execution, or an uncertain
external effect. Retain the original owner and pending evidence. Keep independent
controls and unaffected work available where their ownership is established.
Never guess a recipient, broaden permissions, switch providers, recreate an
uncertain operation, or count an advisory as a completion event.

Classify exceptions by the named effect and current phase, separately from
resource health. An unsupported export or failed control stream before calling
the artifact publisher cannot mean an unknown publication. Known rejections
permit corrected input, an independent alternative or a useful text result.
Local immutable publication can use one exact readback after receipt loss;
reconciliation never repeats the write. Arbitrary execution and ambiguous
external delivery retain UNKNOWN when their outcomes cannot be established.
Their failure notice preserves this task's confirmed workspace/checkpoint/local
artifact observations without claiming task completion or platform delivery.
The same phase separation applies to workspace checkpoints: unsupported primitive
context is a no-save rejection; a bad capture stream fences the workspace without
inventing a save. An exception after invoking the checkpoint writer retains the
possible write as unknown. Native child wake/status events cannot consume an old
terminal twice or reuse a completed call ID to establish parent completion.

Workspace admission before interpreter invocation is a known no-code-effect
failure, including temporary writer contention. Return actionable feedback and
permit a distinct current-owner call instead of poisoning the parent turn.
After a trustworthy cell reply, a busy step-receipt writer permits one exact
active-owner readback: an already-ready head resolves the receipt; an executing
head permits one same-owner receipt CAS. This does not repeat interpreter code.
Failed readback, revoked identity and exhausted receipt recovery retain UNKNOWN
at that effect. Preserve the original fixed failure phase/category even when
cleanup also fails, and record cleanup health separately. Optional diagnostic
metadata cannot change acceptance or expose arbitrary exception payloads.
Each synchronous workspace invocation retains its own execution-started, failure
and recovery observations through tool projection. A concurrent denied admission
cannot change the active invocation's cleanup or contaminate another call's
recovery metadata. Returned identity is the confirmed identity of that call;
possible-effect failure fencing occurs before leaving its admitted scope.

## Interface consistency

Transient Discord connection failures rearm after a cancellable cooldown. A
connection burst limit protects socket churn; it is not a lifetime reception
quota. Fatal authentication/configuration, failed session invalidation and
unavailable Identify quota admission remain affected-effect pauses. The shared
task engine and other independently owned interfaces continue.

READY, RESUMED, dispatch and invalid-session callbacks retain durable failure
categories before protocol exception projection. Failed checkpoint/admission
does not use the transient socket retry branch: the receiver preserves prior
sequence and pauses to avoid borrowing another writer's state or duplicating
input. This is a receiver-local pause, not a product-wide task stop.

| Interface | Shared behavior | Interface-specific protected boundary |
|---|---|---|
| Local CLI | Use the same task admission, backend, optional recall, acceptance and cleanup contract. Channel-only presentation is absent. | Selected workspace, session, policy and durable operation identity. |
| Telegram | Optional command-menu/toast failures, unavailable quotes and presentation failures do not suppress independent questions or controls. | Bound bot/chat/sender, durable update cursor, original destination and consume-once callback/outbox ownership. |
| Discord | Optional slash-menu setup runs independently of Gateway work. Unavailable quotes and presentation failures do not suppress independent questions or controls. | Bound account/channel/sender, Identify admission, Gateway sequence/resume transaction, exact interaction ownership and original destination. |

When a reply reference has an unusable shape, omit its content, identifiers and
files and record `quote_status=unavailable`. Continue only the independently
verified current message. A reference to a foreign channel is discarded without
reading its content or files. Neither case grants access to a guessed reference
or turns quoted text into a control command. If the requested answer requires
unavailable quoted material, report that limitation rather than inventing it.
Corrupted authoritative task/history records remain a separate integrity failure.

Verify both channel adapters through receive, current question, deterministic
control reply, final delivery, a subsequent distinct task and restart/no-replay.
Include a failed optional presentation and unusable reply metadata in the composed
path. A passing Telegram route cannot substitute for Discord evidence. Controlled
transport/model peers qualify the local composition, not live server delivery.

## Workflow disposition

The table identifies the current boundaries and the checks selected for removal
or degradation. It is an audit disposition, not a claim that every product feature
has completed live acceptance.

| Stage | Product behavior | Implementation seam / disposition |
|---|---|---|
| Startup and configuration | Parse each retained configuration once. Reject a foreign agent, incompatible declared configuration, or undeclared permission. | `foreground_selection.select_foreground`: remove duplicate parsing of the identical retained document; retain comparison for a different path. |
| Dependencies and workspace | Missing prerequisites disable their dependent capability. Do not run an unowned fallback interpreter or silently change grants. | `ForegroundRuntime.start` and workspace creation: retain exact owner admission; workspace-independent controls already remain available on typed startup failures. |
| Channel receive | Admit only verified messages for the configured route; keep pending input across temporary transport failure. | Telegram/Discord receivers: retain identity, cursor and bounded recovery. Transport retry is not permission to replay work. |
| Control commands | `/status`, `/stop`, model settings and approvals use the deterministic control path independently of model work. | `ChannelControlPump` / `ChannelControlStore`: retain route, consume-once approval and durable reply ownership; local writer contention already defers without permanently stopping ingress. |
| Task acceptance | Admit one operation once under the current session, goal, capabilities and workspace. | `RuntimeService` / runtime repository: retain transactions, generation fences and duplicate reconciliation. |
| History and quoted input | Supply only correctly bound historical data; unusable optional reply context becomes unavailable while verified current input proceeds. | `telegram_reply` / `discord_reply` discard unverified nested quote/file effects; foreign references cannot leak content. Conversation readers retain authoritative history integrity and unfinished-work distinctions. |
| Automatic recall | Ordinary questions proceed without automatic recall when the service is missing or returns typed unavailable. | `foreground_turn_context.turn_context`: downgrade missing service to unavailable; retain profile, scope and grant checks in `MemoryPackService`. Explicit memory actions retain their own result. |
| Skills and learning | Recommendations and experience capture do not grant execution. Preserve pinned ownership and honest use evidence. | `SkillTurnService` / `WorkspaceLearningTools`: retain revision and manifest integrity; experience capture already records a gap without blocking the task. Skill validation failure never authorizes an unpinned substitute. |
| Backend settings | Compare the selected effective semantics using explicitly supported representations. | `validate_thread_configuration`: retain cwd, model, provider, approval and permission readback. Linux temporary-directory compatibility remains bounded to its existing supported projection. |
| Advisory notifications | Discard unused no-id notifications, including account, quota, warning, plans, diffs, tool progress, error/retry observations and future informational methods. Their payloads have no execution or completion role. | `consume_advisory`: retain a fixed advisory category without payload/enum/timestamp checks. Requests with an RPC `id`, reserved actionable methods, policy, lifecycle and owned stream handlers retain their actual validation. No advisory enrolls or settles a descendant or extends the task deadline. |
| Usage observation | Validate counters before reporting them. A bad current-owner sample invalidates that owner's prior sample; foreign samples are discarded without changing independent owners. | `consume_status`: degrade token observation failures; retain exact thread/turn and typed counter checks for usable observations. A usage event cannot enroll a child. |
| Typing and activity cards | Main work continues when presentation construction/start or status observation fails. | `ForegroundRuntime`: retain a constructed presentation owner; successful close releases custody even if its status accessor fails. An unverified close still fences conflicting work. |
| Result and delivery | Seal a verified result and send through the original durable outbox. Preserve unknown deliveries without automatic resend. | `accept_observation`, `ChannelDelivery` and outboxes: retain completion correlation, atomic acceptance and destination checks. No success may be inferred from a progress card. |
| Cleanup, diagnostics and continuation | Separate resource cleanup from optional status. Continue independent controls; retry only under the existing finite recovery contract. | Foreground cleanup and Canvas observer: retain process/workspace custody; status/Canvas failures cannot replace an accepted answer. A subsequent distinct task must be exercised after a downgrade. |

## Rules for future checks

Windows/WSL interop and operator entry are optional to an already running Linux
core. A failed observation leaves Linux state unknown; it cannot cancel work or
block native admission. Requested Windows-only operations return unavailable
locally while independent Linux work continues. A timed-out control may already
have committed and remains unknown without replay. Native core paths exclude
Windows mounts; WSL startup/lifetime qualification is a separate platform claim.
These rules apply equally to local CLI, Telegram, Discord and every declared agent.

Before adding or retaining a blocking check, name the protected effect, its owner,
the evidence it uses, and the consequence of proceeding without it. Prefer deleting
a duplicate, using the existing boundary, or skipping an optional enhancement.
Revalidate mutable identity immediately at its effect; do not mistake duplicate
parsing or presentation health for additional authority protection.

Keep permissive handling narrow: known inert notification methods can be discarded,
but generic protocol exceptions cannot all be converted into successful turns.
Advisory diagnostics retain only method/category/code and bounded counts;
never copy warning text, account data, credentials or arbitrary exception text.
Diagnose a recurring downgrade when it reduces useful capability; it does not
automatically become a blocker for unrelated tasks or a reason for a wider review.

## Acceptance and release boundary

Focused regressions must distinguish the former blocking behavior from degraded
execution, demonstrate a subsequent distinct task, and retain negative tests for
settings drift, foreign identity, missing completion, notification floods and
uncertain cleanup/delivery. Simulated protocol peers do not prove a real provider
turn or user-visible delivery. Installed-package and service selection evidence
remain separate from source results.

This decision supersedes blanket fail-closed interpretations for optional
enhancements and discarded metadata. D-009, D-010, D-028 and the remaining D-037
authority, custody and no-replay contracts continue to govern their named effects.
Accepted historical evidence retains its original policy. This decision grants
no additional migration, credentials, external publication or live-operation scope.
