# Channel Notifications

Updated: 2026-11-08. Status: source implementation with controlled verification. Deployment and real-client rendering remain separate.

## Presentation Policy

| Surface | Interactive conversation | Scheduled work, including long runs |
| --- | --- | --- |
| Typing | Available when enabled | Suppressed |
| Working timer/activity card | Available when enabled | Suppressed |
| Commentary and explicit progress | Bounded public progress | Suppressed without a send claim |
| New approval conversation | Existing policy applies | No prompt or wait; no new authority |
| Final response | 💬 **回覆** or 🔔 **任務提醒** | ⏰ **定時任務** or ⏰ **定時任務提醒** |
| Ordinary new user message | Existing queue/steer preference | Independent foreground conversation request |

Saved occurrence and handoff provenance determine the category. A long interactive task remains interactive. Ordinary child tasks return to their parent and do not independently acquire its chat route. Channel-controlled trusted runtimes use one background owner with separate schedule sessions/workbenches in the same store. Already-submitted legacy tasks are not automatically migrated or replayed.

Headings use emoji and bold text. Telegram projects bold headings into HTML entities; Discord uses Markdown. The text examples below show the presentation source.

## Examples

```text
💬 **回覆**
This batch of skill checks is complete. Two candidates passed;
the remaining candidates and activation are still pending.
```

```text
⏰ **定時任務**
Historical-memory follow-up: synchronization is progressing.
Retrieval and final inventory acceptance still await backfill completion.
```

```text
⏰ **定時任務提醒**
The scheduled check needs an authorization unavailable in the background.
No additional permission was granted; user action is required.
```

The model supplies task scope and conclusions; the runtime supplies the category. A category does not declare the project complete. One final text may have its existing result attachments. A scheduled result whose complete output is exactly `NO_REPLY` is acknowledged durably with `suppressed:scheduled:no-reply` and produces no text or attachment send. Its pending selected-attachment intents receive the same suppression receipt, including through direct attachment delivery. The stored result, artifact selection and artifact bytes remain intact; historical sent/unknown receipts are not rewritten or retried. Whitespace variants, interactive results and other text remain ordinary output. This is an explicit sentinel policy, not semantic deduplication; the schedule instruction must decide when silence is appropriate.

## Compatibility And Failure Handling

Prefixes are projections for claimed sends. Historical result bytes, outbox payloads, answer seals and sent/unknown receipts are neither rewritten nor replayed. Occurrence provenance survives restart and schedule pause/removal.

When a long conclusion is delivered as a text document, its caption retains the category and the file contains the complete conclusion. Untyped legacy text documents retain their existing caption.

A quiet task cannot acquire approval implicitly. Prompt-required effects receive unavailable feedback, allowing a final action-required report. Uncertain sends retain the existing no-replay boundary.

The main channel remains the user-interaction surface. A background model runs independently of the foreground model and its response delivery. `/new` and `/stop` act on the foreground; background failures retain their own custody. `/restart` first pauses new background admission, then waits for current work and its delivery. Existing model, effort, YOLO permissions, memory and tools remain available in both execution scopes. Backup/restore keeps its exclusive global gate.

## Reference Products

[Hermes scheduled tasks](https://hermes-agent.nousresearch.com/docs/user-guide/features/cron) execute in fresh agent sessions without a chat platform attached and automatically deliver final results. [OpenClaw automations](https://docs.openclaw.ai/cli/cron) separate isolated execution from conversation result delivery. These references informed the presentation policy; AHC retains its existing execution/session model and does not claim implementation parity. References checked on 2026-10-18.
