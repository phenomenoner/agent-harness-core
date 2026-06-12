# Agent Harness Assistant Narration Tech Note

## Purpose

Before 2026-06-11, Agent Harness delivered some Codex app-server intermediate
assistant updates as part of the final channel reply. That made
Telegram/Discord replies look like a combined work log plus final answer, even
when the operator only wanted the final response.

This note records the target behavior, implementation plan, and completed
implementation status for assistant narration routing.

## Implementation Status

Implemented on 2026-06-11.

- `assistantNarrationMode` config is parsed from `.agent-harness/harness-config.json`
  or `.agent-harness/config/harness-config.json`; default is `progress_panel`.
- Codex app-server `agentMessage` item ids/phases are captured structurally.
  `phase=commentary` becomes `assistant_narration`; `phase=final_answer`
  becomes the final assistant reply. Legacy delta-only streams fall back to raw
  assistant text instead of dropping content.
- `progress_panel` emits `AgentProgressKind::AssistantNarration` and renders the
  latest item as `Current step: ...` under the editable status panel.
- Transcript output stores `assistant_narration` rows before the final
  `assistant` row; completion receipts include selected mode, final character
  count, and narration item count.
- `runtime-run-once` records memory lifecycle from final assistant text and uses
  config only for channel outbound formatting.
- `inline_preface` is implemented for debug/operator channels; `off` suppresses
  narration in progress and final replies while retaining raw runtime artifacts.
- `emojiAccentMode` was added later as a separate final-reply tone policy. It is
  not part of assistant narration segmentation and is applied only at the
  successful `agent-reply` outbox boundary.
- Verification: `cargo test --workspace` passed with 172 core tests and 16 CLI
  tests after implementation.

## Pre-Implementation Behavior

Observed flow before the 2026-06-11 implementation:

1. Codex app-server emits assistant message deltas during a turn.
2. `codex_runtime.rs` appended all matching `agentMessage` / `message/delta`
   fragments into one `assistant_message` string.
3. The completion sink wrote that entire string as one transcript message with
   `role=assistant`.
4. `runtime_pipeline.rs` read the latest assistant transcript message and wrote
   it directly to the channel outbox as `agent-reply`.

Important code points:

- `crates/agent-harness-core/src/codex_runtime.rs`
  - `AssistantOutputCapture`
  - `record_completion_outputs(...)`
- `crates/agent-harness-core/src/runtime_pipeline.rs`
  - `latest_assistant_response(...)`
  - `ChannelOutboundMessageKind::AgentReply`
- `crates/agent-harness-core/src/progress.rs`
  - progress delivery supports editable Body and Status lanes and now renders
    assistant narration as the current step.

The leaked prefix is not hidden chain-of-thought. It is user-visible assistant
narration / progress narration that used to be flattened into the final reply.

## Product Decision

Add an operator-configurable assistant narration mode.

Default mode should be `progress_panel`.

Rationale:

- Long-running turns benefit from visible progress.
- Final channel replies should remain focused on the final answer.
- Existing progress delivery already edits/rate-limits status messages, which is
  the right surface for transient work narration.
- Operators can still disable narration or keep inline prefacing for debug use.

## Proposed Config

Location: `.agent-harness/harness-config.json`

Suggested schema:

```json
{
  "response": {
    "assistantNarrationMode": "progress_panel",
    "assistantNarrationMaxChars": 500,
    "assistantNarrationProgressMinUpdateMs": 2500,
    "assistantNarrationFinalPrefix": "Work log",
    "emojiAccentMode": "subtle",
    "emojiAccentAgentModes": {
      "ops": "off"
    },
    "emojiAccentChannelModes": {
      "telegram:12345": "off"
    }
  }
}
```

Modes:

- `off`
  - Do not show intermediate assistant narration in channel progress or final
    replies.
  - Preserve raw events/transcript artifacts for debugging and audit.
- `progress_panel`
  - Default.
  - Send intermediate assistant narration to progress delivery.
  - Render as the latest work step under the existing Working/Done status panel.
  - Final channel reply contains only the final assistant answer.
- `inline_preface`
  - Keep intermediate narration attached before the final answer.
  - Add clear formatting separators so operators can distinguish work narration
    from the final reply.
  - Intended for debugging/internal channels, not the default DM experience.

Final-reply tone policy:

- `emojiAccentMode = "subtle"`
  - Default.
  - Appends one small accent only to successful `agent-reply` text.
  - Does not alter command replies, `/status`, error/failure replies, progress
    status, fenced code, code-heavy replies, risk/security/status-style replies,
    or text that already ends in an emoji.
- `emojiAccentMode = "off"`
  - Disables the accent globally, per agent, or per channel.

Channel overrides win over agent overrides, which win over the global default.
Channel selectors can be `platform:channelId:userId`, `platform:channelId`,
`channelId`, or `platform`.

## Target Data Model

Do not continue treating assistant output as one flat `String`.

Introduce a structured capture model:

```rust
struct AssistantOutputCapture {
    raw_text: String,
    items: Vec<AssistantOutputItem>,
}

struct AssistantOutputItem {
    item_id: Option<String>,
    role: String,
    phase: AssistantOutputPhase,
    text: String,
    started_at_ms: Option<i64>,
    completed_at_ms: Option<i64>,
}

enum AssistantOutputPhase {
    Narration,
    Final,
    Unknown,
}
```

If app-server JSONL exposes item/message identifiers or completion status, use
those protocol fields as the primary separator. Heuristics should only be a
fallback.

## Segmentation Strategy

Preferred:

1. Capture assistant deltas per protocol item/message id.
2. Mark non-final assistant items as `Narration`.
3. Mark the final assistant item, or the assistant item nearest
   `turn/completed`, as `Final`.
4. Preserve all raw text in trajectory/debug artifacts.

Fallback when protocol metadata is insufficient:

1. Store raw accumulated text.
2. Split by explicit final-answer boundary markers when reliable.
3. If no reliable boundary exists, treat the whole text as final and log a
   warning instead of silently dropping user-visible content.

Avoid content-heavy semantic guessing as the primary implementation.

## Progress Panel Behavior

When `assistantNarrationMode = "progress_panel"`:

1. Intermediate narration emits an `AgentProgressEvent`.
2. Suggested event kind:
   - Add `AgentProgressKind::AssistantNarration`, or reuse a dedicated
     non-final assistant progress kind rather than `AssistantStream`.
3. Render only compact, sanitized text.
4. Respect:
   - `assistantNarrationMaxChars`
   - `assistantNarrationProgressMinUpdateMs`
   - existing progress delivery dedupe/rate-limit behavior
5. Keep final `agent-reply` free of intermediate narration.

Suggested status rendering:

```text
Working - 4 min - running tools
Current step: verifying skills-index readback
```

For Telegram/Discord providers, update the existing progress message when a
provider message id exists; avoid sending a new chat message for every narration
fragment.

## Inline Preface Behavior

When `assistantNarrationMode = "inline_preface"`:

```text
Work log
---
<intermediate narration, truncated or compacted>

Final reply
---
<final answer>
```

This mode should not be the default because it can produce noisy DM replies.

## Off Behavior

When `assistantNarrationMode = "off"`:

- Do not emit assistant narration progress events.
- Do not include narration in the final outbox reply.
- Still persist raw runtime artifacts for debug and regression analysis.

## Transcript and Audit Requirements

The runtime should write enough structured data to debug future incidents:

- Raw app-server JSONL remains unchanged.
- Transcript should contain the final assistant reply separately from narration.
- Trajectory should record that narration was captured and routed according to
  config.
- Completion/run receipts should include:
  - selected narration mode
  - final reply length
  - narration item count
  - narration routed/dropped/prefaced status

## Testing Plan

Unit tests:

1. Two assistant items: narration then final.
   - `progress_panel`: final outbox contains only final.
   - progress events contain narration.
2. Single final assistant item.
   - All modes preserve final answer.
3. No reliable final boundary.
   - Do not drop content.
   - Log warning.
4. `off` mode.
   - No narration progress event.
   - Final outbox excludes narration when final is known.
5. `inline_preface` mode.
   - Output contains clear work/final separators.
6. Progress rendering.
   - Narration is truncated, sanitized, deduped, and rate-limited.

Integration tests:

1. Simulated app-server JSONL with multiple assistant message items.
2. Telegram outbox generation.
3. Discord outbox generation.
4. Progress delivery edit path with existing provider message ids.

Regression checks:

- Final replies must not duplicate the progress panel text.
- `MEDIA:` directives must still be split from the final reply.
- Memory lifecycle recording should use final assistant text, not narration.
- Existing `AssistantStream` suppression remains intact.

## Rollout Plan

1. Inspect representative raw app-server JSONL from affected turns to confirm
   available item/message metadata.
2. Add response config parsing with default `progress_panel`.
3. Replace flat assistant delta accumulation with structured assistant output
   capture.
4. Split transcript output into final reply plus optional narration artifacts.
5. Route narration according to config.
6. Add unit and integration tests.
7. Run local harness smoke:
   - runtime run-once
   - Telegram outbox
   - Discord outbox
   - progress delivery plan/record
8. Deploy and restart Agent Harness.
9. Verify one Telegram DM and one Discord DM long turn.

## Open Questions

- Which app-server JSONL fields reliably identify assistant item boundaries?
- Should `progress_panel` show only the latest narration line or a compact list
  of the latest N steps?
- Should per-channel overrides be supported, for example Telegram using
  `progress_panel` while a debug Discord channel uses `inline_preface`?
- Should narration be stored in transcript as `role=assistant_narration` or only
  in trajectory/debug receipts?

## Recommended Defaults

```json
{
  "response": {
    "assistantNarrationMode": "progress_panel",
    "assistantNarrationMaxChars": 500,
    "assistantNarrationProgressMinUpdateMs": 2500
  }
}
```

`progress_panel` should be the production default for both Telegram DM and
Discord DM.
