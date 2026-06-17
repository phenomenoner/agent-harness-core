# Checkpoint - round6-2 openclaw-mem and progress stream review

Created: 2026-06-17 01:36 Asia/Taipei

## Status

Checkpoint saved for the round6-2 investigation line.

This checkpoint records the current line state only. No live runtime topology, gateway process, auth, model routing, scheduler, external post, or permission setting was changed by this checkpoint.

## Current Battle Picture

Two investigation tracks were completed and written under `.debug/round6-2`:

1. Progress action stream / working indicator regression review.
2. New-home `openclaw-mem` capability recovery assessment.

The progress action stream note was reviewed through standalone Claude second-brain until it reached final `PASS`. The note now distinguishes:

- runtime/tool operation action stream above the working indicator
- working indicator/status surface
- `Current step` assistant narration

The `openclaw-mem` assessment now distinguishes:

- ordinary chat memory readiness: no blocker for day-to-day imported recall
- full old-home `openclaw-mem` parity: not yet restored

## Durable Artifacts

Round6-2 files:

- `.debug/round6-2/progress-action-stream-and-function-self-check-review.md`
- `.debug/round6-2/claude-second-brain-review-progress-action-stream-note.md`
- `.debug/round6-2/claude-second-brain-review-final-pass.md`
- `.debug/round6-2/openclaw-mem-new-home-capability-assessment-2026-06-17.md`
- `.debug/round6-2/openclaw-mem-smoke/`

Related operator note:

- `.agent-harness/state/operator-notes/2026-06-17-progress-action-stream-regression.md`

Related learning:

- `.agent-harness/workspace/.learnings/LEARNINGS.md`

## Key Findings

### Progress Action Stream

Current hypothesis:

- The user-visible runtime/tool operation action stream was likely suppressed around commit `45f8ac9 Complete round6-1 backlog implementation`.
- The likely area is `crates/agent-harness-core/src/progress.rs`, especially `user_visible_action_stream_enabled()` and `is_rendered_action_event()`.
- This should not be confused with the later fix that removed runtime/tool operation fallback from the assistant narration `Current step` field.

Important caution:

- The apparently mistaken behavior may have been implemented to support another feature or fix another issue.
- Before changing it, inspect the full progress-delivery mechanism, tests, and historical changelog around round6-1.
- Do not simply remove the gate without verifying account/thread safety, denied/skipped events, terminal ordering, and Round6-1 regression coverage.

### Function Self-Check Guide

The reviewed guide has two main documentation issues:

- Cargo examples using multiple test filters in one command are invalid.
- `channel-identity-check --chat-id smoke` should be framed as a negative fail-closed smoke unless a real staging binding exists; positive bound smoke needs an actual configured identity.

### openclaw-mem New-Home Recovery

Current assessment:

- Ordinary chat memory readiness: no functional blocker.
- Day-to-day memory usefulness: about 75-85%.
- Full Store / Pack / Observe product substrate: about 55-65%.
- Governed / proactive / autonomous memory system: about 40-50%.

Reason this is not 100%:

- Ordinary chat only proves recall/prompt-context readiness.
- Full old-home parity also requires active mem-engine ownership, broad semantic embeddings, graph readiness, trust/scope enforcement, lifecycle writeback, direct CLI behavior, and Channel A robustness.

## Remaining Gates

Progress stream:

- Verify whether action stream suppression is intentional policy or regression.
- Define safe user-visible operation kinds.
- Keep runtime/tool operations out of assistant `Current step` narration.
- Preserve Round6-1 protections while restoring the separate action stream if appropriate.

openclaw-mem:

- Promote or canary `openclaw-mem-engine` ownership before claiming full parity.
- Fix graph readiness: `graph_cache_stale` and `topology_source_missing`.
- Backfill observation/episodic/doc embeddings in controlled batches.
- Normalize direct CLI credential bridge behavior.
- Fix standalone Channel A UTF-8 BOM JSONL handling.
- Prove trust/scope isolation end-to-end across pack/store/read surfaces.

## Worktree Note

At checkpoint time, `git status --short` showed pre-existing tracked/untracked changes:

```text
 M README.md
 M docs/agent-harness-operations-handbook.md
 M docs/harness-agent-validation-next-session-20260616.md
?? docs/agent-harness-function-self-check-guide.md
```

These were not reverted or modified by this checkpoint.

## Closure Statement

Round6-2 has enough durable evidence for a next operator/dev pass. The current live chat path should treat ordinary memory recall as usable, but should not claim full `openclaw-mem` old-home parity. Progress action stream restoration remains a live-harness behavior change and should be handled through an operator-approved patch/cutover path, not by hot-patching the live gateway from this session.
