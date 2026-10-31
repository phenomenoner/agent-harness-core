# Large data and Codex terminal settlement

Keep repository and file bytes on the existing execution path. A verified local
Git checkout with working credentials can push directly; API uploads need an
actual file-reading interface or reachable artifact. A path string alone does
not make a file readable by another environment or remote connector. Downloadable
user deliverables continue through the attachment/artifact interface. Return
status, byte size, SHA-256, locator and commit SHA, rather than file bodies.

After uncertain external reporting, inspect the remote ref before any retry. A
successfully updated remote and a missing final chat reply are separate outcomes.
Do not replay an earlier GitHub operation merely to test terminal settlement.
Existing write authorization and credential boundaries still apply. This change
adds guidance to every new backend thread; it does not add a new upload service
or grant a connector access to local paths.

The Codex adapter keeps completed final-answer messages from the exact admitted
thread and turn. It releases them only after that turn reports completed without
an error and tracked descendants settle. Deltas, commentary, tool output and
asynchronous questions cannot complete the task. The in-memory answer budget is
128 items and 4 MiB of UTF-8 text; no tool history is retained by this collector.
The initialization handshake suppresses unused accumulated diff notifications.

If streamed answer evidence is absent, the pinned item-pagination API is queried
with the exact threadId and turnId, one item per descending page. Results are
validated and final messages are restored to chronological order. Readback stops
after 128 pages or 8 MiB of conservatively re-encoded JSON. Duplicate items, wrong
turns, repeated/invalid cursors, unsupported pagination and exhausted budgets
retain UNKNOWN. No full-turn history read or new model turn is used as fallback.
Post-terminal effect admission remains closed during readback.

The transport still caps one incoming frame at 16 MiB. Item count is not a byte
guarantee: one oversized item can still exceed that boundary and is refused.
Normal event settlement avoids reassembling many valid events into one history
response. The cumulative regression streams over 16 MiB in individually valid
events, then covers final acceptance, persistence, delivery, reopen and next input.
It uses a scripted peer over real pipes; that evidence is distinct from a live
provider or Telegram dispatch.
