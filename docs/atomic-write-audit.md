# Atomic Write Audit

Date: 2026-06-12

Scope: mutable state writes in `crates/agent-harness-core` and new roadmap modules. Append-only JSONL receipts are intentionally excluded from atomic replacement; their integrity is covered by JSONL validation, receipt schemas, and trace reconstruction.

## Mutable State Rule

Mutable JSON documents must use `write_json_atomic` or an equivalent temp-file-and-rename flow. Append-only JSONL receipts may use append mode, but must remain parseable one record per line and must not be the only source of execution truth for queue-like lifecycle state.

## Current Coverage

| Area | Mutable state approach | Evidence |
|---|---|---|
| Harness log write helper | JSONL append, not mutable replacement. | `append_harness_log_writes_jsonl`. |
| Atomic JSON helper | Temp file then rename. | `write_json_atomic_replaces_existing_json`. |
| Runtime queue control | Existing queue JSONL append plus terminal control receipts; retry creates fresh queue id. | `queue_control_retries_with_new_queue_id_and_skips_terminally`. |
| Runtime dead-letter | Append-only dead-letter receipt plus user-visible error outbox. | `timeout_policy_retries_then_dead_letters`. |
| Log rotation | File rename plus append-only rotation receipt. | `rotate_harness_log_moves_large_log_and_writes_receipt`. |
| Queue shadow | SQLite table in WorkerStore database. | `shadow_compare_reports_missing_and_matching_rows`. |
| Background registry | SQLite `background_tasks` table in WorkerStore database. | `background_registry_marks_stale_running_tasks`. |
| Task entity | `task.json` via atomic JSON write; checkpoints are append-only JSONL. | `task_budget_learning_and_drift_paths_are_receipted`. |
| Learning proposal | Proposal JSON via atomic JSON write; proposal receipts are append-only JSONL. | `task_budget_learning_and_drift_paths_are_receipted`. |
| Vault | Encrypted vault JSON via atomic JSON write. | `vault_round_trips_without_plaintext_file_content`. |

## Remaining Audit Work

- Add a kill-during-write drill for Windows staging, ideally around task/proposal/vault writes.
- Keep expanding this document whenever a new mutable state file is introduced.
- Convert future queue execution truth to SQLite before retiring JSONL runtime queue state.
