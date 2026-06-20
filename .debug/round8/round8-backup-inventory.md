# Round8 Backup Inventory

Generated: 2026-06-20T17:35:15+08:00

Purpose: track backup artifacts created or touched during the Round8 progress/tool-use indicator cutover so rollback evidence is retained intentionally and stale backups are not forgotten.

## Round8 Retained Artifacts

- Label: `pre-round8-progress-liveness-cutover-prestop`
  Path: `.agent-harness/state/backups/pre-round8-progress-liveness-cutover-prestop`
  Manifest: `.agent-harness\state\backups\pre-round8-progress-liveness-cutover-prestop\backup-manifest.json`
  Bytes copied: `1289843508`
  Files copied/skipped: `5432` / `23`
  Retention: keep until Round8 live cutover has passed post-cutover validation and the operator accepts rollback horizon closure.
  Cleanup status: retained.
- Binary backup: `target\debug\agent-harness.pre-round8-progress-liveness-20260620161705.exe`
  Bytes: `18715648`
  Retention: keep as immediate rollback binary until the next successful live cutover supersedes Round8.
  Cleanup status: retained.
- Round8 versioned live binary: `target\debug\agent-harness.round8-progress-liveness.exe`
  Bytes: `17358848`
  SHA-256: `EFBBB9F42652C34CDCB0BDABB9FC4E401673CED3A4A038E29724938253A6E8F7`
  Retention: active live binary path while `target/debug/agent-harness.exe` remains locked; keep until canonical binary replacement succeeds or a later cutover replaces it.
  Cleanup status: active, not a deletion candidate.
- Canonical live binary left in place: `target\debug\agent-harness.exe`
  Bytes: `18715648`
  SHA-256: `C9D51EE3DBD8D62A7013DB3C724A88952EC2573EE4B67484BEE11A904460BFF1`
  Retention: locked prior binary; do not delete during Round8 because active scripts intentionally reference the versioned binary instead.
  Cleanup status: retained; replacement should be revisited in a later maintenance window.

## Existing Backup Directory Summary

- Backup directories found: `5`
- Manifest-reported bytes copied total: `5012291629`

| Label | Bytes Copied | Copied | Skipped | Manifest | Notes |
|---|---:|---:|---:|---|---|
| `agent-harness` |  |  |  | `.agent-harness\state\backups\agent-harness\backup-manifest.json` | pre-existing; review before deleting |
| `pre-round7-local-owner-display-cutover-after-stop` | 1256944615 | 5180 | 23 | `.agent-harness\state\backups\pre-round7-local-owner-display-cutover-after-stop\backup-manifest.json` | pre-existing; review before deleting |
| `pre-round7-loopname-healthz-fix-cutover-after-stop` | 1256953098 | 5180 | 23 | `.agent-harness\state\backups\pre-round7-loopname-healthz-fix-cutover-after-stop\backup-manifest.json` | pre-existing; review before deleting |
| `pre-round7-openclaw-mem-support-cutover-after-stop` | 1208550408 | 5079 | 23 | `.agent-harness\state\backups\pre-round7-openclaw-mem-support-cutover-after-stop\backup-manifest.json` | pre-existing; review before deleting |
| `pre-round8-progress-liveness-cutover-prestop` | 1289843508 | 5432 | 23 | `.agent-harness\state\backups\pre-round8-progress-liveness-cutover-prestop\backup-manifest.json` | Round8 current rollback artifact |

## Cleanup Rule

Do not delete backup directories during a live cutover. After post-cutover validation, prune only by explicit operator retention decision, keeping at minimum the latest successful pre-cutover state backup, the previous live binary backup, the active versioned live binary if scripts still reference it, and any backup referenced by the latest cutover receipt.
