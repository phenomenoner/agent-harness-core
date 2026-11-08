# Source Verification

Updated: 2026-11-08. Scope: the Python/Linux source synchronization. Source checks, package identity, authenticated-provider execution and live-service acceptance are separate claims. Package version remains 0.3.0; this update does not create a release tag or deploy an instance.

## Current Candidate Checks

| Check | Result |
| --- | --- |
| Focused workspace observations, long jobs, foreground recovery, scheduled silence, backend contracts, attachment delivery and persistent-transport regressions | 173 passed and 6 skipped in 57.30s with Python 3.14.2 on Linux. The ordinary test process lacks a delegated service owner for those six native tests. |
| Supplemental delegated native-workspace tests | 3 passed, 1 failed and 2 skipped. Busy-read/job observation and next-cell scenarios passed on real interpreter processes. The IPython case failed in an environment without the optional IPython dependency; the two namespace cases require unavailable bubblewrap. These gaps are not counted as passes. |
| Source CLI | Candidate source origin was checked; `python -m ahc --help` passed without starting a runtime. |
| Linux backend pin | Official 0.167.0 release asset size and SHA-256 match the manifest. The complete existing tool-managed bundle passed native member/mode verification and reported `codex-cli 0.167.0`. No archive download, authenticated inference or live selection was performed for this source audit. |
| Architecture checker | 28 reported violations, with the same affected source-path set as the 2026-10-31 record. This gate is not clean. |
| Documentation and public diff | Relative-link targets, Python syntax and whitespace are checked on the candidate; exact tracked changes are scanned for private material before publication. This is a bounded check, not security certification. |
| Wheel and installed package | Not rebuilt or installed for this source-only update. Earlier wheel checks apply to their original candidate. |

The focused command, from `python/`, is:

```bash
python -m pytest -q --timeout=45 -p no:cacheprovider \
  tests/test_adaptive_budget_review_regressions.py \
  tests/test_codex_app_server_contract.py \
  tests/test_foreground_budget_recovery.py \
  tests/test_native_workspace.py \
  tests/test_persistent_stdio.py \
  tests/test_scheduled_quiet_delivery.py \
  tests/test_staged_backend_auth.py \
  tests/test_workspace_job_review_regressions.py \
  tests/test_workspace_jobs.py \
  tests/test_channel_background_presentation.py \
  tests/test_codex_workspace_tools.py \
  tests/test_foreground_runtime.py \
  tests/test_channel_attachment_outbox.py \
  tests/test_result_artifact_commit.py
```

The attachment regression fails before repair for both Telegram and Discord fixtures, through the normal composed drain and direct attachment entry point. It checks zero scheduled sends, durable reopen/no-replay behavior and the next interactive text plus attachment; a separate case preserves preexisting UNKNOWN custody.

The unpublished operator recipe and its tests remain local after independent review found ownership-attribution and agent-selection gaps. They are excluded from this public synchronization.

These are controlled tests. Route fixtures and synthetic backend responses do not establish a fresh authenticated model turn or real Telegram/Discord delivery. The native checks execute actual isolated interpreter processes under a disposable delegated user service; they do not operate a deployed AHC service.

## Last Full-Suite Snapshot

The full Linux suite run on 2026-10-30 reported **4,127 passed, 155 failed, 150 skipped and 1 warning** with Python 3.14.2 and `pytest -q --timeout=45`. It was not green and was not rerun for this source synchronization.

An additional 2026-11-08 composition/configuration check reproduced five retained Windows-path fixture failures on Linux. Those configuration tests are outside this delivery repair and remain unresolved; the final focused command above does not include that module.

That full-suite environment omitted the optional MemPalace integration. Recorded failures include retained Windows-path fixtures run on Linux, missing optional package metadata, stale notification expectations and architecture constraints. These categories do not establish that every failure has the same cause or is harmless.

## Retained Transport Evidence

The 2026-10-31 transport candidate passed 168 focused regressions and wheel/member checks. Its controlled real-pipe scenarios cover a multi-megabyte tool event, exact-parent completion, durable result/delivery, a subsequent turn and bounded oversize refusal. The newer main-branch persistent-stdio regression is retained; an older local test that referenced retired cumulative frame limits is not reintroduced.

Codex incoming frames remain bounded at 16 MiB, with a 32 MiB queue ceiling and backpressure. Outgoing frames remain capped at 1 MiB; MCP incoming frames retain their separate 512 KiB limit. Exact-turn terminal recovery and unknown/no-replay custody remain part of the source contract.

## Remaining Acceptance Boundaries

A source commit does not prove an installed wheel, selected backend process or live channel. Authenticated model/tool/continuation paths, namespace isolation where unavailable, platform rendering, guarded deployment, cold boot, endurance and full-product acceptance need evidence for the exact instance and scope. The historical Rust release tags and source test counts do not establish those claims.
