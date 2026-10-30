# Source Verification

Updated: 2026-10-31. Scope: the public Python/Linux source preview. Source checks, isolated package checks, authenticated-provider execution and live-service acceptance are separate claims.

## Current Candidate Checks

| Check | Result |
| --- | --- |
| Focused Codex transport, large-tool, diagnostics, outbox, persistent stdio, notifications, CLI and backend-contract regressions | 168 passed in 47.39s with Python 3.14.2 on Linux. |
| Large-tool lifecycle coverage | Controlled real child pipes exercise a multi-megabyte event, parent completion, durable result/delivery, a subsequent turn and bounded oversize refusal. The Codex event sequence is synthetic; this is not a fresh authenticated-provider or natural Telegram acceptance run. |
| Architecture checker | 28 reported violations, including module-size limits. This gate is not clean. |
| Wheel build and archive member/hash verification | Passed; 482 source members verified. |
| Isolated installed CLI | The wheel was installed with its required dependencies into a task-local environment; its installed package origin was checked and isolated `python -I -B -m ahc --help` passed. No live service was started. |
| Git diff whitespace and publication hygiene | Checked on the publication candidate; private runtime state, local evidence and credentials are excluded. This is a bounded scan, not security certification. |

The transport update uses a 16 MiB incoming-frame limit and a 32 MiB queued-frame ceiling with backpressure. The 1 MiB outgoing-frame and 512 KiB MCP incoming-frame limits remain separate. Rejected frames retain sanitized failure details and UNKNOWN/no-replay custody without rewriting predecessor delivery receipts.

## Last Full-Suite Snapshot

The full Linux suite run on 2026-10-30, before this transport repair, reported **4,127 passed, 155 failed, 150 skipped and 1 warning** with Python 3.14.2, Linux dependencies and `pytest -q --timeout=45`. It was not green. The full suite was not rerun for this focused update.

That environment installed `ahc[linux]` and development tools without the optional MemPalace integration. Recorded failures include retained Windows-path fixtures run on Linux, missing optional package metadata, stale notification expectations and architecture constraints. These categories do not establish that every failure has the same cause or is harmless.

## Reproduce

From `python/`, use the declared dependencies and run:

```bash
uv sync --extra linux
uv run pytest -q --timeout=45
uv run pytest -q tests/test_codex_transport.py tests/test_large_tool_transport.py tests/test_large_tool_lifecycle.py
uv run python tools/check_architecture.py
```

The source remains a preview with these gaps visible. Native service lifecycle, authenticated model execution, optional integrations, platform rendering, cold boot and endurance need their own scoped evidence. Publishing this source does not update an installed service.
