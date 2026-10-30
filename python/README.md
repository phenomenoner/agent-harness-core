# AHC Standalone Python Runtime

AHC is a self-hosted agent runtime for Linux, including WSL, with Telegram, Discord and local CLI entry points. It keeps profiles, virtual sessions, tasks, workspaces, memory and delivery under explicit ownership. The Python package is a pre-release; the retained Rust implementation is not a runtime dependency.

See the [product guide](../docs/python-standalone/PRODUCT-GUIDE.md), [technical overview](../docs/python-standalone/TECHNICAL-OVERVIEW.md), [native configuration](../docs/python-standalone/LINUX-NATIVE-CONFIGURATION.md) and [Linux service guide](../docs/python-standalone/LINUX-SERVICE.md).

## Install And Verify

Use Python 3.14 or later and uv. From this directory:

```bash
uv sync --extra linux
uv run ahc --help
uv run pytest
uv run python tools/check_architecture.py
```

The `linux` extra includes IPython, Discord WebSocket support and MCP schema validation. Memory integrations have separate dependencies declared in `pyproject.toml`. Installation does not provision channel credentials, select a backend bundle or start a service.

## Build A Wheel

From the repository root, with `build`, `packaging` and the declared build requirements available:

```bash
python -m build --wheel --outdir target/python-dist python
python python/tools/verify_wheel.py target/python-dist/ahc-0.3.0-py3-none-any.whl
```

Use a fresh output directory when retaining another candidate. The verifier compares archive members, hashes, dependency metadata and entry points with the checkout. Archive agreement does not establish provider availability or live deployment acceptance.

## Foreground Operation

Keep operator configuration, credentials and runtime state outside the source checkout. The application TOML and foreground JSON have distinct roles. Inspect the exact arguments before operating a configured instance:

```bash
ahc runtime-config-preview --help
ahc doctor --help
ahc run --help
ahc runtime-submit --help
ahc runtime-query --help
ahc status --help
ahc stop --help
```

`tools/prepare_linux_service.py --help` describes service generation. It prepares a unit without installing or starting it. Authentication belongs to the explicitly selected backend home; it must not be committed or copied into routine evidence.

## Continuity And Recovery

Runtime and joint memory backup have separate commands. Use `ahc runtime-backup --help` and `ahc runtime-memory-backup --help`; inspection, preparation, binding and activation remain separate operations. A predecessor database is not automatically a compatible backup.

Ordinary user messages and scheduled occurrences retain separate task identity. Scheduled work emits final notifications without interactive typing or progress. Unknown external effects and delivery results are preserved and are not blindly replayed. See [task lifecycles](../docs/python-standalone/TASK-LIFECYCLES.md) and [failure policy](../docs/python-standalone/WORKFLOW-FAILURE-POLICY.md).

Codex incoming frames are capped at 16 MiB with a 32 MiB queued-frame ceiling and backpressure. Outgoing frames remain at 1 MiB and MCP incoming frames at 512 KiB. Rejected frames retain sanitized reasons and observed byte counts without storing wire payloads.
