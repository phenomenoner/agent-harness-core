# Security Policy

## Supported Scope

This repository is pre-release. Security fixes target the active Python/Linux implementation on `main`. The retained Rust implementation and its historical tags have separate interfaces; their commands and checks do not qualify the Python runtime.

## Reporting

Report security issues privately to the repository owner. Do not include secrets, exploit details, private channel identifiers or runtime state in public issues. Provide the affected commit or release, sanitized reproduction steps and whether credentials, prompts, messages or local execution are involved.

## Current Python Security Boundaries

- Keep channel credentials, backend authentication, configuration and runtime state outside the source checkout. Select the intended backend home explicitly; installation does not import another agent's credentials.
- AHC binds work and delivery to exact agent, account, channel, user and virtual-session identities. Tool and MCP access follows the selected policy and configured capabilities.
- Treat the persistent interpreter and admitted subprocesses as local execution under the configured operating-system user. A trusted workspace policy grants that user's resource access; a path string or a model response does not grant a remote connector access.
- The Linux backend uses a complete, digest-pinned bundle. A manifest update alone neither verifies a running process nor authorizes deployment. See [backend qualification](docs/python-standalone/BACKEND-UPGRADES.md).
- Preserve unknown execution and delivery effects for reconciliation. Do not repeat a write or send merely because its reply was lost.
- Exclude authentication material, environment files, private configuration, runtime databases, channel content, local evidence and ignored private directories from public exports.

## Source And Publication Checks

From `python/`, use the dependency set and commands in [source verification](docs/python-standalone/SOURCE-VERIFICATION.md):

```bash
uv sync --extra linux
uv run pytest
uv run python tools/check_architecture.py
```

Record failures and optional-dependency or platform gaps explicitly. Run `git diff --check` and inspect the exact tracked public diff for credentials, identifiers, private paths and accidental artifacts before publication. A passing source check is not a security certification, authenticated-provider qualification or live-deployment acceptance.

Rust `agent-harness vault-*`, `public-hygiene`, `schema-registry`, `invariants` and Cargo commands belong to the historical Rust implementation. They are not Python CLI commands. The Python CLI is `ahc`; consult `ahc --help` for its supported surface.
