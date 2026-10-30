# Codex Backend Upgrades

Updated: 2026-10-18. The Linux source pin selects **Codex CLI 0.163.0**, the latest stable upstream release returned by the official release API at qualification time, replacing the Linux 0.160.1 pin. Prereleases are not selected automatically.

## Upstream Provenance

- Release: [OpenAI Codex rust-v0.163.0](https://github.com/openai/codex/releases/tag/rust-v0.163.0).
- Target: `x86_64-unknown-linux-musl`.
- Package: `codex-package-x86_64-unknown-linux-musl.tar.gz`, 159,424,486 bytes.
- Official asset SHA-256: `582646c6bf0561eb0de540f7fc85b9121b0091a826bc6e96830b7f1722122182`.
- Entrypoint: `bin/codex`; SHA-256: `399ff69c431c066e40de318fba45c2b07f45a76b19939d247ee4828fc70c4215`.

The [machine-readable manifest](../../python/src/ahc/adapters/codex_linux_backend.json) owns member sizes, modes and hashes. Use the full package: a standalone CLI executable omits pinned helpers and resources. The retained Windows backend pin is a separate compatibility surface and is unchanged by this Linux upgrade.

## Qualification Procedure

1. Resolve the official stable release and explicit target asset. Retain metadata and verify asset digest and size.
2. Reject archive redirects, duplicate names, traversal, unexpected types and drift. Build the exact member manifest from verified bytes.
3. Extract in staging, preserve manifest modes and verify the native tree. Python's data extraction filter can adjust read-only modes; reapply verified manifest modes before asserting equality.
4. Use a fresh credential-free home. Check the executable version, generate app-server JSON schemas and validate consumed protocol surfaces.
5. Exercise initialize/initialized and thread start/read/archive with an inert dynamic-tool declaration. Record process exit without making an inference claim.
6. Run affected adapter and lifecycle checks. Authenticated model/tool/continuation qualification, package installation, guarded live selection and post-selection readback are separate steps.

## Current Evidence And Limits

Actual upstream Linux bytes passed archive and native member/mode verification. An isolated process reported `codex-cli 0.163.0`, generated schemas, initialized app-server, accepted a thread with a dynamic-tool declaration, read and archived it, and exited normally. No credentials, inference turn or platform send were used.

Both staged channel packages subsequently passed two isolated authenticated model turns each using the existing managed AHC login. The model created and read a file through AHC workspace tools, checkpointed a Python variable, and verified both in the next turn. The file digest and workspace generation remained consistent; results were accepted and owned processes retired. No live platform messages were sent.

The protected namespace must explicitly mount the selected bundle read-only. A negative/positive native probe confirmed that a missing mount prevents startup and that the corrected mount allows initialization. Isolated test workspaces also need their own protected-path configuration rather than inheriting assumptions about a live workspace's nested provider home.

This qualifies the exercised model/tool/continuation paths. It does not prove every model preference, expiry-spanning authentication, native child execution, natural channel interaction or long-term reliability. Live selection and post-selection readiness require their separate operator evidence; updating a manifest alone does not select a running release.

See the [official app-server documentation](https://learn.chatgpt.com/docs/app-server). AHC retains its own task acceptance, tool authority and delivery contracts.
