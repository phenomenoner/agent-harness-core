# Codex Backend Upgrades

Updated: 2026-11-08. The Linux source pin selects **Codex CLI 0.167.0**, replacing 0.163.0. Prereleases are not selected automatically. A source pin does not select or qualify a running deployment.

## Upstream Provenance

- Release: [OpenAI Codex rust-v0.167.0](https://github.com/openai/codex/releases/tag/rust-v0.167.0), marked stable by the official release API at this check.
- Target: `x86_64-unknown-linux-musl`.
- Package: `codex-package-x86_64-unknown-linux-musl.tar.gz`, 200,178,910 bytes.
- Official asset SHA-256: `3bbf75e83de6b58aed28f2cf6e33ad423ec31fd4700a1457b94be557ea5a7c6e`.
- Entrypoint: `bin/codex`; source-manifest SHA-256: `cdcb45aaa01d0f14e298985e09465aebccd47f64ecd298aea44814af7b62bbda`.

The [machine-readable manifest](../../python/src/ahc/adapters/codex_linux_backend.json) owns member sizes, modes and hashes. Use the complete package; a standalone CLI executable omits pinned helpers and resources. The retained Windows backend pin is a separate surface.

## Current Evidence And Limits

The official release API's asset size and SHA-256 match the source manifest. The [0.167.0 protocol fixture](../../python/tests/fixtures/codex_0_167_0_surface.json) records the upstream schema generator, executable digest and consumed surfaces; adapter regressions bind the fixture to the selected manifest. See [source verification](SOURCE-VERIFICATION.md) for fresh test results.

The complete existing tool-managed 0.167.0 Linux bundle also passed member/mode verification against the source manifest, and its executable reported `codex-cli 0.167.0`. Root ownership was not required by this check. This audit did not redownload or extract the archive, perform credential-free app-server initialization or run authenticated model/tool/continuation qualification. Publishing these files does not select or restart a live service. Earlier 0.163.0 observations below remain historical evidence for that version.

## Qualification Procedure

1. Resolve the official stable release and explicit target asset. Verify asset digest and size.
2. Reject duplicate names, traversal, unexpected types and member drift. Extract in staging, restore declared modes and verify the native tree against the manifest.
3. Use a fresh credential-free home. Check executable version, generate app-server schemas and validate consumed protocol surfaces.
4. Exercise initialize/initialized and thread start/read/archive with an inert dynamic-tool declaration. Record process exit separately from inference.
5. Run affected adapter and lifecycle checks. Authenticated model/tool/continuation qualification, package installation, guarded live selection and post-selection readback are separate steps.

## Historical 0.163.0 Qualification

Actual upstream Linux bytes passed archive and native member/mode verification. An isolated process reported `codex-cli 0.163.0`, generated schemas, initialized app-server, accepted a thread with a dynamic-tool declaration, read and archived it, and exited normally. No credentials, inference turn or platform send were used.

Both staged channel packages subsequently passed two isolated authenticated model turns each using the existing managed AHC login. The model created and read a file through AHC workspace tools, checkpointed a Python variable, and verified both in the next turn. The file digest and workspace generation remained consistent; results were accepted and owned processes retired. No live platform messages were sent.

The protected namespace must explicitly mount the selected bundle read-only. A negative/positive native probe confirmed that a missing mount prevents startup and that the corrected mount allows initialization. Isolated test workspaces also need their own protected-path configuration rather than inheriting assumptions about a live workspace's nested provider home.

This qualifies the exercised model/tool/continuation paths. It does not prove every model preference, expiry-spanning authentication, native child execution, natural channel interaction or long-term reliability. Live selection and post-selection readiness require their separate operator evidence; updating a manifest alone does not select a running release.


See the [official app-server documentation](https://learn.chatgpt.com/docs/app-server). AHC retains its own task acceptance, tool authority and delivery contracts.
