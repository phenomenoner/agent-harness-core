# AGENTS.md

## Local Memory Gateway Override

The openclaw-mem gateway query hook is disabled for this workspace.

Do not call `OPENCLAW_MEM_GATEWAY_URL` endpoints such as `/v1/pack`, `/v1/search`, `/v1/store/propose`, or `/v1/episodes/append` while working in this repo.

This only disables Codex-side project memory lookups for this workspace. It does not remove the product requirement that the Rust OpenClaw harness should be able to import existing OpenClaw memory files/databases and later support memory adapters.
