# Python Runtime Architecture

The active AHC product is the standalone Python runtime in `python/`. It owns its foreground process, conversation state and tools inside Linux. WSL is a supported deployment option. The existing Rust crates and root Rust documentation remain available as the historical implementation; they are not required to start the Python runtime.

## Execution Ownership

The CLI composes domain contracts, application services, persistence and adapters. A foreground owner admits work, calls an explicitly selected Codex app-server backend, accepts results and drains route-bound channel outboxes. A model thread does not own the AHC conversation or persistent workspace. See the [technical overview](TECHNICAL-OVERVIEW.md) and [task lifecycles](TASK-LIFECYCLES.md).

Telegram, Discord and local submission retain exact agent, account, channel, user and virtual-session identity. Scheduled occurrences are durable work with separate presentation rules. Channel-controlled trusted runtimes dispatch them through an independent background owner with separate schedule sessions and workspaces. Foreground conversation work can continue. Other compositions retain foreground idle admission.

## Tools, Memory And Trust

Python/IPython workspaces, checkpoints, artifacts, memory and admitted children retain their own operation identity. SQLite and the memory journal own durable records. Search and semantic projections can be rebuilt; a projection count does not certify complete source coverage or retrieval quality.

The ordinary-user execution profile uses the configured Linux user's existing filesystem, programs and network access. It does not grant root privileges, create mounts or authorize another agent's resources. Protected workspace and backend profiles remain explicit alternatives. See [execution access](EXECUTION-ACCESS.md) and [native configuration](LINUX-NATIVE-CONFIGURATION.md).

## Interop Failure Containment

Core ingress, backend transport, state and workspaces run inside Linux. A failed optional Windows call must not make native Linux work depend on a shared Windows transport. Windows operator helpers use explicit distribution, user and service arguments and bounded observations. An uncertain control result stays unknown until readback; it is not automatically retried. No recovery step implies permission to shut down unrelated WSL distributions.

## Installation And Recovery

Keep installed releases, operator configuration, authoritative state and user workspaces outside the source checkout. A delegated systemd user service owns its process groups; see [service ownership](LINUX-SERVICE.md), [native process custody](LINUX-NATIVE-PROCESS.md) and [native workspace custody](LINUX-NATIVE-CUSTODY.md).

Backup inspection, restore preparation, binding and activation are separate operations. Unknown external effects and delivery receipts retain no-replay custody. Consult the [failure policy](WORKFLOW-FAILURE-POLICY.md) before interpreting an interruption as a safe retry.

## Evidence Boundaries

This repository is a pre-release source distribution. Source tests, wheel member verification, an installed package, a running foreground owner and authenticated provider-visible behavior are separate claims. Historical operator acceptance records and private deployment receipts are not part of this public source snapshot.
