# AHC Product Guide

Updated: 2026-10-30. This guide describes the Python/Linux source product, rather than certifying an installed release or a running bot.

## Product Purpose

Agent Harness Core (AHC) is a self-hosted Python agent runtime that connects Telegram, Discord and a local CLI to an AHC-managed Codex app-server backend. It owns conversation identity, task state, Python workspaces, tools, memory and delivery so work can continue across backend threads without confusing a model response with task completion.

The core runs inside Linux, including channel ingress, model transport, tools, state, memory and supervision. WSL is one deployment option. Optional Windows interoperation must not be required for independent Linux work. Each configured agent retains its own profile, skills, memory scope and session identity.

## Conversation And Background Work

An interactive task starts with the user's message. When enabled, the interface may show typing, public progress and an editable activity card. A long task directly owned by the conversation stays interactive; duration does not determine background status.

A scheduled task starts from a saved occurrence. It performs work under its existing authority and then publishes a final notification. It creates no typing indicator, Working timer, intermediate progress message or new approval conversation. A step requiring additional authority remains unapproved and can be explained in the final report.

| Category | Meaning |
| --- | --- |
| 💬 **回覆** | Final response to a conversation request. |
| ⏳ **互動進度** | Optional progress for the current interactive task. |
| ⏰ **定時任務** | Final result from a saved schedule occurrence. |
| 🔔 **任務提醒** | A conversation task needs attention or remains unresolved. |
| ⏰ **定時任務提醒** | A scheduled task needs attention or remains unresolved. |

Ordinary user messages received during scheduled work queue as separate conversation requests, including when automatic steering is configured. Channel-controlled trusted runtimes use an independent background owner with dedicated schedule sessions and workspaces in the same store, allowing foreground conversation work to continue. Configurations without that composition retain foreground idle admission. See [channel notifications](CHANNEL-NOTIFICATIONS.md).

## Completion Has A Scope

A model response, a delivered report, an accepted task and a completed objective describe different outcomes. A batch report should state what it checked, what passed and what remains. Candidates awaiting qualification or activation remain pending after a report is delivered. A background check can finish while its monitored migration remains open.

For example, a user can request a skill compatibility check while a memory backfill continues. The compatibility response closes that batch. A later memory notification belongs to the earlier backfill objective; it does not reopen the completed compatibility check.

## Memory, Skills And Tools

Persistent Python/IPython workspaces let an agent continue its own work. Checkpoints bind to an exact session and workspace generation. Original memory records remain in a durable journal; semantic vectors are a rebuildable projection through the configured MemPalace/Qdrant integration. Ingestion, searchable coverage, retrieval quality and complete inventory consistency need separate checks. A zero pending counter alone does not establish full-history acceptance.

Skills are versioned and qualified for their actual dependencies and execution path. Static inventory, isolated tests and adoption in the main conversation are separate states. Protected learning trials can be enabled without claiming demonstrated benefit.

Tools remain governed by the admitted policy. Optional recall and presentation can degrade locally. Identity, credentials, irreversible effects and uncertain deliveries retain their own protected boundaries.

## Installation And Status

Start from the [repository quick start](../../README.md#getting-started), [native Linux configuration](LINUX-NATIVE-CONFIGURATION.md) and [backend upgrades](BACKEND-UPGRADES.md). A checkout, staged bundle, installed package and live service are separate instances. Source edits do not select a running release.

AHC remains pre-release. Current source tests and isolated native checks support their stated contracts; they do not certify every model, channel, cold-boot path, long-running workload or historical-data migration.
