# Securities API Support Agent Product Design

Date: 2026-06-18
Status: Draft for user review
Audience: Product owner, engineering lead, and external implementation vendor

## Summary

Build a local-first, domain-specific support agent for a securities and futures API product. The first release focuses on external customer support, while including the operating loops that make the support agent useful over time: internal escalation, local CRM/case memory, controlled diagnostics, daily system monitoring, historical evidence retention, LLM-maintained wiki construction, and governed self-improvement.

Agent Harness Core is the preferred runtime foundation. The product layer should use the existing harness strengths: channel ingress, fail-closed identity binding, durable runtime and worker queues, cron jobs, receipts, skills, memory hooks, and local state. The product must not become a general chatbot platform. It should be a small, specialized "API support desk agent" with auditable answers and strict safety boundaries.

The system is local-first. Except for LLM completion and embedding calls, CRM data, cases, memory, RAG sources, indexes, wiki pages, diagnostic receipts, monitoring receipts, and support history must stay in the local machine or customer-controlled environment.

## Locked Product Decisions

- Primary MVP scenario: external customer support.
- Customer states: all customer states are accepted, but the first release uses classification, document answers, basic troubleshooting, and human escalation rather than deep production incident automation.
- Safety boundary: strict support mode. The agent must not request or store real customer passwords, certificates, private keys, or credential files. It must not perform real trading operations for customers and must not provide investment advice.
- Channel strategy: define an abstract channel interface. Validate the MVP with existing Telegram and Discord support, then add LINE and web chat later.
- Customer memory: built-in CRM-style support memory is in scope.
- Source of truth: the product owns a lightweight local CRM/case store in the MVP. External CRM integration remains a future interface.
- Escalation: create a case, notify an internal support channel, support human-approved replies back to the external customer.
- Auto-reply policy: document and low-risk technical troubleshooting questions may be answered automatically. Trading/accounting, production incidents, policy questions, uncertain answers, and safety-boundary cases must escalate.
- Diagnostics: the agent may run controlled smoke tests only with the product owner's official test-environment credentials. It must never use customer credentials.
- Knowledge sources: external answers may use official TradeAPI docs, the NeoAPI skill, and approved internal FAQ/wiki. Case-derived drafts require review before becoming approved knowledge.
- Documentation maintenance: daily checks generate diffs. Low-risk documentation sync can auto-apply. High-risk policy, workflow, and troubleshooting changes require human review.
- Self-improvement: typo, broken-link, and documentation-summary syncs may auto-apply. SOP, policy, response style, troubleshooting workflow, and skill changes require review.
- Vendor acceptance must evaluate delivered system capabilities, traceability, configurability, and recovery. Product operating KPIs are not vendor delivery KPIs.

## Reference Inputs

The vendor should review these references before implementation:

- Fubon TradeAPI LLM index: https://www.fbs.com.tw/TradeAPI/llms.txt
- Fubon TradeAPI preparation flow: https://www.fbs.com.tw/TradeAPI/docs/trading/prepare.txt
- Fubon TradeAPI install and compatibility: https://www.fbs.com.tw/TradeAPI/docs/install-compatibility.txt
- Fubon TradeAPI trading rate limits: https://www.fbs.com.tw/TradeAPI/docs/trading/trade-rate-limit.txt
- Fubon TradeAPI market-data rate limits: https://www.fbs.com.tw/TradeAPI/docs/market-data/rate-limit.txt
- NeoAPI skill reference: https://github.com/phenomenoner/neoapi-skill
- Agent Harness Core: https://github.com/phenomenoner/agent-harness-core
- openclaw-mem governance model: https://github.com/phenomenoner/openclaw-mem
- Karpathy LLM wiki pattern: https://gist.github.com/karpathy/442a6bf555914893e9891c11519de94f
- MemPalace local-first memory: https://github.com/mempalace/mempalace
- Qdrant vector engine and Qdrant Edge: https://github.com/qdrant/qdrant
- GBrain agent brain and background-worker pattern: https://github.com/garrytan/gbrain
- Hermes Agent skill and learning loop references: https://github.com/nousresearch/hermes-agent
- Claude-Mem progressive disclosure search pattern: https://github.com/thedotmack/claude-mem
- LanceDB memory plugin source-of-truth/index separation: https://github.com/CortexReach/memory-lancedb-pro

## Product Goals

1. Answer common external customer questions about onboarding, SDK setup, market data, trading/accounting API usage, rate limits, and troubleshooting.
2. Reduce first-response latency for low-risk questions while preserving a clear path to human support.
3. Build and maintain a local, auditable support memory and wiki from official documents, approved internal knowledge, diagnostics, and closed cases.
4. Provide internal support staff with scoped customer history, suggested replies, known issue summaries, and escalation context.
5. Run daily API and market-data monitoring jobs, store receipts, and retain representative historical data samples for support investigations.
6. Improve support knowledge over time through governed proposals, review, receipts, and rollback.

## Non-Goals For The First Release

- No investment advice.
- No customer credential handling beyond safe redacted troubleshooting text.
- No real trading execution on behalf of customers.
- No automatic conclusion for production incidents, trading failures, accounting disputes, entitlement questions, or regulatory/policy questions.
- No cloud-hosted SaaS dependency for CRM, memory, RAG, wiki, or diagnostics.
- No general-purpose support platform scope. The product is specialized for this API product.

## Core Architecture

The first release should be a Harness-native product layer.

Agent Harness Core remains responsible for:

- Channel ingress and outbox.
- Channel identity binding and fail-closed access control.
- Runtime queue, worker queue, cron jobs, retries, and leases.
- LLM turn execution through the configured model backend.
- Prompt assembly, skill selection, memory hooks, and receipts.
- Operator health, readiness, and audit surfaces.

The API Support Product Layer adds:

- Support agent policy and classifier.
- Local CRM and case management.
- Local support memory and approved wiki.
- Local RAG source snapshotting, indexing, and context-pack generation.
- Controlled test-environment diagnostic jobs.
- Internal escalation and human-approved external replies.
- Monitoring and historical evidence retention.
- Self-improvement proposal generation, review, and rollback.
- Vendor-facing admin and internal support console surfaces.

The answer path is fixed:

1. Receive external message.
2. Resolve channel identity and customer profile.
3. Classify customer state and issue category.
4. Retrieve only allowed sources for the current scope.
5. Decide auto-answer, ask clarifying question, run controlled smoke, or escalate.
6. If auto-answering, cite sources and write an answer receipt.
7. If escalating, create/update case and notify internal support.
8. If human approval is required, send only the approved reply externally.
9. On closure, extract lessons and proposals without directly mutating high-risk knowledge.

## Personas

External customer:

- May be new to onboarding, in SDK setup, testing in a sandbox, or running production.
- Needs accurate, low-friction guidance.
- Must not be asked for real credentials or trading secrets.

Internal support person:

- Needs a compact history of the customer, case, prior steps, and evidence.
- Can approve replies, update cases, and review knowledge proposals.
- Needs clear escalation ownership and follow-up tasks.

Product/API owner:

- Needs common issue reports, knowledge gaps, product improvement suggestions, monitoring receipts, and risk visibility.

System administrator:

- Manages model providers, embedding providers, storage backend, backup, retention, access control, channel connectors, and document sync.

## Issue Taxonomy

The classifier should support these first-release categories:

- `onboarding`: account opening, certificate application, API declaration, connection-test flow, entitlement prerequisites.
- `sdk_setup`: SDK download, local package installation, Python/Node/C# setup, version compatibility, import errors.
- `market_data`: HTTP market data, WebSocket subscriptions, no quote received, rate limit, initialization sequence, market-session questions.
- `trading_accounting`: order placement, modification, cancellation, order results, inventories, account queries, rate limits.
- `futures_options`: futures/options API questions where official product documentation exists. If support knowledge is incomplete, escalate.
- `diagnostic_request`: customer reports a technical symptom that may benefit from product-owned smoke tests or redacted log review.
- `production_incident`: production outage, real-account trading failure, suspicious market-data outage, or customer-impacting incident.
- `policy_or_entitlement`: entitlement, eligibility, compliance, official policy, account status, or ambiguous rule questions.
- `product_feedback`: feature request, bug report, docs feedback, product improvement suggestion.

## Auto-Reply And Escalation Policy

Allowed auto-reply examples:

- Official onboarding steps with citations.
- SDK install and version compatibility guidance.
- Safe code examples using dummy values.
- Market-data initialization checklist.
- Published rate-limit explanations.
- Known, approved FAQ answers.
- Low-risk troubleshooting steps that do not require private credentials.

Must escalate:

- Production trading/accounting anomalies.
- Real-account data disputes.
- Policy, entitlement, compliance, or account status questions.
- Requests for investment advice.
- Requests to inspect real certificates, passwords, private keys, or raw credential files.
- Conflicting sources or missing source coverage.
- Low confidence retrieval.
- Repeated customer frustration or failed self-service attempts.
- Any answer that would require a human commitment about release timing, SLA, or incident root cause.

Escalation output must include:

- Case id.
- Customer profile and channel identity.
- Customer message summary.
- Issue category and severity.
- Evidence and citations used.
- Steps already suggested.
- Diagnostic job references, if any.
- Recommended owner or team.
- Suggested draft reply, clearly marked for approval.
- Proposed next follow-up time when applicable.

## Case And CRM Module

The product owns a local CRM-style support memory. It should be built around storage interfaces rather than a hard-coded database.

Conceptual entities:

- `Customer`: customer id, company, contacts, channel identities, customer status, tags, risk flags, support tier, consent flags.
- `Case`: case id, customer id, category, severity, status, owner, SLA target, created time, last action, next follow-up, closure reason.
- `Interaction`: inbound or outbound event, channel reference, actor, message summary, redaction status, source citations, decision summary.
- `DiagnosticRun`: job id, test profile, environment, result, receipt path, linked case, started time, completed time.
- `Escalation`: internal notification, owner assignment, human reply, approval or rejection, final customer response.
- `KnowledgeProposal`: source cases, proposed wiki/FAQ/skill change, risk level, review status, applied version, rollback reference.
- `MonitoringReceipt`: scheduled check, status, sampled data references, alert case linkage, retention metadata.

Storage requirements:

- CRM/case source-of-truth and retrieval index must be separated.
- Storage backend must be replaceable through an abstraction layer.
- The vendor must propose at least two deployment profiles:
  - Single-machine or small-team profile: embedded local DB and local index are acceptable.
  - Team or enterprise profile: relational store plus self-hosted vector/search backend, such as Postgres with pgvector, Qdrant, or another local/self-hosted engine.
- The selected backend must support backup, restore, export, customer-level deletion, encryption at rest where feasible, versioned knowledge artifacts, and index rebuild.

## Local-First Memory And RAG

Memory is not a vector database. The vector index is disposable infrastructure. The durable memory is the source-of-truth evidence, approved wiki, case history, receipts, and provenance.

The memory/RAG design has four layers.

### 1. Raw Evidence Store

Stores immutable or versioned inputs:

- Official TradeAPI document snapshots.
- NeoAPI skill versions and metadata.
- Approved internal FAQ/wiki sources.
- Customer interaction summaries.
- Redacted diagnostic logs, if explicitly provided.
- Smoke test receipts.
- Monitoring receipts.
- Case closure summaries.
- Human review decisions.

Raw evidence must preserve source URL, source version, fetched time, trust tier, checksum, and ingestion receipt.

### 2. Curated Support Wiki

The wiki is the human-readable support knowledge source. It should follow the LLM-wiki pattern:

- Raw sources are ingested into structured markdown or equivalent pages.
- The wiki contains entity pages, concept pages, troubleshooting pages, FAQ pages, release notes, known issues, and support playbooks.
- The agent may propose updates, but approved pages are protected by review rules.
- Every page has source references, last reviewed time, owner, trust tier, and status.

Required wiki states:

- `approved`: allowed for external auto-answer.
- `draft`: allowed for internal use and human review.
- `quarantined`: not allowed in answers.
- `superseded`: retained for history, not used as current truth.

### 3. Retrieval Index

The retrieval index is local-first and rebuildable. The first release may use SQLite FTS plus local vector storage, or another vendor-proposed local/self-hosted stack. Qdrant, Qdrant Edge, LanceDB, and pgvector are acceptable candidates if they satisfy deployment and governance requirements.

Every indexed chunk must include metadata:

- source id and source type.
- version and checksum.
- trust tier.
- approved status.
- customer scope.
- case scope.
- language.
- product area.
- validity time or expiration time, when applicable.
- citation span or path.

Retrieval should support hybrid search:

- lexical search for exact error strings, SDK names, status codes, and rate-limit terms.
- vector search for semantic matching.
- metadata filters for trust tier, customer scope, product area, version, and status.
- optional reranking, provided the reranker does not become a hidden source of truth.

### 4. Context Pack And Answer Evidence

Before an answer, the system creates a bounded context pack:

- included source snippets.
- citation ids.
- source versions.
- trust tiers.
- inclusion reasons.
- exclusion reasons for relevant but disallowed material.
- customer/case scope checks.
- risk classification.

After an answer, the system writes an answer receipt:

- customer id and case id, if applicable.
- prompt category and risk decision.
- sources cited.
- auto-reply or human-approved reply status.
- model used.
- diagnostic references, if any.
- final response hash or stored copy according to retention policy.

## Diagnostics Module

Diagnostics are worker or cron jobs, not free-form external-customer tools.

Allowed first-release diagnostics:

- Login or connectivity smoke with product-owned test credentials.
- Market-data initialization smoke.
- Representative quote or subscription smoke in the official test environment.
- Rate-limit and availability checks that respect published limits.
- Parsing of customer-provided redacted logs or redacted code snippets.

Not allowed:

- Customer credential use.
- Real customer certificate handling.
- Real trading operations for customers.
- Automatic production root-cause determination.

Diagnostics must produce receipts:

- job id.
- environment.
- test profile.
- input redaction status.
- start and end time.
- pass/fail result.
- error summary.
- linked case.
- retention metadata.

## System Monitoring And Historical Evidence

The system should run scheduled checks and retain evidence for future support investigations.

Required monitoring jobs:

- Daily API readiness smoke.
- Daily market-data readiness smoke.
- Daily official document and NeoAPI skill version check.
- Periodic representative market-data sampling, with clear market/session metadata.
- Knowledge-base health check.
- Follow-up due-date scan.
- Open escalation aging scan.

Historical evidence requirements:

- Store representative market-data samples with time, source, product area, market/session metadata, and retention policy.
- Store pass/fail receipts for each monitoring run.
- Link monitoring failures to internal alert cases.
- Allow support staff to query historical smoke and market-data evidence for a date/time range.
- Do not treat representative sampling as an exhaustive market-data archive.

## Internal Support Module

Internal staff need a support console or equivalent internal channel commands.

Required capabilities:

- View customer profile, open cases, closed cases, and prior commitments.
- Ask "what happened with this customer" and receive scoped summaries.
- Ask "how should we answer this type of question" and receive a draft with citations and risk notes.
- Review and approve/reject external reply drafts.
- Assign owner, severity, SLA, and follow-up time.
- View diagnostics and monitoring receipts.
- View recent common issues, suspected bugs, and product feedback clusters.
- Convert a case lesson into a knowledge proposal.

Internal support replies must remain distinct from external customer replies. The system must not send an internal draft externally without explicit approval when approval is required.

## LLM Wiki Construction

The LLM wiki should compound knowledge over time instead of re-summarizing everything at query time.

Required workflows:

- Ingest official document snapshots into raw evidence.
- Generate diff summaries when sources change.
- Update approved wiki automatically only for low-risk documentation syncs.
- Route high-risk policy, workflow, troubleshooting, and response-style updates to review.
- Link wiki pages to sources, cases, and product areas.
- Detect stale pages, orphan pages, source-less claims, and contradictions.
- Preserve superseded facts rather than silently deleting them.
- Support rollback to a previous wiki version.

Wiki health checks should produce:

- stale page list.
- missing citation list.
- contradiction candidates.
- orphan page list.
- suggested pages to create.
- suggested owner/reviewer.

## Self-Improvement Governance

The agent may improve the support system through proposals and low-risk automatic patches.

Allowed automatic changes:

- typo fixes.
- broken link updates.
- low-risk official documentation summary syncs.
- metadata refreshes.
- index rebuilds.

Review-required changes:

- troubleshooting SOP.
- policy explanations.
- external response templates.
- skills or procedural runbooks.
- escalation rules.
- safety rules.
- product claims.
- release timing claims.

Each proposal must include:

- source cases or source documents.
- proposed change.
- risk classification.
- affected wiki/FAQ/skill files or records.
- expected user impact.
- reviewer decision.
- applied version or rejection reason.
- rollback reference.

## Vendor-Facing Scope

The vendor should deliver these product modules or equivalent components:

1. Support Agent Core
   - Issue classification, customer-state classification, auto-answer policy, citation enforcement, escalation decision, and reply drafting.

2. Case/CRM Module
   - Customer profiles, channel identities, case lifecycle, interactions, SLA/follow-up, owner assignment, and retention controls.

3. Local Knowledge/RAG Module
   - Source snapshotting, approved wiki, draft/quarantine workflow, local search/vector indexing, context packs, and answer receipts.

4. Diagnostics Module
   - Product-owned test-environment smoke jobs, receipt storage, diagnostic case linkage, and safe redacted log/code handling.

5. Monitoring And Evidence Module
   - Scheduled smoke checks, document sync checks, representative market-data sampling, historical query surface, and alert case creation.

6. Internal Support Console
   - Case queue, escalation review, reply approval, customer history, evidence viewer, diagnostics viewer, and internal Q&A.

7. Self-Improvement And Wiki Admin
   - FAQ/wiki/skill proposals, low-risk auto-apply, review workflow, wiki health reports, rollback, and product insight reports.

8. Security And Governance
   - Local-first data policy, access control, redaction, audit receipts, backup/restore, export/delete, and source trust policy.

## Vendor Acceptance Criteria

Vendor delivery is accepted against system capability, traceability, configurability, and recovery. It is not accepted against the support team's future operating KPIs.

Acceptance indicators:

- Scenario coverage: all required acceptance scenarios run successfully.
- Citation completeness: every automatic external answer can trace to source, version, and trust tier.
- Escalation completeness: high-risk questions create cases, notify internal support, preserve summaries, and support human-approved replies.
- Receipt completeness: external replies, diagnostics, monitoring, wiki updates, and self-improvement proposals produce receipts.
- Configurability: auto-answer rules, escalation rules, document sources, review rules, model provider, embedding provider, storage backend, and index backend are configurable.
- Recovery: backup/restore, knowledge rollback, and index rebuild are demonstrated.
- Local-first compliance: CRM, cases, wiki, memory, RAG index, diagnostics, monitoring data, and support history remain local or self-hosted, except for explicit LLM and embedding calls.
- Fail-closed behavior: missing source, low confidence, retrieval conflict, diagnostic failure, and policy ambiguity do not produce unsupported automatic external answers.

## Product Operating Metrics

These metrics are for the product owner after launch. They should not be used as direct vendor delivery acceptance criteria:

- Average first response time.
- Automatic resolution rate.
- Human escalation quality.
- Average case closure time.
- Agent draft reply edit rate.
- FAQ hit rate.
- Knowledge gap trend.
- Follow-up overdue count.
- Monitoring pass rate.

## Required Acceptance Scenarios

External customer support:

- A new customer asks how to start using the API. The agent cites official onboarding documentation and answers safely.
- A developer asks about SDK installation and version compatibility. The agent answers by language and SDK version with source citations.
- A customer says they cannot receive market data. The agent provides a low-risk checklist and, when appropriate, runs a controlled product-owned test-environment smoke.
- A customer asks about a production order/accounting anomaly. The agent does not auto-conclude; it creates a case and notifies internal support.
- A human approves a draft reply. The system sends the approved reply externally and updates the case.

Internal support:

- Internal staff ask for a customer's historical interaction summary. The system returns scoped history, open cases, commitments, and next follow-up.
- Internal staff ask how to answer a recurring problem. The system drafts a response with citations and risk notes.
- Internal staff ask for recent common bugs or glitches. The system summarizes case patterns, suspected impact, and suggested owners.

System monitoring and historical evidence:

- A daily API and market-data smoke runs and creates pass/fail receipts.
- Representative market-data samples are retained with timestamp, source, product area, and market/session metadata.
- A monitoring failure creates an internal alert case.
- Support staff query historical monitoring and market-data evidence for a specific date/time range.

LLM wiki and knowledge construction:

- Official docs or the NeoAPI skill change. The system creates a raw snapshot, diff summary, and wiki update proposal.
- A low-risk documentation update is auto-applied to approved wiki and remains rollback-capable.
- A high-risk policy or troubleshooting update enters review instead of being auto-applied.
- Wiki health check finds stale pages, orphan pages, contradictory claims, and source-less claims.

Self-improvement:

- Closed cases produce FAQ drafts, troubleshooting SOP drafts, and product insight proposals.
- Low-risk typo, broken-link, or summary sync changes auto-apply with receipts.
- SOP, policy, response template, and skill changes require human review.
- A proposal can be traced back to source cases, review decision, applied version, and rollback reference.

Local-first and recovery:

- The vendor demonstrates that CRM, cases, wiki, memory, indexes, diagnostics, monitoring data, and receipts are local or self-hosted.
- The vendor demonstrates backup and restore.
- The vendor demonstrates deleting or exporting one customer profile and its linked case data according to policy.
- The vendor demonstrates rebuilding the retrieval index from source-of-truth data.

## Security And Compliance Rules

- Do not store real customer passwords, certificates, private keys, or credential files.
- Redact or reject messages containing credentials or certificate material.
- Do not provide investment advice.
- Do not execute real trades or real account actions for customers.
- Treat external channel text as untrusted.
- Treat draft knowledge as untrusted for external answers.
- Require approval for high-risk knowledge and external replies.
- Preserve receipts without leaking secrets.
- Support role-based access control for internal support, administrator, and auditor roles.

## Open Product Decisions For Vendor Proposal

These are not blockers for the product spec. The vendor must propose options and trade-offs:

- Final storage backend for single-machine and team deployment profiles.
- Final vector/search backend for MVP and scale-up profile.
- Internal support console shape: web UI, channel commands, or both.
- LINE and web chat connector sequencing after Telegram/Discord validation.
- Retention periods for market-data samples, monitoring receipts, case data, and raw evidence.
- Exact redaction policy and human review queue UX.
- Futures/options documentation coverage and diagnostic depth, based on official available APIs and test assets.

## Review Notes

This spec intentionally separates three concepts:

- Product source of truth: CRM, cases, approved wiki, raw evidence, receipts.
- Retrieval infrastructure: FTS, vector store, reranker, search backend.
- Operating outcomes: support speed, resolution rate, and knowledge quality after launch.

The vendor must implement the first two well enough that the product owner can improve the third over time.
