use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use serde::Serialize;

const QUALITY_REPORT_SCHEMA: &str = "agent-harness.quality-report.v1";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublicHygieneOptions {
    pub root: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct InvariantEntry {
    pub id: &'static str,
    pub statement: &'static str,
    pub owner: &'static str,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SchemaRegistryEntry {
    pub schema: &'static str,
    pub owner_module: &'static str,
    pub compatibility: &'static str,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PublicHygieneReport {
    pub schema: &'static str,
    pub root: PathBuf,
    pub passed: bool,
    pub forbidden_hits: Vec<PathBuf>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ReleaseChecklist {
    pub schema: &'static str,
    pub required_items: Vec<&'static str>,
}

pub fn invariant_catalog() -> Vec<InvariantEntry> {
    vec![
        InvariantEntry {
            id: "I1",
            statement: "one allowed inbound triggers at most one model turn",
            owner: "channel/runtime_queue",
        },
        InvariantEntry {
            id: "I2",
            statement: "every completed turn has exactly one delivery or dead-letter notification",
            owner: "runtime_pipeline/channel_delivery",
        },
        InvariantEntry {
            id: "I3",
            statement: "terminal states are irreversible",
            owner: "runtime_pipeline/workers",
        },
        InvariantEntry {
            id: "I4",
            statement: "cancel only affects the requested turn, queue item, job, or scope",
            owner: "admission/channel_state",
        },
        InvariantEntry {
            id: "I5",
            statement: "crash recovery loses no work and duplicates no side effects",
            owner: "queue_shadow/supervision",
        },
        InvariantEntry {
            id: "I6",
            statement: "over-budget work is deferred or blocked, not dropped",
            owner: "autonomy",
        },
        InvariantEntry {
            id: "I7",
            statement: "ingress always has a terminal trace chain",
            owner: "trace",
        },
    ]
}

pub fn schema_registry_entries() -> Vec<SchemaRegistryEntry> {
    vec![
        SchemaRegistryEntry {
            schema: "agent-harness.runtime-run-once.v1",
            owner_module: "runtime_pipeline",
            compatibility: "append-only JSONL, additive fields only in v1",
        },
        SchemaRegistryEntry {
            schema: "agent-harness.runtime-dead-letter.v1",
            owner_module: "runtime_pipeline",
            compatibility: "additive fields only in v1; terminal semantics are immutable",
        },
        SchemaRegistryEntry {
            schema: "agent-harness.runtime-queue-control.v1",
            owner_module: "runtime_queue",
            compatibility: "append-only receipts; retry creates fresh ids",
        },
        SchemaRegistryEntry {
            schema: "agent-harness.progress-delivery-state.v1",
            owner_module: "progress",
            compatibility: "state JSON may add cursor/cache fields in v1; existing lane cursors remain readable",
        },
        SchemaRegistryEntry {
            schema: "agent-harness.codex-context-preflight.v1",
            owner_module: "codex_runtime",
            compatibility: "append-only JSONL plus per-execution JSON; additive fields only in v1",
        },
        SchemaRegistryEntry {
            schema: "agent-harness.codex-context-checkpoint.v1",
            owner_module: "codex_runtime",
            compatibility: "per-execution recovery artifact; additive fields only in v1",
        },
        SchemaRegistryEntry {
            schema: "agent-harness.codex-context-rollover.v1",
            owner_module: "codex_runtime",
            compatibility: "per-execution recovery artifact; binding backup path remains optional",
        },
        SchemaRegistryEntry {
            schema: "agent-harness.channel-identity-check.v1",
            owner_module: "channel_identity",
            compatibility: "additive fields only in v1; non-bound statuses remain fail-closed",
        },
        SchemaRegistryEntry {
            schema: "agent-harness.channel-identity-registry.v1",
            owner_module: "channel_identity",
            compatibility: "additive binding fields only in v1; ambiguous bindings must fail closed",
        },
        SchemaRegistryEntry {
            schema: "agent-harness.channel-delivery-intent.v1",
            owner_module: "channel_runtime",
            compatibility: "additive fields only in v1; provider ids must come from captured inbound context",
        },
        SchemaRegistryEntry {
            schema: "agent-harness.cron-scheduler.run-once.v1",
            owner_module: "cron_scheduler",
            compatibility: "additive fields only in v1; dry-run must not enqueue or write watermarks",
        },
        SchemaRegistryEntry {
            schema: "agent-harness.cron-scheduler.lint.v1",
            owner_module: "cron_scheduler",
            compatibility: "additive diagnostics only in v1; error status remains fail-closed",
        },
        SchemaRegistryEntry {
            schema: "agent-harness.cron-scheduler.tick.v1",
            owner_module: "cron_scheduler",
            compatibility: "append-only receipts; additive fields only in v1",
        },
        SchemaRegistryEntry {
            schema: "agent-harness.cron-scheduler.job-decision.v1",
            owner_module: "cron_scheduler",
            compatibility: "append-only receipts; idempotency key semantics are stable in v1",
        },
        SchemaRegistryEntry {
            schema: "agent-harness.cron-runs.v1",
            owner_module: "cron_runs",
            compatibility: "SQLite state table; additive columns only in v1; status enum changes require migration",
        },
        SchemaRegistryEntry {
            schema: "agent-harness.config-validation.v1",
            owner_module: "config",
            compatibility: "additive diagnostics only in v1; invalid config remains fail-closed",
        },
        SchemaRegistryEntry {
            schema: "agent-harness.log-rotation.v1",
            owner_module: "logging",
            compatibility: "append-only rotation receipts; additive fields only in v1",
        },
        SchemaRegistryEntry {
            schema: "agent-harness.supervision-evaluation.v1",
            owner_module: "supervision",
            compatibility: "additive child fields only in v1",
        },
        SchemaRegistryEntry {
            schema: "agent-harness.supervisor-stop-file.v1",
            owner_module: "ops",
            compatibility: "JSON stop-file envelope may add metadata in v1; legacy plain-text reasons stay readable",
        },
        SchemaRegistryEntry {
            schema: "agent-harness.queue-shadow-compare.v1",
            owner_module: "queue_shadow",
            compatibility: "additive divergence fields only in v1",
        },
        SchemaRegistryEntry {
            schema: "agent-harness.queue-shadow-record.v1",
            owner_module: "queue_shadow",
            compatibility: "additive fields only in v1",
        },
        SchemaRegistryEntry {
            schema: "agent-harness.healthz.v1",
            owner_module: "health",
            compatibility: "local JSON status, additive fields only in v1",
        },
        SchemaRegistryEntry {
            schema: "agent-harness.trace.v1",
            owner_module: "trace",
            compatibility: "additive record fields only in v1",
        },
        SchemaRegistryEntry {
            schema: "agent-harness.metrics.v1",
            owner_module: "metrics",
            compatibility: "counter names are stable once published; additive counters allowed",
        },
        SchemaRegistryEntry {
            schema: "agent-harness.supervise-deploy-canary.v1",
            owner_module: "deploy",
            compatibility: "additive fields only in v1; commit/rollback decisions remain stable",
        },
        SchemaRegistryEntry {
            schema: "agent-harness.admission-decision.v1",
            owner_module: "admission",
            compatibility: "additive fields only in v1; refusal remains explicit",
        },
        SchemaRegistryEntry {
            schema: "agent-harness.scoped-stop.v1",
            owner_module: "admission",
            compatibility: "target shape is stable in v1; new target kinds require compatibility tests",
        },
        SchemaRegistryEntry {
            schema: "agent-harness.background-registry.v1",
            owner_module: "background",
            compatibility: "task JSON may add fields in v1; status enum changes require migration",
        },
        SchemaRegistryEntry {
            schema: "agent-harness.token-efficiency.v1",
            owner_module: "token_efficiency",
            compatibility: "additive fields only in v1",
        },
        SchemaRegistryEntry {
            schema: "agent-harness.task-entity.v1",
            owner_module: "autonomy",
            compatibility: "additive fields only in v1; checkpoints remain append-only",
        },
        SchemaRegistryEntry {
            schema: "agent-harness.budget-decision.v1",
            owner_module: "autonomy",
            compatibility: "additive fields only in v1; decision semantics remain stable",
        },
        SchemaRegistryEntry {
            schema: "agent-harness.encrypted-vault.v1",
            owner_module: "vault",
            compatibility: "breaking crypto/KDF changes require v2 and migration receipt",
        },
        SchemaRegistryEntry {
            schema: "agent-harness.learning-proposal.v1",
            owner_module: "autonomy",
            compatibility: "proposal JSON may add review fields in v1",
        },
        SchemaRegistryEntry {
            schema: "agent-harness.skill-invocation-envelope.v1",
            owner_module: "skill_envelope",
            compatibility: "byte-framed envelope; declared length/checksum fields are stable in v1",
        },
        SchemaRegistryEntry {
            schema: "agent-harness.skill-selection.v1",
            owner_module: "skills",
            compatibility: "append-only selection receipts; matcher metadata and score components may add fields in v1",
        },
        SchemaRegistryEntry {
            schema: "agent-harness.prompt-injection-ledger.v2",
            owner_module: "prompt",
            compatibility: "v2 skill entries are keyed by session, agent, skill id, body checksum, and delivery mode; v1 path/fingerprint ledgers remain readable for one release cycle",
        },
        SchemaRegistryEntry {
            schema: "agent-harness.skill-usage.v1",
            owner_module: "skill_usage",
            compatibility: "append-only usage JSONL; action enum additions require status and curator compatibility tests",
        },
        SchemaRegistryEntry {
            schema: "agent-harness.skill-usage-snapshot.v1",
            owner_module: "skill_usage",
            compatibility: "derived compact status artifact; rebuildable from skill-usage JSONL",
        },
        SchemaRegistryEntry {
            schema: "agent-harness.skill-proposal.v1",
            owner_module: "skill_learning",
            compatibility: "append-only proposal state records; apply requires checksum match and explicit operator action",
        },
        SchemaRegistryEntry {
            schema: "agent-harness.skill-apply-receipt.v1",
            owner_module: "skill_apply",
            compatibility: "append-only apply receipts; stale-base quarantine semantics are stable in v1",
        },
        SchemaRegistryEntry {
            schema: "agent-harness.learning-review.v1",
            owner_module: "skill_learning",
            compatibility: "deterministic review reports only; worker must not mutate skill files directly",
        },
        SchemaRegistryEntry {
            schema: "agent-harness.drift-report.v1",
            owner_module: "autonomy",
            compatibility: "additive fields only in v1",
        },
        SchemaRegistryEntry {
            schema: "agent-harness.context-pack.v1",
            owner_module: "memory_contracts",
            compatibility: "canonical normalized memory context pack; breaking changes require v2 and fail-open consumer tests",
        },
        SchemaRegistryEntry {
            schema: "openclaw-mem.context-pack.v1",
            owner_module: "memory_contracts",
            compatibility: "accepted producer schema translated to agent-harness.context-pack.v1; unknown versions fail open",
        },
        SchemaRegistryEntry {
            schema: "agent-harness.openclaw-mem-local-owner-prepare.v1",
            owner_module: "memory",
            compatibility: "append-only receipts; local prepare may add diagnostics but must not promote without operator approval",
        },
        SchemaRegistryEntry {
            schema: "agent-harness.security-scan.v1",
            owner_module: "security",
            compatibility: "additive findings only in v1",
        },
        SchemaRegistryEntry {
            schema: "agent-harness.quality-report.v1",
            owner_module: "quality",
            compatibility: "additive fields only in v1",
        },
    ]
}

pub fn run_public_hygiene(options: PublicHygieneOptions) -> io::Result<PublicHygieneReport> {
    let mut forbidden_hits = Vec::new();
    visit(&options.root, &mut |path| {
        let rendered = path.to_string_lossy().to_ascii_lowercase();
        if rendered.contains(".agent-harness")
            || rendered.contains("\\secrets\\")
            || rendered.contains("/secrets/")
            || rendered.contains(".review")
            || rendered.contains(".debug")
        {
            forbidden_hits.push(path.to_path_buf());
        }
    })?;
    Ok(PublicHygieneReport {
        schema: QUALITY_REPORT_SCHEMA,
        root: options.root,
        passed: forbidden_hits.is_empty(),
        forbidden_hits,
    })
}

pub fn release_checklist() -> ReleaseChecklist {
    ReleaseChecklist {
        schema: QUALITY_REPORT_SCHEMA,
        required_items: vec![
            "cargo fmt --all",
            "cargo test --workspace",
            "schema registry updated",
            "CHANGELOG.md updated",
            "docs/skills/help stale guidance review completed",
            "public hygiene report passed",
            "rollback notes recorded",
            "staging healthz and trace samples captured",
        ],
    }
}

fn visit(root: &Path, on_path: &mut impl FnMut(&Path)) -> io::Result<()> {
    if !root.exists() {
        return Ok(());
    }
    on_path(root);
    if root.is_dir() {
        for entry in fs::read_dir(root)? {
            let entry = entry?;
            visit(&entry.path(), on_path)?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn quality_catalogs_and_hygiene_report_are_actionable() {
        assert!(invariant_catalog().len() >= 7);
        assert!(
            schema_registry_entries()
                .iter()
                .any(|entry| entry.schema == "agent-harness.encrypted-vault.v1")
        );
        assert!(
            schema_registry_entries()
                .iter()
                .any(|entry| entry.schema == "agent-harness.codex-context-preflight.v1")
        );
        assert!(
            release_checklist()
                .required_items
                .contains(&"public hygiene report passed")
        );
        assert!(
            release_checklist()
                .required_items
                .contains(&"docs/skills/help stale guidance review completed")
        );

        let root = temp_root("quality_catalogs_and_hygiene_report_are_actionable");
        fs::create_dir_all(root.join(".agent-harness").join("secrets")).unwrap();
        fs::write(
            root.join(".agent-harness").join("secrets").join("key.env"),
            "secret",
        )
        .unwrap();
        let report = run_public_hygiene(PublicHygieneOptions { root: root.clone() }).unwrap();
        assert!(!report.passed);
        assert!(!report.forbidden_hits.is_empty());

        let _ = fs::remove_dir_all(root);
    }

    fn temp_root(test_name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "agent-harness-quality-{test_name}-{}-{nanos}",
            std::process::id()
        ))
    }
}
