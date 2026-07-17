use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Component, Path, PathBuf};

use ring::digest::{Context, SHA256};
use serde::{Deserialize, Serialize};

pub const SKILL_REPLAY_MANIFEST_SCHEMA: &str = "agent-harness.skill-replay-manifest.v1";
pub const SKILL_REPLAY_CASE_SCHEMA: &str = "agent-harness.skill-replay-case.v1";
pub const SKILL_REPLAY_BASELINE_SCHEMA: &str = "agent-harness.skill-replay-baseline.v1";
pub const REQUIRED_REPLAY_CLASSES: &[&str] = &[
    "explicit-single-skill-invocation",
    "explicit-multi-skill-bundle",
    "strong-automatic-match",
    "ambiguous-confusing-pair",
    "no-skill-general-task",
    "short-continuation-social-turn",
    "cross-channel-platform-negative",
    "cjk-mixed-language-intent",
    "novel-reusable-workflow",
    "existing-skill-improvement",
    "concrete-session-rollover",
    "new-virtual-task-boundary",
    "knowledge-store-classification",
    "existing-memory-reclassification",
    "dream-source-admission-negative",
    "topology-operation",
    "nightly-rerun-recovery",
    "trigger-equivalence",
    "unchanged-dream-workspace",
    "memory-local-skill-bridge",
];

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ReplayPrivacyClass {
    Synthetic,
    Redacted,
    PrivateLocal,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ExpectedDelivery {
    None,
    CatalogCard,
    FullBody,
    Reference,
    Explicit,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SkillReplayLabelsV1 {
    #[serde(default)]
    pub required_skills: Vec<String>,
    #[serde(default)]
    pub supporting_skills: Vec<String>,
    #[serde(default)]
    pub forbidden_skills: Vec<String>,
    pub expected_abstain: bool,
    pub expected_delivery: ExpectedDelivery,
    pub privacy_class: ReplayPrivacyClass,
    pub outcome_verifier: String,
    #[serde(default)]
    pub reviewers: Vec<String>,
    #[serde(default)]
    pub high_risk: bool,
    #[serde(default)]
    pub routing_labels_reviewed: bool,
    #[serde(default)]
    pub novel_intent: String,
    #[serde(default)]
    pub expected_delivery_reason: String,
    #[serde(default)]
    pub canonical_disposition: String,
    #[serde(default)]
    pub dream_admission: String,
    #[serde(default)]
    pub topology_operation: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SkillReplayObservedV1 {
    #[serde(default)]
    pub selected_skills: Vec<String>,
    pub delivered_bytes: u64,
    #[serde(default)]
    pub duplicate_source_count: u64,
    pub learning_target: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SkillReplayCaseV1 {
    pub schema: String,
    pub case_id: String,
    pub class: String,
    pub exact_lane_digest: String,
    pub turn_hash: String,
    pub labels: SkillReplayLabelsV1,
    pub observed: SkillReplayObservedV1,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SkillReplayManifestEntryV1 {
    pub path: String,
    pub sha256: String,
    pub case_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SkillReplayManifestV1 {
    pub schema: String,
    pub corpus_id: String,
    pub immutable: bool,
    pub privacy_classification: String,
    #[serde(default)]
    pub required_class_owners: BTreeMap<String, String>,
    pub entries: Vec<SkillReplayManifestEntryV1>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct LoadedSkillReplayCorpus {
    pub manifest: SkillReplayManifestV1,
    pub cases: Vec<SkillReplayCaseV1>,
    pub manifest_sha256: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SkillReplayMetricsV1 {
    pub case_count: usize,
    pub strict_precision: f64,
    pub soft_precision: f64,
    pub recall_at_2: f64,
    pub abstain_accuracy: f64,
    pub forbidden_rate: f64,
    pub selected_count: usize,
    pub prompt_bytes: u64,
    pub duplicate_source_count: u64,
    pub improvement_target_accuracy: f64,
    pub class_counts: BTreeMap<String, usize>,
    pub lane_counts: BTreeMap<String, usize>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SkillReplayBaselineV1 {
    pub schema: String,
    pub baseline_id: String,
    pub corpus_id: String,
    pub corpus_manifest_sha256: String,
    pub policy_revision: String,
    pub immutable: bool,
    pub metrics: SkillReplayMetricsV1,
}

pub fn load_skill_replay_corpus(
    manifest_path: &Path,
    allow_private_local: bool,
) -> Result<LoadedSkillReplayCorpus, String> {
    let manifest_bytes = fs::read(manifest_path).map_err(|error| {
        format!(
            "failed to read replay manifest {}: {error}",
            manifest_path.display()
        )
    })?;
    let manifest: SkillReplayManifestV1 = serde_json::from_slice(&manifest_bytes)
        .map_err(|error| format!("invalid replay manifest JSON: {error}"))?;
    if manifest.schema != SKILL_REPLAY_MANIFEST_SCHEMA {
        return Err(format!(
            "unsupported replay manifest schema: {}",
            manifest.schema
        ));
    }
    if manifest.corpus_id.trim().is_empty() || !manifest.immutable {
        return Err("replay corpus must have a named immutable manifest".to_string());
    }
    for (class, owner) in &manifest.required_class_owners {
        if class.trim().is_empty() || owner.trim().is_empty() {
            return Err("required replay class owners must be non-empty".to_string());
        }
    }
    let missing_classes: Vec<&str> = REQUIRED_REPLAY_CLASSES
        .iter()
        .copied()
        .filter(|class| !manifest.required_class_owners.contains_key(*class))
        .collect();
    if !missing_classes.is_empty() {
        return Err(format!(
            "replay manifest is missing required class owners: {}",
            missing_classes.join(", ")
        ));
    }
    let root = manifest_path.parent().unwrap_or_else(|| Path::new("."));
    let mut cases = Vec::new();
    let mut case_ids = BTreeSet::new();
    for entry in &manifest.entries {
        let relative = safe_relative_path(&entry.path)?;
        let path = root.join(relative);
        let bytes = fs::read(&path).map_err(|error| {
            format!("failed to read replay fixture {}: {error}", path.display())
        })?;
        let actual = sha256_prefixed(&bytes);
        if !actual.eq_ignore_ascii_case(&entry.sha256) {
            return Err(format!(
                "replay fixture checksum mismatch for {}: expected {}, got {actual}",
                entry.path, entry.sha256
            ));
        }
        let fixture_cases: Vec<SkillReplayCaseV1> = serde_json::from_slice(&bytes)
            .map_err(|error| format!("invalid replay fixture {}: {error}", entry.path))?;
        if fixture_cases.len() != entry.case_count {
            return Err(format!(
                "replay fixture count mismatch for {}: expected {}, got {}",
                entry.path,
                entry.case_count,
                fixture_cases.len()
            ));
        }
        for case in fixture_cases {
            validate_case(&case, allow_private_local)?;
            if !case_ids.insert(case.case_id.to_ascii_lowercase()) {
                return Err(format!("duplicate replay case id: {}", case.case_id));
            }
            cases.push(case);
        }
    }
    Ok(LoadedSkillReplayCorpus {
        manifest,
        cases,
        manifest_sha256: sha256_prefixed(&manifest_bytes),
    })
}

pub fn report_current_policy_baseline(
    corpus: &LoadedSkillReplayCorpus,
    baseline_id: &str,
    policy_revision: &str,
) -> Result<SkillReplayBaselineV1, String> {
    if baseline_id.trim().is_empty() || policy_revision.trim().is_empty() {
        return Err("baselineId and policyRevision are required".to_string());
    }
    let mut directly_required_selected = 0usize;
    let mut supporting_selected = 0usize;
    let mut selected_count = 0usize;
    let mut required_turns = 0usize;
    let mut recalled_turns = 0usize;
    let mut expected_abstain_turns = 0usize;
    let mut correct_abstentions = 0usize;
    let mut forbidden_selected = 0usize;
    let mut target_labeled = 0usize;
    let mut target_correct = 0usize;
    let mut prompt_bytes = 0u64;
    let mut duplicate_source_count = 0u64;
    let mut class_counts = BTreeMap::new();
    let mut lane_counts = BTreeMap::new();

    for case in &corpus.cases {
        *class_counts.entry(case.class.clone()).or_insert(0) += 1;
        *lane_counts
            .entry(case.exact_lane_digest.clone())
            .or_insert(0) += 1;
        let selected: Vec<String> = case
            .observed
            .selected_skills
            .iter()
            .map(|value| value.to_ascii_lowercase())
            .collect();
        selected_count += selected.len();
        prompt_bytes += case.observed.delivered_bytes;
        duplicate_source_count += case.observed.duplicate_source_count;
        if case.labels.expected_abstain {
            expected_abstain_turns += 1;
            if selected.is_empty() {
                correct_abstentions += 1;
            }
        }
        if !case.labels.required_skills.is_empty() {
            required_turns += 1;
            if case.labels.required_skills.iter().any(|skill| {
                selected
                    .iter()
                    .take(2)
                    .any(|item| item.eq_ignore_ascii_case(skill))
            }) {
                recalled_turns += 1;
            }
        }
        if case.labels.routing_labels_reviewed {
            for skill in &selected {
                if case
                    .labels
                    .required_skills
                    .iter()
                    .any(|required| required.eq_ignore_ascii_case(skill))
                {
                    directly_required_selected += 1;
                } else if case
                    .labels
                    .supporting_skills
                    .iter()
                    .any(|supporting| supporting.eq_ignore_ascii_case(skill))
                {
                    supporting_selected += 1;
                }
                if case
                    .labels
                    .forbidden_skills
                    .iter()
                    .any(|forbidden| forbidden.eq_ignore_ascii_case(skill))
                {
                    forbidden_selected += 1;
                }
            }
        }
        if let Some(expected) = case.labels.required_skills.first() {
            target_labeled += 1;
            if case
                .observed
                .learning_target
                .as_deref()
                .is_some_and(|target| target.eq_ignore_ascii_case(expected))
            {
                target_correct += 1;
            }
        }
    }
    let ratio = |numerator: usize, denominator: usize| {
        if denominator == 0 {
            1.0
        } else {
            numerator as f64 / denominator as f64
        }
    };
    Ok(SkillReplayBaselineV1 {
        schema: SKILL_REPLAY_BASELINE_SCHEMA.to_string(),
        baseline_id: baseline_id.to_string(),
        corpus_id: corpus.manifest.corpus_id.clone(),
        corpus_manifest_sha256: corpus.manifest_sha256.clone(),
        policy_revision: policy_revision.to_string(),
        immutable: true,
        metrics: SkillReplayMetricsV1 {
            case_count: corpus.cases.len(),
            strict_precision: ratio(
                directly_required_selected,
                corpus
                    .cases
                    .iter()
                    .filter(|case| case.labels.routing_labels_reviewed)
                    .map(|case| case.observed.selected_skills.len())
                    .sum(),
            ),
            soft_precision: if corpus
                .cases
                .iter()
                .filter(|case| case.labels.routing_labels_reviewed)
                .all(|case| case.observed.selected_skills.is_empty())
            {
                1.0
            } else {
                (directly_required_selected as f64 + 0.5 * supporting_selected as f64)
                    / corpus
                        .cases
                        .iter()
                        .filter(|case| case.labels.routing_labels_reviewed)
                        .map(|case| case.observed.selected_skills.len())
                        .sum::<usize>() as f64
            },
            recall_at_2: ratio(recalled_turns, required_turns),
            abstain_accuracy: ratio(correct_abstentions, expected_abstain_turns),
            forbidden_rate: ratio(
                forbidden_selected,
                corpus
                    .cases
                    .iter()
                    .filter(|case| case.labels.routing_labels_reviewed)
                    .map(|case| case.observed.selected_skills.len())
                    .sum(),
            ),
            selected_count,
            prompt_bytes,
            duplicate_source_count,
            improvement_target_accuracy: ratio(target_correct, target_labeled),
            class_counts,
            lane_counts,
        },
    })
}

fn validate_case(case: &SkillReplayCaseV1, allow_private_local: bool) -> Result<(), String> {
    if case.schema != SKILL_REPLAY_CASE_SCHEMA {
        return Err(format!(
            "unsupported replay case schema for {}",
            case.case_id
        ));
    }
    if case.case_id.trim().is_empty()
        || case.class.trim().is_empty()
        || case.exact_lane_digest.trim().is_empty()
        || case.turn_hash.trim().is_empty()
        || case.labels.outcome_verifier.trim().is_empty()
    {
        return Err(format!(
            "replay case {} is missing required fields",
            case.case_id
        ));
    }
    if case.labels.high_risk && case.labels.reviewers.len() < 2 {
        return Err(format!(
            "high-risk replay case {} requires two reviewers",
            case.case_id
        ));
    }
    if case.labels.privacy_class == ReplayPrivacyClass::PrivateLocal && !allow_private_local {
        return Err(format!(
            "private-local replay case {} is not allowed in this load mode",
            case.case_id
        ));
    }
    Ok(())
}

fn safe_relative_path(value: &str) -> Result<PathBuf, String> {
    let path = Path::new(value);
    if path.is_absolute()
        || path.components().any(|component| {
            matches!(
                component,
                Component::ParentDir | Component::RootDir | Component::Prefix(_)
            )
        })
    {
        return Err(format!("replay fixture path must stay relative: {value}"));
    }
    Ok(path.to_path_buf())
}

fn sha256_prefixed(bytes: &[u8]) -> String {
    let mut context = Context::new(&SHA256);
    context.update(bytes);
    let digest = context.finish();
    let mut value = String::with_capacity(71);
    value.push_str("sha256:");
    for byte in digest.as_ref() {
        value.push_str(&format!("{byte:02x}"));
    }
    value
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_root(label: &str) -> PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!("agent-harness-{label}-{nonce}"));
        fs::create_dir_all(&root).unwrap();
        root
    }

    fn sample_case(id: &str, high_risk: bool) -> SkillReplayCaseV1 {
        SkillReplayCaseV1 {
            schema: SKILL_REPLAY_CASE_SCHEMA.to_string(),
            case_id: id.to_string(),
            class: "strong-automatic-match".to_string(),
            exact_lane_digest: "lane-a".to_string(),
            turn_hash: "turn-a".to_string(),
            labels: SkillReplayLabelsV1 {
                required_skills: vec!["skill-a".to_string()],
                supporting_skills: vec!["skill-b".to_string()],
                forbidden_skills: vec!["skill-x".to_string()],
                expected_abstain: false,
                expected_delivery: ExpectedDelivery::CatalogCard,
                privacy_class: ReplayPrivacyClass::Synthetic,
                outcome_verifier: "scenario:test".to_string(),
                reviewers: if high_risk {
                    vec!["reviewer-a".to_string(), "reviewer-b".to_string()]
                } else {
                    Vec::new()
                },
                high_risk,
                routing_labels_reviewed: true,
                novel_intent: "none".to_string(),
                expected_delivery_reason: "first-load".to_string(),
                canonical_disposition: "skill".to_string(),
                dream_admission: "admit".to_string(),
                topology_operation: "none".to_string(),
            },
            observed: SkillReplayObservedV1 {
                selected_skills: vec!["skill-a".to_string(), "skill-b".to_string()],
                delivered_bytes: 128,
                duplicate_source_count: 0,
                learning_target: Some("skill-a".to_string()),
            },
        }
    }

    fn write_corpus(root: &Path, cases: &[SkillReplayCaseV1]) -> PathBuf {
        let fixture_bytes = serde_json::to_vec_pretty(cases).unwrap();
        fs::write(root.join("cases.json"), &fixture_bytes).unwrap();
        let manifest = SkillReplayManifestV1 {
            schema: SKILL_REPLAY_MANIFEST_SCHEMA.to_string(),
            corpus_id: "skill-v4-frozen".to_string(),
            immutable: true,
            privacy_classification: "synthetic-only".to_string(),
            required_class_owners: REQUIRED_REPLAY_CLASSES
                .iter()
                .map(|class| ((*class).to_string(), "fixture:case-a".to_string()))
                .collect(),
            entries: vec![SkillReplayManifestEntryV1 {
                path: "cases.json".to_string(),
                sha256: sha256_prefixed(&fixture_bytes),
                case_count: cases.len(),
            }],
        };
        let path = root.join("manifest.json");
        fs::write(&path, serde_json::to_vec_pretty(&manifest).unwrap()).unwrap();
        path
    }

    #[test]
    fn corpus_loader_verifies_checksum_and_high_risk_reviewers() {
        let root = temp_root("skill-replay-load");
        let path = write_corpus(&root, &[sample_case("case-a", true)]);
        let corpus = load_skill_replay_corpus(&path, false).unwrap();
        assert_eq!(corpus.cases.len(), 1);
        assert!(corpus.manifest_sha256.starts_with("sha256:"));
    }

    #[test]
    fn corpus_loader_fails_closed_on_checksum_drift() {
        let root = temp_root("skill-replay-drift");
        let path = write_corpus(&root, &[sample_case("case-a", false)]);
        fs::write(root.join("cases.json"), b"[]").unwrap();
        let error = load_skill_replay_corpus(&path, false).unwrap_err();
        assert!(error.contains("checksum mismatch"));
    }

    #[test]
    fn public_load_rejects_private_local_cases() {
        let root = temp_root("skill-replay-private");
        let mut case = sample_case("case-private", false);
        case.labels.privacy_class = ReplayPrivacyClass::PrivateLocal;
        let path = write_corpus(&root, &[case]);
        let error = load_skill_replay_corpus(&path, false).unwrap_err();
        assert!(error.contains("private-local"));
    }

    #[test]
    fn baseline_report_is_named_immutable_and_reproducible() {
        let root = temp_root("skill-replay-baseline");
        let reviewed = sample_case("case-a", false);
        let mut automatic_only = sample_case("case-b", false);
        automatic_only.labels.routing_labels_reviewed = false;
        automatic_only.labels.required_skills.clear();
        automatic_only.labels.supporting_skills.clear();
        automatic_only.observed.selected_skills = vec!["skill-x".to_string()];
        let path = write_corpus(&root, &[reviewed, automatic_only]);
        let corpus = load_skill_replay_corpus(&path, false).unwrap();
        let first = report_current_policy_baseline(&corpus, "v4-48h", "matcher-v4").unwrap();
        let second = report_current_policy_baseline(&corpus, "v4-48h", "matcher-v4").unwrap();
        assert_eq!(first, second);
        assert!(first.immutable);
        assert_eq!(first.metrics.strict_precision, 0.5);
        assert_eq!(first.metrics.soft_precision, 0.75);
        assert_eq!(first.metrics.selected_count, 3);
        assert_eq!(first.metrics.recall_at_2, 1.0);
        assert_eq!(first.metrics.forbidden_rate, 0.0);
        assert_eq!(first.metrics.improvement_target_accuracy, 1.0);
    }
}
