use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use ring::digest;
use serde::{Deserialize, Serialize};

use crate::append_jsonl_value_once_by_event_key;

pub const GOAL_CLOSURE_INTENT_SCHEMA: &str = "agent-harness.goal-closure-intent.v1";
pub const GOAL_CLOSURE_RESOLUTION_SCHEMA: &str = "agent-harness.goal-closure-resolution.v1";
pub const GOAL_CLOSURE_RECEIPT_SCHEMA: &str = "agent-harness.goal-closure-receipt.v1";

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum GoalClosureTriggerV1 {
    OperatorHistorical,
    ChannelStop,
    ChannelNew,
    Recovery,
    #[default]
    #[serde(other)]
    Unknown,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum GoalClosureDispositionV1 {
    Completed,
    Canceled,
    #[default]
    #[serde(other)]
    Unknown,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum GoalClosurePhaseV1 {
    #[default]
    IntentRecorded,
    BackendResultRecorded,
    TerminalProjectionRecorded,
    LineageReconciled,
    Completed,
    #[serde(other)]
    Unknown,
}

impl GoalClosurePhaseV1 {
    fn ordinal(self) -> u8 {
        match self {
            Self::IntentRecorded => 0,
            Self::BackendResultRecorded => 1,
            Self::TerminalProjectionRecorded => 2,
            Self::LineageReconciled => 3,
            Self::Completed => 4,
            Self::Unknown => u8::MAX,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum GoalClosureResultV1 {
    #[default]
    Pending,
    Succeeded,
    NotApplicable,
    RetryableFailure,
    Rejected,
    #[serde(other)]
    Unknown,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum GoalClosureTargetResolutionDispositionV1 {
    #[default]
    NotApplicable,
    Exact,
    Ambiguous,
    Error,
}

/// Protected authority material used to resolve the original goal binding.
///
/// This value belongs in the private closure-intent ledger. Ordinary status,
/// logs, and phase receipts should use `authority_digest` instead.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GoalClosureAuthorityV1 {
    pub lane_digest: String,
    pub concrete_session_key: String,
    pub virtual_session_id: String,
    pub backend_context_generation: String,
    pub source_thread_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GoalClosureIntentV1 {
    pub schema: String,
    pub event_key: String,
    pub closure_id: String,
    pub trigger: GoalClosureTriggerV1,
    pub disposition: GoalClosureDispositionV1,
    pub authority: GoalClosureAuthorityV1,
    pub authority_digest: String,
    pub goal_identity: String,
    pub goal_generation: String,
    pub expected_projection_checksum: Option<String>,
    pub caller_effect_identity: String,
    pub caller_effect_digest: String,
    pub reason: String,
    pub intent_checksum: String,
}

impl GoalClosureIntentV1 {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        trigger: GoalClosureTriggerV1,
        disposition: GoalClosureDispositionV1,
        authority: GoalClosureAuthorityV1,
        goal_identity: impl Into<String>,
        goal_generation: impl Into<String>,
        expected_projection_checksum: Option<String>,
        caller_effect_identity: impl Into<String>,
        reason: impl Into<String>,
    ) -> io::Result<Self> {
        let goal_identity = goal_identity.into();
        let goal_generation = goal_generation.into();
        let caller_effect_identity = caller_effect_identity.into();
        let reason = reason.into();
        validate_authority(&authority)?;
        validate_nonempty("goal identity", &goal_identity)?;
        validate_nonempty("goal generation", &goal_generation)?;
        validate_nonempty("caller effect identity", &caller_effect_identity)?;
        validate_nonempty("reason", &reason)?;
        if expected_projection_checksum
            .as_deref()
            .is_none_or(|value| value.trim().is_empty())
        {
            return Err(invalid_input(
                "goal closure intent requires an expected projection checksum",
            ));
        }
        if trigger == GoalClosureTriggerV1::Unknown {
            return Err(invalid_input("goal closure trigger must be known"));
        }
        if disposition == GoalClosureDispositionV1::Unknown {
            return Err(invalid_input("goal closure disposition must be known"));
        }
        if matches!(
            trigger,
            GoalClosureTriggerV1::ChannelStop | GoalClosureTriggerV1::ChannelNew
        ) && disposition != GoalClosureDispositionV1::Canceled
        {
            return Err(invalid_input(
                "automatic channel-command closure may only cancel a goal",
            ));
        }

        let authority_digest = checksum_json(&authority)?;
        let caller_effect_digest = checksum_json(&caller_effect_identity)?;
        let closure_id = checksum_json(&ClosureIdentityPayload {
            lane_digest: &authority.lane_digest,
            concrete_session_key: &authority.concrete_session_key,
            virtual_session_id: &authority.virtual_session_id,
            backend_context_generation: &authority.backend_context_generation,
            source_thread_id: &authority.source_thread_id,
            goal_identity: &goal_identity,
            goal_generation: &goal_generation,
            disposition,
            caller_effect_identity: &caller_effect_identity,
        })?;
        let intent_checksum = checksum_json(&IntentChecksumPayload {
            closure_id: &closure_id,
            trigger,
            disposition,
            authority: &authority,
            goal_identity: &goal_identity,
            goal_generation: &goal_generation,
            expected_projection_checksum: expected_projection_checksum.as_deref(),
            caller_effect_identity: &caller_effect_identity,
            reason: &reason,
        })?;

        Ok(Self {
            schema: GOAL_CLOSURE_INTENT_SCHEMA.to_string(),
            event_key: closure_id.clone(),
            closure_id,
            trigger,
            disposition,
            authority,
            authority_digest,
            goal_identity,
            goal_generation,
            expected_projection_checksum,
            caller_effect_identity,
            caller_effect_digest,
            reason,
            intent_checksum,
        })
    }

    pub fn validate(&self) -> io::Result<()> {
        let rebuilt = Self::new(
            self.trigger,
            self.disposition,
            self.authority.clone(),
            self.goal_identity.clone(),
            self.goal_generation.clone(),
            self.expected_projection_checksum.clone(),
            self.caller_effect_identity.clone(),
            self.reason.clone(),
        )?;
        if self.schema != GOAL_CLOSURE_INTENT_SCHEMA
            || self.event_key != self.closure_id
            || self.closure_id != rebuilt.closure_id
            || self.authority_digest != rebuilt.authority_digest
            || self.caller_effect_digest != rebuilt.caller_effect_digest
            || self.intent_checksum != rebuilt.intent_checksum
        {
            return Err(invalid_data(
                "goal closure intent identity or checksum validation failed",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GoalClosureTargetCandidateV1 {
    pub authority: GoalClosureAuthorityV1,
    pub goal_identity: String,
    pub goal_generation: String,
    pub projection_checksum: String,
    pub active: bool,
    pub original_binding: bool,
    pub latest_authoritative_projection: bool,
    pub latest_authoritative_lineage: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GoalClosureResolvedTargetV1 {
    pub authority: GoalClosureAuthorityV1,
    pub goal_identity: String,
    pub goal_generation: String,
    pub projection_checksum: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GoalClosureTargetResolutionV1 {
    pub schema: String,
    pub closure_id: String,
    pub disposition: GoalClosureTargetResolutionDispositionV1,
    pub authority_digest: String,
    pub candidate_count: usize,
    pub target: Option<GoalClosureResolvedTargetV1>,
    pub reason: String,
}

pub fn resolve_goal_closure_target(
    intent: &GoalClosureIntentV1,
    candidates: &[GoalClosureTargetCandidateV1],
) -> GoalClosureTargetResolutionV1 {
    let base = |disposition, target, reason: &str| GoalClosureTargetResolutionV1 {
        schema: GOAL_CLOSURE_RESOLUTION_SCHEMA.to_string(),
        closure_id: intent.closure_id.clone(),
        disposition,
        authority_digest: intent.authority_digest.clone(),
        candidate_count: candidates.len(),
        target,
        reason: reason.to_string(),
    };
    if intent.validate().is_err() {
        return base(
            GoalClosureTargetResolutionDispositionV1::Error,
            None,
            "closure intent failed identity/checksum validation",
        );
    }

    let active = candidates
        .iter()
        .filter(|candidate| candidate.active)
        .collect::<Vec<_>>();
    if active.is_empty() {
        return base(
            GoalClosureTargetResolutionDispositionV1::NotApplicable,
            None,
            "no active goal exists for the requested closure",
        );
    }

    let exact = active
        .iter()
        .copied()
        .filter(|candidate| {
            candidate.authority == intent.authority
                && candidate.goal_identity == intent.goal_identity
                && candidate.goal_generation == intent.goal_generation
                && candidate.original_binding
                && candidate.latest_authoritative_projection
                && candidate.latest_authoritative_lineage
        })
        .collect::<Vec<_>>();
    if exact.len() > 1 {
        return base(
            GoalClosureTargetResolutionDispositionV1::Ambiguous,
            None,
            "more than one active candidate claims the exact authoritative binding",
        );
    }
    if exact.is_empty() || active.len() != exact.len() {
        return base(
            GoalClosureTargetResolutionDispositionV1::Error,
            None,
            "active evidence is cross-boundary, incomplete, or not the original authoritative binding",
        );
    }

    let candidate = exact[0];
    if intent
        .expected_projection_checksum
        .as_deref()
        .is_some_and(|expected| expected != candidate.projection_checksum)
    {
        return base(
            GoalClosureTargetResolutionDispositionV1::Error,
            None,
            "the authoritative projection checksum changed after closure planning",
        );
    }
    base(
        GoalClosureTargetResolutionDispositionV1::Exact,
        Some(GoalClosureResolvedTargetV1 {
            authority: candidate.authority.clone(),
            goal_identity: candidate.goal_identity.clone(),
            goal_generation: candidate.goal_generation.clone(),
            projection_checksum: candidate.projection_checksum.clone(),
        }),
        "one exact original authoritative goal binding is eligible for closure",
    )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum GoalClosureAppendDispositionV1 {
    Appended,
    Replayed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GoalClosureIntentRecordOutcomeV1 {
    pub disposition: GoalClosureAppendDispositionV1,
    pub intent: GoalClosureIntentV1,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GoalClosureReceiptRecordOutcomeV1 {
    pub disposition: GoalClosureAppendDispositionV1,
    pub receipt: GoalClosureReceiptV1,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GoalClosurePhaseInputV1 {
    pub result: GoalClosureResultV1,
    pub projection_checksum: Option<String>,
    pub lineage_checksum: Option<String>,
    pub result_evidence_digest: Option<String>,
    pub recorded_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GoalClosureReceiptV1 {
    pub schema: String,
    pub event_key: String,
    pub receipt_id: String,
    pub closure_id: String,
    pub intent_checksum: String,
    pub authority_digest: String,
    pub target_digest: String,
    pub caller_effect_digest: String,
    pub trigger: GoalClosureTriggerV1,
    pub disposition: GoalClosureDispositionV1,
    pub phase: GoalClosurePhaseV1,
    pub result: GoalClosureResultV1,
    pub projection_checksum: Option<String>,
    pub lineage_checksum: Option<String>,
    pub result_evidence_digest: Option<String>,
    pub phase_checksum: String,
    pub recorded_at_ms: i64,
}

pub fn goal_closure_intents_file(harness_home: impl AsRef<Path>) -> PathBuf {
    harness_home
        .as_ref()
        .join("state")
        .join("goal-closure")
        .join("protected-intents.jsonl")
}

pub fn goal_closure_receipts_file(harness_home: impl AsRef<Path>) -> PathBuf {
    harness_home
        .as_ref()
        .join("state")
        .join("goal-closure")
        .join("receipts.jsonl")
}

pub fn record_goal_closure_intent(
    harness_home: impl AsRef<Path>,
    intent: &GoalClosureIntentV1,
) -> io::Result<GoalClosureIntentRecordOutcomeV1> {
    intent.validate()?;
    let harness_home = harness_home.as_ref();
    let file = goal_closure_intents_file(harness_home);
    let mut terminal_matches = Vec::new();
    for stored in read_jsonl::<GoalClosureIntentV1>(&file)?
        .into_iter()
        .filter(|stored| same_goal_generation(stored, intent))
    {
        stored.validate()?;
        let terminal = goal_closure_receipts_for_id(harness_home, &stored.closure_id)?
            .last()
            .is_some_and(|receipt| {
                receipt.phase == GoalClosurePhaseV1::Completed
                    && receipt.result == GoalClosureResultV1::Succeeded
            });
        if terminal {
            terminal_matches.push(stored);
        }
    }
    if terminal_matches.len() > 1 {
        return Err(invalid_data(
            "more than one terminal closure exists for the same goal generation",
        ));
    }
    if let Some(stored) = terminal_matches.into_iter().next() {
        stored.validate()?;
        return Ok(GoalClosureIntentRecordOutcomeV1 {
            disposition: GoalClosureAppendDispositionV1::Replayed,
            intent: stored,
        });
    }
    let appended = append_jsonl_value_once_by_event_key(&file, intent)?;
    let stored = find_one_by_event_key::<GoalClosureIntentV1>(&file, &intent.event_key)?
        .ok_or_else(|| invalid_data("goal closure intent append was not durable"))?;
    stored.validate()?;
    if stored.intent_checksum != intent.intent_checksum {
        return Err(invalid_data(
            "closure ID already exists with a different intent checksum",
        ));
    }
    Ok(GoalClosureIntentRecordOutcomeV1 {
        disposition: if appended {
            GoalClosureAppendDispositionV1::Appended
        } else {
            GoalClosureAppendDispositionV1::Replayed
        },
        intent: stored,
    })
}

pub fn record_goal_closure_phase(
    harness_home: impl AsRef<Path>,
    intent: &GoalClosureIntentV1,
    phase: GoalClosurePhaseV1,
    input: GoalClosurePhaseInputV1,
) -> io::Result<GoalClosureReceiptRecordOutcomeV1> {
    intent.validate()?;
    let harness_home = harness_home.as_ref();
    let recorded_intent = find_one_by_event_key::<GoalClosureIntentV1>(
        &goal_closure_intents_file(harness_home),
        &intent.event_key,
    )?
    .ok_or_else(|| invalid_input("goal closure intent must be durable before phase receipts"))?;
    recorded_intent.validate()?;
    if recorded_intent.intent_checksum != intent.intent_checksum {
        return Err(invalid_data(
            "durable closure intent checksum differs from the requested phase",
        ));
    }

    let receipts = goal_closure_receipts_for_id(harness_home, &intent.closure_id)?;
    let latest = receipts.last();
    let expected_ordinal = latest
        .map(|receipt| receipt.phase.ordinal().saturating_add(1))
        .unwrap_or(0);
    if phase.ordinal() > expected_ordinal {
        return Err(invalid_input(
            "goal closure phases must be recorded in durable causal order",
        ));
    }
    if phase.ordinal() < expected_ordinal
        && !receipts
            .iter()
            .any(|receipt| receipt.closure_id == intent.closure_id && receipt.phase == phase)
    {
        return Err(invalid_data(
            "goal closure receipt ledger has a non-contiguous phase history",
        ));
    }
    if phase.ordinal() == expected_ordinal {
        if phase.ordinal() > 1
            && latest.is_some_and(|receipt| receipt.result != GoalClosureResultV1::Succeeded)
        {
            return Err(invalid_input(
                "goal closure cannot advance past an unsuccessful durable phase",
            ));
        }
        if phase == GoalClosurePhaseV1::Completed
            && latest.and_then(|receipt| receipt.lineage_checksum.as_deref())
                != input.lineage_checksum.as_deref()
        {
            return Err(invalid_input(
                "completion receipt must retain the reconciled lineage checksum",
            ));
        }
    }

    validate_phase_evidence(phase, &input)?;
    let target_digest = checksum_json(&ClosureTargetDigestPayload {
        authority_digest: &intent.authority_digest,
        goal_identity: &intent.goal_identity,
        goal_generation: &intent.goal_generation,
    })?;
    let receipt_id = checksum_json(&(intent.closure_id.as_str(), phase))?;
    let phase_checksum = checksum_json(&PhaseChecksumPayload {
        closure_id: &intent.closure_id,
        intent_checksum: &intent.intent_checksum,
        phase,
        result: input.result,
        projection_checksum: input.projection_checksum.as_deref(),
        lineage_checksum: input.lineage_checksum.as_deref(),
        result_evidence_digest: input.result_evidence_digest.as_deref(),
    })?;
    let receipt = GoalClosureReceiptV1 {
        schema: GOAL_CLOSURE_RECEIPT_SCHEMA.to_string(),
        event_key: receipt_id.clone(),
        receipt_id,
        closure_id: intent.closure_id.clone(),
        intent_checksum: intent.intent_checksum.clone(),
        authority_digest: intent.authority_digest.clone(),
        target_digest,
        caller_effect_digest: intent.caller_effect_digest.clone(),
        trigger: intent.trigger,
        disposition: intent.disposition,
        phase,
        result: input.result,
        projection_checksum: input.projection_checksum,
        lineage_checksum: input.lineage_checksum,
        result_evidence_digest: input.result_evidence_digest,
        phase_checksum,
        recorded_at_ms: input.recorded_at_ms,
    };
    let file = goal_closure_receipts_file(harness_home);
    let appended = append_jsonl_value_once_by_event_key(&file, &receipt)?;
    let stored = find_one_by_event_key::<GoalClosureReceiptV1>(&file, &receipt.event_key)?
        .ok_or_else(|| invalid_data("goal closure receipt append was not durable"))?;
    validate_receipt(&stored)?;
    if stored.phase_checksum != receipt.phase_checksum {
        return Err(invalid_data(
            "closure phase already exists with a different checksum",
        ));
    }
    Ok(GoalClosureReceiptRecordOutcomeV1 {
        disposition: if appended {
            GoalClosureAppendDispositionV1::Appended
        } else {
            GoalClosureAppendDispositionV1::Replayed
        },
        receipt: stored,
    })
}

pub fn goal_closure_receipts_for_id(
    harness_home: impl AsRef<Path>,
    closure_id: &str,
) -> io::Result<Vec<GoalClosureReceiptV1>> {
    let mut receipts =
        read_jsonl::<GoalClosureReceiptV1>(&goal_closure_receipts_file(harness_home))?
            .into_iter()
            .filter(|receipt| receipt.closure_id == closure_id)
            .collect::<Vec<_>>();
    for receipt in &receipts {
        validate_receipt(receipt)?;
    }
    receipts.sort_by_key(|receipt| receipt.phase.ordinal());
    for (expected, receipt) in receipts.iter().enumerate() {
        if usize::from(receipt.phase.ordinal()) != expected {
            return Err(invalid_data(
                "goal closure receipt phases are duplicated or non-contiguous",
            ));
        }
    }
    Ok(receipts)
}

fn validate_phase_evidence(
    phase: GoalClosurePhaseV1,
    input: &GoalClosurePhaseInputV1,
) -> io::Result<()> {
    if input.result == GoalClosureResultV1::Unknown {
        return Err(invalid_input("goal closure result must be known"));
    }
    let result_is_valid_for_phase = match phase {
        GoalClosurePhaseV1::IntentRecorded => input.result == GoalClosureResultV1::Pending,
        GoalClosurePhaseV1::BackendResultRecorded => matches!(
            input.result,
            GoalClosureResultV1::Succeeded
                | GoalClosureResultV1::NotApplicable
                | GoalClosureResultV1::RetryableFailure
                | GoalClosureResultV1::Rejected
        ),
        GoalClosurePhaseV1::TerminalProjectionRecorded
        | GoalClosurePhaseV1::LineageReconciled
        | GoalClosurePhaseV1::Completed => input.result == GoalClosureResultV1::Succeeded,
        GoalClosurePhaseV1::Unknown => false,
    };
    if !result_is_valid_for_phase {
        return Err(invalid_input(
            "goal closure result is not valid for this durable phase",
        ));
    }
    if phase == GoalClosurePhaseV1::TerminalProjectionRecorded
        && input
            .projection_checksum
            .as_deref()
            .is_none_or(str::is_empty)
    {
        return Err(invalid_input(
            "terminal projection phase requires a projection checksum",
        ));
    }
    if matches!(
        phase,
        GoalClosurePhaseV1::LineageReconciled | GoalClosurePhaseV1::Completed
    ) && input.lineage_checksum.as_deref().is_none_or(str::is_empty)
    {
        return Err(invalid_input(
            "lineage and completion phases require a lineage checksum",
        ));
    }
    Ok(())
}

fn same_goal_generation(left: &GoalClosureIntentV1, right: &GoalClosureIntentV1) -> bool {
    left.authority == right.authority
        && left.goal_identity == right.goal_identity
        && left.goal_generation == right.goal_generation
}

fn validate_receipt(receipt: &GoalClosureReceiptV1) -> io::Result<()> {
    if receipt.schema != GOAL_CLOSURE_RECEIPT_SCHEMA || receipt.event_key != receipt.receipt_id {
        return Err(invalid_data(
            "goal closure receipt schema or event key is invalid",
        ));
    }
    let expected_id = checksum_json(&(receipt.closure_id.as_str(), receipt.phase))?;
    let expected_checksum = checksum_json(&PhaseChecksumPayload {
        closure_id: &receipt.closure_id,
        intent_checksum: &receipt.intent_checksum,
        phase: receipt.phase,
        result: receipt.result,
        projection_checksum: receipt.projection_checksum.as_deref(),
        lineage_checksum: receipt.lineage_checksum.as_deref(),
        result_evidence_digest: receipt.result_evidence_digest.as_deref(),
    })?;
    if receipt.receipt_id != expected_id || receipt.phase_checksum != expected_checksum {
        return Err(invalid_data(
            "goal closure receipt identity or checksum validation failed",
        ));
    }
    validate_phase_evidence(
        receipt.phase,
        &GoalClosurePhaseInputV1 {
            result: receipt.result,
            projection_checksum: receipt.projection_checksum.clone(),
            lineage_checksum: receipt.lineage_checksum.clone(),
            result_evidence_digest: receipt.result_evidence_digest.clone(),
            recorded_at_ms: receipt.recorded_at_ms,
        },
    )
}

fn find_one_by_event_key<T>(path: &Path, event_key: &str) -> io::Result<Option<T>>
where
    T: for<'de> Deserialize<'de> + EventKey,
{
    let mut matches = read_jsonl::<T>(path)?
        .into_iter()
        .filter(|value| value.event_key() == event_key)
        .collect::<Vec<_>>();
    if matches.len() > 1 {
        return Err(invalid_data(
            "duplicate event key in append-only closure ledger",
        ));
    }
    Ok(matches.pop())
}

fn read_jsonl<T>(path: &Path) -> io::Result<Vec<T>>
where
    T: for<'de> Deserialize<'de>,
{
    let text = match fs::read_to_string(path) {
        Ok(text) => text,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(error),
    };
    text.lines()
        .enumerate()
        .filter(|(_, line)| !line.trim().is_empty())
        .map(|(index, line)| {
            serde_json::from_str(line).map_err(|error| {
                invalid_data(format!(
                    "invalid goal closure JSONL record at line {}: {error}",
                    index + 1
                ))
            })
        })
        .collect()
}

trait EventKey {
    fn event_key(&self) -> &str;
}

impl EventKey for GoalClosureIntentV1 {
    fn event_key(&self) -> &str {
        &self.event_key
    }
}

impl EventKey for GoalClosureReceiptV1 {
    fn event_key(&self) -> &str {
        &self.event_key
    }
}

fn validate_authority(authority: &GoalClosureAuthorityV1) -> io::Result<()> {
    validate_nonempty("lane digest", &authority.lane_digest)?;
    validate_nonempty("concrete session key", &authority.concrete_session_key)?;
    validate_nonempty("virtual session ID", &authority.virtual_session_id)?;
    validate_nonempty(
        "backend context generation",
        &authority.backend_context_generation,
    )?;
    validate_nonempty("source thread ID", &authority.source_thread_id)
}

fn validate_nonempty(label: &str, value: &str) -> io::Result<()> {
    if value.trim().is_empty() {
        Err(invalid_input(format!("{label} must not be empty")))
    } else {
        Ok(())
    }
}

fn checksum_json(value: &impl Serialize) -> io::Result<String> {
    let bytes = serde_json::to_vec(value).map_err(io::Error::other)?;
    let hash = digest::digest(&digest::SHA256, &bytes);
    let mut result = String::with_capacity(71);
    result.push_str("sha256:");
    for byte in hash.as_ref() {
        use std::fmt::Write as _;
        let _ = write!(result, "{byte:02x}");
    }
    Ok(result)
}

fn invalid_input(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message.into())
}

fn invalid_data(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ClosureIdentityPayload<'a> {
    lane_digest: &'a str,
    concrete_session_key: &'a str,
    virtual_session_id: &'a str,
    backend_context_generation: &'a str,
    source_thread_id: &'a str,
    goal_identity: &'a str,
    goal_generation: &'a str,
    disposition: GoalClosureDispositionV1,
    caller_effect_identity: &'a str,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct IntentChecksumPayload<'a> {
    closure_id: &'a str,
    trigger: GoalClosureTriggerV1,
    disposition: GoalClosureDispositionV1,
    authority: &'a GoalClosureAuthorityV1,
    goal_identity: &'a str,
    goal_generation: &'a str,
    expected_projection_checksum: Option<&'a str>,
    caller_effect_identity: &'a str,
    reason: &'a str,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ClosureTargetDigestPayload<'a> {
    authority_digest: &'a str,
    goal_identity: &'a str,
    goal_generation: &'a str,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct PhaseChecksumPayload<'a> {
    closure_id: &'a str,
    intent_checksum: &'a str,
    phase: GoalClosurePhaseV1,
    result: GoalClosureResultV1,
    projection_checksum: Option<&'a str>,
    lineage_checksum: Option<&'a str>,
    result_evidence_digest: Option<&'a str>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn goal_history_close_dry_run_requires_one_exact_authority() {
        let intent = intent("reason");
        let exact = candidate(&intent);
        let resolution = resolve_goal_closure_target(&intent, &[exact.clone()]);
        assert_eq!(
            resolution.disposition,
            GoalClosureTargetResolutionDispositionV1::Exact
        );
        assert_eq!(
            resolution
                .target
                .as_ref()
                .map(|target| target.authority.clone()),
            Some(intent.authority.clone())
        );

        let mut cross_session = exact.clone();
        cross_session.authority.concrete_session_key = "other-session".to_string();
        let rejected = resolve_goal_closure_target(&intent, &[cross_session]);
        assert_eq!(
            rejected.disposition,
            GoalClosureTargetResolutionDispositionV1::Error
        );

        let ambiguous = resolve_goal_closure_target(&intent, &[exact.clone(), exact]);
        assert_eq!(
            ambiguous.disposition,
            GoalClosureTargetResolutionDispositionV1::Ambiguous
        );
        let not_applicable = resolve_goal_closure_target(&intent, &[]);
        assert_eq!(
            not_applicable.disposition,
            GoalClosureTargetResolutionDispositionV1::NotApplicable
        );
    }

    #[test]
    fn goal_history_close_rejects_changed_projection_checksum() {
        let intent = intent("reason");
        let mut changed = candidate(&intent);
        changed.projection_checksum = "projection-v2".to_string();
        let resolution = resolve_goal_closure_target(&intent, &[changed]);
        assert_eq!(
            resolution.disposition,
            GoalClosureTargetResolutionDispositionV1::Error
        );
        assert!(resolution.reason.contains("checksum changed"));
    }

    #[test]
    fn goal_history_close_id_binds_all_required_authority_and_effect_axes() {
        let first = intent("reason");
        let same = intent("reason");
        assert_eq!(first.closure_id, same.closure_id);
        let changed_ids = [
            rebuilt_id(&first, |authority| {
                authority.lane_digest = "lane-2".to_string()
            }),
            rebuilt_id(&first, |authority| {
                authority.concrete_session_key = "session-2".to_string()
            }),
            rebuilt_id(&first, |authority| {
                authority.virtual_session_id = "virtual-2".to_string()
            }),
            rebuilt_id(&first, |authority| {
                authority.backend_context_generation = "backend-2".to_string()
            }),
            rebuilt_id(&first, |authority| {
                authority.source_thread_id = "thread-2".to_string()
            }),
            GoalClosureIntentV1::new(
                first.trigger,
                GoalClosureDispositionV1::Canceled,
                first.authority.clone(),
                first.goal_identity.clone(),
                first.goal_generation.clone(),
                first.expected_projection_checksum.clone(),
                first.caller_effect_identity.clone(),
                first.reason.clone(),
            )
            .unwrap()
            .closure_id,
            GoalClosureIntentV1::new(
                first.trigger,
                first.disposition,
                first.authority.clone(),
                "goal-2",
                first.goal_generation.clone(),
                first.expected_projection_checksum.clone(),
                first.caller_effect_identity.clone(),
                first.reason.clone(),
            )
            .unwrap()
            .closure_id,
            GoalClosureIntentV1::new(
                first.trigger,
                first.disposition,
                first.authority.clone(),
                first.goal_identity.clone(),
                "goal-generation-2",
                first.expected_projection_checksum.clone(),
                first.caller_effect_identity.clone(),
                first.reason.clone(),
            )
            .unwrap()
            .closure_id,
            GoalClosureIntentV1::new(
                first.trigger,
                first.disposition,
                first.authority.clone(),
                first.goal_identity.clone(),
                first.goal_generation.clone(),
                first.expected_projection_checksum.clone(),
                "other-effect",
                first.reason.clone(),
            )
            .unwrap()
            .closure_id,
        ];
        assert!(changed_ids.iter().all(|id| id != &first.closure_id));
    }

    #[test]
    fn goal_history_close_phase_order_is_fail_closed() {
        let root = temp_root("phase-order");
        let intent = intent("reason");
        record_goal_closure_intent(&root, &intent).unwrap();
        let error = record_goal_closure_phase(
            &root,
            &intent,
            GoalClosurePhaseV1::BackendResultRecorded,
            phase_input(GoalClosureResultV1::Succeeded, None, None, 1),
        )
        .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);

        record_goal_closure_phase(
            &root,
            &intent,
            GoalClosurePhaseV1::IntentRecorded,
            phase_input(GoalClosureResultV1::Pending, None, None, 2),
        )
        .unwrap();
        record_goal_closure_phase(
            &root,
            &intent,
            GoalClosurePhaseV1::BackendResultRecorded,
            phase_input(GoalClosureResultV1::Succeeded, None, None, 3),
        )
        .unwrap();
        let error = record_goal_closure_phase(
            &root,
            &intent,
            GoalClosurePhaseV1::LineageReconciled,
            phase_input(GoalClosureResultV1::Succeeded, None, Some("lineage"), 4),
        )
        .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn goal_history_close_replay_is_exactly_once_and_changed_checksum_is_rejected() {
        let root = temp_root("replay");
        let closure_intent = intent("reason-a");
        let first = record_goal_closure_intent(&root, &closure_intent).unwrap();
        let replay = record_goal_closure_intent(&root, &closure_intent).unwrap();
        assert_eq!(first.disposition, GoalClosureAppendDispositionV1::Appended);
        assert_eq!(replay.disposition, GoalClosureAppendDispositionV1::Replayed);
        assert_eq!(
            fs::read_to_string(goal_closure_intents_file(&root))
                .unwrap()
                .lines()
                .count(),
            1
        );

        let changed = intent("reason-b");
        assert_eq!(changed.closure_id, closure_intent.closure_id);
        let error = record_goal_closure_intent(&root, &changed).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);

        let phase = phase_input(GoalClosureResultV1::Pending, None, None, 10);
        let first = record_goal_closure_phase(
            &root,
            &closure_intent,
            GoalClosurePhaseV1::IntentRecorded,
            phase.clone(),
        )
        .unwrap();
        let replay = record_goal_closure_phase(
            &root,
            &closure_intent,
            GoalClosurePhaseV1::IntentRecorded,
            GoalClosurePhaseInputV1 {
                recorded_at_ms: 99,
                ..phase
            },
        )
        .unwrap();
        assert_eq!(first.disposition, GoalClosureAppendDispositionV1::Appended);
        assert_eq!(replay.disposition, GoalClosureAppendDispositionV1::Replayed);
        assert_eq!(first.receipt.recorded_at_ms, replay.receipt.recorded_at_ms);
        assert_eq!(
            fs::read_to_string(goal_closure_receipts_file(&root))
                .unwrap()
                .lines()
                .count(),
            1
        );
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn terminal_goal_generation_reuses_original_closure_across_caller_effects() {
        let root = temp_root("terminal-generation-convergence");
        let original = intent("first command");
        record_goal_closure_intent(&root, &original).unwrap();
        record_goal_closure_phase(
            &root,
            &original,
            GoalClosurePhaseV1::IntentRecorded,
            phase_input(GoalClosureResultV1::Pending, None, None, 1),
        )
        .unwrap();
        record_goal_closure_phase(
            &root,
            &original,
            GoalClosurePhaseV1::BackendResultRecorded,
            phase_input(GoalClosureResultV1::Succeeded, None, None, 2),
        )
        .unwrap();
        record_goal_closure_phase(
            &root,
            &original,
            GoalClosurePhaseV1::TerminalProjectionRecorded,
            phase_input(
                GoalClosureResultV1::Succeeded,
                Some("projection-v2"),
                None,
                3,
            ),
        )
        .unwrap();
        record_goal_closure_phase(
            &root,
            &original,
            GoalClosurePhaseV1::LineageReconciled,
            phase_input(GoalClosureResultV1::Succeeded, None, Some("lineage-v2"), 4),
        )
        .unwrap();
        record_goal_closure_phase(
            &root,
            &original,
            GoalClosurePhaseV1::Completed,
            phase_input(GoalClosureResultV1::Succeeded, None, Some("lineage-v2"), 5),
        )
        .unwrap();

        let repeated = GoalClosureIntentV1::new(
            GoalClosureTriggerV1::ChannelStop,
            GoalClosureDispositionV1::Canceled,
            original.authority.clone(),
            original.goal_identity.clone(),
            original.goal_generation.clone(),
            original.expected_projection_checksum.clone(),
            "second-command-effect",
            "duplicate command",
        )
        .unwrap();
        assert_ne!(repeated.closure_id, original.closure_id);
        let replay = record_goal_closure_intent(&root, &repeated).unwrap();
        assert_eq!(replay.disposition, GoalClosureAppendDispositionV1::Replayed);
        assert_eq!(replay.intent.closure_id, original.closure_id);
        assert_eq!(
            fs::read_to_string(goal_closure_intents_file(&root))
                .unwrap()
                .lines()
                .count(),
            1
        );
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn historical_goal_close_receipt_does_not_expose_raw_session_or_thread_authority() {
        let root = temp_root("receipt-hygiene");
        let intent = intent("operator private reason");
        record_goal_closure_intent(&root, &intent).unwrap();
        let receipt = record_goal_closure_phase(
            &root,
            &intent,
            GoalClosurePhaseV1::IntentRecorded,
            phase_input(GoalClosureResultV1::Pending, None, None, 1),
        )
        .unwrap()
        .receipt;
        let json = serde_json::to_string(&receipt).unwrap();
        assert!(!json.contains("session-secret"));
        assert!(!json.contains("thread-secret"));
        assert!(!json.contains("operator private reason"));
        assert!(json.contains(&intent.authority_digest));
        let _ = fs::remove_dir_all(root);
    }

    fn intent(reason: &str) -> GoalClosureIntentV1 {
        GoalClosureIntentV1::new(
            GoalClosureTriggerV1::OperatorHistorical,
            GoalClosureDispositionV1::Completed,
            GoalClosureAuthorityV1 {
                lane_digest: "lane-digest".to_string(),
                concrete_session_key: "session-secret".to_string(),
                virtual_session_id: "virtual-session".to_string(),
                backend_context_generation: "backend-generation".to_string(),
                source_thread_id: "thread-secret".to_string(),
            },
            "goal-id",
            "goal-generation",
            Some("projection-v1".to_string()),
            "operator-effect-1",
            reason,
        )
        .unwrap()
    }

    fn candidate(intent: &GoalClosureIntentV1) -> GoalClosureTargetCandidateV1 {
        GoalClosureTargetCandidateV1 {
            authority: intent.authority.clone(),
            goal_identity: intent.goal_identity.clone(),
            goal_generation: intent.goal_generation.clone(),
            projection_checksum: intent.expected_projection_checksum.clone().unwrap(),
            active: true,
            original_binding: true,
            latest_authoritative_projection: true,
            latest_authoritative_lineage: true,
        }
    }

    fn rebuilt_id(
        intent: &GoalClosureIntentV1,
        mutate: impl FnOnce(&mut GoalClosureAuthorityV1),
    ) -> String {
        let mut authority = intent.authority.clone();
        mutate(&mut authority);
        GoalClosureIntentV1::new(
            intent.trigger,
            intent.disposition,
            authority,
            intent.goal_identity.clone(),
            intent.goal_generation.clone(),
            intent.expected_projection_checksum.clone(),
            intent.caller_effect_identity.clone(),
            intent.reason.clone(),
        )
        .unwrap()
        .closure_id
    }

    fn phase_input(
        result: GoalClosureResultV1,
        projection_checksum: Option<&str>,
        lineage_checksum: Option<&str>,
        recorded_at_ms: i64,
    ) -> GoalClosurePhaseInputV1 {
        GoalClosurePhaseInputV1 {
            result,
            projection_checksum: projection_checksum.map(str::to_string),
            lineage_checksum: lineage_checksum.map(str::to_string),
            result_evidence_digest: None,
            recorded_at_ms,
        }
    }

    fn temp_root(name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("agent-harness-goal-closure-{name}-{nanos}"))
    }
}
