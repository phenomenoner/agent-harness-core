use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use ring::digest;
use serde::Serialize;

use crate::lane::FullLaneKeyV1;
use crate::{
    PromptBundle, PromptSectionKind, SKILL_DELIVERY_SCHEMA, SkillContractError, SkillDeliveryMode,
    SkillDeliveryReasonV2, SkillDeliveryReceiptV2, SkillEcosystemIdentity, SkillRoutingReceiptV2,
    VIRTUAL_SKILL_MANIFEST_SCHEMA, VirtualSkillDeliveryLedgerEntryV1, VirtualSkillManifestEntryV1,
    VirtualSkillManifestStatus, VirtualSkillManifestV1, VirtualSkillState, write_json_atomic,
};

const VIRTUAL_SKILL_RUNTIME_HASH_DOMAIN: &[u8] = b"agent-harness/virtual-skill-runtime/v1";

pub struct VirtualSkillRuntimeObserveOptions<'a> {
    pub harness_home: &'a Path,
    pub full_lane: &'a FullLaneKeyV1,
    pub virtual_session_id: &'a str,
    pub backend_generation: &'a str,
    pub queue_id: &'a str,
    pub routing_receipt: &'a SkillRoutingReceiptV2,
    pub prompt_bundle: &'a PromptBundle,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct VirtualSkillRuntimeObserveReport {
    pub manifest_file: PathBuf,
    pub manifest_created: bool,
    pub rollover_applied: bool,
    pub delivery_receipt_files: Vec<PathBuf>,
    pub delivery_receipts: Vec<SkillDeliveryReceiptV2>,
}

pub fn virtual_skill_manifest_dir(harness_home: impl AsRef<Path>) -> PathBuf {
    harness_home
        .as_ref()
        .join("state")
        .join("skills")
        .join("virtual-manifests")
}

pub fn virtual_skill_manifest_file(
    harness_home: impl AsRef<Path>,
    virtual_session_id: &str,
) -> PathBuf {
    virtual_skill_manifest_dir(harness_home)
        .join(format!("{}.json", safe_component(virtual_session_id)))
}

pub fn create_virtual_skill_manifest(
    harness_home: impl AsRef<Path>,
    identity: SkillEcosystemIdentity,
    task_intent_ref: Option<String>,
    catalog_revision: String,
    topology_revision: String,
) -> io::Result<VirtualSkillManifestV1> {
    let manifest = VirtualSkillManifestV1 {
        schema: VIRTUAL_SKILL_MANIFEST_SCHEMA.to_string(),
        identity,
        task_intent_ref,
        catalog_revision,
        topology_revision,
        skills: Vec::new(),
        deliveries: Vec::new(),
        rollover_count: 0,
        status: VirtualSkillManifestStatus::Active,
        close_reason: None,
    };
    validate_io(&manifest)?;
    persist_manifest(harness_home, &manifest)?;
    Ok(manifest)
}

pub fn skill_delivery_receipt_dir(
    harness_home: impl AsRef<Path>,
    virtual_session_id: &str,
) -> PathBuf {
    harness_home
        .as_ref()
        .join("state")
        .join("skills")
        .join("delivery-receipts")
        .join(safe_component(virtual_session_id))
}

pub fn virtual_skill_manifest_observation_enabled(
    harness_home: impl AsRef<Path>,
) -> io::Result<bool> {
    let harness_home = harness_home.as_ref();
    let Some(config_file) = crate::config::harness_config_candidates(harness_home)
        .into_iter()
        .find(|path| path.is_file())
    else {
        return Ok(false);
    };
    let value: serde_json::Value =
        serde_json::from_slice(&fs::read(config_file)?).map_err(io::Error::other)?;
    Ok(value
        .get("skills")
        .and_then(|skills| skills.get("virtualManifest"))
        .and_then(|manifest| manifest.get("observeEnabled"))
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false))
}

/// Observe the serving v4 prompt without changing its selections or bytes.
/// The shadow routing receipt supplies exact-lane identity and frozen catalog
/// revisions; the already-built prompt bundle supplies what was actually sent.
pub fn observe_virtual_skill_runtime(
    options: VirtualSkillRuntimeObserveOptions<'_>,
) -> io::Result<VirtualSkillRuntimeObserveReport> {
    options.full_lane.validate().map_err(io::Error::other)?;
    options
        .routing_receipt
        .validate()
        .map_err(io::Error::other)?;
    if options.backend_generation.trim().is_empty() || options.queue_id.trim().is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "virtual skill observation requires backend generation and queue id",
        ));
    }
    let expected_lane_digest = options
        .full_lane
        .virtual_identity_hash()
        .map_err(io::Error::other)?;
    let route_identity = &options.routing_receipt.identity;
    if route_identity.virtual_session_id != options.virtual_session_id
        || route_identity.exact_lane_digest != expected_lane_digest
        || route_identity.root_session_key_hash
            != sha256_hex(options.full_lane.root_virtual_session().as_bytes())
        || route_identity.concrete_session_hash
            != sha256_hex(options.full_lane.concrete_session().as_bytes())
        || !route_identity
            .agent_id
            .eq_ignore_ascii_case(options.full_lane.agent_id())
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "virtual skill observation identity differs from exact runtime lane",
        ));
    }
    let selected_ids = options
        .prompt_bundle
        .selected_skills
        .iter()
        .map(|skill| skill.skill_id.as_str())
        .collect::<Vec<_>>();
    let active_ids = options
        .routing_receipt
        .active_serving_skill_ids
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    if selected_ids != active_ids {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "shadow receipt active serving selection differs from prompt bundle",
        ));
    }

    let existing = load_virtual_skill_manifest(options.harness_home, options.virtual_session_id)?;
    let manifest_created = existing.is_none();
    let mut manifest = match existing {
        Some(manifest) => manifest,
        None => create_virtual_skill_manifest(
            options.harness_home,
            route_identity.clone(),
            Some(format!("turn-sha256:{}", options.routing_receipt.turn_hash)),
            options.routing_receipt.catalog_revision.clone(),
            options.routing_receipt.topology_revision.clone(),
        )?,
    };
    if !manifest.identity.same_virtual_lane(route_identity) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "virtual skill manifest belongs to another lane",
        ));
    }
    if manifest.catalog_revision != options.routing_receipt.catalog_revision
        || manifest.topology_revision != options.routing_receipt.topology_revision
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "active virtual skill manifest freezes a different catalog/topology revision",
        ));
    }
    let rollover_applied =
        manifest.identity.concrete_session_hash != route_identity.concrete_session_hash;
    if rollover_applied {
        rollover_virtual_skill_manifest(
            &mut manifest,
            route_identity.concrete_session_hash.clone(),
        )
        .map_err(io::Error::other)?;
    }

    let mut delivery_receipts = Vec::new();
    let mut delivery_receipt_files = Vec::new();
    for skill in &options.prompt_bundle.selected_skills {
        let prompt_section = options.prompt_bundle.sections.iter().find(|section| {
            section.kind == PromptSectionKind::Skill
                && section
                    .skill_id
                    .as_deref()
                    .is_some_and(|id| id.eq_ignore_ascii_case(&skill.skill_id))
        });
        let state = if prompt_section.is_some() {
            VirtualSkillState::Active
        } else {
            VirtualSkillState::Candidate
        };
        register_manifest_skill(
            &mut manifest,
            skill.skill_id.clone(),
            skill.body_checksum.clone(),
            state,
            if skill.delivery_mode == SkillDeliveryMode::InvocationEnvelope {
                "explicit-serving-selection"
            } else {
                "serving-selection"
            },
        )
        .map_err(io::Error::other)?;

        if let Some((receipt_file, receipt)) = find_existing_delivery_for_observation(
            options.harness_home,
            options.queue_id,
            options.virtual_session_id,
            options.backend_generation,
            &skill.skill_id,
            &skill.body_checksum,
        )? {
            if receipt.identity != *route_identity
                || receipt.backend_generation != options.backend_generation
                || !receipt.skill_id.eq_ignore_ascii_case(&skill.skill_id)
                || receipt.skill_revision != skill.body_checksum
            {
                return Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    "existing delivery observation belongs to different exact evidence",
                ));
            }
            record_manifest_delivery_receipt(&mut manifest, &receipt).map_err(io::Error::other)?;
            delivery_receipts.push(receipt);
            delivery_receipt_files.push(receipt_file);
            continue;
        }

        let prior_delivery = manifest
            .deliveries
            .iter()
            .rev()
            .find(|delivery| delivery.skill_id.eq_ignore_ascii_case(&skill.skill_id));
        let first_delivery_exists = manifest
            .skills
            .iter()
            .find(|entry| entry.skill_id.eq_ignore_ascii_case(&skill.skill_id))
            .and_then(|entry| entry.first_delivery_id.as_ref())
            .is_some();
        let delivery_reason = match prompt_section {
            None => SkillDeliveryReasonV2::None,
            Some(_) if !first_delivery_exists => {
                if skill.delivery_mode == SkillDeliveryMode::InvocationEnvelope {
                    SkillDeliveryReasonV2::Explicit
                } else {
                    SkillDeliveryReasonV2::FirstLoad
                }
            }
            Some(_)
                if prior_delivery.is_some_and(|prior| {
                    prior.backend_generation != options.backend_generation
                        || prior.concrete_session_hash != route_identity.concrete_session_hash
                }) =>
            {
                SkillDeliveryReasonV2::Rehydration
            }
            Some(_) => SkillDeliveryReasonV2::Reference,
        };
        let included_bytes = prompt_section
            .map(|section| section.bytes_included)
            .unwrap_or(0);
        let reused_bytes = if delivery_reason == SkillDeliveryReasonV2::None
            && first_delivery_exists
            && matches!(
                skill.delivery_mode,
                SkillDeliveryMode::InjectedBody | SkillDeliveryMode::InvocationEnvelope
            ) {
            fs::metadata(skill.directory.join(crate::SKILL_FILE_NAME))
                .ok()
                .and_then(|metadata| usize::try_from(metadata.len()).ok())
                .unwrap_or(0)
        } else {
            0
        };
        let delivery_id = delivery_receipt_id(
            options.queue_id,
            options.virtual_session_id,
            options.backend_generation,
            &skill.skill_id,
            &skill.body_checksum,
            delivery_reason,
        );
        let receipt = SkillDeliveryReceiptV2 {
            schema: SKILL_DELIVERY_SCHEMA.to_string(),
            receipt_id: delivery_id,
            routing_receipt_id: (delivery_reason != SkillDeliveryReasonV2::Rehydration)
                .then(|| options.routing_receipt.receipt_id.clone()),
            identity: route_identity.clone(),
            backend_generation: options.backend_generation.to_string(),
            skill_id: skill.skill_id.clone(),
            skill_revision: skill.body_checksum.clone(),
            body_checksum: skill.body_checksum.clone(),
            delivery_kind: match prompt_section {
                Some(section) => section
                    .delivery_mode
                    .unwrap_or(skill.delivery_mode)
                    .as_str()
                    .to_string(),
                None => "none".to_string(),
            },
            delivery_reason,
            included_bytes,
            reused_bytes,
            cache_revision: format!(
                "{}:{}:{}",
                manifest.catalog_revision, manifest.topology_revision, options.backend_generation
            ),
        };
        receipt.validate().map_err(io::Error::other)?;
        record_manifest_delivery_receipt(&mut manifest, &receipt).map_err(io::Error::other)?;
        let receipt_file = write_delivery_receipt_once(options.harness_home, &receipt)?;
        delivery_receipts.push(receipt);
        delivery_receipt_files.push(receipt_file);
    }
    let manifest_file = persist_manifest(options.harness_home, &manifest)?;
    Ok(VirtualSkillRuntimeObserveReport {
        manifest_file,
        manifest_created,
        rollover_applied,
        delivery_receipt_files,
        delivery_receipts,
    })
}

pub fn register_manifest_skill(
    manifest: &mut VirtualSkillManifestV1,
    skill_id: impl Into<String>,
    revision: impl Into<String>,
    state: VirtualSkillState,
    reason: impl Into<String>,
) -> Result<(), SkillContractError> {
    ensure_active(manifest)?;
    let skill_id = skill_id.into();
    let revision = revision.into();
    let reason = reason.into();
    if let Some(existing) = manifest
        .skills
        .iter_mut()
        .find(|entry| entry.skill_id.eq_ignore_ascii_case(&skill_id))
    {
        if existing.revision != revision {
            return Err(SkillContractError::InvalidField(format!(
                "active virtual task freezes {} at revision {}",
                existing.skill_id, existing.revision
            )));
        }
        if matches!(state, VirtualSkillState::Viewed | VirtualSkillState::Active)
            && existing.state == VirtualSkillState::Candidate
        {
            existing.state = state;
        }
        return manifest.validate();
    }
    manifest.skills.push(VirtualSkillManifestEntryV1 {
        skill_id,
        revision,
        state,
        first_delivery_id: None,
        reason,
    });
    manifest
        .skills
        .sort_by(|left, right| left.skill_id.cmp(&right.skill_id));
    manifest.validate()
}

pub fn load_virtual_skill_manifest(
    harness_home: impl AsRef<Path>,
    virtual_session_id: &str,
) -> io::Result<Option<VirtualSkillManifestV1>> {
    let path = virtual_skill_manifest_file(harness_home, virtual_session_id);
    let text = match fs::read_to_string(path) {
        Ok(text) => text,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    let manifest: VirtualSkillManifestV1 = serde_json::from_str(&text).map_err(io::Error::other)?;
    validate_io(&manifest)?;
    Ok(Some(manifest))
}

pub fn activate_manifest_skill(
    manifest: &mut VirtualSkillManifestV1,
    skill_id: impl Into<String>,
    revision: impl Into<String>,
    reason: impl Into<String>,
) -> Result<(), SkillContractError> {
    register_manifest_skill(
        manifest,
        skill_id,
        revision,
        VirtualSkillState::Active,
        reason,
    )
}

pub fn record_manifest_delivery_receipt(
    manifest: &mut VirtualSkillManifestV1,
    receipt: &SkillDeliveryReceiptV2,
) -> Result<bool, SkillContractError> {
    ensure_active(manifest)?;
    receipt.validate()?;
    if !manifest.identity.same_virtual_lane(&receipt.identity) {
        return Err(SkillContractError::IdentityMismatch);
    }
    let Some(entry) = manifest
        .skills
        .iter_mut()
        .find(|entry| entry.skill_id.eq_ignore_ascii_case(&receipt.skill_id))
    else {
        return Err(SkillContractError::InvalidField(format!(
            "delivery references non-manifest skill {}",
            receipt.skill_id
        )));
    };
    if entry.revision != receipt.skill_revision || receipt.body_checksum != receipt.skill_revision {
        return Err(SkillContractError::InvalidField(format!(
            "delivery revision differs from frozen manifest revision for {}",
            receipt.skill_id
        )));
    }
    let ledger_entry = VirtualSkillDeliveryLedgerEntryV1 {
        delivery_id: receipt.receipt_id.clone(),
        routing_receipt_id: receipt.routing_receipt_id.clone(),
        skill_id: receipt.skill_id.clone(),
        revision: receipt.skill_revision.clone(),
        backend_generation: receipt.backend_generation.clone(),
        concrete_session_hash: receipt.identity.concrete_session_hash.clone(),
        reason: receipt.delivery_reason,
        included_bytes: receipt.included_bytes,
        reused_bytes: receipt.reused_bytes,
    };
    if let Some(existing) = manifest
        .deliveries
        .iter()
        .find(|delivery| delivery.delivery_id == receipt.receipt_id)
    {
        if existing == &ledger_entry {
            return Ok(false);
        }
        return Err(SkillContractError::InvalidField(format!(
            "delivery id {} collides with different evidence",
            receipt.receipt_id
        )));
    }
    match receipt.delivery_reason {
        SkillDeliveryReasonV2::FirstLoad | SkillDeliveryReasonV2::Explicit => {
            if entry.first_delivery_id.is_some() {
                return Err(SkillContractError::InvalidField(format!(
                    "{} already has a first delivery",
                    entry.skill_id
                )));
            }
            entry.first_delivery_id = Some(receipt.receipt_id.clone());
            entry.state = VirtualSkillState::Active;
        }
        SkillDeliveryReasonV2::Rehydration => {
            if entry.first_delivery_id.is_none() {
                return Err(SkillContractError::InvalidField(
                    "rehydration requires a prior first delivery".to_string(),
                ));
            }
            entry.state = VirtualSkillState::Active;
        }
        SkillDeliveryReasonV2::Reference => {
            if receipt.included_bytes > 0 && entry.state == VirtualSkillState::Candidate {
                entry.state = VirtualSkillState::Viewed;
            }
        }
        SkillDeliveryReasonV2::None => {}
    }
    manifest.deliveries.push(ledger_entry);
    manifest.validate()?;
    Ok(true)
}

pub fn record_manifest_delivery(
    manifest: &mut VirtualSkillManifestV1,
    skill_id: &str,
    delivery_id: &str,
    reason: SkillDeliveryReasonV2,
) -> Result<bool, SkillContractError> {
    ensure_active(manifest)?;
    let Some(entry) = manifest
        .skills
        .iter_mut()
        .find(|entry| entry.skill_id.eq_ignore_ascii_case(skill_id))
    else {
        return Err(SkillContractError::InvalidField(format!(
            "delivery references non-manifest skill {skill_id}"
        )));
    };
    match reason {
        SkillDeliveryReasonV2::FirstLoad | SkillDeliveryReasonV2::Explicit => {
            if entry.first_delivery_id.is_none() {
                entry.first_delivery_id = Some(delivery_id.to_string());
                entry.state = VirtualSkillState::Active;
                Ok(true)
            } else {
                Err(SkillContractError::InvalidField(format!(
                    "{} already has a first delivery",
                    entry.skill_id
                )))
            }
        }
        SkillDeliveryReasonV2::Rehydration => {
            if entry.first_delivery_id.is_none() {
                return Err(SkillContractError::InvalidField(
                    "rehydration requires a prior first delivery".to_string(),
                ));
            }
            Ok(false)
        }
        SkillDeliveryReasonV2::Reference => Ok(false),
        SkillDeliveryReasonV2::None => Err(SkillContractError::InvalidField(
            "none is not a persisted delivery".to_string(),
        )),
    }
}

pub fn rollover_virtual_skill_manifest(
    manifest: &mut VirtualSkillManifestV1,
    next_concrete_session_hash: String,
) -> Result<Vec<(String, String)>, SkillContractError> {
    ensure_active(manifest)?;
    if next_concrete_session_hash == manifest.identity.concrete_session_hash {
        return Err(SkillContractError::InvalidField(
            "rollover requires a new concrete session".to_string(),
        ));
    }
    manifest.identity.concrete_session_hash = next_concrete_session_hash;
    manifest.rollover_count = manifest.rollover_count.saturating_add(1);
    manifest.validate()?;
    Ok(manifest
        .skills
        .iter()
        .filter(|entry| {
            matches!(
                entry.state,
                VirtualSkillState::Viewed | VirtualSkillState::Active
            )
        })
        .map(|entry| (entry.skill_id.clone(), entry.revision.clone()))
        .collect())
}

pub fn close_virtual_skill_manifest(
    manifest: &mut VirtualSkillManifestV1,
    status: VirtualSkillManifestStatus,
    reason: impl Into<String>,
) -> Result<(), SkillContractError> {
    if status == VirtualSkillManifestStatus::Active {
        return Err(SkillContractError::InvalidField(
            "close status cannot be active".to_string(),
        ));
    }
    ensure_active(manifest)?;
    manifest.status = status;
    manifest.close_reason = Some(reason.into());
    manifest.validate()
}

pub fn persist_manifest(
    harness_home: impl AsRef<Path>,
    manifest: &VirtualSkillManifestV1,
) -> io::Result<PathBuf> {
    validate_io(manifest)?;
    let path = virtual_skill_manifest_file(harness_home, &manifest.identity.virtual_session_id);
    write_json_atomic(&path, manifest)?;
    Ok(path)
}

fn ensure_active(manifest: &VirtualSkillManifestV1) -> Result<(), SkillContractError> {
    if manifest.status != VirtualSkillManifestStatus::Active {
        return Err(SkillContractError::InvalidField(
            "virtual skill manifest is terminal".to_string(),
        ));
    }
    Ok(())
}

fn validate_io(manifest: &VirtualSkillManifestV1) -> io::Result<()> {
    manifest.validate().map_err(io::Error::other)
}

fn safe_component(value: &str) -> String {
    let sanitized = value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_') {
                ch
            } else {
                '_'
            }
        })
        .take(160)
        .collect::<String>();
    if sanitized.is_empty() {
        "unknown".to_string()
    } else {
        sanitized
    }
}

fn write_delivery_receipt_once(
    harness_home: &Path,
    receipt: &SkillDeliveryReceiptV2,
) -> io::Result<PathBuf> {
    let dir = skill_delivery_receipt_dir(harness_home, &receipt.identity.virtual_session_id);
    fs::create_dir_all(&dir)?;
    let path = dir.join(format!("{}.json", safe_component(&receipt.receipt_id)));
    if path.is_file() {
        let existing: SkillDeliveryReceiptV2 =
            serde_json::from_slice(&fs::read(&path)?).map_err(io::Error::other)?;
        if existing != *receipt {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "delivery receipt id collides with different evidence",
            ));
        }
        return Ok(path);
    }
    write_json_atomic(&path, receipt)?;
    Ok(path)
}

fn find_existing_delivery_for_observation(
    harness_home: &Path,
    queue_id: &str,
    virtual_session_id: &str,
    backend_generation: &str,
    skill_id: &str,
    skill_revision: &str,
) -> io::Result<Option<(PathBuf, SkillDeliveryReceiptV2)>> {
    let dir = skill_delivery_receipt_dir(harness_home, virtual_session_id);
    let mut found = None;
    for reason in [
        SkillDeliveryReasonV2::FirstLoad,
        SkillDeliveryReasonV2::Explicit,
        SkillDeliveryReasonV2::Rehydration,
        SkillDeliveryReasonV2::Reference,
        SkillDeliveryReasonV2::None,
    ] {
        let receipt_id = delivery_receipt_id(
            queue_id,
            virtual_session_id,
            backend_generation,
            skill_id,
            skill_revision,
            reason,
        );
        let path = dir.join(format!("{}.json", safe_component(&receipt_id)));
        if !path.is_file() {
            continue;
        }
        let receipt: SkillDeliveryReceiptV2 =
            serde_json::from_slice(&fs::read(&path)?).map_err(io::Error::other)?;
        receipt.validate().map_err(io::Error::other)?;
        if found.is_some() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "one delivery observation resolved to multiple reason receipts",
            ));
        }
        found = Some((path, receipt));
    }
    Ok(found)
}

fn delivery_receipt_id(
    queue_id: &str,
    virtual_session_id: &str,
    backend_generation: &str,
    skill_id: &str,
    skill_revision: &str,
    reason: SkillDeliveryReasonV2,
) -> String {
    format!(
        "skill-delivery-{}",
        hash_components(&[
            queue_id.as_bytes(),
            virtual_session_id.as_bytes(),
            backend_generation.as_bytes(),
            skill_id.as_bytes(),
            skill_revision.as_bytes(),
            delivery_reason_label(reason).as_bytes(),
        ])
    )
}

fn delivery_reason_label(reason: SkillDeliveryReasonV2) -> &'static str {
    match reason {
        SkillDeliveryReasonV2::FirstLoad => "first-load",
        SkillDeliveryReasonV2::Rehydration => "rehydration",
        SkillDeliveryReasonV2::Explicit => "explicit",
        SkillDeliveryReasonV2::Reference => "reference",
        SkillDeliveryReasonV2::None => "none",
    }
}

fn hash_components(components: &[&[u8]]) -> String {
    let mut context = digest::Context::new(&digest::SHA256);
    context.update(VIRTUAL_SKILL_RUNTIME_HASH_DOMAIN);
    for component in components {
        context.update(&(component.len() as u64).to_be_bytes());
        context.update(component);
    }
    let bytes = context.finish();
    let mut output = String::with_capacity(bytes.as_ref().len() * 2);
    for byte in bytes.as_ref() {
        use std::fmt::Write as _;
        write!(&mut output, "{byte:02x}").expect("writing to String cannot fail");
    }
    output
}

fn sha256_hex(value: &[u8]) -> String {
    let bytes = digest::digest(&digest::SHA256, value);
    let mut output = String::with_capacity(bytes.as_ref().len() * 2);
    for byte in bytes.as_ref() {
        use std::fmt::Write as _;
        write!(&mut output, "{byte:02x}").expect("writing to String cannot fail");
    }
    output
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::*;
    use crate::{
        AgentPromptManifestV1, PromptBundleSummary, PromptSection, PromptSectionTier,
        SkillRoutingCandidateV2, SkillSelection, SkillSourceKind, TurnDispatch,
        TurnProviderRequestPolicy,
    };

    fn temp_root(name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        std::env::temp_dir().join(format!("virtual-skill-manifest-{name}-{nanos}"))
    }

    fn identity(virtual_session_id: &str, concrete: &str) -> SkillEcosystemIdentity {
        SkillEcosystemIdentity {
            virtual_session_id: virtual_session_id.to_string(),
            root_session_key_hash: "abcdef123456".to_string(),
            concrete_session_hash: concrete.to_string(),
            exact_lane_digest: "feedface1234".to_string(),
            agent_id: "main".to_string(),
        }
    }

    fn lane(root: &str, concrete: &str) -> FullLaneKeyV1 {
        FullLaneKeyV1::new(
            "discord",
            "default",
            "channel",
            "user",
            "main",
            "interactive",
            root,
            concrete,
        )
        .unwrap()
    }

    fn routing_receipt(
        lane: &FullLaneKeyV1,
        virtual_session_id: &str,
        receipt_id: &str,
        turn_seed: &str,
        active_serving_skill_ids: Vec<String>,
    ) -> SkillRoutingReceiptV2 {
        SkillRoutingReceiptV2 {
            schema: crate::SKILL_ROUTING_SCHEMA.to_string(),
            receipt_id: receipt_id.to_string(),
            turn_hash: sha256_hex(turn_seed.as_bytes()),
            identity: SkillEcosystemIdentity {
                virtual_session_id: virtual_session_id.to_string(),
                root_session_key_hash: sha256_hex(lane.root_virtual_session().as_bytes()),
                concrete_session_hash: sha256_hex(lane.concrete_session().as_bytes()),
                exact_lane_digest: lane.virtual_identity_hash().unwrap(),
                agent_id: lane.agent_id().to_string(),
            },
            channel: lane.platform().to_string(),
            catalog_revision: "catalog-1".to_string(),
            topology_revision: "topology-1".to_string(),
            method: "router-v2".to_string(),
            method_version: "2".to_string(),
            task_text_bytes: 10,
            virtual_task_intent_bytes: 0,
            ambient_notes_excluded_bytes: 0,
            candidates: Vec::<SkillRoutingCandidateV2>::new(),
            selected_count: 0,
            active_serving_skill_ids,
            shadow_selected_skill_ids: Vec::new(),
            abstention_reason: Some("shadow-test".to_string()),
            shadow: true,
            duration_ms: 1,
        }
    }

    fn prompt_bundle(root: &Path, include_skill: bool) -> PromptBundle {
        let skill_dir = root.join("demo");
        fs::create_dir_all(&skill_dir).unwrap();
        let body = "# Demo\n\nUse the verified procedure.\n";
        fs::write(skill_dir.join(crate::SKILL_FILE_NAME), body).unwrap();
        let checksum = crate::skill_body_checksum(body);
        let selected_skills = vec![SkillSelection {
            skill_id: "workspace:demo".to_string(),
            original_id: "demo".to_string(),
            source_kind: SkillSourceKind::Workspace,
            title: "Demo".to_string(),
            description: Some("Demo procedure".to_string()),
            category: None,
            tags: Vec::new(),
            directory: skill_dir.clone(),
            score: 1,
            score_components: Vec::new(),
            reasons: vec!["test".to_string()],
            delivery_mode: SkillDeliveryMode::InjectedBody,
            user_instruction: None,
            body_checksum: checksum.clone(),
            selection_receipt_id: None,
        }];
        let sections = include_skill
            .then(|| PromptSection {
                kind: PromptSectionKind::Skill,
                tier: PromptSectionTier::TurnContext,
                title: "Demo (workspace:demo)".to_string(),
                path: Some(skill_dir.join(crate::SKILL_FILE_NAME)),
                bytes_original: body.len(),
                bytes_included: body.len(),
                truncated: false,
                skill_id: Some("workspace:demo".to_string()),
                body_checksum: Some(checksum),
                delivery_mode: Some(SkillDeliveryMode::InjectedBody),
                content: body.to_string(),
            })
            .into_iter()
            .collect();
        PromptBundle {
            schema: "agent-harness.prompt-bundle.v1",
            source_home: root.to_path_buf(),
            source_workspace: root.to_path_buf(),
            dispatch: TurnDispatch::AgentTurn,
            session_key: "root-session".to_string(),
            agent_id: Some("main".to_string()),
            provider: Some("openai".to_string()),
            model: Some("gpt-5".to_string()),
            provider_request_policy: TurnProviderRequestPolicy::default(),
            thinking_enabled: false,
            thinking_level: None,
            reasoning_preference: None,
            backend_reasoning_policy: None,
            static_config_revision: None,
            static_config: None,
            requires_fresh_backend_thread: false,
            prompt_manifest: AgentPromptManifestV1 {
                schema: "agent-harness.prompt-manifest.v1".to_string(),
                agent_id: Some("main".to_string()),
                lane_digest: None,
                backend_context_generation: None,
                static_config_revision: None,
                static_config: None,
                requires_fresh_backend_thread: false,
                entries: Vec::new(),
            },
            summary: PromptBundleSummary::default(),
            selected_skills,
            sections,
            warnings: Vec::new(),
        }
    }

    fn empty_prompt_bundle(root: &Path) -> PromptBundle {
        let mut bundle = prompt_bundle(root, false);
        bundle.selected_skills.clear();
        bundle
    }

    #[test]
    fn runtime_observer_rehydrates_across_concrete_rollover_without_new_route_link() {
        let root = temp_root("runtime-rollover");
        let harness_home = root.join("harness");
        let workspace = root.join("workspace");
        let first_lane = lane("root-1", "concrete-1");
        let first_route = routing_receipt(
            &first_lane,
            "vs-1",
            "route-1",
            "turn-1",
            vec!["workspace:demo".to_string()],
        );
        let first_bundle = prompt_bundle(&workspace, true);
        let first = observe_virtual_skill_runtime(VirtualSkillRuntimeObserveOptions {
            harness_home: &harness_home,
            full_lane: &first_lane,
            virtual_session_id: "vs-1",
            backend_generation: "backend-1",
            queue_id: "queue-1",
            routing_receipt: &first_route,
            prompt_bundle: &first_bundle,
        })
        .unwrap();
        assert_eq!(first.delivery_receipts.len(), 1);
        assert_eq!(
            first.delivery_receipts[0].delivery_reason,
            SkillDeliveryReasonV2::FirstLoad
        );
        let first_delivery_id = first.delivery_receipts[0].receipt_id.clone();

        let second_lane = lane("root-1", "concrete-2");
        let second_route = routing_receipt(
            &second_lane,
            "vs-1",
            "route-2",
            "turn-2",
            vec!["workspace:demo".to_string()],
        );
        let second_bundle = prompt_bundle(&workspace, true);
        let second = observe_virtual_skill_runtime(VirtualSkillRuntimeObserveOptions {
            harness_home: &harness_home,
            full_lane: &second_lane,
            virtual_session_id: "vs-1",
            backend_generation: "backend-2",
            queue_id: "queue-2",
            routing_receipt: &second_route,
            prompt_bundle: &second_bundle,
        })
        .unwrap();
        assert!(second.rollover_applied);
        assert_eq!(
            second.delivery_receipts[0].delivery_reason,
            SkillDeliveryReasonV2::Rehydration
        );
        assert!(second.delivery_receipts[0].routing_receipt_id.is_none());
        let manifest = load_virtual_skill_manifest(&harness_home, "vs-1")
            .unwrap()
            .unwrap();
        assert_eq!(manifest.rollover_count, 1);
        assert_eq!(
            manifest.skills[0].first_delivery_id.as_deref(),
            Some(first_delivery_id.as_str())
        );
        assert_eq!(manifest.deliveries.len(), 2);
        fs::remove_dir_all(root).ok();
    }

    #[test]
    fn runtime_observer_new_virtual_session_inherits_no_skill_state() {
        let root = temp_root("runtime-new");
        let harness_home = root.join("harness");
        let workspace = root.join("workspace");
        let prior_lane = lane("root-1", "concrete-1");
        let prior_route = routing_receipt(
            &prior_lane,
            "vs-old",
            "route-old",
            "turn-old",
            vec!["workspace:demo".to_string()],
        );
        let prior_bundle = prompt_bundle(&workspace, true);
        observe_virtual_skill_runtime(VirtualSkillRuntimeObserveOptions {
            harness_home: &harness_home,
            full_lane: &prior_lane,
            virtual_session_id: "vs-old",
            backend_generation: "backend-1",
            queue_id: "queue-old",
            routing_receipt: &prior_route,
            prompt_bundle: &prior_bundle,
        })
        .unwrap();

        let new_lane = lane("root-2", "concrete-2");
        let new_route = routing_receipt(&new_lane, "vs-new", "route-new", "turn-new", Vec::new());
        let new_bundle = empty_prompt_bundle(&workspace);
        observe_virtual_skill_runtime(VirtualSkillRuntimeObserveOptions {
            harness_home: &harness_home,
            full_lane: &new_lane,
            virtual_session_id: "vs-new",
            backend_generation: "backend-2",
            queue_id: "queue-new",
            routing_receipt: &new_route,
            prompt_bundle: &new_bundle,
        })
        .unwrap();
        let next = load_virtual_skill_manifest(&harness_home, "vs-new")
            .unwrap()
            .unwrap();
        assert!(next.skills.is_empty());
        assert!(next.deliveries.is_empty());
        assert_ne!(
            next.task_intent_ref,
            load_virtual_skill_manifest(&harness_home, "vs-old")
                .unwrap()
                .unwrap()
                .task_intent_ref
        );
        fs::remove_dir_all(root).ok();
    }

    #[test]
    fn rollover_rehydrates_without_rematch_or_use() {
        let root = temp_root("rollover");
        let mut manifest = create_virtual_skill_manifest(
            &root,
            identity("vs-1", "11111111aaaa"),
            Some("intent-1".to_string()),
            "catalog-1".to_string(),
            "topology-1".to_string(),
        )
        .expect("create");
        activate_manifest_skill(&mut manifest, "demo", "r1", "explicit").expect("activate");
        assert!(
            record_manifest_delivery(
                &mut manifest,
                "demo",
                "delivery-1",
                SkillDeliveryReasonV2::FirstLoad,
            )
            .expect("first load")
        );
        let rehydrate = rollover_virtual_skill_manifest(&mut manifest, "22222222bbbb".to_string())
            .expect("rollover");
        assert_eq!(rehydrate, vec![("demo".to_string(), "r1".to_string())]);
        assert!(
            !record_manifest_delivery(
                &mut manifest,
                "demo",
                "delivery-2",
                SkillDeliveryReasonV2::Rehydration,
            )
            .expect("rehydration")
        );
        assert_eq!(
            manifest.skills[0].first_delivery_id.as_deref(),
            Some("delivery-1")
        );
        assert_eq!(manifest.rollover_count, 1);
        fs::remove_dir_all(root).ok();
    }

    #[test]
    fn new_task_closes_manifest_and_inherits_nothing() {
        let root = temp_root("new");
        let mut prior = create_virtual_skill_manifest(
            &root,
            identity("vs-old", "11111111aaaa"),
            Some("old-intent".to_string()),
            "catalog-1".to_string(),
            "topology-1".to_string(),
        )
        .expect("create old");
        activate_manifest_skill(&mut prior, "demo", "r1", "automatic").expect("activate");
        close_virtual_skill_manifest(&mut prior, VirtualSkillManifestStatus::Completed, "/new")
            .expect("close");
        persist_manifest(&root, &prior).expect("persist old");

        let next = create_virtual_skill_manifest(
            &root,
            identity("vs-new", "22222222bbbb"),
            None,
            "catalog-1".to_string(),
            "topology-1".to_string(),
        )
        .expect("create new");
        assert!(next.skills.is_empty());
        assert!(next.task_intent_ref.is_none());
        assert_eq!(prior.status, VirtualSkillManifestStatus::Completed);
        fs::remove_dir_all(root).ok();
    }

    #[test]
    fn active_manifest_rejects_mid_task_revision_change() {
        let root = temp_root("freeze");
        let mut manifest = create_virtual_skill_manifest(
            &root,
            identity("vs-1", "11111111aaaa"),
            None,
            "catalog-1".to_string(),
            "topology-1".to_string(),
        )
        .expect("create");
        activate_manifest_skill(&mut manifest, "demo", "r1", "automatic").expect("activate");
        assert!(activate_manifest_skill(&mut manifest, "demo", "r2", "automatic").is_err());
        fs::remove_dir_all(root).ok();
    }

    #[test]
    fn t3_skill_virtual_evidence_replay_is_exact_idempotent_and_proposal_only() {
        let root = temp_root("t3-virtual-evidence");
        let harness_home = root.join("harness");
        let workspace = root.join("workspace");
        let first_lane = lane("root-t3", "root-t3");
        let first_route = routing_receipt(
            &first_lane,
            "vs-t3",
            "route-t3-first",
            "turn-t3-first",
            vec!["workspace:demo".to_string()],
        );
        let first_bundle = prompt_bundle(&workspace, true);
        let first = observe_virtual_skill_runtime(VirtualSkillRuntimeObserveOptions {
            harness_home: &harness_home,
            full_lane: &first_lane,
            virtual_session_id: "vs-t3",
            backend_generation: "backend-t3-1",
            queue_id: "queue-t3-1",
            routing_receipt: &first_route,
            prompt_bundle: &first_bundle,
        })
        .unwrap();
        assert_eq!(first.delivery_receipts.len(), 1);
        assert_eq!(
            first.delivery_receipts[0].delivery_reason,
            SkillDeliveryReasonV2::FirstLoad
        );

        let rollover_lane = lane("root-t3", "root-t3:cont-1");
        let rollover_route = routing_receipt(
            &rollover_lane,
            "vs-t3",
            "route-t3-rollover",
            "turn-t3-rollover",
            vec!["workspace:demo".to_string()],
        );
        let rollover_bundle = prompt_bundle(&workspace, true);
        let rollover = observe_virtual_skill_runtime(VirtualSkillRuntimeObserveOptions {
            harness_home: &harness_home,
            full_lane: &rollover_lane,
            virtual_session_id: "vs-t3",
            backend_generation: "backend-t3-2",
            queue_id: "queue-t3-2",
            routing_receipt: &rollover_route,
            prompt_bundle: &rollover_bundle,
        })
        .unwrap();
        assert!(rollover.rollover_applied);
        assert_eq!(
            rollover.delivery_receipts[0].delivery_reason,
            SkillDeliveryReasonV2::Rehydration
        );
        assert!(rollover.delivery_receipts[0].routing_receipt_id.is_none());

        let retry = observe_virtual_skill_runtime(VirtualSkillRuntimeObserveOptions {
            harness_home: &harness_home,
            full_lane: &rollover_lane,
            virtual_session_id: "vs-t3",
            backend_generation: "backend-t3-2",
            queue_id: "queue-t3-2",
            routing_receipt: &rollover_route,
            prompt_bundle: &rollover_bundle,
        })
        .unwrap();
        assert_eq!(
            retry.delivery_receipt_files,
            rollover.delivery_receipt_files
        );

        let second_rollover_lane = lane("root-t3", "root-t3:cont-2");
        let second_rollover_route = routing_receipt(
            &second_rollover_lane,
            "vs-t3",
            "route-t3-rollover-2",
            "turn-t3-rollover-2",
            vec!["workspace:demo".to_string()],
        );
        let second_rollover = observe_virtual_skill_runtime(VirtualSkillRuntimeObserveOptions {
            harness_home: &harness_home,
            full_lane: &second_rollover_lane,
            virtual_session_id: "vs-t3",
            backend_generation: "backend-t3-3",
            queue_id: "queue-t3-3",
            routing_receipt: &second_rollover_route,
            prompt_bundle: &rollover_bundle,
        })
        .unwrap();
        assert!(second_rollover.rollover_applied);
        assert_eq!(
            second_rollover.delivery_receipts[0].delivery_reason,
            SkillDeliveryReasonV2::Rehydration
        );
        assert!(
            second_rollover.delivery_receipts[0]
                .routing_receipt_id
                .is_none()
        );
        let manifest = load_virtual_skill_manifest(&harness_home, "vs-t3")
            .unwrap()
            .unwrap();
        assert_eq!(manifest.rollover_count, 2);
        assert_eq!(
            manifest.deliveries.len(),
            3,
            "retry cannot duplicate delivery"
        );

        let wrong_lane = FullLaneKeyV1::new(
            "discord",
            "default",
            "other-channel",
            "user",
            "main",
            "interactive",
            "root-t3",
            "root-t3:cont-2",
        )
        .unwrap();
        let wrong_route = routing_receipt(
            &wrong_lane,
            "vs-t3",
            "route-t3-wrong-lane",
            "turn-t3-wrong-lane",
            vec!["workspace:demo".to_string()],
        );
        assert!(
            observe_virtual_skill_runtime(VirtualSkillRuntimeObserveOptions {
                harness_home: &harness_home,
                full_lane: &wrong_lane,
                virtual_session_id: "vs-t3",
                backend_generation: "backend-t3-3",
                queue_id: "queue-t3-wrong",
                routing_receipt: &wrong_route,
                prompt_bundle: &rollover_bundle,
            })
            .is_err(),
            "another exact lane cannot reuse the virtual manifest"
        );

        let mut delivery_files = first.delivery_receipt_files.clone();
        delivery_files.extend(rollover.delivery_receipt_files.clone());
        delivery_files.extend(second_rollover.delivery_receipt_files.clone());
        delivery_files.extend(retry.delivery_receipt_files.clone());
        let capture = |now_ms| {
            crate::capture_skill_episode_runtime_evidence(
                crate::SkillEpisodeRuntimeCaptureOptions {
                    harness_home: harness_home.clone(),
                    manifest_file: second_rollover.manifest_file.clone(),
                    delivery_receipt_files: delivery_files.clone(),
                    queue_id: "queue-t3-terminal".to_string(),
                    execution_class: "interactive".to_string(),
                    source_origin: "channel".to_string(),
                    outcome_status: crate::SkillOutcomeStatusV1::Unknown,
                    verifier_type: None,
                    verifier_ref: None,
                    correction_ref: None,
                    terminal_eligible: true,
                    now_ms,
                },
            )
            .unwrap()
        };
        let terminal = capture(10);
        let terminal_retry = capture(11);
        assert_eq!(terminal.episodes.len(), 3);
        assert!(terminal.episodes.iter().all(|episode| {
            !episode.positive_learning_eligible()
                && episode.outcome_status == Some(crate::SkillOutcomeStatusV1::Unknown)
        }));
        assert_eq!(
            terminal
                .terminal_review
                .as_ref()
                .map(|review| review.review_id.as_str()),
            terminal_retry
                .terminal_review
                .as_ref()
                .map(|review| review.review_id.as_str())
        );

        let classification =
            crate::classify_knowledge_candidate(crate::KnowledgeClassificationOptions {
                identity: manifest.identity.clone(),
                receipt_id: "classification-t3".to_string(),
                candidate_id: "candidate-t3".to_string(),
                evidence_refs: terminal
                    .episodes
                    .iter()
                    .map(|episode| episode.episode_id.clone())
                    .collect(),
                source_class: "channel".to_string(),
                candidate_kind: "skill-procedure".to_string(),
                typed_refs: BTreeMap::new(),
                contradiction_refs: Vec::new(),
                confidence: 0.9,
                ambiguous: false,
                dream_run_id: None,
            })
            .unwrap();
        crate::persist_knowledge_classification_once(&harness_home, &classification).unwrap();
        assert!(
            crate::propose_novel_skill_synthesis(crate::SkillSynthesisProposalOptions {
                identity: manifest.identity.clone(),
                classification: &classification,
                episodes: &terminal.episodes,
                proposed_name: "unverified-procedure".to_string(),
                semantic_sections: BTreeMap::from([(
                    "procedure".to_string(),
                    "Use bounded replay evidence.".to_string(),
                )]),
                nearest_skill_refs: Vec::new(),
                explicit_learn: true,
                now_ms: 12,
            })
            .is_err(),
            "unknown model final cannot authorize synthesis"
        );
        assert!(!crate::skill_improvement_proposal_dir(&harness_home, "vs-t3").exists());

        let mut closed = manifest;
        close_virtual_skill_manifest(&mut closed, VirtualSkillManifestStatus::Completed, "/new")
            .unwrap();
        persist_manifest(&harness_home, &closed).unwrap();
        let new_lane = lane("root-new", "root-new");
        let new_route = routing_receipt(
            &new_lane,
            "vs-new",
            "route-t3-new",
            "turn-t3-new",
            Vec::new(),
        );
        let new_bundle = empty_prompt_bundle(&workspace);
        observe_virtual_skill_runtime(VirtualSkillRuntimeObserveOptions {
            harness_home: &harness_home,
            full_lane: &new_lane,
            virtual_session_id: "vs-new",
            backend_generation: "backend-new",
            queue_id: "queue-new",
            routing_receipt: &new_route,
            prompt_bundle: &new_bundle,
        })
        .unwrap();
        let next = load_virtual_skill_manifest(&harness_home, "vs-new")
            .unwrap()
            .unwrap();
        assert!(next.skills.is_empty());
        assert!(next.deliveries.is_empty());
        assert_eq!(closed.status, VirtualSkillManifestStatus::Completed);

        fs::remove_dir_all(root).ok();
    }
}
