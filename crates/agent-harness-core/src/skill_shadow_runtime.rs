use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::time::Instant;

use ring::digest;

use crate::lane::FullLaneKeyV1;
use crate::{
    SKILL_ROUTING_SCHEMA, SkillEcosystemIdentity, SkillIndex, SkillRouterV2Policy,
    SkillRoutingCandidateV2, SkillRoutingQueryV2, SkillRoutingReceiptV2, SkillSelection,
    route_skills_v2, routing_feature_map, write_json_atomic,
};

const SHADOW_TOPOLOGY_REVISION: &str = "topology-dormant-v1";
const HASH_DOMAIN: &[u8] = b"agent-harness/skill-shadow-runtime/v1";

pub struct SkillShadowRuntimeReceiptOptions<'a> {
    pub harness_home: &'a Path,
    pub full_lane: &'a FullLaneKeyV1,
    pub virtual_session_id: &'a str,
    pub query: &'a SkillRoutingQueryV2,
    pub skill_index: &'a SkillIndex,
    pub active_serving_skills: &'a [SkillSelection],
    pub policy: SkillRouterV2Policy,
}

pub fn skill_shadow_routing_receipt_dir(harness_home: impl AsRef<Path>) -> PathBuf {
    harness_home
        .as_ref()
        .join("state")
        .join("skills")
        .join("shadow-routing")
}

pub fn record_skill_shadow_runtime_receipt(
    options: SkillShadowRuntimeReceiptOptions<'_>,
) -> io::Result<(PathBuf, SkillRoutingReceiptV2)> {
    options.full_lane.validate().map_err(io::Error::other)?;
    let started = Instant::now();
    let result = route_skills_v2(options.skill_index, options.query, options.policy);
    let exact_lane_digest = options
        .full_lane
        .virtual_identity_hash()
        .map_err(io::Error::other)?;
    let identity = SkillEcosystemIdentity {
        virtual_session_id: options.virtual_session_id.to_string(),
        root_session_key_hash: sha256_hex(options.full_lane.root_virtual_session().as_bytes()),
        concrete_session_hash: sha256_hex(options.full_lane.concrete_session().as_bytes()),
        exact_lane_digest,
        agent_id: options.full_lane.agent_id().to_string(),
    };
    let turn_hash = hash_components(&[
        options
            .full_lane
            .identity_hash()
            .map_err(io::Error::other)?
            .as_bytes(),
        options.query.task_text.as_bytes(),
    ]);
    let receipt_id = format!(
        "shadow-route-{}",
        hash_components(&[
            turn_hash.as_bytes(),
            result.method.as_bytes(),
            result.version.as_bytes(),
        ])
    );

    let mut candidates = result
        .candidates
        .iter()
        .map(|candidate| SkillRoutingCandidateV2 {
            skill_id: candidate.selection.skill_id.clone(),
            revision: candidate.selection.body_checksum.clone(),
            confidence: candidate.confidence,
            rank: u16::try_from(candidate.rank).unwrap_or(u16::MAX),
            disposition: if candidate.selected {
                "shadow-candidate-card".to_string()
            } else {
                "shadow-not-selected".to_string()
            },
            reason_codes: candidate.reason_codes.clone(),
            features: routing_feature_map(candidate),
        })
        .collect::<Vec<_>>();
    let eligible_count = candidates.len();
    for (offset, rejection) in result.rejected.iter().enumerate() {
        let revision = options
            .skill_index
            .skills
            .iter()
            .find(|skill| skill.id == rejection.skill_id)
            .map(|skill| skill.body_checksum.clone())
            .unwrap_or_else(|| "unknown-revision".to_string());
        candidates.push(SkillRoutingCandidateV2 {
            skill_id: rejection.skill_id.clone(),
            revision,
            confidence: 0.0,
            rank: u16::try_from(eligible_count + offset + 1).unwrap_or(u16::MAX),
            disposition: format!("shadow-rejected:{}", rejection.reason_code),
            reason_codes: vec![rejection.reason_code.clone()],
            features: Default::default(),
        });
    }
    let active_serving_skill_ids = options
        .active_serving_skills
        .iter()
        .map(|skill| skill.skill_id.clone())
        .collect::<Vec<_>>();
    let receipt = SkillRoutingReceiptV2 {
        schema: SKILL_ROUTING_SCHEMA.to_string(),
        receipt_id: receipt_id.clone(),
        turn_hash,
        identity,
        channel: options.query.channel.clone(),
        catalog_revision: skill_catalog_revision(options.skill_index)?,
        topology_revision: SHADOW_TOPOLOGY_REVISION.to_string(),
        method: result.method,
        method_version: result.version,
        task_text_bytes: options.query.task_text.len(),
        virtual_task_intent_bytes: options
            .query
            .virtual_task_intent
            .as_deref()
            .map(str::len)
            .unwrap_or(0),
        ambient_notes_excluded_bytes: result.ambient_notes_excluded_bytes,
        candidates,
        selected_count: result.selected_skill_ids.len(),
        active_serving_skill_ids,
        shadow_selected_skill_ids: result.selected_skill_ids,
        abstention_reason: result.abstention_reason,
        shadow: true,
        duration_ms: u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
    };
    receipt.validate().map_err(io::Error::other)?;

    let dir = skill_shadow_routing_receipt_dir(options.harness_home);
    fs::create_dir_all(&dir)?;
    let file = dir.join(format!("{receipt_id}.json"));
    write_json_atomic(&file, &receipt)?;
    Ok((file, receipt))
}

fn skill_catalog_revision(index: &SkillIndex) -> io::Result<String> {
    let mut skills = index.skills.iter().collect::<Vec<_>>();
    skills.sort_by(|left, right| left.id.cmp(&right.id));
    let mut components = Vec::with_capacity(skills.len() * 3 + 1);
    components.push(index.schema.as_bytes());
    for skill in skills {
        components.push(skill.id.as_bytes());
        components.push(skill.original_id.as_bytes());
        components.push(skill.body_checksum.as_bytes());
    }
    Ok(format!("catalog-sha256-{}", hash_components(&components)))
}

fn hash_components(components: &[&[u8]]) -> String {
    let mut context = digest::Context::new(&digest::SHA256);
    context.update(HASH_DOMAIN);
    for component in components {
        context.update(&(component.len() as u64).to_be_bytes());
        context.update(component);
    }
    lower_hex(context.finish().as_ref())
}

fn sha256_hex(value: &[u8]) -> String {
    lower_hex(digest::digest(&digest::SHA256, value).as_ref())
}

fn lower_hex(bytes: &[u8]) -> String {
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write as _;
        write!(&mut output, "{byte:02x}").expect("writing to String cannot fail");
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{AgentSource, build_runtime_skill_index};
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_root(name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        std::env::temp_dir().join(format!("skill-shadow-runtime-{name}-{nanos}"))
    }

    #[test]
    fn rejected_candidates_remain_explainable_without_delivery() {
        let root = temp_root("rejections");
        let home = root.join("home");
        let workspace = root.join("workspace");
        let harness = root.join("harness");
        let skill = workspace.join("skills").join("telegram-only");
        fs::create_dir_all(&skill).unwrap();
        fs::write(
            skill.join("SKILL.md"),
            "---\nchannels: [telegram]\n---\n# Telegram only\n",
        )
        .unwrap();
        let index =
            build_runtime_skill_index(&AgentSource::with_workspace(home, workspace), &harness)
                .unwrap();
        let lane = FullLaneKeyV1::new(
            "discord",
            "default",
            "channel",
            "user",
            "main",
            "interactive",
            "root",
            "concrete",
        )
        .unwrap();
        let query = SkillRoutingQueryV2 {
            task_text: "use telegram only".to_string(),
            explicit_invocations: Vec::new(),
            agent_id: "main".to_string(),
            channel: "discord".to_string(),
            available_tools: Vec::new(),
            available_toolsets: Vec::new(),
            risk_context: Vec::new(),
            virtual_task_intent: None,
            ambient_notes_excluded_bytes: 0,
            usage_snapshot: None,
        };
        let (_, receipt) = record_skill_shadow_runtime_receipt(SkillShadowRuntimeReceiptOptions {
            harness_home: &harness,
            full_lane: &lane,
            virtual_session_id: "vs-test",
            query: &query,
            skill_index: &index,
            active_serving_skills: &[],
            policy: SkillRouterV2Policy::default(),
        })
        .unwrap();
        assert_eq!(receipt.selected_count, 0);
        assert!(receipt.candidates.iter().any(|candidate| {
            candidate.disposition == "shadow-rejected:wrong-channel"
                && candidate.reason_codes == ["wrong-channel"]
        }));
        fs::remove_dir_all(root).ok();
    }
}
