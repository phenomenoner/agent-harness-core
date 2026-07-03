use std::fs;
use std::io::{self, Read};
use std::path::{Component, Path, PathBuf};

use serde::{Deserialize, Serialize};

pub const INBOUND_MEDIA_ARTIFACT_SCHEMA: &str = "agent-harness.inbound-media-artifact.v1";
pub const INBOUND_MEDIA_INPUT_PLAN_SCHEMA: &str = "agent-harness.inbound-media-input-plan.v1";
pub const INBOUND_MEDIA_VISION_ANALYSIS_SCHEMA: &str =
    "agent-harness.inbound-media-vision-analysis.v1";
pub const INBOUND_MEDIA_SAFETY_REPORT_SCHEMA: &str = "agent-harness.inbound-media-safety.v1";
pub const INBOUND_MEDIA_CACHE_REPORT_SCHEMA: &str = "agent-harness.inbound-media-cache.v1";
pub const DEFAULT_INBOUND_MEDIA_MAX_ITEMS_PER_TURN: usize = 10;
pub const DEFAULT_INBOUND_MEDIA_MAX_BYTES_PER_ITEM: u64 = 20 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InboundMediaArtifact {
    #[serde(default = "default_inbound_media_artifact_schema")]
    pub schema: String,
    pub platform: String,
    pub kind: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub media_group_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub variant_count: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub selected_variant: Option<InboundMediaSelectedVariant>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub local_path: Option<PathBuf>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub artifact_uri: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mime: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sha256: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub byte_len: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub caption_preview: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lifecycle_status: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub extraction_summary: Option<ArtifactExtractionSummary>,
    pub source: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provenance: Option<String>,
    #[serde(default)]
    pub download_status: InboundMediaDownloadStatus,
    #[serde(default)]
    pub model_attachment_status: InboundMediaModelAttachmentStatus,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub warnings: Vec<String>,
}

impl Default for InboundMediaArtifact {
    fn default() -> Self {
        Self {
            schema: default_inbound_media_artifact_schema(),
            platform: String::new(),
            kind: String::new(),
            media_group_id: None,
            message_id: None,
            variant_count: None,
            selected_variant: None,
            local_path: None,
            artifact_uri: None,
            mime: None,
            sha256: None,
            byte_len: None,
            caption_preview: None,
            lifecycle_status: None,
            extraction_summary: None,
            source: String::new(),
            provenance: None,
            download_status: InboundMediaDownloadStatus::default(),
            model_attachment_status: InboundMediaModelAttachmentStatus::default(),
            warnings: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ArtifactExtractionSummary {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub artifact_class: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub modality: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub summary: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub facts: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub uncertainty: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InboundMediaSelectedVariant {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub width: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub height: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub file_size: Option<u64>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum InboundMediaDownloadStatus {
    #[default]
    Detected,
    DetectedSkipped,
    Downloaded,
    DownloadFailed,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum InboundMediaModelAttachmentStatus {
    #[default]
    NotEvaluated,
    PromptOnly,
    ModelAttached,
    VisionToolAvailable,
    DownloadedButNotModelAttached,
    Unsupported,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InboundMediaInputPlanOptions {
    pub harness_home: PathBuf,
    pub native_image_input_enabled: bool,
    pub vision_tool_available: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InboundMediaInputPlan {
    #[serde(default = "default_inbound_media_input_plan_schema")]
    pub schema: String,
    #[serde(default)]
    pub native_image_input_enabled: bool,
    #[serde(default)]
    pub vision_tool_available: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub artifacts: Vec<InboundMediaArtifact>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub native_input_parts: Vec<InboundMediaNativeInputPart>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub warnings: Vec<String>,
}

impl InboundMediaInputPlan {
    pub fn is_empty(&self) -> bool {
        self.artifacts.is_empty() && self.native_input_parts.is_empty() && self.warnings.is_empty()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InboundMediaNativeInputPart {
    pub local_path: PathBuf,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub artifact_uri: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mime: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sha256: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InboundMediaVisionAnalysis {
    #[serde(default = "default_inbound_media_vision_analysis_schema")]
    pub schema: String,
    pub artifact_ref: String,
    pub local_path: PathBuf,
    pub mime: Option<String>,
    pub byte_len: u64,
    pub width: Option<u32>,
    pub height: Option<u32>,
    pub status: String,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InboundMediaSafetyPolicy {
    pub max_items_per_turn: usize,
    pub max_bytes_per_item: u64,
}

impl Default for InboundMediaSafetyPolicy {
    fn default() -> Self {
        Self {
            max_items_per_turn: DEFAULT_INBOUND_MEDIA_MAX_ITEMS_PER_TURN,
            max_bytes_per_item: DEFAULT_INBOUND_MEDIA_MAX_BYTES_PER_ITEM,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InboundMediaSafetyReport {
    #[serde(default = "default_inbound_media_safety_report_schema")]
    pub schema: String,
    pub artifact_count: usize,
    pub total_declared_bytes: u64,
    pub policy: InboundMediaSafetyPolicy,
    pub within_limits: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub violations: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InboundMediaCacheReport {
    #[serde(default = "default_inbound_media_cache_report_schema")]
    pub schema: String,
    pub attachment_root: PathBuf,
    pub file_count: usize,
    pub total_bytes: u64,
}

pub fn default_inbound_media_artifact_schema() -> String {
    INBOUND_MEDIA_ARTIFACT_SCHEMA.to_string()
}

pub fn default_inbound_media_input_plan_schema() -> String {
    INBOUND_MEDIA_INPUT_PLAN_SCHEMA.to_string()
}

pub fn default_inbound_media_vision_analysis_schema() -> String {
    INBOUND_MEDIA_VISION_ANALYSIS_SCHEMA.to_string()
}

pub fn default_inbound_media_safety_report_schema() -> String {
    INBOUND_MEDIA_SAFETY_REPORT_SCHEMA.to_string()
}

pub fn default_inbound_media_cache_report_schema() -> String {
    INBOUND_MEDIA_CACHE_REPORT_SCHEMA.to_string()
}

pub fn inbound_media_attachment_root(harness_home: impl AsRef<Path>) -> PathBuf {
    harness_home
        .as_ref()
        .join("state")
        .join("channels")
        .join("telegram-attachments")
}

pub fn validate_inbound_media_artifact_paths(
    harness_home: impl AsRef<Path>,
    artifacts: &[InboundMediaArtifact],
) -> Result<(), Vec<String>> {
    let root = inbound_media_attachment_root(harness_home);
    let errors = artifacts
        .iter()
        .enumerate()
        .filter_map(|(index, artifact)| {
            let path = artifact.local_path.as_ref()?;
            if path_is_within_root(&root, path) {
                None
            } else {
                Some(format!(
                    "inbound media artifact {index} localPath `{}` is outside attachment root `{}`",
                    path.display(),
                    root.display()
                ))
            }
        })
        .collect::<Vec<_>>();
    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors)
    }
}

pub fn plan_inbound_media_inputs(
    options: InboundMediaInputPlanOptions,
    artifacts: &[InboundMediaArtifact],
) -> InboundMediaInputPlan {
    let root = inbound_media_attachment_root(&options.harness_home);
    let mut planned_artifacts = Vec::new();
    let mut native_input_parts = Vec::new();
    let mut warnings = Vec::new();
    let mut ordered_artifacts = artifacts.iter().collect::<Vec<_>>();
    ordered_artifacts.sort_by_key(|artifact| {
        usize::from(
            artifact
                .provenance
                .as_deref()
                .is_some_and(|value| value == "referenced"),
        )
    });
    for (index, artifact) in ordered_artifacts.into_iter().enumerate() {
        let mut planned = artifact.clone();
        if index >= DEFAULT_INBOUND_MEDIA_MAX_ITEMS_PER_TURN {
            planned.model_attachment_status = InboundMediaModelAttachmentStatus::PromptOnly;
            planned
                .warnings
                .push("media item limit exceeded; model input blocked".to_string());
            warnings.push(format!(
                "inbound media artifact {index} exceeds maxItemsPerTurn={DEFAULT_INBOUND_MEDIA_MAX_ITEMS_PER_TURN}"
            ));
            planned_artifacts.push(planned);
            continue;
        }
        if artifact
            .byte_len
            .is_some_and(|bytes| bytes > DEFAULT_INBOUND_MEDIA_MAX_BYTES_PER_ITEM)
        {
            planned.model_attachment_status = InboundMediaModelAttachmentStatus::PromptOnly;
            planned
                .warnings
                .push("media byte limit exceeded; model input blocked".to_string());
            warnings.push(format!(
                "inbound media artifact {index} exceeds maxBytesPerItem={DEFAULT_INBOUND_MEDIA_MAX_BYTES_PER_ITEM}"
            ));
            planned_artifacts.push(planned);
            continue;
        }
        let Some(local_path) = artifact.local_path.as_ref() else {
            planned.model_attachment_status = InboundMediaModelAttachmentStatus::PromptOnly;
            planned_artifacts.push(planned);
            continue;
        };
        if !path_is_within_root(&root, local_path) {
            planned.model_attachment_status = InboundMediaModelAttachmentStatus::PromptOnly;
            planned.warnings.push(
                "local media path is outside the harness attachment root; model input blocked"
                    .to_string(),
            );
            warnings.push(format!(
                "inbound media artifact {index} local path is outside attachment root"
            ));
            planned_artifacts.push(planned);
            continue;
        }
        if artifact.download_status != InboundMediaDownloadStatus::Downloaded
            || !is_supported_image_mime(artifact.mime.as_deref())
        {
            planned.model_attachment_status = InboundMediaModelAttachmentStatus::PromptOnly;
            planned_artifacts.push(planned);
            continue;
        }
        if options.native_image_input_enabled {
            planned.model_attachment_status = InboundMediaModelAttachmentStatus::ModelAttached;
            native_input_parts.push(InboundMediaNativeInputPart {
                local_path: local_path.clone(),
                artifact_uri: artifact.artifact_uri.clone(),
                mime: artifact.mime.clone(),
                sha256: artifact.sha256.clone(),
            });
        } else if options.vision_tool_available {
            planned.model_attachment_status =
                InboundMediaModelAttachmentStatus::VisionToolAvailable;
        } else {
            planned.model_attachment_status =
                InboundMediaModelAttachmentStatus::DownloadedButNotModelAttached;
        }
        planned_artifacts.push(planned);
    }
    InboundMediaInputPlan {
        schema: default_inbound_media_input_plan_schema(),
        native_image_input_enabled: options.native_image_input_enabled,
        vision_tool_available: options.vision_tool_available,
        artifacts: planned_artifacts,
        native_input_parts,
        warnings,
    }
}

pub fn validate_inbound_media_safety(
    harness_home: impl AsRef<Path>,
    artifacts: &[InboundMediaArtifact],
    policy: InboundMediaSafetyPolicy,
) -> InboundMediaSafetyReport {
    let root = inbound_media_attachment_root(harness_home);
    let mut violations = Vec::new();
    if artifacts.len() > policy.max_items_per_turn {
        violations.push(format!(
            "artifact count {} exceeds maxItemsPerTurn={}",
            artifacts.len(),
            policy.max_items_per_turn
        ));
    }
    let mut total_declared_bytes = 0u64;
    for (index, artifact) in artifacts.iter().enumerate() {
        if let Some(byte_len) = artifact.byte_len {
            total_declared_bytes = total_declared_bytes.saturating_add(byte_len);
            if byte_len > policy.max_bytes_per_item {
                violations.push(format!(
                    "artifact {index} byteLen={byte_len} exceeds maxBytesPerItem={}",
                    policy.max_bytes_per_item
                ));
            }
        }
        if let Some(local_path) = artifact.local_path.as_ref() {
            if !path_is_within_root(&root, local_path) {
                violations.push(format!(
                    "artifact {index} localPath is outside attachment root"
                ));
            }
            if !allowed_artifact_extension(local_path) {
                violations.push(format!(
                    "artifact {index} localPath extension is not in the allowed artifact set"
                ));
            }
        }
        if artifact.download_status == InboundMediaDownloadStatus::Downloaded
            && !is_supported_artifact_mime(artifact.mime.as_deref())
        {
            violations.push(format!(
                "artifact {index} MIME is not in the allowed artifact set"
            ));
        }
    }
    InboundMediaSafetyReport {
        schema: default_inbound_media_safety_report_schema(),
        artifact_count: artifacts.len(),
        total_declared_bytes,
        policy,
        within_limits: violations.is_empty(),
        violations,
    }
}

pub fn collect_inbound_media_cache_report(
    harness_home: impl AsRef<Path>,
) -> io::Result<InboundMediaCacheReport> {
    let attachment_root = inbound_media_attachment_root(harness_home);
    let mut report = InboundMediaCacheReport {
        schema: default_inbound_media_cache_report_schema(),
        attachment_root: attachment_root.clone(),
        file_count: 0,
        total_bytes: 0,
    };
    if attachment_root.is_dir() {
        collect_cache_files(&attachment_root, &mut report)?;
    }
    Ok(report)
}

pub fn resolve_inbound_media_artifact_reference(
    harness_home: impl AsRef<Path>,
    artifact_ref: &str,
) -> Result<PathBuf, String> {
    let harness_home = harness_home.as_ref();
    let root = inbound_media_attachment_root(harness_home);
    let trimmed = artifact_ref.trim();
    if trimmed.is_empty() {
        return Err("artifact reference is empty".to_string());
    }
    let path = if let Some(relative) = trimmed.strip_prefix("agent-harness://inbound-media/") {
        let relative = if let Some(relative) = relative.strip_prefix("telegram/") {
            relative
        } else if relative.starts_with("discord/") {
            relative
        } else {
            return Err(
                "artifact URI platform is not supported by this attachment root".to_string(),
            );
        };
        let relative_path = safe_relative_artifact_path(relative)?;
        root.join(relative_path)
    } else {
        PathBuf::from(trimmed)
    };
    if !path_is_within_root(&root, &path) {
        return Err("artifact path is outside the harness attachment root".to_string());
    }
    if !path.is_file() {
        return Err("artifact path does not exist or is not a file".to_string());
    }
    Ok(path)
}

pub fn analyze_inbound_media_file(
    harness_home: impl AsRef<Path>,
    artifact_ref: &str,
    max_read_bytes: usize,
) -> Result<InboundMediaVisionAnalysis, String> {
    let local_path = resolve_inbound_media_artifact_reference(harness_home, artifact_ref)?;
    let metadata = fs::metadata(&local_path).map_err(|err| err.to_string())?;
    let byte_len = metadata.len();
    if byte_len > max_read_bytes as u64 {
        return Err(format!(
            "artifact is {byte_len} bytes, exceeding maxReadBytes={max_read_bytes}"
        ));
    }
    let mut file = fs::File::open(&local_path).map_err(|err| err.to_string())?;
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)
        .map_err(|err| err.to_string())?;
    let mime = image_mime_from_bytes(&bytes).map(ToString::to_string);
    let (width, height) = image_dimensions_from_bytes(&bytes);
    Ok(InboundMediaVisionAnalysis {
        schema: default_inbound_media_vision_analysis_schema(),
        artifact_ref: artifact_ref.to_string(),
        local_path,
        mime,
        byte_len,
        width,
        height,
        status: "analyzed".to_string(),
        reason: "local harness-contained image header analysis completed".to_string(),
    })
}

pub fn render_inbound_media_artifacts_for_prompt(
    artifacts: &[InboundMediaArtifact],
    harness_home: Option<&Path>,
) -> String {
    let platform = artifacts
        .first()
        .map(|artifact| title_case_ascii(&artifact.platform))
        .unwrap_or_else(|| "Channel".to_string());
    let mut lines = vec![format!("## InboundMedia: {platform} attachments")];
    let root = harness_home.map(inbound_media_attachment_root);
    for (index, artifact) in artifacts.iter().enumerate() {
        lines.push(render_inbound_media_artifact_line(
            index,
            artifact,
            harness_home,
            root.as_deref(),
        ));
    }
    lines.join("\n")
}

fn render_inbound_media_artifact_line(
    index: usize,
    artifact: &InboundMediaArtifact,
    harness_home: Option<&Path>,
    attachment_root: Option<&Path>,
) -> String {
    let mut fields = vec![
        format!("index={index}"),
        format!("kind={}", sanitize_prompt_field(&artifact.kind)),
    ];
    push_optional_field(&mut fields, "messageId", artifact.message_id.as_deref());
    push_optional_field(
        &mut fields,
        "mediaGroupId",
        artifact.media_group_id.as_deref(),
    );
    if let Some(variant_count) = artifact.variant_count {
        fields.push(format!("variantCount={variant_count}"));
    }
    if let Some(variant) = &artifact.selected_variant {
        if let Some(width) = variant.width {
            fields.push(format!("width={width}"));
        }
        if let Some(height) = variant.height {
            fields.push(format!("height={height}"));
        }
        if let Some(file_size) = variant.file_size {
            fields.push(format!("file_size={file_size}"));
        }
    }
    if let Some(uri) = artifact.artifact_uri.as_deref() {
        if is_prompt_safe_artifact_uri(uri) {
            fields.push(format!("artifactUri={}", sanitize_prompt_field(uri)));
        } else {
            fields.push("artifactUri=redacted-provider-uri".to_string());
        }
    }
    if let Some(path) = artifact.local_path.as_deref() {
        match prompt_safe_artifact_path(path, harness_home, attachment_root) {
            Some(path) => fields.push(format!("localPath={}", sanitize_prompt_field(&path))),
            None => fields.push("localPath=blocked-outside-attachment-root".to_string()),
        }
    }
    push_optional_field(&mut fields, "mime", artifact.mime.as_deref());
    push_optional_field(&mut fields, "sha256", artifact.sha256.as_deref());
    if let Some(byte_len) = artifact.byte_len {
        fields.push(format!("byteLen={byte_len}"));
    }
    push_optional_field(
        &mut fields,
        "captionPreview",
        artifact.caption_preview.as_deref(),
    );
    push_optional_field(
        &mut fields,
        "lifecycleStatus",
        artifact.lifecycle_status.as_deref(),
    );
    if let Some(summary) = artifact.extraction_summary.as_ref() {
        push_extraction_summary_fields(&mut fields, summary);
    }
    fields.push(format!(
        "source={}",
        sanitize_prompt_source_label(&artifact.source)
    ));
    fields.push(format!(
        "provenance={}",
        sanitize_prompt_source_label(artifact.provenance.as_deref().unwrap_or("current"))
    ));
    fields.push(format!(
        "downloadStatus={}",
        download_status_label(artifact.download_status)
    ));
    fields.push(format!(
        "modelAttachmentStatus={}",
        model_attachment_status_label(artifact.model_attachment_status)
    ));
    fields.push(format!(
        "visual={}",
        visual_readiness_label(artifact.model_attachment_status)
    ));
    if !artifact.warnings.is_empty() {
        fields.push(format!("warningsCount={}", artifact.warnings.len()));
    }
    format!("- {}", fields.join(" "))
}

fn push_optional_field(fields: &mut Vec<String>, key: &str, value: Option<&str>) {
    if let Some(value) = value.map(str::trim).filter(|value| !value.is_empty()) {
        fields.push(format!("{key}={}", sanitize_prompt_field(value)));
    }
}

fn push_extraction_summary_fields(fields: &mut Vec<String>, summary: &ArtifactExtractionSummary) {
    push_optional_field(fields, "artifactClass", summary.artifact_class.as_deref());
    push_optional_field(fields, "modality", summary.modality.as_deref());
    if let Some(text) = summary.summary.as_deref() {
        fields.push(format!(
            "extractionSummary={}",
            sanitize_bounded_summary_field(text, 320)
        ));
    }
    if !summary.facts.is_empty() {
        let facts = summary
            .facts
            .iter()
            .take(6)
            .map(|fact| sanitize_bounded_summary_field(fact, 160))
            .filter(|fact| !fact.is_empty())
            .collect::<Vec<_>>();
        if !facts.is_empty() {
            fields.push(format!("extractedFacts={}", facts.join("|")));
        }
    }
    if let Some(text) = summary.uncertainty.as_deref() {
        fields.push(format!(
            "uncertainty={}",
            sanitize_bounded_summary_field(text, 180)
        ));
    }
}

fn prompt_safe_artifact_path(
    path: &Path,
    harness_home: Option<&Path>,
    attachment_root: Option<&Path>,
) -> Option<String> {
    let attachment_root = attachment_root?;
    if !path_is_within_root(attachment_root, path) {
        return None;
    }
    let normalized = normalize_path_lexically(path);
    if let Some(harness_home) = harness_home {
        let normalized_home = normalize_path_lexically(harness_home);
        if let Ok(relative) = normalized.strip_prefix(&normalized_home) {
            return Some(path_to_forward_slash(relative));
        }
    }
    Some(path_to_forward_slash(
        normalized
            .strip_prefix(attachment_root)
            .unwrap_or(normalized.as_path()),
    ))
}

fn path_is_within_root(root: &Path, path: &Path) -> bool {
    if !root.is_absolute() || !path.is_absolute() {
        return false;
    }
    let root = normalize_path_lexically(root);
    let path = normalize_path_lexically(path);
    let root_components = comparable_components(&root);
    let path_components = comparable_components(&path);
    path_components.len() >= root_components.len()
        && path_components
            .iter()
            .zip(root_components.iter())
            .all(|(left, right)| left == right)
}

fn normalize_path_lexically(path: &Path) -> PathBuf {
    let mut out = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Prefix(prefix) => out.push(prefix.as_os_str()),
            Component::RootDir => out.push(component.as_os_str()),
            Component::CurDir => {}
            Component::ParentDir => {
                out.pop();
            }
            Component::Normal(part) => out.push(part),
        }
    }
    out
}

fn comparable_components(path: &Path) -> Vec<String> {
    path.components()
        .map(|component| {
            #[cfg(windows)]
            {
                component.as_os_str().to_string_lossy().to_ascii_lowercase()
            }
            #[cfg(not(windows))]
            {
                component.as_os_str().to_string_lossy().to_string()
            }
        })
        .collect()
}

fn path_to_forward_slash(path: &Path) -> String {
    path.components()
        .filter_map(|component| match component {
            Component::Normal(part) => Some(part.to_string_lossy().to_string()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("/")
}

fn sanitize_prompt_field(value: &str) -> String {
    let mut out = value
        .chars()
        .map(|ch| if ch.is_control() { ' ' } else { ch })
        .collect::<String>();
    out = out.replace(char::is_whitespace, " ");
    let trimmed = out.trim();
    if prompt_field_contains_artifact_payload(trimmed) {
        "redacted-artifact-payload".to_string()
    } else if trimmed.chars().count() > 256 {
        format!("{}...", truncate_chars(trimmed, 256))
    } else {
        trimmed.to_string()
    }
}

fn sanitize_bounded_summary_field(value: &str, max_chars: usize) -> String {
    let sanitized = sanitize_prompt_field(value);
    if sanitized.chars().count() <= max_chars {
        sanitized
    } else {
        format!("{}...", truncate_chars(&sanitized, max_chars))
    }
}

fn truncate_chars(value: &str, max_chars: usize) -> String {
    value.chars().take(max_chars).collect()
}

fn prompt_field_contains_artifact_payload(value: &str) -> bool {
    let normalized = value.to_ascii_lowercase();
    normalized.contains("data:image")
        || normalized.contains("data:audio")
        || normalized.contains("data:video")
        || normalized.contains(";base64,")
        || normalized.contains("api.telegram.org")
        || normalized.contains("cdn.discordapp.com")
        || normalized.contains("discord.com/api")
        || normalized.contains("file_id=")
        || normalized.contains("fileid=")
        || normalized.contains("bot_token")
        || normalized.contains("bottoken")
        || normalized.contains("authorization:")
        || normalized.contains("cookie:")
        || looks_like_large_base64(value)
}

fn looks_like_large_base64(value: &str) -> bool {
    let mut run = 0usize;
    for ch in value.chars() {
        if ch.is_ascii_alphanumeric() || matches!(ch, '+' | '/' | '=') {
            run += 1;
            if run >= 160 {
                return true;
            }
        } else {
            run = 0;
        }
    }
    false
}

fn sanitize_prompt_source_label(value: &str) -> String {
    let normalized = value.to_ascii_lowercase();
    if normalized.contains("://")
        || normalized.contains("bot")
        || normalized.contains("token")
        || normalized.contains("file_id")
        || normalized.contains("fileid")
        || normalized.contains("cookie")
        || normalized.contains("authorization")
    {
        "redacted-source".to_string()
    } else {
        sanitize_prompt_field(value)
    }
}

fn is_prompt_safe_artifact_uri(value: &str) -> bool {
    let normalized = value.to_ascii_lowercase();
    (normalized.starts_with("agent-harness://inbound-media/")
        || normalized.starts_with("agent-harness://artifact/"))
        && !prompt_field_contains_artifact_payload(value)
        && !normalized.contains("token")
        && !normalized.contains("bot")
}

fn title_case_ascii(value: &str) -> String {
    let mut chars = value.chars();
    match chars.next() {
        Some(first) => format!("{}{}", first.to_ascii_uppercase(), chars.as_str()),
        None => "Channel".to_string(),
    }
}

fn download_status_label(status: InboundMediaDownloadStatus) -> &'static str {
    match status {
        InboundMediaDownloadStatus::Detected => "detected",
        InboundMediaDownloadStatus::DetectedSkipped => "detected-skipped",
        InboundMediaDownloadStatus::Downloaded => "downloaded",
        InboundMediaDownloadStatus::DownloadFailed => "download-failed",
    }
}

fn model_attachment_status_label(status: InboundMediaModelAttachmentStatus) -> &'static str {
    match status {
        InboundMediaModelAttachmentStatus::NotEvaluated => "not-evaluated",
        InboundMediaModelAttachmentStatus::PromptOnly => "prompt-only",
        InboundMediaModelAttachmentStatus::ModelAttached => "model-attached",
        InboundMediaModelAttachmentStatus::VisionToolAvailable => "vision-tool-available",
        InboundMediaModelAttachmentStatus::DownloadedButNotModelAttached => {
            "downloaded-but-not-model-attached"
        }
        InboundMediaModelAttachmentStatus::Unsupported => "unsupported",
    }
}

fn visual_readiness_label(status: InboundMediaModelAttachmentStatus) -> &'static str {
    match status {
        InboundMediaModelAttachmentStatus::ModelAttached => "model-attached",
        InboundMediaModelAttachmentStatus::VisionToolAvailable => "vision-tool",
        InboundMediaModelAttachmentStatus::DownloadedButNotModelAttached
        | InboundMediaModelAttachmentStatus::PromptOnly
        | InboundMediaModelAttachmentStatus::NotEvaluated => "prompt-only",
        InboundMediaModelAttachmentStatus::Unsupported => "unsupported",
    }
}

fn is_supported_image_mime(mime: Option<&str>) -> bool {
    mime.is_some_and(|mime| {
        matches!(
            mime.to_ascii_lowercase().as_str(),
            "image/jpeg" | "image/jpg" | "image/png" | "image/gif" | "image/webp"
        )
    })
}

fn is_supported_artifact_mime(mime: Option<&str>) -> bool {
    mime.is_some_and(|mime| {
        matches!(
            mime.to_ascii_lowercase().as_str(),
            "image/jpeg"
                | "image/jpg"
                | "image/png"
                | "image/gif"
                | "image/webp"
                | "text/plain"
                | "text/markdown"
                | "application/json"
                | "application/pdf"
                | "audio/mpeg"
                | "audio/mp3"
                | "audio/wav"
                | "audio/x-wav"
                | "video/mp4"
                | "video/webm"
        )
    })
}

fn allowed_artifact_extension(path: &Path) -> bool {
    path.extension()
        .and_then(|extension| extension.to_str())
        .is_some_and(|extension| {
            matches!(
                extension.to_ascii_lowercase().as_str(),
                "jpg"
                    | "jpeg"
                    | "png"
                    | "gif"
                    | "webp"
                    | "txt"
                    | "md"
                    | "log"
                    | "json"
                    | "pdf"
                    | "mp3"
                    | "wav"
                    | "mp4"
                    | "webm"
            )
        })
}

fn collect_cache_files(dir: &Path, report: &mut InboundMediaCacheReport) -> io::Result<()> {
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            collect_cache_files(&path, report)?;
        } else if path.is_file() {
            report.file_count += 1;
            report.total_bytes = report.total_bytes.saturating_add(entry.metadata()?.len());
        }
    }
    Ok(())
}

fn safe_relative_artifact_path(value: &str) -> Result<PathBuf, String> {
    let mut path = PathBuf::new();
    for component in Path::new(value).components() {
        match component {
            Component::Normal(part) => path.push(part),
            _ => return Err("artifact URI contains an unsafe path component".to_string()),
        }
    }
    if path.as_os_str().is_empty() {
        Err("artifact URI has no relative path".to_string())
    } else {
        Ok(path)
    }
}

fn image_mime_from_bytes(bytes: &[u8]) -> Option<&'static str> {
    if bytes.starts_with(&[0xff, 0xd8, 0xff]) {
        Some("image/jpeg")
    } else if bytes.starts_with(b"\x89PNG\r\n\x1a\n") {
        Some("image/png")
    } else if bytes.starts_with(b"GIF87a") || bytes.starts_with(b"GIF89a") {
        Some("image/gif")
    } else if bytes.len() >= 12 && bytes.starts_with(b"RIFF") && &bytes[8..12] == b"WEBP" {
        Some("image/webp")
    } else {
        None
    }
}

fn image_dimensions_from_bytes(bytes: &[u8]) -> (Option<u32>, Option<u32>) {
    if bytes.starts_with(b"\x89PNG\r\n\x1a\n") && bytes.len() >= 24 {
        let width = u32::from_be_bytes([bytes[16], bytes[17], bytes[18], bytes[19]]);
        let height = u32::from_be_bytes([bytes[20], bytes[21], bytes[22], bytes[23]]);
        return (Some(width), Some(height));
    }
    if (bytes.starts_with(b"GIF87a") || bytes.starts_with(b"GIF89a")) && bytes.len() >= 10 {
        let width = u16::from_le_bytes([bytes[6], bytes[7]]) as u32;
        let height = u16::from_le_bytes([bytes[8], bytes[9]]) as u32;
        return (Some(width), Some(height));
    }
    if bytes.starts_with(&[0xff, 0xd8, 0xff]) {
        return jpeg_dimensions_from_bytes(bytes);
    }
    (None, None)
}

fn jpeg_dimensions_from_bytes(bytes: &[u8]) -> (Option<u32>, Option<u32>) {
    let mut offset = 2usize;
    while offset + 9 < bytes.len() {
        if bytes[offset] != 0xff {
            offset += 1;
            continue;
        }
        while offset < bytes.len() && bytes[offset] == 0xff {
            offset += 1;
        }
        if offset >= bytes.len() {
            break;
        }
        let marker = bytes[offset];
        offset += 1;
        if matches!(marker, 0xd8 | 0xd9) {
            continue;
        }
        if offset + 2 > bytes.len() {
            break;
        }
        let segment_len = u16::from_be_bytes([bytes[offset], bytes[offset + 1]]) as usize;
        if segment_len < 2 || offset + segment_len > bytes.len() {
            break;
        }
        if matches!(
            marker,
            0xc0 | 0xc1
                | 0xc2
                | 0xc3
                | 0xc5
                | 0xc6
                | 0xc7
                | 0xc9
                | 0xca
                | 0xcb
                | 0xcd
                | 0xce
                | 0xcf
        ) && segment_len >= 7
        {
            let height = u16::from_be_bytes([bytes[offset + 3], bytes[offset + 4]]) as u32;
            let width = u16::from_be_bytes([bytes[offset + 5], bytes[offset + 6]]) as u32;
            return (Some(width), Some(height));
        }
        offset += segment_len;
    }
    (None, None)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_paths_under_attachment_root_only() {
        let harness_home = std::env::temp_dir()
            .join("agent-harness-core-media-test")
            .join(".agent-harness");
        let root = inbound_media_attachment_root(&harness_home);
        let inside = root.join("queue-1").join("0.jpg");
        let outside = harness_home
            .join("state")
            .join("channels")
            .join("other")
            .join("0.jpg");

        assert_eq!(
            validate_inbound_media_artifact_paths(
                &harness_home,
                &[InboundMediaArtifact {
                    platform: "telegram".to_string(),
                    kind: "photo".to_string(),
                    local_path: Some(inside),
                    source: "telegram.getFile".to_string(),
                    ..InboundMediaArtifact::default()
                }]
            ),
            Ok(())
        );

        assert!(
            validate_inbound_media_artifact_paths(
                &harness_home,
                &[InboundMediaArtifact {
                    platform: "telegram".to_string(),
                    kind: "photo".to_string(),
                    local_path: Some(outside),
                    source: "telegram.getFile".to_string(),
                    ..InboundMediaArtifact::default()
                }]
            )
            .is_err()
        );
    }

    #[test]
    fn prompt_rendering_uses_safe_relative_paths_and_redacts_provider_urls() {
        let harness_home = std::env::temp_dir()
            .join("agent-harness-core-media-test-render")
            .join(".agent-harness");
        let local_path = inbound_media_attachment_root(&harness_home)
            .join("queue-1")
            .join("0.jpg");
        let artifact = InboundMediaArtifact {
            platform: "telegram".to_string(),
            kind: "photo".to_string(),
            message_id: Some("42".to_string()),
            variant_count: Some(4),
            selected_variant: Some(InboundMediaSelectedVariant {
                width: Some(961),
                height: Some(1280),
                file_size: Some(179414),
            }),
            local_path: Some(local_path),
            artifact_uri: Some("https://api.telegram.org/file/botTOKEN/photos/x.jpg".to_string()),
            mime: Some("image/jpeg".to_string()),
            sha256: Some("abc123".to_string()),
            source: "https://api.telegram.org/botTOKEN/getFile?file_id=secret".to_string(),
            download_status: InboundMediaDownloadStatus::Downloaded,
            model_attachment_status: InboundMediaModelAttachmentStatus::PromptOnly,
            warnings: vec!["file_id=secret".to_string()],
            ..InboundMediaArtifact::default()
        };

        let rendered = render_inbound_media_artifacts_for_prompt(&[artifact], Some(&harness_home));

        assert!(rendered.contains("## InboundMedia: Telegram attachments"));
        assert!(rendered.contains("localPath=state/channels/telegram-attachments/queue-1/0.jpg"));
        assert!(rendered.contains("mime=image/jpeg"));
        assert!(rendered.contains("sha256=abc123"));
        assert!(rendered.contains("width=961"));
        assert!(rendered.contains("height=1280"));
        assert!(rendered.contains("downloadStatus=downloaded"));
        assert!(rendered.contains("modelAttachmentStatus=prompt-only"));
        assert!(rendered.contains("artifactUri=redacted-provider-uri"));
        assert!(rendered.contains("source=redacted-source"));
        assert!(rendered.contains("warningsCount=1"));
        assert!(!rendered.contains("file_id=secret"));
        assert!(!rendered.contains("botTOKEN"));
        assert!(!rendered.contains("api.telegram.org/file"));
    }

    #[test]
    fn codex_media_planner_marks_downloaded_images_for_vision_tool_without_native_support() {
        let root = temp_media_root("codex_media_planner_marks_downloaded_images");
        let harness_home = root.join(".agent-harness");
        let attachment = inbound_media_attachment_root(&harness_home)
            .join("update-1")
            .join("0.png");
        fs::create_dir_all(attachment.parent().unwrap()).unwrap();
        fs::write(&attachment, png_header_bytes(2, 3)).unwrap();
        let artifact = InboundMediaArtifact {
            platform: "telegram".to_string(),
            kind: "photo".to_string(),
            local_path: Some(attachment),
            artifact_uri: Some("agent-harness://inbound-media/telegram/update-1/0.png".to_string()),
            mime: Some("image/png".to_string()),
            download_status: InboundMediaDownloadStatus::Downloaded,
            ..InboundMediaArtifact::default()
        };

        let plan = plan_inbound_media_inputs(
            InboundMediaInputPlanOptions {
                harness_home: harness_home.clone(),
                native_image_input_enabled: false,
                vision_tool_available: true,
            },
            &[artifact],
        );

        assert!(plan.native_input_parts.is_empty());
        assert_eq!(
            plan.artifacts[0].model_attachment_status,
            InboundMediaModelAttachmentStatus::VisionToolAvailable
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn codex_media_planner_model_attaches_only_when_native_enabled_and_path_contained() {
        let root = temp_media_root("codex_media_planner_model_attaches");
        let harness_home = root.join(".agent-harness");
        let attachment = inbound_media_attachment_root(&harness_home)
            .join("update-1")
            .join("0.png");
        fs::create_dir_all(attachment.parent().unwrap()).unwrap();
        fs::write(&attachment, png_header_bytes(2, 3)).unwrap();
        let outside = root.join("outside.png");
        fs::write(&outside, png_header_bytes(2, 3)).unwrap();
        let inside = InboundMediaArtifact {
            platform: "telegram".to_string(),
            kind: "photo".to_string(),
            local_path: Some(attachment.clone()),
            artifact_uri: Some("agent-harness://inbound-media/telegram/update-1/0.png".to_string()),
            mime: Some("image/png".to_string()),
            download_status: InboundMediaDownloadStatus::Downloaded,
            ..InboundMediaArtifact::default()
        };
        let outside = InboundMediaArtifact {
            platform: "telegram".to_string(),
            kind: "photo".to_string(),
            local_path: Some(outside),
            mime: Some("image/png".to_string()),
            download_status: InboundMediaDownloadStatus::Downloaded,
            ..InboundMediaArtifact::default()
        };

        let plan = plan_inbound_media_inputs(
            InboundMediaInputPlanOptions {
                harness_home: harness_home.clone(),
                native_image_input_enabled: true,
                vision_tool_available: true,
            },
            &[inside, outside],
        );

        assert_eq!(plan.native_input_parts.len(), 1);
        assert_eq!(plan.native_input_parts[0].local_path, attachment);
        assert_eq!(
            plan.artifacts[0].model_attachment_status,
            InboundMediaModelAttachmentStatus::ModelAttached
        );
        assert_eq!(
            plan.artifacts[1].model_attachment_status,
            InboundMediaModelAttachmentStatus::PromptOnly
        );
        assert_eq!(plan.warnings.len(), 1);

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn codex_media_vision_analysis_resolves_artifact_uri_and_reads_png_dimensions() {
        let root = temp_media_root("codex_media_vision_analysis");
        let harness_home = root.join(".agent-harness");
        let attachment = inbound_media_attachment_root(&harness_home)
            .join("update-1")
            .join("0.png");
        fs::create_dir_all(attachment.parent().unwrap()).unwrap();
        fs::write(&attachment, png_header_bytes(9, 7)).unwrap();

        let analysis = analyze_inbound_media_file(
            &harness_home,
            "agent-harness://inbound-media/telegram/update-1/0.png",
            1024,
        )
        .unwrap();

        assert_eq!(analysis.mime.as_deref(), Some("image/png"));
        assert_eq!(analysis.width, Some(9));
        assert_eq!(analysis.height, Some(7));
        assert_eq!(analysis.byte_len, 24);

        let outside = root.join("outside.png");
        fs::write(&outside, png_header_bytes(1, 1)).unwrap();
        assert!(
            analyze_inbound_media_file(&harness_home, outside.to_str().unwrap(), 1024).is_err()
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn media_safety_reports_limits_cache_quota_and_prompt_redaction() {
        let root = temp_media_root("media_safety_reports_limits_cache_quota");
        let harness_home = root.join(".agent-harness");
        let inside = inbound_media_attachment_root(&harness_home)
            .join("update-1")
            .join("0.png");
        fs::create_dir_all(inside.parent().unwrap()).unwrap();
        fs::write(&inside, png_header_bytes(3, 4)).unwrap();
        let outside = root.join("outside.exe");
        fs::write(&outside, b"not image").unwrap();
        let artifacts = vec![
            InboundMediaArtifact {
                platform: "telegram".to_string(),
                kind: "photo".to_string(),
                local_path: Some(inside.clone()),
                artifact_uri: Some(
                    "agent-harness://inbound-media/telegram/update-1/0.png".to_string(),
                ),
                mime: Some("image/png".to_string()),
                byte_len: Some(24),
                download_status: InboundMediaDownloadStatus::Downloaded,
                source: "telegram.getFile".to_string(),
                ..InboundMediaArtifact::default()
            },
            InboundMediaArtifact {
                platform: "telegram".to_string(),
                kind: "document".to_string(),
                local_path: Some(outside),
                artifact_uri: Some("https://api.telegram.org/file/botTOKEN/secret".to_string()),
                mime: Some("application/x-msdownload".to_string()),
                byte_len: Some(99),
                source: "https://api.telegram.org/botTOKEN/getFile?file_id=secret".to_string(),
                download_status: InboundMediaDownloadStatus::Downloaded,
                ..InboundMediaArtifact::default()
            },
        ];

        let report = validate_inbound_media_safety(
            &harness_home,
            &artifacts,
            InboundMediaSafetyPolicy {
                max_items_per_turn: 1,
                max_bytes_per_item: 32,
            },
        );

        assert!(!report.within_limits);
        assert!(
            report
                .violations
                .iter()
                .any(|violation| violation.contains("artifact count"))
        );
        assert!(
            report
                .violations
                .iter()
                .any(|violation| violation.contains("outside attachment root"))
        );
        assert!(
            report
                .violations
                .iter()
                .any(|violation| violation.contains("MIME"))
        );
        let cache = collect_inbound_media_cache_report(&harness_home).unwrap();
        assert_eq!(cache.file_count, 1);
        assert_eq!(cache.total_bytes, 24);
        let rendered = render_inbound_media_artifacts_for_prompt(&artifacts, Some(&harness_home));
        assert!(rendered.contains("artifactUri=redacted-provider-uri"));
        assert!(rendered.contains("source=redacted-source"));
        assert!(!rendered.contains("botTOKEN"));
        assert!(!rendered.contains("file_id=secret"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn discord_text_artifact_uri_resolves_and_passes_generic_safety() {
        let root = temp_media_root("discord_text_artifact_uri_resolves_and_passes_generic_safety");
        let harness_home = root.join(".agent-harness");
        let attachment = inbound_media_attachment_root(&harness_home)
            .join("discord")
            .join("message-1")
            .join("0.txt");
        fs::create_dir_all(attachment.parent().unwrap()).unwrap();
        fs::write(&attachment, b"bounded text").unwrap();
        let artifacts = vec![InboundMediaArtifact {
            platform: "discord".to_string(),
            kind: "attachment-text".to_string(),
            local_path: Some(attachment.clone()),
            artifact_uri: Some("agent-harness://inbound-media/discord/message-1/0.txt".to_string()),
            mime: Some("text/plain".to_string()),
            byte_len: Some(12),
            source: "discord.attachment".to_string(),
            download_status: InboundMediaDownloadStatus::Downloaded,
            model_attachment_status: InboundMediaModelAttachmentStatus::PromptOnly,
            ..InboundMediaArtifact::default()
        }];

        let resolved = resolve_inbound_media_artifact_reference(
            &harness_home,
            "agent-harness://inbound-media/discord/message-1/0.txt",
        )
        .unwrap();
        assert_eq!(resolved, attachment);

        let report = validate_inbound_media_safety(
            &harness_home,
            &artifacts,
            InboundMediaSafetyPolicy::default(),
        );
        assert!(report.within_limits, "{:?}", report.violations);

        let rendered = render_inbound_media_artifacts_for_prompt(&artifacts, Some(&harness_home));
        assert!(
            rendered.contains("artifactUri=agent-harness://inbound-media/discord/message-1/0.txt")
        );
        assert!(
            rendered
                .contains("localPath=state/channels/telegram-attachments/discord/message-1/0.txt")
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn artifact_prompt_hygiene_renders_generic_bounded_summaries_not_payloads() {
        let root = temp_media_root("artifact_prompt_hygiene_renders_generic_summaries");
        let harness_home = root.join(".agent-harness");
        let attachment_root = inbound_media_attachment_root(&harness_home).join("queue-artifacts");
        fs::create_dir_all(&attachment_root).unwrap();
        let huge_base64 = "A".repeat(220);
        let artifacts = vec![
            generic_artifact(
                &attachment_root,
                "0.jpg",
                "photo",
                "image",
                "image/jpeg",
                "Subject pose, wardrobe, composition, and style constraints extracted.",
            ),
            generic_artifact(
                &attachment_root,
                "1.wav",
                "voice-memo",
                "audio-transcript",
                "audio/wav",
                "Two speakers discussed the release gate and one follow-up action.",
            ),
            generic_artifact(
                &attachment_root,
                "2.mp3",
                "generated-speech",
                "generated-speech",
                "audio/mpeg",
                "Generated speech output stored as media with only duration and intent summarized.",
            ),
            generic_artifact(
                &attachment_root,
                "3.png",
                "browser-capture",
                "browser-capture",
                "image/png",
                "Browser capture summarized by title, capture time, and relevant claims.",
            ),
            generic_artifact(
                &attachment_root,
                "4.pdf",
                "downloaded-document",
                "downloaded-document",
                "application/pdf",
                "Downloaded document summarized by title, source label, and extracted claims.",
            ),
            generic_artifact(
                &attachment_root,
                "5.log",
                "tool-log",
                "tool-log",
                "text/plain",
                "Large tool log summarized by command, exit status, findings, and next action.",
            ),
            generic_artifact(
                &attachment_root,
                "6.json",
                "worker-report",
                "worker-report",
                "application/json",
                "Worker dataset and report summarized with artifact pointers and result counts.",
            ),
            InboundMediaArtifact {
                platform: "discord".to_string(),
                kind: "provider-native-media".to_string(),
                artifact_uri: Some(
                    "https://cdn.discordapp.com/attachments/private/raw.png".to_string(),
                ),
                mime: Some("image/png".to_string()),
                sha256: Some("sha-provider".to_string()),
                byte_len: Some(4096),
                caption_preview: Some(format!("data:image/png;base64,{huge_base64}")),
                lifecycle_status: Some("summarized".to_string()),
                extraction_summary: Some(ArtifactExtractionSummary {
                    artifact_class: Some("provider-native-media".to_string()),
                    modality: Some("image".to_string()),
                    summary: Some(
                        "Provider-native attachment was copied into artifact storage before use."
                            .to_string(),
                    ),
                    facts: vec![
                        "raw provider URL intentionally withheld from main prompt".to_string(),
                    ],
                    uncertainty: Some("raw bytes require artifact lookup".to_string()),
                }),
                source: "https://cdn.discordapp.com/attachments/private/raw.png?token=secret"
                    .to_string(),
                download_status: InboundMediaDownloadStatus::Downloaded,
                model_attachment_status: InboundMediaModelAttachmentStatus::PromptOnly,
                warnings: vec!["Cookie: secret".to_string()],
                ..InboundMediaArtifact::default()
            },
        ];

        let rendered = render_inbound_media_artifacts_for_prompt(&artifacts, Some(&harness_home));

        for expected in [
            "artifactClass=image",
            "artifactClass=audio-transcript",
            "artifactClass=generated-speech",
            "artifactClass=browser-capture",
            "artifactClass=downloaded-document",
            "artifactClass=tool-log",
            "artifactClass=worker-report",
            "artifactClass=provider-native-media",
            "lifecycleStatus=summarized",
            "extractionSummary=Subject pose",
            "Large tool log summarized by command",
        ] {
            assert!(
                rendered.contains(expected),
                "missing `{expected}` in {rendered}"
            );
        }
        for forbidden in [
            "data:image",
            "base64",
            "cdn.discordapp.com",
            "token=secret",
            "Cookie: secret",
            &huge_base64,
        ] {
            assert!(
                !rendered.contains(forbidden),
                "artifact prompt hygiene leaked `{forbidden}` in {rendered}"
            );
        }
        assert!(rendered.contains("artifactUri=redacted-provider-uri"));
        assert!(rendered.contains("captionPreview=redacted-artifact-payload"));
        assert!(rendered.contains("source=redacted-source"));

        let _ = fs::remove_dir_all(root);
    }

    fn generic_artifact(
        attachment_root: &Path,
        file_name: &str,
        kind: &str,
        artifact_class: &str,
        mime: &str,
        summary: &str,
    ) -> InboundMediaArtifact {
        let local_path = attachment_root.join(file_name);
        fs::write(&local_path, b"artifact placeholder").unwrap();
        InboundMediaArtifact {
            platform: "telegram".to_string(),
            kind: kind.to_string(),
            local_path: Some(local_path),
            artifact_uri: Some(format!(
                "agent-harness://inbound-media/telegram/queue-artifacts/{file_name}"
            )),
            mime: Some(mime.to_string()),
            sha256: Some(format!("sha-{file_name}")),
            byte_len: Some(20),
            lifecycle_status: Some("summarized".to_string()),
            extraction_summary: Some(ArtifactExtractionSummary {
                artifact_class: Some(artifact_class.to_string()),
                modality: Some(kind.to_string()),
                summary: Some(summary.to_string()),
                facts: vec![
                    format!("{artifact_class} bounded fact one"),
                    "artifact reference retained for raw inspection".to_string(),
                ],
                uncertainty: Some("details beyond summary require artifact lookup".to_string()),
            }),
            source: format!("{artifact_class}-artifact-store"),
            download_status: InboundMediaDownloadStatus::Downloaded,
            model_attachment_status: InboundMediaModelAttachmentStatus::PromptOnly,
            ..InboundMediaArtifact::default()
        }
    }

    fn temp_media_root(test_name: &str) -> PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "agent-harness-core-media-{test_name}-{}-{nanos}",
            std::process::id()
        ))
    }

    fn png_header_bytes(width: u32, height: u32) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(b"\x89PNG\r\n\x1a\n");
        bytes.extend_from_slice(&[0, 0, 0, 13]);
        bytes.extend_from_slice(b"IHDR");
        bytes.extend_from_slice(&width.to_be_bytes());
        bytes.extend_from_slice(&height.to_be_bytes());
        bytes.truncate(24);
        bytes
    }
}
