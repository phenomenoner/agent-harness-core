use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

use ring::digest;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
    ChannelOutboundAttachmentKind, append_jsonl_value, config::harness_config_candidates,
    current_log_time_ms, inbound_media_attachment_root,
};

pub const OUTBOUND_MEDIA_POLICY_SCHEMA: &str = "agent-harness.outbound-media-policy.v1";
pub const DEFAULT_OUTBOUND_MEDIA_MAX_MB_PER_ATTACHMENT: u64 = 50;
pub const DEFAULT_OUTBOUND_MEDIA_TRUST_RECENT_SECONDS: u64 = 600;

const IMAGE_EXTENSIONS: &[(&str, &str)] = &[
    ("png", "image/png"),
    ("jpg", "image/jpeg"),
    ("jpeg", "image/jpeg"),
    ("gif", "image/gif"),
    ("webp", "image/webp"),
    ("bmp", "image/bmp"),
    ("tiff", "image/tiff"),
];

const VIDEO_EXTENSIONS: &[(&str, &str)] = &[
    ("mp4", "video/mp4"),
    ("mov", "video/quicktime"),
    ("webm", "video/webm"),
    ("mkv", "video/x-matroska"),
    ("avi", "video/x-msvideo"),
];

const AUDIO_EXTENSIONS: &[(&str, &str)] = &[
    ("mp3", "audio/mpeg"),
    ("wav", "audio/wav"),
    ("ogg", "audio/ogg"),
    ("opus", "audio/opus"),
    ("m4a", "audio/mp4"),
    ("flac", "audio/flac"),
];

const DOCUMENT_EXTENSIONS: &[(&str, &str)] = &[
    ("pdf", "application/pdf"),
    (
        "docx",
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    ),
    (
        "xlsx",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    ),
    (
        "pptx",
        "application/vnd.openxmlformats-officedocument.presentationml.presentation",
    ),
    ("csv", "text/csv"),
    ("tsv", "text/tab-separated-values"),
    ("txt", "text/plain"),
    ("md", "text/markdown"),
    ("log", "text/plain"),
    ("json", "application/json"),
    ("xml", "application/xml"),
    ("yaml", "application/yaml"),
    ("yml", "application/yaml"),
    ("html", "text/html"),
    ("zip", "application/zip"),
    ("tar", "application/x-tar"),
    ("gz", "application/gzip"),
    ("7z", "application/x-7z-compressed"),
];

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MediaDeliveryPolicy {
    pub max_mb_per_attachment: u64,
    pub allow_dirs: Vec<PathBuf>,
    pub trust_recent_seconds: Option<u64>,
    pub strict: bool,
}

impl Default for MediaDeliveryPolicy {
    fn default() -> Self {
        Self {
            max_mb_per_attachment: DEFAULT_OUTBOUND_MEDIA_MAX_MB_PER_ATTACHMENT,
            allow_dirs: Vec::new(),
            trust_recent_seconds: Some(DEFAULT_OUTBOUND_MEDIA_TRUST_RECENT_SECONDS),
            strict: false,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MediaDeliveryLintConfig {
    pub fail_closed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HarnessMediaConfig {
    pub policy: MediaDeliveryPolicy,
    pub lint: MediaDeliveryLintConfig,
    pub native_image_input: Option<bool>,
}

impl Default for HarnessMediaConfig {
    fn default() -> Self {
        Self {
            policy: MediaDeliveryPolicy::default(),
            lint: MediaDeliveryLintConfig { fail_closed: false },
            native_image_input: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MediaDeliveryEvaluation {
    pub path: PathBuf,
    pub path_hash: String,
    pub verdict: MediaDeliveryVerdict,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MediaDeliveryVerdict {
    Accepted {
        kind: ChannelOutboundAttachmentKind,
        mime: Option<String>,
        byte_len: u64,
    },
    Rejected {
        reason_code: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OutboundMediaPolicyReceipt {
    #[serde(default = "default_outbound_media_policy_schema")]
    pub schema: &'static str,
    pub at_ms: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub queue_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub platform: Option<String>,
    pub path_hash: String,
    pub verdict: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason_code: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<ChannelOutboundAttachmentKind>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mime: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub byte_len: Option<u64>,
}

fn default_outbound_media_policy_schema() -> &'static str {
    OUTBOUND_MEDIA_POLICY_SCHEMA
}

pub fn media_policy_receipts_file(harness_home: impl AsRef<Path>) -> PathBuf {
    harness_home
        .as_ref()
        .join("state")
        .join("channels")
        .join("outbound-media-policy-receipts.jsonl")
}

pub fn load_harness_media_config(harness_home: impl AsRef<Path>) -> io::Result<HarnessMediaConfig> {
    let mut config = HarnessMediaConfig::default();
    let Some(config_file) = harness_config_candidates(harness_home)
        .into_iter()
        .find(|path| path.is_file())
    else {
        return Ok(config);
    };
    let text = fs::read_to_string(config_file)?;
    let value: Value = serde_json::from_str(&text)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    let Some(media) = value.get("media").and_then(Value::as_object) else {
        return Ok(config);
    };
    if let Some(max_mb) = media
        .get("maxMbPerAttachment")
        .or_else(|| media.get("max_mb_per_attachment"))
        .and_then(Value::as_u64)
        .filter(|value| *value > 0)
    {
        config.policy.max_mb_per_attachment = max_mb;
    }
    if let Some(strict) = media.get("strict").and_then(Value::as_bool) {
        config.policy.strict = strict;
    }
    if let Some(trust_recent) = media
        .get("trustRecentSeconds")
        .or_else(|| media.get("trust_recent_seconds"))
    {
        config.policy.trust_recent_seconds = if trust_recent.is_null() {
            None
        } else {
            trust_recent.as_u64()
        };
    }
    if let Some(allow_dirs) = media
        .get("allowDirs")
        .or_else(|| media.get("allow_dirs"))
        .and_then(Value::as_array)
    {
        config.policy.allow_dirs = allow_dirs
            .iter()
            .filter_map(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(PathBuf::from)
            .collect();
    }
    if let Some(lint_fail_closed) = media
        .get("lintFailClosed")
        .or_else(|| media.get("lint_fail_closed"))
        .and_then(Value::as_bool)
    {
        config.lint.fail_closed = lint_fail_closed;
    }
    config.native_image_input = media
        .get("nativeImageInput")
        .or_else(|| media.get("native_image_input"))
        .and_then(Value::as_bool);
    Ok(config)
}

pub fn attachment_kind_from_extension(extension: &str) -> Option<ChannelOutboundAttachmentKind> {
    let extension = extension.trim_start_matches('.').to_ascii_lowercase();
    if IMAGE_EXTENSIONS.iter().any(|(ext, _)| *ext == extension) {
        Some(ChannelOutboundAttachmentKind::Image)
    } else if VIDEO_EXTENSIONS.iter().any(|(ext, _)| *ext == extension) {
        Some(ChannelOutboundAttachmentKind::Video)
    } else if AUDIO_EXTENSIONS.iter().any(|(ext, _)| *ext == extension) {
        Some(ChannelOutboundAttachmentKind::Audio)
    } else if DOCUMENT_EXTENSIONS.iter().any(|(ext, _)| *ext == extension) {
        Some(ChannelOutboundAttachmentKind::Document)
    } else {
        None
    }
}

pub fn attachment_kind_from_path(path: &Path) -> Option<ChannelOutboundAttachmentKind> {
    path.extension()
        .and_then(|extension| extension.to_str())
        .and_then(attachment_kind_from_extension)
}

pub fn attachment_mime_from_path(path: &Path) -> Option<String> {
    let extension = path
        .extension()
        .and_then(|extension| extension.to_str())?
        .trim_start_matches('.')
        .to_ascii_lowercase();
    all_deliverable_extensions()
        .iter()
        .find_map(|(ext, mime)| (*ext == extension).then(|| (*mime).to_string()))
}

pub fn is_deliverable_media_path(path: &Path) -> bool {
    attachment_kind_from_path(path).is_some()
}

pub fn all_deliverable_extensions() -> Vec<(&'static str, &'static str)> {
    IMAGE_EXTENSIONS
        .iter()
        .chain(VIDEO_EXTENSIONS.iter())
        .chain(AUDIO_EXTENSIONS.iter())
        .chain(DOCUMENT_EXTENSIONS.iter())
        .copied()
        .collect()
}

pub fn evaluate_outbound_media_path(
    harness_home: impl AsRef<Path>,
    path: &Path,
    policy: &MediaDeliveryPolicy,
    forced_kind: Option<ChannelOutboundAttachmentKind>,
) -> MediaDeliveryEvaluation {
    let harness_home = harness_home.as_ref();
    let path_hash = path_hash(path);
    let normalized_path = normalize_existing_or_raw_path(path);
    let reject = |reason_code: &str| MediaDeliveryEvaluation {
        path: path.to_path_buf(),
        path_hash: path_hash.clone(),
        verdict: MediaDeliveryVerdict::Rejected {
            reason_code: reason_code.to_string(),
        },
    };

    if !path.is_absolute() {
        return reject("not-absolute");
    }
    let Some(base_kind) = attachment_kind_from_path(path) else {
        return reject("unsupported-extension");
    };
    let kind = forced_kind.unwrap_or(base_kind);
    let metadata = match fs::metadata(path) {
        Ok(metadata) => metadata,
        Err(_) => return reject("not-found"),
    };
    if !metadata.is_file() {
        return reject("not-regular-file");
    }
    if fs::File::open(path).is_err() {
        return reject("unreadable");
    }
    if is_denied_media_path(harness_home, &normalized_path) {
        return reject("denied-prefix");
    }
    let max_bytes = policy.max_mb_per_attachment.saturating_mul(1024 * 1024);
    if metadata.len() > max_bytes {
        return reject("size-cap");
    }
    if !is_under_any_root(&normalized_path, &safe_media_roots(harness_home, policy))
        && !fresh_enough(&metadata, policy)
    {
        return reject("outside-safe-roots");
    }
    MediaDeliveryEvaluation {
        path: path.to_path_buf(),
        path_hash,
        verdict: MediaDeliveryVerdict::Accepted {
            kind,
            mime: attachment_mime_from_path(path),
            byte_len: metadata.len(),
        },
    }
}

pub fn write_media_policy_receipt(
    harness_home: impl AsRef<Path>,
    queue_id: Option<&str>,
    platform: Option<&str>,
    evaluation: &MediaDeliveryEvaluation,
) -> io::Result<()> {
    let (verdict, reason_code, kind, mime, byte_len) = match &evaluation.verdict {
        MediaDeliveryVerdict::Accepted {
            kind,
            mime,
            byte_len,
        } => (
            "accepted".to_string(),
            None,
            Some(*kind),
            mime.clone(),
            Some(*byte_len),
        ),
        MediaDeliveryVerdict::Rejected { reason_code } => (
            "rejected".to_string(),
            Some(reason_code.clone()),
            None,
            None,
            None,
        ),
    };
    append_jsonl_value(
        &media_policy_receipts_file(harness_home),
        &OutboundMediaPolicyReceipt {
            schema: OUTBOUND_MEDIA_POLICY_SCHEMA,
            at_ms: current_log_time_ms().unwrap_or(0),
            queue_id: queue_id.map(ToString::to_string),
            platform: platform.map(ToString::to_string),
            path_hash: evaluation.path_hash.clone(),
            verdict,
            reason_code,
            kind,
            mime,
            byte_len,
        },
    )
}

fn safe_media_roots(harness_home: &Path, policy: &MediaDeliveryPolicy) -> Vec<PathBuf> {
    let mut roots = vec![
        harness_home.join("workspace"),
        harness_home.join("codex-home").join("generated_images"),
        inbound_media_attachment_root(harness_home),
        harness_home.join("state").join("rich-presentation"),
        harness_home.join("state").join("generated-media"),
        harness_home.join("state").join("media-exports"),
    ];
    roots.extend(policy.allow_dirs.iter().cloned());
    roots
        .into_iter()
        .map(|root| normalize_existing_or_raw_path(&root))
        .collect()
}

fn is_denied_media_path(harness_home: &Path, path: &Path) -> bool {
    let normalized_harness = normalize_existing_or_raw_path(harness_home);
    let inbound_root = normalize_existing_or_raw_path(&inbound_media_attachment_root(harness_home));
    let allowed_state_roots = [
        inbound_root,
        normalize_existing_or_raw_path(&harness_home.join("state").join("rich-presentation")),
        normalize_existing_or_raw_path(&harness_home.join("state").join("generated-media")),
        normalize_existing_or_raw_path(&harness_home.join("state").join("media-exports")),
    ];
    let state_root = normalize_existing_or_raw_path(&harness_home.join("state"));
    if path_starts_with(path, &state_root)
        && !allowed_state_roots
            .iter()
            .any(|root| path_starts_with(path, root))
    {
        return true;
    }
    let codex_home = normalize_existing_or_raw_path(&harness_home.join("codex-home"));
    let generated_images =
        normalize_existing_or_raw_path(&harness_home.join("codex-home").join("generated_images"));
    if path_starts_with(path, &codex_home) && !path_starts_with(path, &generated_images) {
        return true;
    }
    for sensitive in [
        normalized_harness.join("harness-config.json"),
        normalized_harness
            .join("config")
            .join("harness-config.json"),
    ] {
        if same_path(path, &sensitive) {
            return true;
        }
    }
    if let Some(home) = user_profile_home() {
        for sensitive in [home.join(".ssh"), home.join(".aws"), home.join(".gnupg")] {
            if path_starts_with(path, &normalize_existing_or_raw_path(&sensitive)) {
                return true;
            }
        }
    }
    path.file_name()
        .and_then(|name| name.to_str())
        .map(|name| {
            let name = name.to_ascii_lowercase();
            name == ".env"
                || name.ends_with(".pem")
                || name.ends_with(".key")
                || name.ends_with(".pfx")
                || name.contains("credential")
                || name.contains("secret")
                || name.contains("token")
        })
        .unwrap_or(false)
}

fn fresh_enough(metadata: &fs::Metadata, policy: &MediaDeliveryPolicy) -> bool {
    if policy.strict {
        return false;
    }
    let Some(seconds) = policy.trust_recent_seconds else {
        return false;
    };
    let window = Duration::from_secs(seconds);
    [metadata.modified(), metadata.created()]
        .into_iter()
        .flatten()
        .any(|time| {
            SystemTime::now()
                .duration_since(time)
                .map(|age| age <= window)
                .unwrap_or(false)
        })
}

fn is_under_any_root(path: &Path, roots: &[PathBuf]) -> bool {
    roots.iter().any(|root| path_starts_with(path, root))
}

fn path_starts_with(path: &Path, root: &Path) -> bool {
    let path = normalize_path_string(path);
    let root = normalize_path_string(root);
    path == root || path.starts_with(&(root + "\\"))
}

fn same_path(left: &Path, right: &Path) -> bool {
    normalize_path_string(left) == normalize_path_string(right)
}

fn normalize_existing_or_raw_path(path: &Path) -> PathBuf {
    fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf())
}

fn normalize_path_string(path: &Path) -> String {
    path.to_string_lossy()
        .replace('/', "\\")
        .trim_end_matches('\\')
        .to_ascii_lowercase()
}

fn user_profile_home() -> Option<PathBuf> {
    std::env::var_os("USERPROFILE")
        .or_else(|| std::env::var_os("HOME"))
        .map(PathBuf::from)
}

fn path_hash(path: &Path) -> String {
    let digest = digest::digest(&digest::SHA256, path.to_string_lossy().as_bytes());
    digest
        .as_ref()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_root(name: &str) -> PathBuf {
        let root = std::env::temp_dir().join(format!(
            "agent_harness_media_policy_{}_{}",
            name,
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).unwrap();
        root
    }

    #[test]
    fn deliverable_extension_table_covers_core_media_kinds() {
        assert_eq!(
            attachment_kind_from_extension("png"),
            Some(ChannelOutboundAttachmentKind::Image)
        );
        assert_eq!(
            attachment_kind_from_extension("mp4"),
            Some(ChannelOutboundAttachmentKind::Video)
        );
        assert_eq!(
            attachment_kind_from_extension("ogg"),
            Some(ChannelOutboundAttachmentKind::Audio)
        );
        assert_eq!(
            attachment_kind_from_extension("pdf"),
            Some(ChannelOutboundAttachmentKind::Document)
        );
        assert!(
            all_deliverable_extensions()
                .iter()
                .all(|(extension, _)| attachment_kind_from_extension(extension).is_some())
        );
    }

    #[test]
    fn policy_accepts_workspace_file_and_rejects_denied_state_file() {
        let harness_home = temp_root("accepts_workspace_rejects_state");
        let workspace_file = harness_home.join("workspace").join("result.png");
        fs::create_dir_all(workspace_file.parent().unwrap()).unwrap();
        fs::write(&workspace_file, b"png").unwrap();
        let accepted = evaluate_outbound_media_path(
            &harness_home,
            &workspace_file,
            &MediaDeliveryPolicy {
                strict: true,
                ..MediaDeliveryPolicy::default()
            },
            None,
        );
        assert!(matches!(
            accepted.verdict,
            MediaDeliveryVerdict::Accepted {
                kind: ChannelOutboundAttachmentKind::Image,
                ..
            }
        ));

        let state_file = harness_home
            .join("state")
            .join("channels")
            .join("secret.png");
        fs::create_dir_all(state_file.parent().unwrap()).unwrap();
        fs::write(&state_file, b"secret").unwrap();
        let rejected = evaluate_outbound_media_path(
            &harness_home,
            &state_file,
            &MediaDeliveryPolicy {
                allow_dirs: vec![harness_home.join("state")],
                ..MediaDeliveryPolicy::default()
            },
            None,
        );
        assert_eq!(
            rejected.verdict,
            MediaDeliveryVerdict::Rejected {
                reason_code: "denied-prefix".to_string()
            }
        );
        let _ = fs::remove_dir_all(harness_home);
    }
}
