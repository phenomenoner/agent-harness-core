use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};

#[cfg(test)]
use std::cell::RefCell;
#[cfg(test)]
use std::collections::BTreeMap;

use agent_harness_core::{
    ArtifactExtractionSummary, DEFAULT_INBOUND_MEDIA_MAX_BYTES_PER_ITEM,
    DEFAULT_INBOUND_MEDIA_MAX_ITEMS_PER_TURN, InboundMediaArtifact, InboundMediaDownloadStatus,
    InboundMediaModelAttachmentStatus, InboundMediaSelectedVariant, append_jsonl_value,
    inbound_media_attachment_root,
};
use ring::digest;
use serde::{Deserialize, Serialize};
use serde_json::Value;

const TELEGRAM_MEDIA_RECEIPT_SCHEMA: &str = "agent-harness.telegram-media-ingest.v1";
const TELEGRAM_MEDIA_GROUP_STATE_SCHEMA: &str = "agent-harness.telegram-media-group-state.v1";
const TELEGRAM_MEDIA_GROUP_RECEIPT_SCHEMA: &str = "agent-harness.telegram-media-group.v1";
pub const DEFAULT_TELEGRAM_MEDIA_GROUP_DEBOUNCE_MS: i64 = 800;
pub const DEFAULT_TELEGRAM_MEDIA_GROUP_STALE_MS: i64 = 60_000;

pub trait TelegramMediaFetcher {
    fn get_file_path(&self, file_id: &str) -> Result<String, String>;
    fn download_file(&self, file_path: &str) -> Result<Vec<u8>, String>;
}

pub struct TelegramBotApiMediaFetcher<'a> {
    token: &'a str,
}

impl<'a> TelegramBotApiMediaFetcher<'a> {
    pub fn new(token: &'a str) -> Self {
        Self { token }
    }
}

impl TelegramMediaFetcher for TelegramBotApiMediaFetcher<'_> {
    fn get_file_path(&self, file_id: &str) -> Result<String, String> {
        let url = format!("https://api.telegram.org/bot{}/getFile", self.token);
        let response = crate::channel_http_short_agent()
            .post(&url)
            .send_json(serde_json::json!({ "file_id": file_id }))
            .map_err(crate::telegram_http_error)?;
        let value: Value = response
            .into_json()
            .map_err(|err| format!("Telegram getFile response was not JSON: {err}"))?;
        if value.get("ok").and_then(Value::as_bool) != Some(true) {
            return Err("Telegram getFile returned ok=false".to_string());
        }
        value
            .get("result")
            .and_then(|result| result.get("file_path"))
            .and_then(Value::as_str)
            .map(ToString::to_string)
            .ok_or_else(|| "Telegram getFile response had no file_path".to_string())
    }

    fn download_file(&self, file_path: &str) -> Result<Vec<u8>, String> {
        let url = format!(
            "https://api.telegram.org/file/bot{}/{}",
            self.token, file_path
        );
        let response = crate::channel_http_short_agent()
            .get(&url)
            .call()
            .map_err(crate::telegram_http_error)?;
        let mut bytes = Vec::new();
        response
            .into_reader()
            .read_to_end(&mut bytes)
            .map_err(|err| format!("Telegram file download read failed: {err}"))?;
        Ok(bytes)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TelegramMediaIngestReport {
    pub artifacts: Vec<InboundMediaArtifact>,
    pub receipt_file: PathBuf,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
struct TelegramMediaReceipt {
    schema: &'static str,
    update_id: i64,
    artifact_count: usize,
    artifacts: Vec<InboundMediaArtifact>,
    warnings: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TelegramMediaGroupDecision {
    Buffered(TelegramMediaGroupReceipt),
    Flush(TelegramMediaGroupFlush),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TelegramMediaGroupFlush {
    pub account_id: String,
    pub chat_id: String,
    pub media_group_id: String,
    pub status: TelegramMediaGroupStatus,
    pub members: Vec<TelegramMediaGroupMember>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TelegramMediaGroupMember {
    pub update_id: i64,
    pub message_id: Option<String>,
    pub caption_preview: Option<String>,
    pub message: Value,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum TelegramMediaGroupStatus {
    GroupBuffered,
    GroupFlushed,
    GroupStaleFlushed,
    GroupDiscardedNoAgent,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct TelegramMediaGroupState {
    schema: String,
    account_id: String,
    chat_id: String,
    media_group_id: String,
    first_seen_ms: i64,
    last_seen_ms: i64,
    members: Vec<TelegramMediaGroupMember>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TelegramMediaGroupReceipt {
    schema: String,
    status: TelegramMediaGroupStatus,
    account_id: String,
    chat_id: String,
    media_group_id: String,
    member_count: usize,
    state_file: PathBuf,
    reason: String,
}

#[derive(Debug, Clone)]
struct TelegramMediaCandidate {
    kind: String,
    file_id: String,
    message_id: Option<String>,
    media_group_id: Option<String>,
    variant_count: Option<usize>,
    selected_variant: Option<InboundMediaSelectedVariant>,
    expected_mime: Option<String>,
    caption_preview: Option<String>,
    provenance: Option<String>,
    source: String,
    requires_image_validation: bool,
}

pub fn ingest_telegram_media<F: TelegramMediaFetcher>(
    harness_home: &Path,
    update_id: i64,
    message: &Value,
    fetcher: &F,
) -> Result<TelegramMediaIngestReport, String> {
    let receipt_file = harness_home
        .join("state")
        .join("channels")
        .join("telegram-media-receipts.jsonl");
    let mut warnings = Vec::new();
    let mut artifacts = skipped_media_artifacts(message);
    let candidates = telegram_media_candidates(message);
    let attachment_root = inbound_media_attachment_root(harness_home);
    let update_dir = attachment_root.join(format!("update-{update_id}"));

    for (index, candidate) in candidates.iter().enumerate() {
        if artifacts.len() >= DEFAULT_INBOUND_MEDIA_MAX_ITEMS_PER_TURN {
            warnings.push(format!(
                "Telegram media item limit reached at maxItemsPerTurn={DEFAULT_INBOUND_MEDIA_MAX_ITEMS_PER_TURN}; remaining items were not downloaded"
            ));
            break;
        }
        let artifact = ingest_candidate(&update_dir, update_id, index, candidate, fetcher);
        if artifact.download_status != InboundMediaDownloadStatus::Downloaded {
            warnings.push(format!(
                "Telegram media item {index} kind={} was not downloaded: {}",
                candidate.kind,
                artifact
                    .warnings
                    .first()
                    .map(String::as_str)
                    .unwrap_or("unknown media ingest failure")
            ));
        }
        artifacts.push(artifact);
    }

    if !artifacts.is_empty() {
        if let Some(parent) = receipt_file.parent() {
            fs::create_dir_all(parent).map_err(|err| err.to_string())?;
        }
        append_jsonl_value(
            &receipt_file,
            &TelegramMediaReceipt {
                schema: TELEGRAM_MEDIA_RECEIPT_SCHEMA,
                update_id,
                artifact_count: artifacts.len(),
                artifacts: artifacts.clone(),
                warnings: warnings.clone(),
            },
        )
        .map_err(|err| err.to_string())?;
    }

    Ok(TelegramMediaIngestReport {
        artifacts,
        receipt_file,
        warnings,
    })
}

pub fn buffer_telegram_media_group(
    harness_home: &Path,
    account_id: &str,
    chat_id: &str,
    media_group_id: &str,
    update_id: i64,
    message: &Value,
    now_ms: i64,
    debounce_ms: i64,
) -> Result<TelegramMediaGroupDecision, String> {
    let state_file =
        telegram_media_group_state_file(harness_home, account_id, chat_id, media_group_id);
    let mut state = if state_file.is_file() {
        let bytes = fs::read(&state_file).map_err(|err| err.to_string())?;
        serde_json::from_slice::<TelegramMediaGroupState>(&bytes)
            .unwrap_or_else(|_| new_media_group_state(account_id, chat_id, media_group_id, now_ms))
    } else {
        new_media_group_state(account_id, chat_id, media_group_id, now_ms)
    };
    state.last_seen_ms = now_ms;
    if !state
        .members
        .iter()
        .any(|member| member.update_id == update_id)
    {
        state.members.push(TelegramMediaGroupMember {
            update_id,
            message_id: telegram_id_string(message.get("message_id")),
            caption_preview: telegram_caption_preview(message),
            message: message.clone(),
        });
        sort_media_group_members(&mut state.members);
    }

    if now_ms.saturating_sub(state.first_seen_ms) >= debounce_ms {
        let flush = TelegramMediaGroupFlush {
            account_id: state.account_id.clone(),
            chat_id: state.chat_id.clone(),
            media_group_id: state.media_group_id.clone(),
            status: TelegramMediaGroupStatus::GroupFlushed,
            members: state.members.clone(),
        };
        let _ = fs::remove_file(&state_file);
        write_media_group_receipt(
            harness_home,
            TelegramMediaGroupStatus::GroupFlushed,
            &state,
            &state_file,
            "media group debounce elapsed",
        )?;
        return Ok(TelegramMediaGroupDecision::Flush(flush));
    }

    if let Some(parent) = state_file.parent() {
        fs::create_dir_all(parent).map_err(|err| err.to_string())?;
    }
    fs::write(
        &state_file,
        serde_json::to_vec_pretty(&state).map_err(|err| err.to_string())?,
    )
    .map_err(|err| err.to_string())?;
    let receipt = write_media_group_receipt(
        harness_home,
        TelegramMediaGroupStatus::GroupBuffered,
        &state,
        &state_file,
        "media group member buffered",
    )?;
    Ok(TelegramMediaGroupDecision::Buffered(receipt))
}

pub fn take_due_telegram_media_groups(
    harness_home: &Path,
    account_id: &str,
    now_ms: i64,
    debounce_ms: i64,
    stale_ms: i64,
) -> Result<Vec<TelegramMediaGroupFlush>, String> {
    let root = harness_home
        .join("state")
        .join("channels")
        .join("telegram-media-groups")
        .join(safe_path_segment(account_id));
    if !root.is_dir() {
        return Ok(Vec::new());
    }
    let mut flushes = Vec::new();
    collect_due_media_group_states(
        harness_home,
        &root,
        now_ms,
        debounce_ms,
        stale_ms,
        &mut flushes,
    )?;
    Ok(flushes)
}

fn collect_due_media_group_states(
    harness_home: &Path,
    dir: &Path,
    now_ms: i64,
    debounce_ms: i64,
    stale_ms: i64,
    flushes: &mut Vec<TelegramMediaGroupFlush>,
) -> Result<(), String> {
    for entry in fs::read_dir(dir).map_err(|err| err.to_string())? {
        let entry = entry.map_err(|err| err.to_string())?;
        let path = entry.path();
        if path.is_dir() {
            collect_due_media_group_states(
                harness_home,
                &path,
                now_ms,
                debounce_ms,
                stale_ms,
                flushes,
            )?;
            continue;
        }
        if path.extension().and_then(|ext| ext.to_str()) != Some("json") {
            continue;
        }
        let bytes = fs::read(&path).map_err(|err| err.to_string())?;
        let Ok(mut state) = serde_json::from_slice::<TelegramMediaGroupState>(&bytes) else {
            let _ = fs::remove_file(&path);
            continue;
        };
        let age_ms = now_ms.saturating_sub(state.last_seen_ms);
        if age_ms < debounce_ms {
            continue;
        }
        sort_media_group_members(&mut state.members);
        let status = if age_ms >= stale_ms {
            TelegramMediaGroupStatus::GroupStaleFlushed
        } else {
            TelegramMediaGroupStatus::GroupFlushed
        };
        let _ = fs::remove_file(&path);
        write_media_group_receipt(
            harness_home,
            status,
            &state,
            &path,
            if status == TelegramMediaGroupStatus::GroupStaleFlushed {
                "stale media group flushed after cleanup"
            } else {
                "media group debounce elapsed"
            },
        )?;
        flushes.push(TelegramMediaGroupFlush {
            account_id: state.account_id,
            chat_id: state.chat_id,
            media_group_id: state.media_group_id,
            status,
            members: state.members,
        });
    }
    Ok(())
}

pub fn record_telegram_media_group_discarded_no_agent(
    harness_home: &Path,
    flush: &TelegramMediaGroupFlush,
    reason: &str,
) -> Result<(), String> {
    let state = TelegramMediaGroupState {
        schema: TELEGRAM_MEDIA_GROUP_STATE_SCHEMA.to_string(),
        account_id: flush.account_id.clone(),
        chat_id: flush.chat_id.clone(),
        media_group_id: flush.media_group_id.clone(),
        first_seen_ms: 0,
        last_seen_ms: 0,
        members: flush.members.clone(),
    };
    let state_file = telegram_media_group_state_file(
        harness_home,
        &flush.account_id,
        &flush.chat_id,
        &flush.media_group_id,
    );
    write_media_group_receipt(
        harness_home,
        TelegramMediaGroupStatus::GroupDiscardedNoAgent,
        &state,
        &state_file,
        reason,
    )?;
    Ok(())
}

pub fn telegram_media_group_state_file(
    harness_home: &Path,
    account_id: &str,
    chat_id: &str,
    media_group_id: &str,
) -> PathBuf {
    harness_home
        .join("state")
        .join("channels")
        .join("telegram-media-groups")
        .join(safe_path_segment(account_id))
        .join(safe_path_segment(chat_id))
        .join(format!("{}.json", safe_path_segment(media_group_id)))
}

fn telegram_media_group_receipts_file(harness_home: &Path) -> PathBuf {
    harness_home
        .join("state")
        .join("channels")
        .join("telegram-media-group-receipts.jsonl")
}

fn new_media_group_state(
    account_id: &str,
    chat_id: &str,
    media_group_id: &str,
    now_ms: i64,
) -> TelegramMediaGroupState {
    TelegramMediaGroupState {
        schema: TELEGRAM_MEDIA_GROUP_STATE_SCHEMA.to_string(),
        account_id: account_id.to_string(),
        chat_id: chat_id.to_string(),
        media_group_id: media_group_id.to_string(),
        first_seen_ms: now_ms,
        last_seen_ms: now_ms,
        members: Vec::new(),
    }
}

fn write_media_group_receipt(
    harness_home: &Path,
    status: TelegramMediaGroupStatus,
    state: &TelegramMediaGroupState,
    state_file: &Path,
    reason: &str,
) -> Result<TelegramMediaGroupReceipt, String> {
    let receipt = TelegramMediaGroupReceipt {
        schema: TELEGRAM_MEDIA_GROUP_RECEIPT_SCHEMA.to_string(),
        status,
        account_id: state.account_id.clone(),
        chat_id: state.chat_id.clone(),
        media_group_id: state.media_group_id.clone(),
        member_count: state.members.len(),
        state_file: state_file.to_path_buf(),
        reason: reason.to_string(),
    };
    let receipt_file = telegram_media_group_receipts_file(harness_home);
    if let Some(parent) = receipt_file.parent() {
        fs::create_dir_all(parent).map_err(|err| err.to_string())?;
    }
    append_jsonl_value(&receipt_file, &receipt).map_err(|err| err.to_string())?;
    Ok(receipt)
}

fn sort_media_group_members(members: &mut [TelegramMediaGroupMember]) {
    members.sort_by(|left, right| {
        let left_message = left
            .message_id
            .as_deref()
            .and_then(|value| value.parse::<i64>().ok());
        let right_message = right
            .message_id
            .as_deref()
            .and_then(|value| value.parse::<i64>().ok());
        left_message
            .cmp(&right_message)
            .then_with(|| left.update_id.cmp(&right.update_id))
    });
}

fn safe_path_segment(value: &str) -> String {
    let mut out = String::new();
    for ch in value.chars() {
        if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.') {
            out.push(ch.to_ascii_lowercase());
        } else {
            out.push('_');
        }
    }
    if out.is_empty() {
        "unknown".to_string()
    } else {
        out
    }
}

fn ingest_candidate<F: TelegramMediaFetcher>(
    update_dir: &Path,
    update_id: i64,
    index: usize,
    candidate: &TelegramMediaCandidate,
    fetcher: &F,
) -> InboundMediaArtifact {
    let mut artifact = base_artifact(candidate);
    if let Some(file_size) = candidate
        .selected_variant
        .as_ref()
        .and_then(|variant| variant.file_size)
        && file_size > DEFAULT_INBOUND_MEDIA_MAX_BYTES_PER_ITEM
    {
        artifact.download_status = InboundMediaDownloadStatus::DownloadFailed;
        artifact.byte_len = Some(file_size);
        artifact.warnings.push(format!(
            "telegram media metadata exceeded maxBytesPerItem={DEFAULT_INBOUND_MEDIA_MAX_BYTES_PER_ITEM}"
        ));
        return artifact;
    }
    let file_path = match fetcher.get_file_path(&candidate.file_id) {
        Ok(file_path) => file_path,
        Err(_) => {
            artifact.download_status = InboundMediaDownloadStatus::DownloadFailed;
            artifact
                .warnings
                .push("telegram getFile failed".to_string());
            return artifact;
        }
    };
    let bytes = match fetcher.download_file(&file_path) {
        Ok(bytes) => bytes,
        Err(_) => {
            artifact.download_status = InboundMediaDownloadStatus::DownloadFailed;
            artifact
                .warnings
                .push("telegram file download failed".to_string());
            return artifact;
        }
    };
    if bytes.len() as u64 > DEFAULT_INBOUND_MEDIA_MAX_BYTES_PER_ITEM {
        artifact.download_status = InboundMediaDownloadStatus::DownloadFailed;
        artifact.byte_len = Some(bytes.len() as u64);
        artifact.warnings.push(format!(
            "downloaded media exceeded maxBytesPerItem={DEFAULT_INBOUND_MEDIA_MAX_BYTES_PER_ITEM}"
        ));
        return artifact;
    }
    let image_mime = image_mime_from_bytes(&bytes);
    if candidate.requires_image_validation {
        if image_mime.is_none() {
            artifact.download_status = InboundMediaDownloadStatus::DownloadFailed;
            artifact.byte_len = Some(bytes.len() as u64);
            artifact
                .warnings
                .push("downloaded bytes failed image header validation".to_string());
            return artifact;
        }
        let detected_image_mime = image_mime.unwrap();
        if let Some(expected_mime) = candidate.expected_mime.as_deref()
            && expected_mime.starts_with("image/")
            && !image_mime_compatible(expected_mime, detected_image_mime)
        {
            artifact.download_status = InboundMediaDownloadStatus::DownloadFailed;
            artifact.byte_len = Some(bytes.len() as u64);
            artifact
                .warnings
                .push("downloaded image MIME did not match Telegram metadata".to_string());
            return artifact;
        }
    }
    let detected_mime = image_mime
        .map(ToString::to_string)
        .or_else(|| candidate.expected_mime.clone())
        .unwrap_or_else(|| "application/octet-stream".to_string());

    if let Err(_) = fs::create_dir_all(update_dir) {
        artifact.download_status = InboundMediaDownloadStatus::DownloadFailed;
        artifact
            .warnings
            .push("attachment cache directory could not be created".to_string());
        return artifact;
    }
    let extension = extension_for_file(
        &file_path,
        candidate.expected_mime.as_deref(),
        &detected_mime,
    );
    let local_path = update_dir.join(format!("{index}.{extension}"));
    if let Err(_) = fs::write(&local_path, &bytes) {
        artifact.download_status = InboundMediaDownloadStatus::DownloadFailed;
        artifact
            .warnings
            .push("attachment cache write failed".to_string());
        return artifact;
    }

    artifact.download_status = InboundMediaDownloadStatus::Downloaded;
    artifact.model_attachment_status = InboundMediaModelAttachmentStatus::PromptOnly;
    artifact.local_path = Some(local_path);
    artifact.artifact_uri = Some(format!(
        "agent-harness://inbound-media/telegram/update-{update_id}/{index}.{extension}"
    ));
    artifact.mime = Some(detected_mime.clone());
    artifact.sha256 = Some(sha256_hex(&bytes));
    artifact.byte_len = Some(bytes.len() as u64);
    artifact.extraction_summary =
        telegram_extraction_summary(&detected_mime, &bytes, &candidate.kind);
    artifact
}

fn base_artifact(candidate: &TelegramMediaCandidate) -> InboundMediaArtifact {
    InboundMediaArtifact {
        platform: "telegram".to_string(),
        kind: candidate.kind.clone(),
        media_group_id: candidate.media_group_id.clone(),
        message_id: candidate.message_id.clone(),
        variant_count: candidate.variant_count,
        selected_variant: candidate.selected_variant.clone(),
        mime: candidate.expected_mime.clone(),
        caption_preview: candidate.caption_preview.clone(),
        source: candidate.source.clone(),
        provenance: candidate.provenance.clone(),
        model_attachment_status: InboundMediaModelAttachmentStatus::PromptOnly,
        ..InboundMediaArtifact::default()
    }
}

fn telegram_media_candidates(message: &Value) -> Vec<TelegramMediaCandidate> {
    let mut candidates = Vec::new();
    if let Some(photos) = message.get("photo").and_then(Value::as_array)
        && let Some((_, photo)) = select_best_photo_variant(photos)
        && let Some(file_id) = photo.get("file_id").and_then(Value::as_str)
    {
        candidates.push(TelegramMediaCandidate {
            kind: "photo".to_string(),
            file_id: file_id.to_string(),
            message_id: telegram_id_string(message.get("message_id")),
            media_group_id: message
                .get("media_group_id")
                .and_then(Value::as_str)
                .map(ToString::to_string),
            variant_count: Some(photos.len()),
            selected_variant: Some(selected_variant(photo)),
            expected_mime: Some("image/jpeg".to_string()),
            caption_preview: telegram_caption_preview(message),
            provenance: None,
            source: "telegram.getFile".to_string(),
            requires_image_validation: true,
        });
    }

    if let Some(document) = message.get("document")
        && let Some(file_id) = document.get("file_id").and_then(Value::as_str)
    {
        let is_image = telegram_document_is_image(document);
        candidates.push(TelegramMediaCandidate {
            kind: if is_image {
                "document-image".to_string()
            } else {
                "document".to_string()
            },
            file_id: file_id.to_string(),
            message_id: telegram_id_string(message.get("message_id")),
            media_group_id: message
                .get("media_group_id")
                .and_then(Value::as_str)
                .map(ToString::to_string),
            variant_count: None,
            selected_variant: Some(selected_variant(document)),
            expected_mime: document
                .get("mime_type")
                .and_then(Value::as_str)
                .map(ToString::to_string),
            caption_preview: telegram_caption_preview(message),
            provenance: None,
            source: "telegram.getFile".to_string(),
            requires_image_validation: is_image,
        });
    }

    for (kind, default_mime) in [
        ("voice", Some("audio/ogg")),
        ("audio", None),
        ("video", Some("video/mp4")),
    ] {
        if let Some(value) = message.get(kind)
            && let Some(file_id) = value.get("file_id").and_then(Value::as_str)
        {
            candidates.push(TelegramMediaCandidate {
                kind: kind.to_string(),
                file_id: file_id.to_string(),
                message_id: telegram_id_string(message.get("message_id")),
                media_group_id: message
                    .get("media_group_id")
                    .and_then(Value::as_str)
                    .map(ToString::to_string),
                variant_count: None,
                selected_variant: Some(selected_variant(value)),
                expected_mime: value
                    .get("mime_type")
                    .and_then(Value::as_str)
                    .map(ToString::to_string)
                    .or_else(|| default_mime.map(ToString::to_string)),
                caption_preview: telegram_caption_preview(message),
                provenance: None,
                source: "telegram.getFile".to_string(),
                requires_image_validation: false,
            });
        }
    }

    if let Some(sticker) = message.get("sticker")
        && !sticker
            .get("is_animated")
            .and_then(Value::as_bool)
            .unwrap_or(false)
        && !sticker
            .get("is_video")
            .and_then(Value::as_bool)
            .unwrap_or(false)
        && let Some(file_id) = sticker.get("file_id").and_then(Value::as_str)
    {
        candidates.push(TelegramMediaCandidate {
            kind: "sticker-image".to_string(),
            file_id: file_id.to_string(),
            message_id: telegram_id_string(message.get("message_id")),
            media_group_id: message
                .get("media_group_id")
                .and_then(Value::as_str)
                .map(ToString::to_string),
            variant_count: None,
            selected_variant: Some(selected_variant(sticker)),
            expected_mime: Some("image/webp".to_string()),
            caption_preview: telegram_caption_preview(message),
            provenance: None,
            source: "telegram.getFile".to_string(),
            requires_image_validation: true,
        });
    }
    if let Some(reply) = message
        .get("reply_to_message")
        .filter(|value| value.is_object())
    {
        let mut referenced = telegram_media_candidates_without_replies(reply);
        for candidate in &mut referenced {
            candidate.provenance = Some("referenced".to_string());
            candidate.source = "telegram.reply_to_message.getFile".to_string();
        }
        candidates.extend(referenced.into_iter().take(3));
    }
    candidates
}

fn telegram_media_candidates_without_replies(message: &Value) -> Vec<TelegramMediaCandidate> {
    let mut clone = message.clone();
    if let Some(object) = clone.as_object_mut() {
        object.remove("reply_to_message");
    }
    telegram_media_candidates(&clone)
}

fn skipped_media_artifacts(message: &Value) -> Vec<InboundMediaArtifact> {
    let mut artifacts = Vec::new();
    for kind in ["animation", "video_note"] {
        if let Some(value) = message.get(kind) {
            artifacts.push(skipped_artifact(
                message,
                kind,
                value.get("mime_type").and_then(Value::as_str),
                "telegram media kind is not supported by this ingest path",
            ));
        }
    }
    if let Some(sticker) = message.get("sticker")
        && (sticker
            .get("is_animated")
            .and_then(Value::as_bool)
            .unwrap_or(false)
            || sticker
                .get("is_video")
                .and_then(Value::as_bool)
                .unwrap_or(false))
    {
        artifacts.push(skipped_artifact(
            message,
            "sticker",
            sticker.get("mime_type").and_then(Value::as_str),
            "telegram animated sticker is unsupported",
        ));
    }
    artifacts
}

fn skipped_artifact(
    message: &Value,
    kind: &str,
    mime: Option<&str>,
    warning: &str,
) -> InboundMediaArtifact {
    InboundMediaArtifact {
        platform: "telegram".to_string(),
        kind: kind.to_string(),
        message_id: telegram_id_string(message.get("message_id")),
        media_group_id: message
            .get("media_group_id")
            .and_then(Value::as_str)
            .map(ToString::to_string),
        mime: mime.map(ToString::to_string),
        source: "telegram.update".to_string(),
        download_status: InboundMediaDownloadStatus::DetectedSkipped,
        model_attachment_status: InboundMediaModelAttachmentStatus::Unsupported,
        warnings: vec![warning.to_string()],
        ..InboundMediaArtifact::default()
    }
}

fn select_best_photo_variant(photos: &[Value]) -> Option<(usize, &Value)> {
    let mut best: Option<(usize, &Value, i64, i64)> = None;
    for (index, photo) in photos.iter().enumerate() {
        let file_size = photo.get("file_size").and_then(Value::as_i64).unwrap_or(0);
        let area = photo.get("width").and_then(Value::as_i64).unwrap_or(0)
            * photo.get("height").and_then(Value::as_i64).unwrap_or(0);
        match best {
            None => best = Some((index, photo, file_size, area)),
            Some((_, _, best_size, best_area))
                if file_size > best_size || (file_size == best_size && area > best_area) =>
            {
                best = Some((index, photo, file_size, area));
            }
            _ => {}
        }
    }
    best.map(|(index, photo, _, _)| (index, photo))
}

fn selected_variant(value: &Value) -> InboundMediaSelectedVariant {
    InboundMediaSelectedVariant {
        width: value
            .get("width")
            .and_then(Value::as_u64)
            .map(|value| value as u32),
        height: value
            .get("height")
            .and_then(Value::as_u64)
            .map(|value| value as u32),
        file_size: value.get("file_size").and_then(Value::as_u64),
    }
}

fn telegram_document_is_image(document: &Value) -> bool {
    document
        .get("mime_type")
        .and_then(Value::as_str)
        .is_some_and(|mime| mime.starts_with("image/"))
        || document
            .get("file_name")
            .and_then(Value::as_str)
            .is_some_and(|name| {
                image_extension(Path::new(name).extension().and_then(|ext| ext.to_str())).is_some()
            })
}

fn telegram_caption_preview(message: &Value) -> Option<String> {
    message
        .get("caption")
        .and_then(Value::as_str)
        .map(|caption| compact_preview(caption, 240))
}

fn compact_preview(value: &str, max_chars: usize) -> String {
    let mut out = String::new();
    let mut truncated = false;
    for (index, ch) in value.chars().enumerate() {
        if index >= max_chars {
            truncated = true;
            break;
        }
        if ch == '\r' || ch == '\n' || ch == '\t' {
            out.push(' ');
        } else {
            out.push(ch);
        }
    }
    let text = out.split_whitespace().collect::<Vec<_>>().join(" ");
    if truncated {
        format!("{text}...")
    } else {
        text
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

fn image_mime_compatible(expected: &str, detected: &str) -> bool {
    expected == detected
        || (expected == "image/jpg" && detected == "image/jpeg")
        || (expected == "image/jpeg" && detected == "image/jpg")
}

fn extension_for_file(file_path: &str, expected_mime: Option<&str>, detected_mime: &str) -> String {
    Path::new(file_path)
        .extension()
        .and_then(|ext| ext.to_str())
        .and_then(media_extension)
        .or_else(|| expected_mime.and_then(extension_for_mime))
        .or_else(|| extension_for_mime(detected_mime))
        .unwrap_or("bin")
        .to_string()
}

fn media_extension(extension: &str) -> Option<&'static str> {
    match extension.to_ascii_lowercase().as_str() {
        "jpg" | "jpeg" => Some("jpg"),
        "png" => Some("png"),
        "gif" => Some("gif"),
        "webp" => Some("webp"),
        "pdf" => Some("pdf"),
        "txt" => Some("txt"),
        "md" => Some("md"),
        "json" => Some("json"),
        "csv" => Some("csv"),
        "mp3" => Some("mp3"),
        "wav" => Some("wav"),
        "ogg" => Some("ogg"),
        "opus" => Some("opus"),
        "m4a" => Some("m4a"),
        "flac" => Some("flac"),
        "mp4" => Some("mp4"),
        "mov" => Some("mov"),
        "webm" => Some("webm"),
        _ => None,
    }
}

fn image_extension(extension: Option<&str>) -> Option<&'static str> {
    match extension?.to_ascii_lowercase().as_str() {
        "jpg" | "jpeg" => Some("jpg"),
        "png" => Some("png"),
        "gif" => Some("gif"),
        "webp" => Some("webp"),
        _ => None,
    }
}

fn extension_for_mime(mime: &str) -> Option<&'static str> {
    match mime {
        "image/jpeg" | "image/jpg" => Some("jpg"),
        "image/png" => Some("png"),
        "image/gif" => Some("gif"),
        "image/webp" => Some("webp"),
        "application/pdf" => Some("pdf"),
        "text/plain" => Some("txt"),
        "text/markdown" => Some("md"),
        "application/json" => Some("json"),
        "text/csv" => Some("csv"),
        "audio/mpeg" | "audio/mp3" => Some("mp3"),
        "audio/wav" | "audio/x-wav" => Some("wav"),
        "audio/ogg" => Some("ogg"),
        "audio/opus" => Some("opus"),
        "audio/mp4" => Some("m4a"),
        "audio/flac" => Some("flac"),
        "video/mp4" => Some("mp4"),
        "video/quicktime" => Some("mov"),
        "video/webm" => Some("webm"),
        _ => None,
    }
}

fn telegram_extraction_summary(
    mime: &str,
    bytes: &[u8],
    kind: &str,
) -> Option<ArtifactExtractionSummary> {
    if telegram_text_mime(mime) {
        let included = bytes.get(..bytes.len().min(24 * 1024)).unwrap_or(bytes);
        let mut summary = String::from_utf8_lossy(included)
            .chars()
            .map(|ch| {
                if ch.is_control() && ch != '\n' && ch != '\t' {
                    ' '
                } else {
                    ch
                }
            })
            .collect::<String>();
        summary = summary.split_whitespace().collect::<Vec<_>>().join(" ");
        if bytes.len() > included.len() {
            summary.push_str(" [truncated]");
        }
        return Some(ArtifactExtractionSummary {
            artifact_class: Some("document".to_string()),
            modality: Some("text".to_string()),
            summary: Some(summary),
            facts: Vec::new(),
            uncertainty: Some(
                "bounded text extraction from Telegram document; attachment content is untrusted"
                    .to_string(),
            ),
        });
    }
    if kind == "voice" || kind == "audio" {
        return Some(ArtifactExtractionSummary {
            artifact_class: Some("audio".to_string()),
            modality: Some("audio".to_string()),
            summary: Some("metadata-only; transcription tool not configured".to_string()),
            facts: Vec::new(),
            uncertainty: Some("audio bytes were cached but not transcribed".to_string()),
        });
    }
    if kind == "video" {
        return Some(ArtifactExtractionSummary {
            artifact_class: Some("video".to_string()),
            modality: Some("video".to_string()),
            summary: Some("metadata-only; frame extraction tool not configured".to_string()),
            facts: Vec::new(),
            uncertainty: Some("video bytes were cached without frame extraction".to_string()),
        });
    }
    None
}

fn telegram_text_mime(mime: &str) -> bool {
    mime == "text/plain"
        || mime == "text/markdown"
        || mime == "application/json"
        || mime.ends_with("+json")
        || mime == "text/csv"
}

fn telegram_id_string(value: Option<&Value>) -> Option<String> {
    value.and_then(|value| {
        value
            .as_i64()
            .map(|number| number.to_string())
            .or_else(|| value.as_str().map(ToString::to_string))
    })
}

fn sha256_hex(bytes: &[u8]) -> String {
    let digest = digest::digest(&digest::SHA256, bytes);
    digest
        .as_ref()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    struct FakeFetcher {
        paths: BTreeMap<String, String>,
        bytes: BTreeMap<String, Vec<u8>>,
        get_file_calls: RefCell<Vec<String>>,
    }

    impl FakeFetcher {
        fn new(paths: BTreeMap<String, String>, bytes: BTreeMap<String, Vec<u8>>) -> Self {
            Self {
                paths,
                bytes,
                get_file_calls: RefCell::new(Vec::new()),
            }
        }
    }

    impl TelegramMediaFetcher for FakeFetcher {
        fn get_file_path(&self, file_id: &str) -> Result<String, String> {
            self.get_file_calls.borrow_mut().push(file_id.to_string());
            self.paths
                .get(file_id)
                .cloned()
                .ok_or_else(|| "missing fake path".to_string())
        }

        fn download_file(&self, file_path: &str) -> Result<Vec<u8>, String> {
            self.bytes
                .get(file_path)
                .cloned()
                .ok_or_else(|| "missing fake bytes".to_string())
        }
    }

    #[test]
    fn telegram_media_selects_best_photo_by_size_area_then_order() {
        let photos = vec![
            serde_json::json!({"file_id":"small","width":200,"height":200,"file_size":1000}),
            serde_json::json!({"file_id":"wide","width":800,"height":400,"file_size":2000}),
            serde_json::json!({"file_id":"large","width":640,"height":640,"file_size":2000}),
        ];

        let (index, selected) = select_best_photo_variant(&photos).unwrap();

        assert_eq!(index, 2);
        assert_eq!(selected["file_id"], "large");
    }

    #[test]
    fn telegram_media_ingests_photo_to_cache_receipt_without_file_id_or_url() {
        let root =
            temp_root("telegram_media_ingests_photo_to_cache_receipt_without_file_id_or_url");
        let harness_home = root.join(".agent-harness");
        let message = serde_json::json!({
            "message_id": 10,
            "media_group_id": "album-1",
            "photo": [
                {"file_id":"secret-small","width":100,"height":100,"file_size":10},
                {"file_id":"secret-large","width":961,"height":1280,"file_size":179414}
            ]
        });
        let file_path = "photos/file_1.jpg";
        let bytes = jpeg_bytes();
        let fetcher = FakeFetcher::new(
            BTreeMap::from([("secret-large".to_string(), file_path.to_string())]),
            BTreeMap::from([(file_path.to_string(), bytes.clone())]),
        );

        let report = ingest_telegram_media(&harness_home, 1234, &message, &fetcher).unwrap();

        assert_eq!(
            fetcher.get_file_calls.borrow().as_slice(),
            ["secret-large".to_string()]
        );
        assert_eq!(report.artifacts.len(), 1);
        let artifact = &report.artifacts[0];
        assert_eq!(artifact.kind, "photo");
        assert_eq!(artifact.media_group_id.as_deref(), Some("album-1"));
        assert_eq!(artifact.message_id.as_deref(), Some("10"));
        assert_eq!(artifact.variant_count, Some(2));
        assert_eq!(artifact.selected_variant.as_ref().unwrap().width, Some(961));
        assert_eq!(artifact.mime.as_deref(), Some("image/jpeg"));
        assert_eq!(artifact.byte_len, Some(bytes.len() as u64));
        assert_eq!(
            artifact.sha256.as_deref(),
            Some(sha256_hex(&bytes).as_str())
        );
        assert_eq!(
            artifact.download_status,
            InboundMediaDownloadStatus::Downloaded
        );
        assert!(artifact.local_path.as_ref().unwrap().is_file());
        assert_eq!(
            fs::read(artifact.local_path.as_ref().unwrap()).unwrap(),
            bytes
        );

        let receipt = fs::read_to_string(&report.receipt_file).unwrap();
        assert!(receipt.contains("\"downloadStatus\":\"downloaded\""));
        assert!(receipt.contains("\"sha256\""));
        assert!(!receipt.contains("secret-large"));
        assert!(!receipt.contains("file_1.jpg"));
        assert!(!receipt.contains("api.telegram.org"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn telegram_media_rejects_non_image_bytes_for_image_artifact() {
        let root = temp_root("telegram_media_rejects_non_image_bytes_for_image_artifact");
        let harness_home = root.join(".agent-harness");
        let message = serde_json::json!({
            "message_id": 10,
            "photo": [{"file_id":"secret-photo","width":100,"height":100,"file_size":10}]
        });
        let file_path = "photos/file_1.jpg";
        let fetcher = FakeFetcher::new(
            BTreeMap::from([("secret-photo".to_string(), file_path.to_string())]),
            BTreeMap::from([(file_path.to_string(), b"not an image".to_vec())]),
        );

        let report = ingest_telegram_media(&harness_home, 1234, &message, &fetcher).unwrap();

        let artifact = &report.artifacts[0];
        assert_eq!(
            artifact.download_status,
            InboundMediaDownloadStatus::DownloadFailed
        );
        assert!(artifact.local_path.is_none());
        assert!(artifact.sha256.is_none());
        assert!(
            artifact
                .warnings
                .iter()
                .any(|warning| warning.contains("image header validation"))
        );
        let receipt = fs::read_to_string(&report.receipt_file).unwrap();
        assert!(!receipt.contains("secret-photo"));
        assert!(!receipt.contains("api.telegram.org"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn telegram_media_downloads_non_image_document_with_bounded_extraction() {
        let root = temp_root("telegram_media_downloads_non_image_document_with_bounded_extraction");
        let harness_home = root.join(".agent-harness");
        let message = serde_json::json!({
            "message_id": 10,
            "document": {
                "file_id": "secret-doc",
                "file_name": "notes.txt",
                "mime_type": "text/plain",
                "file_size": 20
            }
        });
        let file_path = "documents/notes.txt";
        let bytes = b"hello from telegram document".to_vec();
        let fetcher = FakeFetcher::new(
            BTreeMap::from([("secret-doc".to_string(), file_path.to_string())]),
            BTreeMap::from([(file_path.to_string(), bytes.clone())]),
        );

        let report = ingest_telegram_media(&harness_home, 1234, &message, &fetcher).unwrap();

        assert_eq!(report.artifacts.len(), 1);
        let artifact = &report.artifacts[0];
        assert_eq!(artifact.kind, "document");
        assert_eq!(
            artifact.download_status,
            InboundMediaDownloadStatus::Downloaded
        );
        assert_eq!(
            artifact.model_attachment_status,
            InboundMediaModelAttachmentStatus::PromptOnly
        );
        assert_eq!(artifact.mime.as_deref(), Some("text/plain"));
        assert!(artifact.local_path.as_ref().unwrap().is_file());
        assert!(
            artifact
                .extraction_summary
                .as_ref()
                .and_then(|summary| summary.summary.as_deref())
                .is_some_and(|summary| summary.contains("hello from telegram document"))
        );
        let receipt = fs::read_to_string(&report.receipt_file).unwrap();
        assert!(!receipt.contains("secret-doc"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn telegram_media_downloads_voice_as_audio_metadata_only() {
        let root = temp_root("telegram_media_downloads_voice_as_audio_metadata_only");
        let harness_home = root.join(".agent-harness");
        let message = serde_json::json!({
            "message_id": 11,
            "voice": {
                "file_id": "secret-voice",
                "mime_type": "audio/ogg",
                "file_size": 8
            }
        });
        let file_path = "voice/file_1.ogg";
        let fetcher = FakeFetcher::new(
            BTreeMap::from([("secret-voice".to_string(), file_path.to_string())]),
            BTreeMap::from([(file_path.to_string(), b"OggSdata".to_vec())]),
        );

        let report = ingest_telegram_media(&harness_home, 1235, &message, &fetcher).unwrap();

        assert_eq!(report.artifacts.len(), 1);
        let artifact = &report.artifacts[0];
        assert_eq!(artifact.kind, "voice");
        assert_eq!(
            artifact.download_status,
            InboundMediaDownloadStatus::Downloaded
        );
        assert_eq!(artifact.mime.as_deref(), Some("audio/ogg"));
        assert!(
            artifact
                .extraction_summary
                .as_ref()
                .and_then(|summary| summary.summary.as_deref())
                .is_some_and(|summary| summary.contains("transcription tool not configured"))
        );
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn telegram_reply_to_message_media_is_referenced_provenance() {
        let root = temp_root("telegram_reply_to_message_media_is_referenced_provenance");
        let harness_home = root.join(".agent-harness");
        let message = serde_json::json!({
            "message_id": 12,
            "text": "use that image",
            "reply_to_message": {
                "message_id": 9,
                "photo": [{
                    "file_id": "secret-referenced-photo",
                    "width": 320,
                    "height": 240,
                    "file_size": 12
                }]
            }
        });
        let file_path = "photos/referenced.jpg";
        let bytes = jpeg_bytes();
        let fetcher = FakeFetcher::new(
            BTreeMap::from([("secret-referenced-photo".to_string(), file_path.to_string())]),
            BTreeMap::from([(file_path.to_string(), bytes)]),
        );

        let report = ingest_telegram_media(&harness_home, 1236, &message, &fetcher).unwrap();

        assert_eq!(report.artifacts.len(), 1);
        let artifact = &report.artifacts[0];
        assert_eq!(artifact.kind, "photo");
        assert_eq!(artifact.message_id.as_deref(), Some("9"));
        assert_eq!(artifact.provenance.as_deref(), Some("referenced"));
        assert_eq!(artifact.source, "telegram.reply_to_message.getFile");
        assert!(
            artifact
                .artifact_uri
                .as_deref()
                .unwrap()
                .contains("update-1236")
        );
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn telegram_static_webp_sticker_downloads_as_image_artifact() {
        let root = temp_root("telegram_static_webp_sticker_downloads_as_image_artifact");
        let harness_home = root.join(".agent-harness");
        let message = serde_json::json!({
            "message_id": 13,
            "sticker": {
                "file_id": "secret-sticker",
                "is_animated": false,
                "is_video": false,
                "file_size": 12
            }
        });
        let file_path = "stickers/static.webp";
        let bytes = webp_bytes();
        let fetcher = FakeFetcher::new(
            BTreeMap::from([("secret-sticker".to_string(), file_path.to_string())]),
            BTreeMap::from([(file_path.to_string(), bytes)]),
        );

        let report = ingest_telegram_media(&harness_home, 1237, &message, &fetcher).unwrap();

        assert_eq!(report.artifacts.len(), 1);
        let artifact = &report.artifacts[0];
        assert_eq!(artifact.kind, "sticker-image");
        assert_eq!(
            artifact.download_status,
            InboundMediaDownloadStatus::Downloaded
        );
        assert_eq!(artifact.mime.as_deref(), Some("image/webp"));
        assert!(artifact.local_path.as_ref().unwrap().is_file());
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn telegram_media_group_buffers_then_due_flushes_members_in_order() {
        let root = temp_root("telegram_media_group_buffers_then_due_flushes_members_in_order");
        let harness_home = root.join(".agent-harness");
        let second_message = serde_json::json!({
            "message_id": 12,
            "media_group_id": "Album/Unsafe",
            "caption": "second caption",
            "chat": { "id": -1001, "type": "supergroup" },
            "from": { "id": 123 },
            "photo": [{"file_id":"secret-second","width":100,"height":100,"file_size":10}]
        });
        let third_message = serde_json::json!({
            "message_id": 13,
            "media_group_id": "Album/Unsafe",
            "caption": "third caption",
            "chat": { "id": -1001, "type": "supergroup" },
            "from": { "id": 123 },
            "photo": [{"file_id":"secret-third","width":100,"height":100,"file_size":10}]
        });
        let first_message = serde_json::json!({
            "message_id": 11,
            "media_group_id": "Album/Unsafe",
            "caption": "first caption",
            "chat": { "id": -1001, "type": "supergroup" },
            "from": { "id": 123 },
            "photo": [{"file_id":"secret-first","width":100,"height":100,"file_size":10}]
        });

        let decision = buffer_telegram_media_group(
            &harness_home,
            "Default",
            "-1001",
            "Album/Unsafe",
            102,
            &second_message,
            1_000,
            DEFAULT_TELEGRAM_MEDIA_GROUP_DEBOUNCE_MS,
        )
        .unwrap();
        assert!(matches!(decision, TelegramMediaGroupDecision::Buffered(_)));
        let state_file =
            telegram_media_group_state_file(&harness_home, "Default", "-1001", "Album/Unsafe");
        assert!(state_file.is_file());

        let decision = buffer_telegram_media_group(
            &harness_home,
            "Default",
            "-1001",
            "Album/Unsafe",
            101,
            &first_message,
            1_100,
            DEFAULT_TELEGRAM_MEDIA_GROUP_DEBOUNCE_MS,
        )
        .unwrap();
        assert!(matches!(decision, TelegramMediaGroupDecision::Buffered(_)));

        let decision = buffer_telegram_media_group(
            &harness_home,
            "Default",
            "-1001",
            "Album/Unsafe",
            103,
            &third_message,
            1_200,
            DEFAULT_TELEGRAM_MEDIA_GROUP_DEBOUNCE_MS,
        )
        .unwrap();
        assert!(matches!(decision, TelegramMediaGroupDecision::Buffered(_)));

        let flushes = take_due_telegram_media_groups(
            &harness_home,
            "Default",
            2_000,
            DEFAULT_TELEGRAM_MEDIA_GROUP_DEBOUNCE_MS,
            DEFAULT_TELEGRAM_MEDIA_GROUP_STALE_MS,
        )
        .unwrap();

        assert_eq!(flushes.len(), 1);
        let flush = &flushes[0];
        assert_eq!(flush.status, TelegramMediaGroupStatus::GroupFlushed);
        assert_eq!(flush.members.len(), 3);
        assert_eq!(flush.members[0].message_id.as_deref(), Some("11"));
        assert_eq!(
            flush.members[0].caption_preview.as_deref(),
            Some("first caption")
        );
        assert_eq!(flush.members[1].message_id.as_deref(), Some("12"));
        assert_eq!(flush.members[2].message_id.as_deref(), Some("13"));
        assert!(!state_file.exists());

        let receipts =
            fs::read_to_string(telegram_media_group_receipts_file(&harness_home)).unwrap();
        assert!(receipts.contains("\"status\":\"group-buffered\""));
        assert!(receipts.contains("\"status\":\"group-flushed\""));
        assert!(!receipts.contains("secret-first"));
        assert!(!receipts.contains("secret-second"));
        assert!(!receipts.contains("secret-third"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn telegram_media_group_stale_cleanup_records_stale_flush() {
        let root = temp_root("telegram_media_group_stale_cleanup_records_stale_flush");
        let harness_home = root.join(".agent-harness");
        let message = serde_json::json!({
            "message_id": 21,
            "media_group_id": "album-stale",
            "chat": { "id": -1001, "type": "supergroup" },
            "from": { "id": 123 },
            "photo": [{"file_id":"secret-stale","width":100,"height":100,"file_size":10}]
        });

        let decision = buffer_telegram_media_group(
            &harness_home,
            "default",
            "-1001",
            "album-stale",
            201,
            &message,
            1_000,
            DEFAULT_TELEGRAM_MEDIA_GROUP_DEBOUNCE_MS,
        )
        .unwrap();
        assert!(matches!(decision, TelegramMediaGroupDecision::Buffered(_)));

        let flushes =
            take_due_telegram_media_groups(&harness_home, "default", 62_000, 800, 60_000).unwrap();

        assert_eq!(flushes.len(), 1);
        assert_eq!(
            flushes[0].status,
            TelegramMediaGroupStatus::GroupStaleFlushed
        );
        let receipts =
            fs::read_to_string(telegram_media_group_receipts_file(&harness_home)).unwrap();
        assert!(receipts.contains("\"status\":\"group-stale-flushed\""));
        assert!(!receipts.contains("secret-stale"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn telegram_media_group_discarded_receipt_is_sanitized() {
        let root = temp_root("telegram_media_group_discarded_receipt_is_sanitized");
        let harness_home = root.join(".agent-harness");
        let flush = TelegramMediaGroupFlush {
            account_id: "default".to_string(),
            chat_id: "-1001".to_string(),
            media_group_id: "album-discard".to_string(),
            status: TelegramMediaGroupStatus::GroupFlushed,
            members: vec![TelegramMediaGroupMember {
                update_id: 301,
                message_id: Some("31".to_string()),
                caption_preview: Some("caption".to_string()),
                message: serde_json::json!({
                    "message_id": 31,
                    "media_group_id": "album-discard",
                    "photo": [{"file_id":"secret-discard","width":100,"height":100,"file_size":10}]
                }),
            }],
        };

        record_telegram_media_group_discarded_no_agent(&harness_home, &flush, "no bound agent")
            .unwrap();

        let receipts =
            fs::read_to_string(telegram_media_group_receipts_file(&harness_home)).unwrap();
        assert!(receipts.contains("\"status\":\"group-discarded-no-agent\""));
        assert!(receipts.contains("no bound agent"));
        assert!(!receipts.contains("secret-discard"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn telegram_media_security_rejects_oversized_photo_before_get_file() {
        let root = temp_root("telegram_media_security_rejects_oversized_photo_before_get_file");
        let harness_home = root.join(".agent-harness");
        let message = serde_json::json!({
            "message_id": 10,
            "photo": [{
                "file_id":"secret-oversized-photo",
                "width":100,
                "height":100,
                "file_size": DEFAULT_INBOUND_MEDIA_MAX_BYTES_PER_ITEM + 1
            }]
        });
        let fetcher = FakeFetcher::new(BTreeMap::new(), BTreeMap::new());

        let report = ingest_telegram_media(&harness_home, 1234, &message, &fetcher).unwrap();

        assert!(fetcher.get_file_calls.borrow().is_empty());
        assert_eq!(report.artifacts.len(), 1);
        assert_eq!(
            report.artifacts[0].download_status,
            InboundMediaDownloadStatus::DownloadFailed
        );
        assert_eq!(
            report.artifacts[0].byte_len,
            Some(DEFAULT_INBOUND_MEDIA_MAX_BYTES_PER_ITEM + 1)
        );
        assert!(report.artifacts[0].local_path.is_none());
        assert!(
            report
                .warnings
                .iter()
                .all(|warning| !warning.contains("secret-oversized-photo"))
        );
        let receipt = fs::read_to_string(report.receipt_file).unwrap();
        assert!(!receipt.contains("secret-oversized-photo"));
        assert!(!receipt.contains("api.telegram.org"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn telegram_media_security_downloaded_receipt_has_no_provider_secrets() {
        let root = temp_root("telegram_media_security_downloaded_receipt_has_no_provider_secrets");
        let harness_home = root.join(".agent-harness");
        let message = serde_json::json!({
            "message_id": 10,
            "caption": "safe caption",
            "photo": [{"file_id":"secret-photo-token","width":100,"height":100,"file_size":10}]
        });
        let file_path = "photos/provider-secret-file-name.jpg";
        let fetcher = FakeFetcher::new(
            BTreeMap::from([("secret-photo-token".to_string(), file_path.to_string())]),
            BTreeMap::from([(file_path.to_string(), jpeg_bytes())]),
        );

        let report = ingest_telegram_media(&harness_home, 1234, &message, &fetcher).unwrap();

        assert_eq!(
            report.artifacts[0].caption_preview.as_deref(),
            Some("safe caption")
        );
        let receipt = fs::read_to_string(report.receipt_file).unwrap();
        assert!(receipt.contains("\"downloadStatus\":\"downloaded\""));
        assert!(!receipt.contains("secret-photo-token"));
        assert!(!receipt.contains("provider-secret-file-name"));
        assert!(!receipt.contains("api.telegram.org"));
        assert!(!receipt.contains("bot"));

        let _ = fs::remove_dir_all(root);
    }

    fn jpeg_bytes() -> Vec<u8> {
        vec![
            0xff, 0xd8, 0xff, 0xe0, 0, 1, b'J', b'F', b'I', b'F', 0xff, 0xd9,
        ]
    }

    fn webp_bytes() -> Vec<u8> {
        b"RIFF\x04\x00\x00\x00WEBPdata".to_vec()
    }

    fn temp_root(test_name: &str) -> PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "agent-harness-cli-{test_name}-{}-{nanos}",
            std::process::id()
        ))
    }
}
