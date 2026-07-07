use crate::write_json_atomic;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::time::UNIX_EPOCH;

pub const DREAM_DIRECTOR_SEND_RECEIPT_SCHEMA: &str = "openclaw.mem.dream-director.send-receipt.v1";
pub const DEFAULT_DREAM_DIRECTOR_MAX_CHARS: usize = 3500;
pub const DEFAULT_DREAM_DIRECTOR_SOURCE_MAX_AGE_HOURS: f64 = 36.0;

#[derive(Debug, Clone)]
pub struct DreamDirectorSendOptions {
    pub harness_home: PathBuf,
    pub target: String,
    pub max_chars: usize,
    pub source_max_age_hours: f64,
    pub dry_run: bool,
    pub force: bool,
    pub now_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DreamDirectorSendReport {
    pub receipt_file: PathBuf,
    pub receipt: DreamDirectorSendReceipt,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DreamDirectorSendReceipt {
    pub schema: String,
    pub ok: bool,
    pub status: String,
    pub generated_at_ms: i64,
    pub dry_run: bool,
    pub target: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub opinion_path: Option<PathBuf>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_receipt_path: Option<PathBuf>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_generated_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_generated_at_ms: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_age_hours: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_run_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_opinion_path: Option<PathBuf>,
    pub stale: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stale_reason: Option<String>,
    pub force_override: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message_chars: Option<usize>,
}

pub fn dream_director_send_receipt_file(harness_home: &Path) -> PathBuf {
    harness_home
        .join("state")
        .join("memory")
        .join("dream-lite-director")
        .join("latest-send.json")
}

pub fn dream_director_daily_state_dir(harness_home: &Path) -> PathBuf {
    harness_home
        .join("state")
        .join("memory")
        .join("dream-lite-daily")
}

pub fn run_dream_director_send(
    options: DreamDirectorSendOptions,
) -> io::Result<DreamDirectorSendReport> {
    let daily_state_dir = dream_director_daily_state_dir(&options.harness_home);
    let source_receipt_path = daily_state_dir.join("latest.json");
    let receipt_file = dream_director_send_receipt_file(&options.harness_home);
    let latest_opinion_path = latest_director_opinion_path(&daily_state_dir)?;
    let freshness = inspect_source_freshness(
        &daily_state_dir,
        &source_receipt_path,
        latest_opinion_path.as_deref(),
        options.now_ms,
        options.source_max_age_hours,
    );
    let mut receipt = DreamDirectorSendReceipt {
        schema: DREAM_DIRECTOR_SEND_RECEIPT_SCHEMA.to_string(),
        ok: false,
        status: "pending".to_string(),
        generated_at_ms: options.now_ms,
        dry_run: options.dry_run,
        target: options.target.clone(),
        opinion_path: latest_opinion_path.clone(),
        source_receipt_path: Some(source_receipt_path),
        source_generated_at: freshness.source_generated_at,
        source_generated_at_ms: freshness.source_generated_at_ms,
        source_age_hours: freshness.source_age_hours,
        source_run_id: freshness.source_run_id,
        source_opinion_path: freshness.source_opinion_path,
        stale: freshness.stale,
        stale_reason: freshness.stale_reason.map(str::to_string),
        force_override: freshness.stale && options.force,
        message_chars: None,
    };

    if receipt.stale && !options.force {
        receipt.status = "stale-source-suppressed".to_string();
        write_json_atomic(&receipt_file, &receipt)?;
        return Ok(DreamDirectorSendReport {
            receipt_file,
            receipt,
        });
    }

    let opinion_path = match latest_opinion_path {
        Some(path) => path,
        None => {
            receipt.status = "missing-opinion".to_string();
            write_json_atomic(&receipt_file, &receipt)?;
            return Ok(DreamDirectorSendReport {
                receipt_file,
                receipt,
            });
        }
    };
    let opinion_text = match fs::read_to_string(&opinion_path) {
        Ok(text) => text,
        Err(_) => {
            receipt.status = "opinion-read-failed".to_string();
            write_json_atomic(&receipt_file, &receipt)?;
            return Ok(DreamDirectorSendReport {
                receipt_file,
                receipt,
            });
        }
    };
    let message = build_director_message(&opinion_text, &receipt, options.max_chars);
    receipt.message_chars = Some(message.chars().count());
    if options.dry_run {
        receipt.ok = true;
        receipt.status = "dry-run".to_string();
    } else {
        receipt.status = "provider-send-deferred".to_string();
    }
    write_json_atomic(&receipt_file, &receipt)?;
    Ok(DreamDirectorSendReport {
        receipt_file,
        receipt,
    })
}

fn build_director_message(
    opinion_text: &str,
    receipt: &DreamDirectorSendReceipt,
    max_chars: usize,
) -> String {
    let mut body = opinion_text.trim().to_string();
    if receipt.force_override {
        let reason = receipt
            .stale_reason
            .as_deref()
            .unwrap_or("source freshness guard was overridden");
        body = format!("STALE SOURCE OVERRIDE: {reason}\n\n{body}");
    }
    if body.chars().count() <= max_chars {
        return body;
    }
    body.chars().take(max_chars).collect()
}

#[derive(Debug, Default)]
struct SourceFreshness {
    source_generated_at: Option<String>,
    source_generated_at_ms: Option<i64>,
    source_age_hours: Option<f64>,
    source_run_id: Option<String>,
    source_opinion_path: Option<PathBuf>,
    stale: bool,
    stale_reason: Option<&'static str>,
}

fn inspect_source_freshness(
    daily_state_dir: &Path,
    source_receipt_path: &Path,
    latest_opinion_path: Option<&Path>,
    now_ms: i64,
    source_max_age_hours: f64,
) -> SourceFreshness {
    let text = match fs::read_to_string(source_receipt_path) {
        Ok(text) => text,
        Err(_) => return stale_source("missing-source-receipt"),
    };
    let value = match serde_json::from_str::<Value>(&text) {
        Ok(value) => value,
        Err(_) => return stale_source("invalid-source-receipt"),
    };
    let mut freshness = SourceFreshness {
        source_generated_at: value
            .get("generatedAt")
            .and_then(Value::as_str)
            .map(ToString::to_string),
        source_run_id: value
            .get("runId")
            .and_then(Value::as_str)
            .map(ToString::to_string),
        source_opinion_path: source_opinion_path(daily_state_dir, &value),
        ..SourceFreshness::default()
    };

    if !value.get("ok").and_then(Value::as_bool).unwrap_or(false) {
        freshness.mark_stale("source-not-ok");
        return freshness;
    }

    match source_generated_at_ms(&value) {
        SourceGeneratedAt::Present(generated_at_ms) => {
            freshness.source_generated_at_ms = Some(generated_at_ms);
            let age_hours = age_hours(now_ms, generated_at_ms);
            freshness.source_age_hours = Some(age_hours);
            if age_hours > source_max_age_hours {
                freshness.mark_stale("source-age-exceeded");
                return freshness;
            }
        }
        SourceGeneratedAt::Missing => {
            freshness.mark_stale("missing-source-generated-at");
            return freshness;
        }
        SourceGeneratedAt::Invalid => {
            freshness.mark_stale("invalid-source-generated-at");
            return freshness;
        }
    }

    if let (Some(source_opinion_path), Some(latest_opinion_path)) = (
        freshness.source_opinion_path.as_deref(),
        latest_opinion_path,
    ) && !paths_equivalent(source_opinion_path, latest_opinion_path)
    {
        freshness.mark_stale("opinion-mismatches-source-receipt");
    }
    freshness
}

fn stale_source(reason: &'static str) -> SourceFreshness {
    let mut freshness = SourceFreshness::default();
    freshness.mark_stale(reason);
    freshness
}

impl SourceFreshness {
    fn mark_stale(&mut self, reason: &'static str) {
        self.stale = true;
        self.stale_reason = Some(reason);
    }
}

enum SourceGeneratedAt {
    Present(i64),
    Missing,
    Invalid,
}

fn source_generated_at_ms(value: &Value) -> SourceGeneratedAt {
    if let Some(ms) = value.get("generatedAtMs") {
        if let Some(ms) = ms.as_i64() {
            return SourceGeneratedAt::Present(ms);
        }
        return SourceGeneratedAt::Invalid;
    }
    match value.get("generatedAt").and_then(Value::as_str) {
        Some(value) => parse_rfc3339_ms(value)
            .map(SourceGeneratedAt::Present)
            .unwrap_or(SourceGeneratedAt::Invalid),
        None => SourceGeneratedAt::Missing,
    }
}

fn source_opinion_path(daily_state_dir: &Path, value: &Value) -> Option<PathBuf> {
    value
        .get("directorOpinion")
        .or_else(|| value.get("opinionPath"))
        .and_then(Value::as_str)
        .map(|raw| resolve_state_path(daily_state_dir, raw))
}

fn latest_director_opinion_path(daily_state_dir: &Path) -> io::Result<Option<PathBuf>> {
    let mut latest: Option<(i64, PathBuf)> = None;
    visit_files(daily_state_dir, &mut |path| {
        if path.file_name().and_then(|name| name.to_str()) != Some("director-opinion.md") {
            return;
        }
        let modified_ms = file_modified_ms(path).unwrap_or(0);
        match &latest {
            Some((current_ms, _)) if *current_ms >= modified_ms => {}
            _ => latest = Some((modified_ms, path.to_path_buf())),
        }
    })?;
    Ok(latest.map(|(_, path)| path))
}

fn visit_files(root: &Path, on_file: &mut impl FnMut(&Path)) -> io::Result<()> {
    if !root.exists() {
        return Ok(());
    }
    if root.is_file() {
        on_file(root);
        return Ok(());
    }
    for entry in fs::read_dir(root)? {
        let path = entry?.path();
        if path.is_dir() {
            visit_files(&path, on_file)?;
        } else {
            on_file(&path);
        }
    }
    Ok(())
}

fn resolve_state_path(daily_state_dir: &Path, raw: &str) -> PathBuf {
    let path = PathBuf::from(raw);
    if path.is_absolute() {
        path
    } else {
        daily_state_dir.join(path)
    }
}

fn paths_equivalent(left: &Path, right: &Path) -> bool {
    normalize_path(left) == normalize_path(right)
}

fn normalize_path(path: &Path) -> String {
    let normalized = path.canonicalize().unwrap_or_else(|_| path.to_path_buf());
    normalized
        .to_string_lossy()
        .replace('/', "\\")
        .trim_end_matches('\\')
        .to_ascii_lowercase()
}

fn age_hours(now_ms: i64, generated_at_ms: i64) -> f64 {
    let age_ms = now_ms.saturating_sub(generated_at_ms).max(0);
    ((age_ms as f64 / 3_600_000.0) * 1000.0).round() / 1000.0
}

fn parse_rfc3339_ms(value: &str) -> Option<i64> {
    let value = value.trim();
    let bytes = value.as_bytes();
    if bytes.len() < 20 {
        return None;
    }
    let year = parse_digits_i32(bytes, 0, 4)?;
    expect_byte(bytes, 4, b'-')?;
    let month = parse_digits_u32(bytes, 5, 7)?;
    expect_byte(bytes, 7, b'-')?;
    let day = parse_digits_u32(bytes, 8, 10)?;
    match *bytes.get(10)? {
        b'T' | b't' | b' ' => {}
        _ => return None,
    }
    let hour = parse_digits_u32(bytes, 11, 13)?;
    expect_byte(bytes, 13, b':')?;
    let minute = parse_digits_u32(bytes, 14, 16)?;
    expect_byte(bytes, 16, b':')?;
    let second = parse_digits_u32(bytes, 17, 19)?;
    if month == 0
        || month > 12
        || day == 0
        || day > days_in_month(year, month)?
        || hour > 23
        || minute > 59
        || second > 60
    {
        return None;
    }
    let mut index = 19;
    let mut millis = 0_i64;
    if bytes.get(index) == Some(&b'.') {
        index += 1;
        let fraction_start = index;
        let mut fraction_millis = 0_i64;
        let mut millis_digits = 0_usize;
        while let Some(byte) = bytes.get(index)
            && byte.is_ascii_digit()
        {
            if millis_digits < 3 {
                fraction_millis = fraction_millis * 10 + i64::from(byte - b'0');
                millis_digits += 1;
            }
            index += 1;
        }
        if index == fraction_start {
            return None;
        }
        while millis_digits < 3 {
            fraction_millis *= 10;
            millis_digits += 1;
        }
        millis = fraction_millis;
    }
    let offset_ms = match bytes.get(index).copied()? {
        b'Z' | b'z' => {
            if index + 1 != bytes.len() {
                return None;
            }
            0_i64
        }
        b'+' | b'-' => {
            let sign = if bytes[index] == b'+' { 1_i64 } else { -1_i64 };
            let offset_hour = parse_digits_u32(bytes, index + 1, index + 3)?;
            expect_byte(bytes, index + 3, b':')?;
            let offset_minute = parse_digits_u32(bytes, index + 4, index + 6)?;
            if index + 6 != bytes.len() || offset_hour > 23 || offset_minute > 59 {
                return None;
            }
            sign * i64::from(offset_hour * 60 + offset_minute) * 60_000
        }
        _ => return None,
    };
    let days = days_from_civil(year, month, day)?;
    let local_ms = days
        .checked_mul(86_400_000)?
        .checked_add(i64::from(hour) * 3_600_000)?
        .checked_add(i64::from(minute) * 60_000)?
        .checked_add(i64::from(second) * 1_000)?
        .checked_add(millis)?;
    local_ms.checked_sub(offset_ms)
}

fn parse_digits_i32(bytes: &[u8], start: usize, end: usize) -> Option<i32> {
    let value = parse_digits_u32(bytes, start, end)?;
    i32::try_from(value).ok()
}

fn parse_digits_u32(bytes: &[u8], start: usize, end: usize) -> Option<u32> {
    if start >= end || end > bytes.len() {
        return None;
    }
    let mut value = 0_u32;
    for byte in &bytes[start..end] {
        if !byte.is_ascii_digit() {
            return None;
        }
        value = value.checked_mul(10)?.checked_add(u32::from(byte - b'0'))?;
    }
    Some(value)
}

fn expect_byte(bytes: &[u8], index: usize, expected: u8) -> Option<()> {
    (*bytes.get(index)? == expected).then_some(())
}

fn days_in_month(year: i32, month: u32) -> Option<u32> {
    Some(match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if is_leap_year(year) => 29,
        2 => 28,
        _ => return None,
    })
}

fn is_leap_year(year: i32) -> bool {
    (year % 4 == 0 && year % 100 != 0) || year % 400 == 0
}

fn days_from_civil(year: i32, month: u32, day: u32) -> Option<i64> {
    let year = year - i32::from(month <= 2);
    let era = if year >= 0 { year } else { year - 399 } / 400;
    let yoe = year - era * 400;
    let month = i32::try_from(month).ok()?;
    let day = i32::try_from(day).ok()?;
    let month_prime = month + if month > 2 { -3 } else { 9 };
    let doy = (153 * month_prime + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    Some(i64::from(era) * 146_097 + i64::from(doe) - 719_468)
}

fn file_modified_ms(path: &Path) -> Option<i64> {
    let modified = fs::metadata(path).ok()?.modified().ok()?;
    let duration = modified.duration_since(UNIX_EPOCH).ok()?;
    i64::try_from(duration.as_millis()).ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn dream_director_sender_suppresses_stale_source() {
        let root = temp_root("dream_director_sender_suppresses_stale_source");
        let harness_home = root.join(".agent-harness");
        let daily_dir = dream_director_daily_state_dir(&harness_home);
        let opinion_path = daily_dir
            .join("runs")
            .join("stale")
            .join("director-opinion.md");
        fs::create_dir_all(opinion_path.parent().unwrap()).unwrap();
        fs::write(&opinion_path, "stale daily opinion").unwrap();
        let now_ms = 1_700_000_000_000_i64;
        let source_ms = now_ms - 72 * 3_600_000;
        write_source_receipt(
            &daily_dir,
            serde_json::json!({
                "ok": true,
                "runId": "stale-run",
                "generatedAtMs": source_ms,
                "generatedAt": "2023-11-11T22:13:20Z",
                "directorOpinion": opinion_path.to_string_lossy()
            }),
        );

        let report = run_dream_director_send(DreamDirectorSendOptions {
            harness_home: harness_home.clone(),
            target: "fixture-private".to_string(),
            max_chars: DEFAULT_DREAM_DIRECTOR_MAX_CHARS,
            source_max_age_hours: 36.0,
            dry_run: true,
            force: false,
            now_ms,
        })
        .unwrap();

        assert!(!report.receipt.ok);
        assert_eq!(report.receipt.status, "stale-source-suppressed");
        assert!(report.receipt.stale);
        assert_eq!(
            report.receipt.stale_reason.as_deref(),
            Some("source-age-exceeded")
        );
        assert_eq!(report.receipt.source_run_id.as_deref(), Some("stale-run"));
        assert_eq!(report.receipt.source_age_hours, Some(72.0));
        assert!(report.receipt_file.exists());
        let written: DreamDirectorSendReceipt =
            serde_json::from_slice(&fs::read(&report.receipt_file).unwrap()).unwrap();
        assert_eq!(written.status, "stale-source-suppressed");
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn dream_director_sender_sends_fresh_source_with_freshness_metadata() {
        let root = temp_root("dream_director_sender_sends_fresh_source_with_freshness_metadata");
        let harness_home = root.join(".agent-harness");
        let daily_dir = dream_director_daily_state_dir(&harness_home);
        let opinion_path = daily_dir
            .join("runs")
            .join("fresh")
            .join("director-opinion.md");
        fs::create_dir_all(opinion_path.parent().unwrap()).unwrap();
        fs::write(&opinion_path, "fresh daily opinion\nwith context").unwrap();
        let now_ms = 1_700_000_000_000_i64;
        let source_ms = now_ms - 2 * 3_600_000;
        write_source_receipt(
            &daily_dir,
            serde_json::json!({
                "ok": true,
                "runId": "fresh-run",
                "generatedAtMs": source_ms,
                "generatedAt": "2023-11-14T20:13:20Z",
                "directorOpinion": opinion_path.to_string_lossy()
            }),
        );

        let report = run_dream_director_send(DreamDirectorSendOptions {
            harness_home: harness_home.clone(),
            target: "fixture-private".to_string(),
            max_chars: DEFAULT_DREAM_DIRECTOR_MAX_CHARS,
            source_max_age_hours: 36.0,
            dry_run: true,
            force: false,
            now_ms,
        })
        .unwrap();

        assert!(report.receipt.ok);
        assert_eq!(report.receipt.status, "dry-run");
        assert!(!report.receipt.stale);
        assert_eq!(report.receipt.stale_reason, None);
        assert_eq!(report.receipt.source_run_id.as_deref(), Some("fresh-run"));
        assert_eq!(report.receipt.source_generated_at_ms, Some(source_ms));
        assert_eq!(report.receipt.source_age_hours, Some(2.0));
        assert_eq!(report.receipt.message_chars, Some(32));
        let written: DreamDirectorSendReceipt =
            serde_json::from_slice(&fs::read(&report.receipt_file).unwrap()).unwrap();
        assert_eq!(written.schema, DREAM_DIRECTOR_SEND_RECEIPT_SCHEMA);
        assert_eq!(written.status, "dry-run");
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn dream_director_sender_accepts_absolute_source_path_from_relative_home() {
        let root = PathBuf::from("target").join("tmp").join(format!(
            "dream-director-relative-home-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let harness_home = root.join(".agent-harness");
        let daily_dir = dream_director_daily_state_dir(&harness_home);
        let opinion_path = daily_dir
            .join("runs")
            .join("fresh")
            .join("director-opinion.md");
        fs::create_dir_all(opinion_path.parent().unwrap()).unwrap();
        fs::write(&opinion_path, "fresh daily opinion\nwith context").unwrap();
        let absolute_opinion_path = std::env::current_dir().unwrap().join(&opinion_path);
        let now_ms = 1_700_000_000_000_i64;
        let source_ms = now_ms - 2 * 3_600_000;
        write_source_receipt(
            &daily_dir,
            serde_json::json!({
                "ok": true,
                "runId": "fresh-absolute-path-run",
                "generatedAtMs": source_ms,
                "generatedAt": "2023-11-14T20:13:20Z",
                "directorOpinion": absolute_opinion_path.to_string_lossy()
            }),
        );

        let report = run_dream_director_send(DreamDirectorSendOptions {
            harness_home: harness_home.clone(),
            target: "fixture-private".to_string(),
            max_chars: DEFAULT_DREAM_DIRECTOR_MAX_CHARS,
            source_max_age_hours: 36.0,
            dry_run: true,
            force: false,
            now_ms,
        })
        .unwrap();

        assert!(report.receipt.ok);
        assert_eq!(report.receipt.status, "dry-run");
        assert!(!report.receipt.stale);
        assert_eq!(report.receipt.stale_reason, None);
        assert_eq!(
            report.receipt.source_run_id.as_deref(),
            Some("fresh-absolute-path-run")
        );
        let _ = fs::remove_dir_all(root);
    }

    fn write_source_receipt(daily_dir: &Path, value: Value) {
        fs::create_dir_all(daily_dir).unwrap();
        fs::write(
            daily_dir.join("latest.json"),
            serde_json::to_string_pretty(&value).unwrap(),
        )
        .unwrap();
    }

    fn temp_root(test_name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "agent-harness-dream-director-{test_name}-{}-{nanos}",
            std::process::id()
        ))
    }
}
