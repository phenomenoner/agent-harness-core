use std::fs::{self, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::Duration;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::Serialize;

use crate::loop_health::process_alive_for_pid;

const HARNESS_LOG_SCHEMA: &str = "agent-harness.log-event.v1";
const HARNESS_LOG_ROTATION_SCHEMA: &str = "agent-harness.log-rotation.v1";
const JSONL_APPEND_LOCK_STALE_MS: u128 = 30_000;
const JSONL_APPEND_LOCK_TIMEOUT_MS: u128 = 10_000;
const JSON_ATOMIC_WRITE_LOCK_STALE_MS: u128 = 30_000;
const JSON_ATOMIC_WRITE_LOCK_TIMEOUT_MS: u128 = 10_000;

static JSON_ATOMIC_TMP_COUNTER: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HarnessLogEvent {
    pub schema: &'static str,
    pub at_ms: i64,
    pub level: HarnessLogLevel,
    pub component: String,
    pub event: String,
    pub message: String,
    pub queue_id: Option<String>,
    pub session_key: Option<String>,
    pub agent_id: Option<String>,
    pub platform: Option<String>,
    pub channel_id: Option<String>,
    pub user_id: Option<String>,
    pub path: Option<PathBuf>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum HarnessLogLevel {
    Debug,
    Info,
    Warn,
    Error,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HarnessLogWrite {
    pub log_file: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HarnessLogRotationOptions {
    pub harness_home: PathBuf,
    pub max_bytes: u64,
    pub max_archives: usize,
    pub now_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HarnessLogRotationReport {
    pub schema: &'static str,
    pub harness_home: PathBuf,
    pub log_file: PathBuf,
    pub receipt_file: PathBuf,
    pub status: HarnessLogRotationStatus,
    pub original_bytes: u64,
    pub rotated_to: Option<PathBuf>,
    pub removed_archives: Vec<PathBuf>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum HarnessLogRotationStatus {
    Missing,
    Unchanged,
    Rotated,
}

impl HarnessLogEvent {
    pub fn new(
        at_ms: i64,
        level: HarnessLogLevel,
        component: impl Into<String>,
        event: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        Self {
            schema: HARNESS_LOG_SCHEMA,
            at_ms,
            level,
            component: component.into(),
            event: event.into(),
            message: message.into(),
            queue_id: None,
            session_key: None,
            agent_id: None,
            platform: None,
            channel_id: None,
            user_id: None,
            path: None,
        }
    }

    pub fn queue_id(mut self, queue_id: Option<String>) -> Self {
        self.queue_id = queue_id;
        self
    }

    pub fn session_key(mut self, session_key: Option<String>) -> Self {
        self.session_key = session_key;
        self
    }

    pub fn agent_id(mut self, agent_id: Option<String>) -> Self {
        self.agent_id = agent_id;
        self
    }

    pub fn channel(
        mut self,
        platform: impl Into<String>,
        channel_id: impl Into<String>,
        user_id: impl Into<String>,
    ) -> Self {
        self.platform = Some(platform.into());
        self.channel_id = Some(channel_id.into());
        self.user_id = Some(user_id.into());
        self
    }

    pub fn path(mut self, path: Option<PathBuf>) -> Self {
        self.path = path;
        self
    }
}

pub fn harness_log_file(harness_home: impl AsRef<Path>) -> PathBuf {
    harness_home
        .as_ref()
        .join("state")
        .join("logs")
        .join("harness.jsonl")
}

pub fn append_harness_log(
    harness_home: impl AsRef<Path>,
    event: &HarnessLogEvent,
) -> io::Result<HarnessLogWrite> {
    let log_file = harness_log_file(harness_home);
    append_jsonl_value(&log_file, event)?;
    Ok(HarnessLogWrite { log_file })
}

pub fn probe_harness_log_writable(harness_home: impl AsRef<Path>) -> io::Result<PathBuf> {
    let log_file = harness_log_file(harness_home);
    let Some(parent) = log_file.parent() else {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("log file has no parent: {}", log_file.display()),
        ));
    };
    fs::create_dir_all(parent)?;
    let probe = parent.join(".agent-harness-log-probe.tmp");
    fs::write(&probe, b"log-probe")?;
    let _ = fs::remove_file(probe);
    Ok(log_file)
}

pub fn rotate_harness_log_if_needed(
    options: HarnessLogRotationOptions,
) -> io::Result<HarnessLogRotationReport> {
    let log_file = harness_log_file(&options.harness_home);
    let receipt_file = options
        .harness_home
        .join("state")
        .join("logs")
        .join("log-rotation-receipts.jsonl");
    let max_bytes = options.max_bytes.max(1);
    let mut report = HarnessLogRotationReport {
        schema: HARNESS_LOG_ROTATION_SCHEMA,
        harness_home: options.harness_home.clone(),
        log_file: log_file.clone(),
        receipt_file: receipt_file.clone(),
        status: HarnessLogRotationStatus::Missing,
        original_bytes: 0,
        rotated_to: None,
        removed_archives: Vec::new(),
    };

    let metadata = match fs::metadata(&log_file) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            append_json_line(&receipt_file, &report)?;
            return Ok(report);
        }
        Err(error) => return Err(error),
    };
    report.original_bytes = metadata.len();
    if metadata.len() <= max_bytes {
        report.status = HarnessLogRotationStatus::Unchanged;
        append_json_line(&receipt_file, &report)?;
        return Ok(report);
    }

    let archive = log_file.with_file_name(format!("harness-{}.jsonl", options.now_ms));
    fs::rename(&log_file, &archive)?;
    fs::write(&log_file, b"")?;
    report.status = HarnessLogRotationStatus::Rotated;
    report.rotated_to = Some(archive);
    report.removed_archives = prune_log_archives(
        log_file.parent().unwrap_or_else(|| Path::new(".")),
        options.max_archives,
    )?;
    append_json_line(&receipt_file, &report)?;
    Ok(report)
}

pub fn write_json_atomic(path: &Path, value: &impl Serialize) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let _guard = JsonAtomicWriteLock::acquire(path)?;
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("state.json");
    let tmp_counter = JSON_ATOMIC_TMP_COUNTER.fetch_add(1, Ordering::Relaxed);
    let tmp_name = format!(
        ".{file_name}.{}.{}.{}.tmp",
        std::process::id(),
        current_log_time_ms().unwrap_or(0),
        tmp_counter
    );
    let tmp_path = path.with_file_name(tmp_name);
    {
        let mut file = fs::File::create(&tmp_path)?;
        serde_json::to_writer_pretty(&mut file, value).map_err(io::Error::other)?;
        file.write_all(b"\n")?;
        file.sync_all()?;
    }
    replace_json_atomic_target(&tmp_path, path)
}

pub fn append_jsonl_value(path: &Path, value: &impl Serialize) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let _guard = JsonlAppendLock::acquire(path)?;
    let needs_leading_newline = jsonl_needs_leading_newline(path)?;
    let mut file = OpenOptions::new().create(true).append(true).open(path)?;
    let mut line = Vec::new();
    if needs_leading_newline {
        line.push(b'\n');
    }
    line.extend(serde_json::to_vec(value).map_err(io::Error::other)?);
    line.push(b'\n');
    file.write_all(&line)?;
    file.flush()?;
    Ok(())
}

/// Runs a transaction that changes a JSONL ledger while holding the same lock
/// used by normal appenders. Callers that compact or replace a ledger must use
/// this helper instead of racing a concurrent append.
pub fn with_jsonl_append_lock<T>(
    path: &Path,
    operation: impl FnOnce() -> io::Result<T>,
) -> io::Result<T> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let _guard = JsonlAppendLock::acquire(path)?;
    operation()
}

/// Runs a JSONL-ledger operation only when the append lock is immediately
/// available.  Hot paths use this to prefer a safe materialized snapshot over
/// waiting behind bounded maintenance such as receipt compaction.
pub(crate) fn try_with_jsonl_append_lock<T>(
    path: &Path,
    operation: impl FnOnce() -> io::Result<T>,
) -> io::Result<Option<T>> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let Some(_guard) = JsonlAppendLock::try_acquire(path)? else {
        return Ok(None);
    };
    operation().map(Some)
}

pub fn append_jsonl_value_once_by_event_key(
    path: &Path,
    value: &impl Serialize,
) -> io::Result<bool> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let serialized = serde_json::to_value(value).map_err(io::Error::other)?;
    let event_key = serialized
        .get("eventKey")
        .and_then(serde_json::Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "eventKey is required"))?;
    let _guard = JsonlAppendLock::acquire(path)?;
    let text = match fs::read_to_string(path) {
        Ok(text) => text,
        Err(error) if error.kind() == io::ErrorKind::NotFound => String::new(),
        Err(error) => return Err(error),
    };
    for (index, line) in text.lines().enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        let existing = serde_json::from_str::<serde_json::Value>(line).map_err(|error| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("invalid JSONL record at line {}: {error}", index + 1),
            )
        })?;
        if existing.get("eventKey").and_then(serde_json::Value::as_str) == Some(event_key) {
            return Ok(false);
        }
    }
    let needs_leading_newline = jsonl_needs_leading_newline(path)?;
    let mut file = OpenOptions::new().create(true).append(true).open(path)?;
    if needs_leading_newline {
        file.write_all(b"\n")?;
    }
    serde_json::to_writer(&mut file, &serialized).map_err(io::Error::other)?;
    file.write_all(b"\n")?;
    file.flush()?;
    Ok(true)
}

fn append_json_line(path: &Path, value: &impl Serialize) -> io::Result<()> {
    append_jsonl_value(path, value)
}

struct JsonlAppendLock {
    path: PathBuf,
}

impl JsonlAppendLock {
    fn acquire(jsonl_path: &Path) -> io::Result<Self> {
        let lock_path = jsonl_append_lock_path(jsonl_path);
        let start = SystemTime::now();
        loop {
            match Self::try_create(&lock_path) {
                Ok(lock) => return Ok(lock),
                Err(error) if lock_acquire_error_is_busy(&error) => {
                    remove_stale_jsonl_lock(&lock_path)?;
                    let elapsed = start.elapsed().map_err(io::Error::other)?.as_millis();
                    if elapsed >= JSONL_APPEND_LOCK_TIMEOUT_MS {
                        return Err(io::Error::new(
                            io::ErrorKind::TimedOut,
                            format!(
                                "timed out waiting for JSONL append lock {}",
                                lock_path.display()
                            ),
                        ));
                    }
                    thread::sleep(Duration::from_millis(5));
                }
                Err(error) => return Err(error),
            }
        }
    }

    fn try_acquire(jsonl_path: &Path) -> io::Result<Option<Self>> {
        let lock_path = jsonl_append_lock_path(jsonl_path);
        match Self::try_create(&lock_path) {
            Ok(lock) => Ok(Some(lock)),
            Err(error) if lock_acquire_error_is_busy(&error) => {
                remove_stale_jsonl_lock(&lock_path)?;
                match Self::try_create(&lock_path) {
                    Ok(lock) => Ok(Some(lock)),
                    Err(error) if lock_acquire_error_is_busy(&error) => Ok(None),
                    Err(error) => Err(error),
                }
            }
            Err(error) => Err(error),
        }
    }

    fn try_create(lock_path: &Path) -> io::Result<Self> {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(lock_path)?;
        let _ = writeln!(file, "pid={}", std::process::id());
        Ok(Self {
            path: lock_path.to_path_buf(),
        })
    }
}

impl Drop for JsonlAppendLock {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

fn jsonl_append_lock_path(jsonl_path: &Path) -> PathBuf {
    let file_name = jsonl_path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("ledger.jsonl");
    jsonl_path.with_file_name(format!(".{file_name}.append.lock"))
}

struct JsonAtomicWriteLock {
    path: PathBuf,
}

impl JsonAtomicWriteLock {
    fn acquire(json_path: &Path) -> io::Result<Self> {
        let lock_path = json_atomic_write_lock_path(json_path);
        let start = SystemTime::now();
        loop {
            match OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&lock_path)
            {
                Ok(mut file) => {
                    let _ = writeln!(file, "pid={}", std::process::id());
                    return Ok(Self { path: lock_path });
                }
                Err(error) if lock_acquire_error_is_busy(&error) => {
                    remove_stale_lock(&lock_path, JSON_ATOMIC_WRITE_LOCK_STALE_MS)?;
                    let elapsed = start.elapsed().map_err(io::Error::other)?.as_millis();
                    if elapsed >= JSON_ATOMIC_WRITE_LOCK_TIMEOUT_MS {
                        return Err(io::Error::new(
                            io::ErrorKind::TimedOut,
                            format!(
                                "timed out waiting for JSON atomic write lock {}",
                                lock_path.display()
                            ),
                        ));
                    }
                    thread::sleep(Duration::from_millis(5));
                }
                Err(error) => return Err(error),
            }
        }
    }
}

impl Drop for JsonAtomicWriteLock {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

fn json_atomic_write_lock_path(json_path: &Path) -> PathBuf {
    let file_name = json_path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("state.json");
    json_path.with_file_name(format!(".{file_name}.atomic.lock"))
}

fn replace_json_atomic_target(tmp_path: &Path, path: &Path) -> io::Result<()> {
    let start = SystemTime::now();
    loop {
        match fs::rename(tmp_path, path) {
            Ok(()) => return Ok(()),
            Err(error)
                if matches!(
                    error.kind(),
                    io::ErrorKind::AlreadyExists | io::ErrorKind::PermissionDenied
                ) || windows_sharing_violation(&error) =>
            {
                match fs::remove_file(path) {
                    Ok(()) => {}
                    Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                    Err(error)
                        if (error.kind() == io::ErrorKind::PermissionDenied
                            || windows_sharing_violation(&error))
                            && start.elapsed().map_err(io::Error::other)?.as_millis()
                                < JSON_ATOMIC_WRITE_LOCK_TIMEOUT_MS =>
                    {
                        thread::sleep(Duration::from_millis(5));
                        continue;
                    }
                    Err(error) => {
                        let _ = fs::remove_file(tmp_path);
                        return Err(error);
                    }
                }
                match fs::rename(tmp_path, path) {
                    Ok(()) => return Ok(()),
                    Err(error)
                        if (error.kind() == io::ErrorKind::PermissionDenied
                            || windows_sharing_violation(&error))
                            && start.elapsed().map_err(io::Error::other)?.as_millis()
                                < JSON_ATOMIC_WRITE_LOCK_TIMEOUT_MS =>
                    {
                        thread::sleep(Duration::from_millis(5));
                    }
                    Err(error) => {
                        let _ = fs::remove_file(tmp_path);
                        return Err(error);
                    }
                }
            }
            Err(error) => {
                let _ = fs::remove_file(tmp_path);
                return Err(error);
            }
        }
    }
}

fn windows_sharing_violation(error: &io::Error) -> bool {
    #[cfg(windows)]
    {
        error.raw_os_error() == Some(32)
    }
    #[cfg(not(windows))]
    {
        let _ = error;
        false
    }
}

fn lock_acquire_error_is_busy(error: &io::Error) -> bool {
    matches!(
        error.kind(),
        io::ErrorKind::AlreadyExists | io::ErrorKind::PermissionDenied | io::ErrorKind::WouldBlock
    ) || windows_sharing_violation(error)
}

fn jsonl_needs_leading_newline(path: &Path) -> io::Result<bool> {
    let metadata = match fs::metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(error),
    };
    if !metadata.is_file() || metadata.len() == 0 {
        return Ok(false);
    }
    let mut file = fs::File::open(path)?;
    file.seek(SeekFrom::End(-1))?;
    let mut last_byte = [0_u8; 1];
    file.read_exact(&mut last_byte)?;
    Ok(last_byte[0] != b'\n')
}

fn remove_stale_jsonl_lock(lock_path: &Path) -> io::Result<()> {
    // A JSONL append lock can now protect a bounded but comparatively expensive
    // receipt compaction.  Age alone is not a safe ownership signal: stealing
    // a live writer's lock can allow an append to race a snapshot replacement.
    // The owner PID is written when the lock is acquired, so retain a lock held
    // by a process we can still prove is alive.  If the owner is dead, recover
    // the orphan immediately; malformed/unknown lock files retain the legacy
    // age-based fallback.
    if let Some(owner_pid) = jsonl_append_lock_owner_pid(lock_path) {
        match process_alive_for_pid(owner_pid) {
            Some(true) => return Ok(()),
            Some(false) => {
                match fs::remove_file(lock_path) {
                    Ok(()) => {}
                    Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                    Err(error) if lock_acquire_error_is_busy(&error) => {}
                    Err(error) => return Err(error),
                }
                return Ok(());
            }
            None => {}
        }
    }
    remove_stale_lock(lock_path, JSONL_APPEND_LOCK_STALE_MS)
}

fn jsonl_append_lock_owner_pid(lock_path: &Path) -> Option<i64> {
    let text = fs::read_to_string(lock_path).ok()?;
    text.lines().find_map(|line| {
        line.strip_prefix("pid=")
            .and_then(|value| value.trim().parse::<i64>().ok())
    })
}

fn remove_stale_lock(lock_path: &Path, stale_ms: u128) -> io::Result<()> {
    let Ok(metadata) = fs::metadata(lock_path) else {
        return Ok(());
    };
    let Ok(modified) = metadata.modified() else {
        return Ok(());
    };
    let Ok(age) = modified.elapsed() else {
        return Ok(());
    };
    if age.as_millis() >= stale_ms {
        match fs::remove_file(lock_path) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) if lock_acquire_error_is_busy(&error) => {}
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

fn prune_log_archives(log_dir: &Path, max_archives: usize) -> io::Result<Vec<PathBuf>> {
    let mut archives = Vec::new();
    if !log_dir.is_dir() {
        return Ok(Vec::new());
    }
    for entry in fs::read_dir(log_dir)? {
        let entry = entry?;
        let path = entry.path();
        let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        if name.starts_with("harness-") && name.ends_with(".jsonl") {
            let modified = entry.metadata()?.modified().ok();
            archives.push((path, modified));
        }
    }
    archives.sort_by(|left, right| right.1.cmp(&left.1).then_with(|| right.0.cmp(&left.0)));
    let mut removed = Vec::new();
    for (path, _) in archives.into_iter().skip(max_archives) {
        fs::remove_file(&path)?;
        removed.push(path);
    }
    Ok(removed)
}

pub fn current_log_time_ms() -> io::Result<i64> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(io::Error::other)?;
    i64::try_from(duration.as_millis()).map_err(io::Error::other)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    #[cfg(windows)]
    use std::fs::FileTimes;
    use std::thread;
    #[cfg(windows)]
    use std::time::Duration;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn append_harness_log_writes_jsonl() {
        let root = temp_root("append_harness_log_writes_jsonl");
        let harness_home = root.join(".agent-harness");

        let write = append_harness_log(
            &harness_home,
            &HarnessLogEvent::new(
                1234,
                HarnessLogLevel::Info,
                "channel",
                "channel.receive",
                "received message",
            )
            .queue_id(Some("queue-1".to_string()))
            .session_key(Some("session-1".to_string()))
            .channel("telegram", "dm", "user"),
        )
        .unwrap();

        assert!(write.log_file.is_file());
        let text = fs::read_to_string(write.log_file).unwrap();
        assert!(text.contains("\"schema\":\"agent-harness.log-event.v1\""));
        assert!(text.contains("\"component\":\"channel\""));
        assert!(text.contains("\"queueId\":\"queue-1\""));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn append_jsonl_value_serializes_concurrent_writers() {
        let root = temp_root("append_jsonl_value_serializes_concurrent_writers");
        let path = root
            .join("state")
            .join("runtime-queue")
            .join("receipts.jsonl");
        let mut handles = Vec::new();

        for writer in 0..12 {
            let path = path.clone();
            handles.push(thread::spawn(move || {
                for entry in 0..100 {
                    append_jsonl_value(
                        &path,
                        &json!({
                            "writer": writer,
                            "entry": entry,
                            "payload": "jsonl append regression"
                        }),
                    )
                    .unwrap();
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        let text = fs::read_to_string(&path).unwrap();
        let lines: Vec<&str> = text.lines().collect();
        assert_eq!(lines.len(), 1200);
        for line in lines {
            let value: serde_json::Value = serde_json::from_str(line).unwrap();
            assert_eq!(value["payload"], "jsonl append regression");
        }
        assert!(!jsonl_append_lock_path(&path).exists());

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn jsonl_append_lock_recovers_dead_owner_without_waiting_for_stale_deadline() {
        let root =
            temp_root("jsonl_append_lock_recovers_dead_owner_without_waiting_for_stale_deadline");
        let path = root.join("state").join("receipts.jsonl");
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        let lock = jsonl_append_lock_path(&path);
        fs::write(&lock, "pid=0\n").unwrap();

        remove_stale_jsonl_lock(&lock).unwrap();

        assert!(
            !lock.exists(),
            "a lock owned by a definitively dead process must be recovered promptly"
        );
        let _ = fs::remove_dir_all(root);
    }

    #[cfg(windows)]
    #[test]
    fn jsonl_append_lock_never_steals_a_live_owner_after_the_legacy_stale_deadline() {
        let root = temp_root(
            "jsonl_append_lock_never_steals_a_live_owner_after_the_legacy_stale_deadline",
        );
        let path = root.join("state").join("receipts.jsonl");
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        let lock = jsonl_append_lock_path(&path);
        fs::write(&lock, format!("pid={}\n", std::process::id())).unwrap();
        let old = SystemTime::now()
            .checked_sub(Duration::from_millis(JSONL_APPEND_LOCK_STALE_MS as u64 + 1))
            .unwrap();
        OpenOptions::new()
            .write(true)
            .open(&lock)
            .unwrap()
            .set_times(FileTimes::new().set_modified(old))
            .unwrap();

        remove_stale_jsonl_lock(&lock).unwrap();

        assert!(
            lock.exists(),
            "a long-running compaction owner must retain its append lock while alive"
        );
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn append_jsonl_value_separates_existing_partial_line() {
        let root = temp_root("append_jsonl_value_separates_existing_partial_line");
        let path = root
            .join("state")
            .join("runtime-queue")
            .join("receipts.jsonl");
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(&path, br#"{"existing":true}"#).unwrap();

        append_jsonl_value(&path, &json!({"next": true})).unwrap();

        let text = fs::read_to_string(&path).unwrap();
        let lines: Vec<&str> = text.lines().collect();
        assert_eq!(lines.len(), 2);
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(lines[0]).unwrap()["existing"],
            true
        );
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(lines[1]).unwrap()["next"],
            true
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn append_jsonl_value_once_deduplicates_stable_event_key() {
        let root = temp_root("append_jsonl_value_once_deduplicates_stable_event_key");
        let path = root.join("state").join("events.jsonl");
        let first = json!({"eventKey":"attempt-1/1","action":"pending"});
        let duplicate = json!({"eventKey":"attempt-1/1","action":"pending-replay"});
        let second = json!({"eventKey":"attempt-1/2","action":"sent"});

        assert!(append_jsonl_value_once_by_event_key(&path, &first).unwrap());
        assert!(!append_jsonl_value_once_by_event_key(&path, &duplicate).unwrap());
        assert!(append_jsonl_value_once_by_event_key(&path, &second).unwrap());

        let lines = fs::read_to_string(&path).unwrap();
        assert_eq!(lines.lines().count(), 2);
        assert!(!lines.contains("pending-replay"));
        assert!(!jsonl_append_lock_path(&path).exists());
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn append_jsonl_value_once_fails_closed_on_unreadable_or_malformed_journal() {
        let root =
            temp_root("append_jsonl_value_once_fails_closed_on_unreadable_or_malformed_journal");
        let malformed = root.join("malformed.jsonl");
        fs::create_dir_all(malformed.parent().unwrap()).unwrap();
        fs::write(&malformed, b"{not-json}\n").unwrap();
        let before = fs::read(&malformed).unwrap();
        let error =
            append_jsonl_value_once_by_event_key(&malformed, &json!({"eventKey":"attempt-1/1"}))
                .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert_eq!(fs::read(&malformed).unwrap(), before);

        let non_utf8 = root.join("non-utf8.jsonl");
        fs::write(&non_utf8, [0xff]).unwrap();
        let error =
            append_jsonl_value_once_by_event_key(&non_utf8, &json!({"eventKey":"attempt-1/2"}))
                .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert_eq!(fs::read(&non_utf8).unwrap(), vec![0xff]);
        assert!(!jsonl_append_lock_path(&malformed).exists());
        assert!(!jsonl_append_lock_path(&non_utf8).exists());
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn write_json_atomic_replaces_existing_json() {
        let root = temp_root("write_json_atomic_replaces_existing_json");
        let path = root.join("state").join("example.json");

        write_json_atomic(&path, &serde_json::json!({"value": 1})).unwrap();
        write_json_atomic(&path, &serde_json::json!({"value": 2})).unwrap();

        let value: serde_json::Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        assert_eq!(value["value"], 2);
        assert!(fs::read_dir(path.parent().unwrap()).unwrap().all(|entry| {
            !entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .ends_with(".tmp")
        }));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn write_json_atomic_serializes_concurrent_replacements() {
        let root = temp_root("write_json_atomic_serializes_concurrent_replacements");
        let path = root
            .join("state")
            .join("runtime-queue")
            .join("run-once-last.json");
        let mut handles = Vec::new();

        for writer in 0..12 {
            let path = path.clone();
            handles.push(thread::spawn(move || {
                for entry in 0..50 {
                    write_json_atomic(
                        &path,
                        &json!({
                            "writer": writer,
                            "entry": entry,
                            "payload": "atomic json regression"
                        }),
                    )
                    .unwrap();
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        let text = fs::read_to_string(&path).unwrap();
        let value: serde_json::Value = serde_json::from_str(&text).unwrap();
        assert_eq!(value["payload"], "atomic json regression");
        assert!(!json_atomic_write_lock_path(&path).exists());

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn rotate_harness_log_moves_large_log_and_writes_receipt() {
        let root = temp_root("rotate_harness_log_moves_large_log_and_writes_receipt");
        let harness_home = root.join(".agent-harness");
        let log_file = harness_log_file(&harness_home);
        fs::create_dir_all(log_file.parent().unwrap()).unwrap();
        fs::write(&log_file, "0123456789\n0123456789\n").unwrap();

        let report = rotate_harness_log_if_needed(HarnessLogRotationOptions {
            harness_home: harness_home.clone(),
            max_bytes: 8,
            max_archives: 2,
            now_ms: 12345,
        })
        .unwrap();

        assert_eq!(report.status, HarnessLogRotationStatus::Rotated);
        assert!(report.rotated_to.as_ref().unwrap().is_file());
        assert_eq!(fs::read_to_string(&log_file).unwrap(), "");
        let receipts = fs::read_to_string(report.receipt_file).unwrap();
        assert!(receipts.contains("\"status\":\"rotated\""));
        assert!(receipts.contains("harness-12345.jsonl"));

        let _ = fs::remove_dir_all(root);
    }

    fn temp_root(test_name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "agent-harness-logging-{test_name}-{}-{nanos}",
            std::process::id()
        ))
    }
}
