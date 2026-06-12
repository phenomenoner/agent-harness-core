use std::fs::{self, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::thread;
use std::time::Duration;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::Serialize;

const HARNESS_LOG_SCHEMA: &str = "agent-harness.log-event.v1";
const HARNESS_LOG_ROTATION_SCHEMA: &str = "agent-harness.log-rotation.v1";
const JSONL_APPEND_LOCK_STALE_MS: u128 = 30_000;
const JSONL_APPEND_LOCK_TIMEOUT_MS: u128 = 10_000;

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
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("state.json");
    let tmp_name = format!(
        ".{file_name}.{}.{}.tmp",
        std::process::id(),
        current_log_time_ms().unwrap_or(0)
    );
    let tmp_path = path.with_file_name(tmp_name);
    {
        let mut file = fs::File::create(&tmp_path)?;
        serde_json::to_writer_pretty(&mut file, value).map_err(io::Error::other)?;
        file.write_all(b"\n")?;
        file.sync_all()?;
    }
    match fs::rename(&tmp_path, path) {
        Ok(()) => Ok(()),
        Err(error)
            if matches!(
                error.kind(),
                io::ErrorKind::AlreadyExists | io::ErrorKind::PermissionDenied
            ) =>
        {
            let _ = fs::remove_file(path);
            fs::rename(&tmp_path, path)
        }
        Err(error) => {
            let _ = fs::remove_file(&tmp_path);
            Err(error)
        }
    }
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
            match OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&lock_path)
            {
                Ok(mut file) => {
                    let _ = writeln!(file, "pid={}", std::process::id());
                    return Ok(Self { path: lock_path });
                }
                Err(error)
                    if error.kind() == io::ErrorKind::AlreadyExists
                        || error.kind() == io::ErrorKind::PermissionDenied =>
                {
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
    let Ok(metadata) = fs::metadata(lock_path) else {
        return Ok(());
    };
    let Ok(modified) = metadata.modified() else {
        return Ok(());
    };
    let Ok(age) = modified.elapsed() else {
        return Ok(());
    };
    if age.as_millis() >= JSONL_APPEND_LOCK_STALE_MS {
        match fs::remove_file(lock_path) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
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
    use std::thread;
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
