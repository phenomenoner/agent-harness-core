use std::fs::{self, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::Serialize;

const HARNESS_LOG_SCHEMA: &str = "agent-harness.log-event.v1";

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
    if let Some(parent) = log_file.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log_file)?;
    let line = serde_json::to_string(event).map_err(io::Error::other)?;
    writeln!(file, "{line}")?;
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

pub fn current_log_time_ms() -> io::Result<i64> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(io::Error::other)?;
    i64::try_from(duration.as_millis()).map_err(io::Error::other)
}

#[cfg(test)]
mod tests {
    use super::*;
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
