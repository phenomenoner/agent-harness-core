use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use serde::Deserialize;
use serde::Serialize;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SupervisorStopFileStatus {
    pub path: PathBuf,
    pub present: bool,
    pub reason: Option<String>,
    pub service_id: Option<String>,
    pub created_by: Option<String>,
    pub created_at_ms: Option<i64>,
    pub expires_at_ms: Option<i64>,
    pub persistent: Option<bool>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SupervisorStopFileEnvelope {
    #[serde(default)]
    service_id: Option<String>,
    #[serde(default)]
    reason: Option<String>,
    #[serde(default)]
    created_by: Option<String>,
    #[serde(default)]
    created_at_ms: Option<i64>,
    #[serde(default)]
    expires_at_ms: Option<i64>,
    #[serde(default)]
    persistent: Option<bool>,
}

pub fn supervisor_stop_file_path(harness_home: impl AsRef<Path>, name: &str) -> PathBuf {
    harness_home
        .as_ref()
        .join("state")
        .join("supervisor")
        .join("windows-scheduled-tasks")
        .join("stop")
        .join(format!("{name}.stop"))
}

pub fn read_supervisor_stop_file(
    harness_home: impl AsRef<Path>,
    name: &str,
) -> io::Result<SupervisorStopFileStatus> {
    let harness_home = harness_home.as_ref();
    let mut first_path = None;
    for path in supervisor_stop_file_candidates(harness_home, name) {
        if first_path.is_none() {
            first_path = Some(path.clone());
        }
        match fs::read_to_string(&path) {
            Ok(text) => {
                let parsed = parse_stop_file_text(&text);
                return Ok(SupervisorStopFileStatus {
                    path,
                    present: true,
                    reason: parsed
                        .reason
                        .or_else(|| Some(truncate_stop_reason(text.trim()))),
                    service_id: parsed.service_id,
                    created_by: parsed.created_by,
                    created_at_ms: parsed.created_at_ms,
                    expires_at_ms: parsed.expires_at_ms,
                    persistent: parsed.persistent,
                });
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
    }
    Ok(SupervisorStopFileStatus {
        path: first_path.unwrap_or_else(|| supervisor_stop_file_path(harness_home, name)),
        present: false,
        reason: None,
        service_id: None,
        created_by: None,
        created_at_ms: None,
        expires_at_ms: None,
        persistent: None,
    })
}

fn supervisor_stop_file_candidates(harness_home: &Path, name: &str) -> [PathBuf; 2] {
    [
        supervisor_stop_file_path(harness_home, name),
        harness_home
            .join("state")
            .join("supervisor")
            .join("stop")
            .join(format!("{name}.stop")),
    ]
}

pub fn process_alive_for_pid(process_id: i64) -> Option<bool> {
    if process_id <= 0 {
        return Some(false);
    }
    let Ok(process_id) = u32::try_from(process_id) else {
        return Some(false);
    };
    process_alive_for_pid_u32(process_id)
}

#[cfg(windows)]
fn process_alive_for_pid_u32(process_id: u32) -> Option<bool> {
    use windows_sys::Win32::Foundation::{CloseHandle, STILL_ACTIVE};
    use windows_sys::Win32::System::Threading::{
        GetExitCodeProcess, OpenProcess, PROCESS_QUERY_LIMITED_INFORMATION,
    };

    unsafe {
        let handle = OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, 0, process_id);
        if handle == 0 {
            return Some(false);
        }
        let mut exit_code = 0u32;
        let ok = GetExitCodeProcess(handle, &mut exit_code);
        let _ = CloseHandle(handle);
        if ok == 0 {
            None
        } else {
            Some(exit_code == STILL_ACTIVE as u32)
        }
    }
}

#[cfg(not(windows))]
fn process_alive_for_pid_u32(_process_id: u32) -> Option<bool> {
    None
}

fn truncate_stop_reason(value: &str) -> String {
    const MAX_CHARS: usize = 512;
    let mut chars = value.chars();
    let truncated = chars.by_ref().take(MAX_CHARS).collect::<String>();
    if chars.next().is_some() {
        format!("{truncated}...")
    } else {
        truncated
    }
}

fn parse_stop_file_text(text: &str) -> SupervisorStopFileEnvelope {
    serde_json::from_str::<SupervisorStopFileEnvelope>(text).unwrap_or(SupervisorStopFileEnvelope {
        service_id: None,
        reason: None,
        created_by: None,
        created_at_ms: None,
        expires_at_ms: None,
        persistent: None,
    })
}
