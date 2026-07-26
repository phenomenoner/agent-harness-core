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
            .join("windows-scheduled-tasks")
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

pub fn process_start_time_ms_for_pid(process_id: i64) -> Option<i64> {
    if process_id <= 0 {
        return None;
    }
    let process_id = u32::try_from(process_id).ok()?;
    process_start_time_ms_for_pid_u32(process_id)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ExactProcessGenerationState {
    Alive,
    DeadOrReused,
    Unknown,
}

pub(crate) fn exact_process_generation_state(
    process_id: i64,
    expected_start_time_ms: i64,
) -> ExactProcessGenerationState {
    if process_id <= 0 || expected_start_time_ms < 0 {
        return ExactProcessGenerationState::Unknown;
    }
    let Ok(process_id) = u32::try_from(process_id) else {
        return ExactProcessGenerationState::Unknown;
    };
    exact_process_generation_state_u32(process_id, expected_start_time_ms)
}

#[cfg(windows)]
fn exact_process_generation_state_u32(
    process_id: u32,
    expected_start_time_ms: i64,
) -> ExactProcessGenerationState {
    use windows_sys::Win32::Foundation::{
        CloseHandle, ERROR_INVALID_PARAMETER, FILETIME, STILL_ACTIVE,
    };
    use windows_sys::Win32::System::Threading::{
        GetExitCodeProcess, GetProcessTimes, OpenProcess, PROCESS_QUERY_LIMITED_INFORMATION,
    };

    const WINDOWS_TO_UNIX_EPOCH_100NS: u64 = 116_444_736_000_000_000;
    unsafe {
        let handle = OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, 0, process_id);
        if handle == 0 {
            return if io::Error::last_os_error().raw_os_error()
                == Some(ERROR_INVALID_PARAMETER as i32)
            {
                ExactProcessGenerationState::DeadOrReused
            } else {
                ExactProcessGenerationState::Unknown
            };
        }
        let mut exit_code = 0_u32;
        if GetExitCodeProcess(handle, &mut exit_code) == 0 {
            let _ = CloseHandle(handle);
            return ExactProcessGenerationState::Unknown;
        }
        if exit_code != STILL_ACTIVE as u32 {
            let _ = CloseHandle(handle);
            return ExactProcessGenerationState::DeadOrReused;
        }
        let mut created = FILETIME {
            dwLowDateTime: 0,
            dwHighDateTime: 0,
        };
        let mut exited = created;
        let mut kernel = created;
        let mut user = created;
        let ok = GetProcessTimes(handle, &mut created, &mut exited, &mut kernel, &mut user);
        let _ = CloseHandle(handle);
        if ok == 0 {
            return ExactProcessGenerationState::Unknown;
        }
        let created_100ns =
            (u64::from(created.dwHighDateTime) << 32) | u64::from(created.dwLowDateTime);
        let Some(unix_100ns) = created_100ns.checked_sub(WINDOWS_TO_UNIX_EPOCH_100NS) else {
            return ExactProcessGenerationState::Unknown;
        };
        let Ok(observed_start_time_ms) = i64::try_from(unix_100ns / 10_000) else {
            return ExactProcessGenerationState::Unknown;
        };
        if observed_start_time_ms == expected_start_time_ms {
            ExactProcessGenerationState::Alive
        } else {
            ExactProcessGenerationState::DeadOrReused
        }
    }
}

#[cfg(target_os = "linux")]
fn exact_process_generation_state_u32(
    process_id: u32,
    expected_start_time_ms: i64,
) -> ExactProcessGenerationState {
    match process_alive_for_pid_u32(process_id) {
        Some(false) => ExactProcessGenerationState::DeadOrReused,
        Some(true) => match process_start_time_ms_for_pid_u32(process_id) {
            Some(observed) if observed == expected_start_time_ms => {
                ExactProcessGenerationState::Alive
            }
            Some(_) => ExactProcessGenerationState::DeadOrReused,
            None => ExactProcessGenerationState::Unknown,
        },
        None => ExactProcessGenerationState::Unknown,
    }
}

#[cfg(all(not(windows), not(target_os = "linux")))]
fn exact_process_generation_state_u32(
    _process_id: u32,
    _expected_start_time_ms: i64,
) -> ExactProcessGenerationState {
    ExactProcessGenerationState::Unknown
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

#[cfg(windows)]
fn process_start_time_ms_for_pid_u32(process_id: u32) -> Option<i64> {
    use windows_sys::Win32::Foundation::{CloseHandle, FILETIME};
    use windows_sys::Win32::System::Threading::{
        GetProcessTimes, OpenProcess, PROCESS_QUERY_LIMITED_INFORMATION,
    };

    const WINDOWS_TO_UNIX_EPOCH_100NS: u64 = 116_444_736_000_000_000;
    unsafe {
        let handle = OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, 0, process_id);
        if handle == 0 {
            return None;
        }
        let mut created = FILETIME {
            dwLowDateTime: 0,
            dwHighDateTime: 0,
        };
        let mut exited = created;
        let mut kernel = created;
        let mut user = created;
        let ok = GetProcessTimes(handle, &mut created, &mut exited, &mut kernel, &mut user);
        let _ = CloseHandle(handle);
        if ok == 0 {
            return None;
        }
        let created_100ns =
            (u64::from(created.dwHighDateTime) << 32) | u64::from(created.dwLowDateTime);
        let unix_100ns = created_100ns.checked_sub(WINDOWS_TO_UNIX_EPOCH_100NS)?;
        i64::try_from(unix_100ns / 10_000).ok()
    }
}

#[cfg(target_os = "linux")]
fn process_alive_for_pid_u32(process_id: u32) -> Option<bool> {
    match linux_process_stat(process_id) {
        Ok((state, _)) => Some(!matches!(state, 'Z' | 'X' | 'x')),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Some(false),
        Err(_) => None,
    }
}

#[cfg(all(not(windows), not(target_os = "linux")))]
fn process_alive_for_pid_u32(_process_id: u32) -> Option<bool> {
    None
}

#[cfg(target_os = "linux")]
fn process_start_time_ms_for_pid_u32(process_id: u32) -> Option<i64> {
    let (_, start_ticks) = linux_process_stat(process_id).ok()?;
    let ticks_per_second = linux_clock_ticks_per_second()?;
    let boot_time_ms = linux_boot_time_ms()?;
    let elapsed_ms = start_ticks
        .checked_mul(1_000)?
        .checked_div(ticks_per_second)?;
    i64::try_from(boot_time_ms.checked_add(elapsed_ms)?).ok()
}

#[cfg(all(not(windows), not(target_os = "linux")))]
fn process_start_time_ms_for_pid_u32(_process_id: u32) -> Option<i64> {
    None
}

#[cfg(target_os = "linux")]
fn linux_process_stat(process_id: u32) -> io::Result<(char, u64)> {
    let text = fs::read_to_string(format!("/proc/{process_id}/stat"))?;
    let command_end = text.rfind(')').ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "Linux process stat has no command terminator",
        )
    })?;
    let fields = text[command_end + 1..]
        .split_whitespace()
        .collect::<Vec<_>>();
    let state = fields
        .first()
        .and_then(|value| value.chars().next())
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "Linux process stat has no state field",
            )
        })?;
    // After removing pid and the parenthesized command, index 0 is field 3
    // (`state`), so index 19 is field 22 (`starttime`).
    let start_ticks = fields
        .get(19)
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "Linux process stat has no starttime field",
            )
        })?
        .parse::<u64>()
        .map_err(|error| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Linux process starttime is invalid: {error}"),
            )
        })?;
    Ok((state, start_ticks))
}

#[cfg(target_os = "linux")]
fn linux_boot_time_ms() -> Option<u64> {
    fs::read_to_string("/proc/stat")
        .ok()?
        .lines()
        .find_map(|line| line.strip_prefix("btime "))?
        .trim()
        .parse::<u64>()
        .ok()?
        .checked_mul(1_000)
}

#[cfg(target_os = "linux")]
fn linux_clock_ticks_per_second() -> Option<u64> {
    const SC_CLK_TCK: i32 = 2;
    unsafe extern "C" {
        fn sysconf(name: i32) -> isize;
    }
    let ticks = unsafe { sysconf(SC_CLK_TCK) };
    (ticks > 0).then(|| ticks as u64)
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

#[cfg(all(test, windows))]
mod windows_tests {
    use super::*;
    use std::os::windows::io::{FromRawHandle, OwnedHandle};
    use std::process::Command;
    use windows_sys::Win32::System::Threading::{OpenProcess, PROCESS_QUERY_LIMITED_INFORMATION};

    #[test]
    fn process_start_time_is_generation_identity_not_liveness() {
        let mut command = Command::new("powershell.exe");
        command.args([
            "-NoLogo",
            "-NoProfile",
            "-NonInteractive",
            "-ExecutionPolicy",
            "Bypass",
            "-Command",
            "Start-Sleep -Milliseconds 200",
        ]);
        crate::process_launch::configure_noninteractive_command(&mut command);
        let mut child = command
            .spawn()
            .expect("spawn hidden process identity probe");
        let pid = child.id();
        let expected_start_time_ms =
            process_start_time_ms_for_pid(i64::from(pid)).expect("capture child start identity");
        let retained_handle = unsafe {
            let handle = OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, 0, pid);
            assert_ne!(handle, 0, "retain the process object for the oracle probe");
            OwnedHandle::from_raw_handle(handle as *mut _)
        };

        let status = child.wait().expect("wait for identity probe");
        assert!(status.success(), "identity probe failed: {status}");
        assert_eq!(
            process_alive_for_pid(i64::from(pid)),
            Some(false),
            "the retained process object must report a terminal exit code"
        );
        assert_eq!(
            process_start_time_ms_for_pid(i64::from(pid)),
            Some(expected_start_time_ms),
            "an exited process object can retain queryable generation identity"
        );
        assert_eq!(
            exact_process_generation_state(i64::from(pid), expected_start_time_ms),
            ExactProcessGenerationState::DeadOrReused,
            "exact-generation state must distinguish exit from retained identity"
        );

        drop(retained_handle);
    }
}

#[cfg(all(test, target_os = "linux"))]
mod linux_tests {
    use super::*;

    #[test]
    fn procfs_process_identity_distinguishes_current_and_missing_pid() {
        let pid = i64::from(std::process::id());
        let start =
            process_start_time_ms_for_pid(pid).expect("current Linux process start identity");
        assert_eq!(process_alive_for_pid(pid), Some(true));
        assert_eq!(
            exact_process_generation_state(pid, start),
            ExactProcessGenerationState::Alive
        );
        assert_eq!(
            exact_process_generation_state(pid, start.saturating_sub(1)),
            ExactProcessGenerationState::DeadOrReused
        );
        assert_eq!(process_alive_for_pid(i64::from(u32::MAX)), Some(false));
    }
}
