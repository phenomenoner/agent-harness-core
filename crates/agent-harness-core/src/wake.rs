use std::fs;
use std::io;
use std::path::Path;
use std::thread;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use serde_json;

use crate::logging::{current_log_time_ms, write_json_atomic};

const WAKE_SEQUENCE_SCHEMA: &str = "agent-harness.wake-sequence.v1";
const WAKE_WAIT_DEFAULT_POLL_MS: u64 = 25;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct WakeSequenceRecord {
    schema: String,
    lane: String,
    sequence: u64,
    reason: String,
    #[serde(rename = "updatedAtMs")]
    updated_at_ms: i64,
}

/// How a wake attempt was observed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum WakeSignalMode {
    /// The platform event was signaled and observed.
    Signaled,
    /// The platform event path was unavailable and polling was used.
    Fallback,
}

/// Result for operations that change or observe a wake sequence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WakeReceipt {
    pub schema: String,
    pub lane: String,
    pub event_name: String,
    pub sequence: u64,
    pub reason: String,
    #[serde(rename = "updatedAtMs")]
    pub updated_at_ms: i64,
    pub mode: WakeSignalMode,
}

/// Published event name for the provided harness home and lane.
pub fn wake_event_name(harness_home: impl AsRef<Path>, lane: &str) -> String {
    let home = harness_home.as_ref();
    let digest = deterministic_hash_string(home.to_string_lossy().as_ref());
    let lane = normalize_wake_lane(lane);
    if cfg!(windows) {
        format!("Local\\agent-harness-core-{digest}-{lane}")
    } else {
        format!("agent-harness-core-{digest}-{lane}")
    }
}

/// Read current wake sequence from a wake sequence file, if present.
pub fn read_wake_sequence(path: impl AsRef<Path>) -> io::Result<u64> {
    let path = path.as_ref();
    if !path.is_file() {
        return Ok(0);
    }

    let text = fs::read_to_string(path)?;
    if text.trim().is_empty() {
        return Ok(0);
    }
    let record: WakeSequenceRecord = serde_json::from_str(&text).map_err(io::Error::other)?;
    Ok(record.sequence)
}

/// Write the next wake sequence to `sequence_file` and attempt to signal the matching named event.
/// The signal path is best-effort: failures here do not fail the caller.
pub fn signal_wake(
    harness_home: impl AsRef<Path>,
    sequence_file: impl AsRef<Path>,
    lane: &str,
    reason: &str,
) -> io::Result<WakeReceipt> {
    signal_wake_with_fallback_mode(harness_home, sequence_file, lane, reason, false)
}

/// Signal helper with test hook to force fallback.
fn signal_wake_with_fallback_mode(
    harness_home: impl AsRef<Path>,
    sequence_file: impl AsRef<Path>,
    lane: &str,
    reason: &str,
    force_fallback: bool,
) -> io::Result<WakeReceipt> {
    let path = sequence_file.as_ref();
    let lane = normalize_wake_lane(lane);
    let updated_at_ms = current_log_time_ms()?;
    let next_sequence = next_wake_sequence(&path)?;
    let record = WakeSequenceRecord {
        schema: WAKE_SEQUENCE_SCHEMA.to_string(),
        lane: lane.clone(),
        sequence: next_sequence,
        reason: reason.to_string(),
        updated_at_ms,
    };
    let event_name = wake_event_name(harness_home, &lane);

    // write sequence first so a wait loop using polling will not miss the update even if signal fails.
    fs::create_dir_all(path.parent().ok_or_else(|| {
        io::Error::new(io::ErrorKind::InvalidInput, "sequence path has no parent")
    })?)?;
    write_json_atomic(path, &record)?;

    let mode = if force_fallback {
        WakeSignalMode::Fallback
    } else if signal_named_event(&event_name).is_ok() {
        WakeSignalMode::Signaled
    } else {
        WakeSignalMode::Fallback
    };
    Ok(WakeReceipt {
        schema: WAKE_SEQUENCE_SCHEMA.to_string(),
        lane,
        event_name,
        sequence: next_sequence,
        reason: reason.to_string(),
        updated_at_ms,
        mode,
    })
}

/// Wait for a wake sequence update and return once sequence changes.
///
/// On supported Windows builds this uses the named event first, then polls the sequence
/// file if event wait fails or if no event edge is observed before timeout.
pub fn wait_for_wake(
    sequence_file: impl AsRef<Path>,
    wake_event_name: impl AsRef<str>,
    previous_sequence: u64,
    timeout_ms: u64,
) -> io::Result<Option<WakeReceipt>> {
    wait_for_wake_with_fallback_mode(
        sequence_file,
        wake_event_name,
        previous_sequence,
        timeout_ms,
        false,
    )
}

fn wait_for_wake_with_fallback_mode(
    sequence_file: impl AsRef<Path>,
    wake_event_name: impl AsRef<str>,
    mut previous_sequence: u64,
    timeout_ms: u64,
    force_fallback: bool,
) -> io::Result<Option<WakeReceipt>> {
    let sequence_file = sequence_file.as_ref();
    let wake_event_name = wake_event_name.as_ref().to_string();
    let start = Instant::now();
    let deadline = start + Duration::from_millis(timeout_ms);
    let mode;

    if !force_fallback {
        if timeout_ms == 0 {
            match read_wake_sequence(sequence_file) {
                Ok(sequence) if sequence > previous_sequence => {
                    return Ok(Some(load_wake_receipt(
                        sequence_file,
                        &wake_event_name,
                        WakeSignalMode::Fallback,
                    )?));
                }
                Ok(_) => return Ok(None),
                Err(_) => {
                    mode = WakeSignalMode::Fallback;
                }
            }
        } else {
            match read_wake_sequence(sequence_file) {
                Ok(sequence) if sequence > previous_sequence => {
                    return Ok(Some(load_wake_receipt(
                        sequence_file,
                        &wake_event_name,
                        WakeSignalMode::Fallback,
                    )?));
                }
                Ok(_) | Err(_) => {}
            }
            match wait_named_event(&wake_event_name, timeout_ms) {
                Ok(true) => {
                    let sequence = read_wake_sequence(sequence_file)?;
                    if sequence > previous_sequence {
                        return Ok(Some(load_wake_receipt(
                            sequence_file,
                            &wake_event_name,
                            WakeSignalMode::Signaled,
                        )?));
                    }
                    mode = WakeSignalMode::Fallback;
                }
                Ok(false) => {
                    mode = WakeSignalMode::Fallback;
                }
                Err(_) => {
                    mode = WakeSignalMode::Fallback;
                }
            }
        }
    } else {
        mode = WakeSignalMode::Fallback;
    }

    if mode == WakeSignalMode::Fallback {
        while Instant::now() <= deadline {
            let sequence = read_wake_sequence(sequence_file).unwrap_or(previous_sequence);
            if sequence > previous_sequence {
                return Ok(Some(load_wake_receipt(
                    sequence_file,
                    &wake_event_name,
                    WakeSignalMode::Fallback,
                )?));
            }
            let elapsed = start.elapsed().as_millis();
            let elapsed_ms = u64::try_from(elapsed).unwrap_or(timeout_ms);
            let remaining = timeout_ms.saturating_sub(elapsed_ms);
            if remaining == 0 {
                break;
            }
            thread::sleep(Duration::from_millis(
                WAKE_WAIT_DEFAULT_POLL_MS.min(remaining),
            ));
            previous_sequence = sequence;
        }
    }

    Ok(None)
}

fn load_wake_receipt(
    sequence_file: &Path,
    wake_event_name: &str,
    mode: WakeSignalMode,
) -> io::Result<WakeReceipt> {
    let sequence = read_wake_sequence(sequence_file)?;
    let record: WakeSequenceRecord = match fs::read_to_string(sequence_file) {
        Ok(text) if !text.trim().is_empty() => {
            serde_json::from_str(&text).map_err(io::Error::other)?
        }
        _ => WakeSequenceRecord {
            schema: WAKE_SEQUENCE_SCHEMA.to_string(),
            lane: String::new(),
            sequence,
            reason: "wake observed".to_string(),
            updated_at_ms: current_log_time_ms()?,
        },
    };

    let lane = normalize_wake_lane(&record.lane);
    let updated_at_ms = record.updated_at_ms;
    Ok(WakeReceipt {
        schema: WAKE_SEQUENCE_SCHEMA.to_string(),
        lane,
        event_name: wake_event_name.to_string(),
        sequence,
        reason: record.reason,
        updated_at_ms,
        mode,
    })
}

fn next_wake_sequence(path: &Path) -> io::Result<u64> {
    if !path.is_file() {
        return Ok(1);
    }
    let existing = read_wake_sequence(path)?;
    Ok(existing.saturating_add(1))
}

fn normalize_wake_lane(lane: &str) -> String {
    let mut sanitized = String::new();
    let mut last_dash = false;
    for c in lane.chars() {
        let part = if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
            c.to_ascii_lowercase()
        } else if c.is_ascii_whitespace() || c == '/' || c == '\\' {
            '-'
        } else {
            '-'
        };
        if sanitized.is_empty() && part == '-' {
            continue;
        }
        if part == '-' && last_dash {
            continue;
        }
        sanitized.push(part);
        last_dash = part == '-';
    }
    while sanitized.ends_with('-') {
        sanitized.pop();
    }
    if sanitized.is_empty() {
        "lane".to_string()
    } else {
        sanitized
    }
}

fn deterministic_hash_string(value: &str) -> String {
    let mut hash: u64 = 14_695_981_039_346_656_037u64;
    for byte in value.to_ascii_lowercase().as_bytes() {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(1_099_511_628_211);
        hash ^= hash >> 32;
    }
    format!("{hash:016x}")
}

#[cfg(windows)]
fn signal_named_event(event_name: &str) -> io::Result<()> {
    use windows_sys::Win32::Foundation::CloseHandle;
    use windows_sys::Win32::System::Threading::{
        CreateEventW, EVENT_MODIFY_STATE, OpenEventW, SYNCHRONIZATION_SYNCHRONIZE, SetEvent,
    };

    unsafe {
        let name = wide_chars(event_name);
        let handle = OpenEventW(
            EVENT_MODIFY_STATE | SYNCHRONIZATION_SYNCHRONIZE,
            0,
            name.as_ptr(),
        );
        let event = if handle == 0 {
            CreateEventW(std::ptr::null(), 0, 0, name.as_ptr())
        } else {
            handle
        };
        if event == 0 {
            return Err(io::Error::last_os_error());
        }
        let ok = SetEvent(event);
        CloseHandle(event);
        if ok == 0 {
            return Err(io::Error::last_os_error());
        }
    }
    Ok(())
}

#[cfg(windows)]
fn wait_named_event(event_name: &str, timeout_ms: u64) -> io::Result<bool> {
    use windows_sys::Win32::Foundation::{CloseHandle, WAIT_FAILED, WAIT_OBJECT_0, WAIT_TIMEOUT};
    use windows_sys::Win32::System::Threading::{
        CreateEventW, EVENT_ALL_ACCESS, OpenEventW, WaitForSingleObject,
    };

    let timeout = timeout_ms.min(u64::from(u32::MAX)) as u32;
    unsafe {
        let name = wide_chars(event_name);
        let handle = OpenEventW(EVENT_ALL_ACCESS, 0, name.as_ptr());
        let event = if handle == 0 {
            CreateEventW(std::ptr::null(), 0, 0, name.as_ptr())
        } else {
            handle
        };
        if event == 0 {
            return Err(io::Error::last_os_error());
        }
        let status = WaitForSingleObject(event, timeout);
        CloseHandle(event);
        match status {
            WAIT_OBJECT_0 => Ok(true),
            WAIT_TIMEOUT => Ok(false),
            WAIT_FAILED => Err(io::Error::last_os_error()),
            _ => Err(io::Error::other(format!(
                "unexpected wait status: {}",
                status
            ))),
        }
    }
}

#[cfg(not(windows))]
fn signal_named_event(_event_name: &str) -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "named events are not supported on this platform",
    ))
}

#[cfg(not(windows))]
fn wait_named_event(_event_name: &str, _timeout_ms: u64) -> io::Result<bool> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "named events are not supported on this platform",
    ))
}

fn wide_chars(value: &str) -> Vec<u16> {
    value.encode_utf16().chain(std::iter::once(0)).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn wake_event_name_stable_for_home_and_lane() {
        let home = PathBuf::from("D:/agent-harness-home");
        let first = wake_event_name(&home, "Runtime Queue");
        let second = wake_event_name(&home, "runtime queue");
        let third = wake_event_name(&home, "  runtime-queue  ");

        assert_eq!(first, second);
        assert_eq!(second, third);
        assert!(first.ends_with("-runtime-queue"));
        if cfg!(windows) {
            assert!(first.starts_with("Local\\agent-harness-core-"));
        } else {
            assert!(first.starts_with("agent-harness-core-"));
        }
    }

    #[test]
    fn wake_lane_sanitization() {
        assert_eq!(normalize_wake_lane("Runtime Queue"), "runtime-queue");
        assert_eq!(normalize_wake_lane("..//foo\\\\bar"), "foo-bar");
        assert_eq!(normalize_wake_lane("   "), "lane");
        assert_eq!(
            normalize_wake_lane("outbox telegram default"),
            "outbox-telegram-default"
        );
    }

    #[test]
    fn wake_sequence_increments() {
        let root = temp_root("wake_sequence_increments");
        let file = root.join("state").join("runtime-queue").join("wake.json");
        let home = root.join("harness");

        let first = signal_wake_with_fallback_mode(&home, &file, "runtime", "first", true).unwrap();
        let second =
            signal_wake_with_fallback_mode(&home, &file, "runtime", "second", true).unwrap();

        assert_eq!(first.sequence, 1);
        assert_eq!(second.sequence, 2);
        assert_eq!(read_wake_sequence(&file).unwrap(), 2);

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn wake_fallback_mode_does_not_fail_caller() {
        let root = temp_root("wake_fallback_mode_does_not_fail_caller");
        let file = root.join("state").join("runtime-queue").join("wake.json");
        let home = root.join("harness");
        let event = wake_event_name(&home, "runtime");

        let seed = signal_wake_with_fallback_mode(&home, &file, "runtime", "seed", true).unwrap();
        assert_eq!(seed.mode, WakeSignalMode::Fallback);

        let next = signal_wake_with_fallback_mode(&home, &file, "runtime", "later", false).unwrap();
        assert!(next.sequence >= seed.sequence + 1);

        let waited =
            wait_for_wake_with_fallback_mode(&file, &event, next.sequence - 1, 1_000, true)
                .unwrap()
                .expect("fallback waiter should observe change");
        assert_eq!(waited.mode, WakeSignalMode::Fallback);

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn wait_for_wake_reports_none_when_no_change() {
        let root = temp_root("wake_wait_reports_none_when_no_change");
        let file = root.join("state").join("runtime-queue").join("wake.json");
        let home = root.join("harness");
        let event = wake_event_name(&home, "worker");

        let receipt = wait_for_wake_with_fallback_mode(&file, &event, 0, 10, true).unwrap();
        assert!(receipt.is_none());

        let _ = fs::remove_dir_all(root);
    }

    fn temp_root(test_name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "agent-harness-wake-{test_name}-{}-{nanos}",
            std::process::id()
        ))
    }
}
