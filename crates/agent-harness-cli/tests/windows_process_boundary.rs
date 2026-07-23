#![cfg(windows)]

use std::fs;
use std::os::windows::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use agent_harness_core::loop_health::process_alive_for_pid;
use windows_sys::Win32::System::Console::{CTRL_BREAK_EVENT, GenerateConsoleCtrlEvent};
use windows_sys::Win32::System::Threading::CREATE_NEW_PROCESS_GROUP;

#[test]
fn production_reconcile_spawn_child_survives_launcher_control_break() {
    let root = repo_target_fixture("launcher-control-break");
    fs::create_dir_all(&root).unwrap();
    let receipt_file = root.join("process-boundary-receipt.json");
    let stop_file = root.join("detached-child.stop");
    let script_file = root.join("detached-child.ps1");
    fs::write(
        &script_file,
        format!(
            "$ErrorActionPreference = 'Stop'\nwhile (-not (Test-Path -LiteralPath {} -PathType Leaf)) {{ Start-Sleep -Milliseconds 50 }}\n",
            ps_quote(&stop_file),
        ),
    )
    .unwrap();

    let harness_cli = PathBuf::from(env!("CARGO_BIN_EXE_agent-harness"));
    let mut launcher = Command::new(&harness_cli)
        .args([
            "__supervisor-process-boundary-test",
            "--receipt-file",
            &receipt_file.display().to_string(),
            "--",
            "powershell.exe",
            "-NoLogo",
            "-NoProfile",
            "-NonInteractive",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            &script_file.display().to_string(),
        ])
        .env("AGENT_HARNESS_PROCESS_BOUNDARY_TEST", "1")
        .creation_flags(CREATE_NEW_PROCESS_GROUP)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();

    wait_for_file(&receipt_file, Duration::from_secs(15));
    let receipt: serde_json::Value =
        serde_json::from_slice(&fs::read(&receipt_file).unwrap()).unwrap();
    assert_eq!(
        receipt["schema"],
        "agent-harness.supervisor-process-boundary-test-receipt.v1"
    );
    assert_eq!(
        receipt["processBoundary"]["schema"],
        "agent-harness.supervisor-process-boundary.v1"
    );
    assert_eq!(receipt["processBoundary"]["platform"], "windows");
    assert_eq!(
        receipt["processBoundary"]["detachedFromLauncherConsole"],
        true
    );
    assert_eq!(receipt["processBoundary"]["newProcessGroup"], true);
    assert_eq!(
        receipt["processBoundary"]["breakawayFromJobRequested"],
        false
    );
    let detached_pid = receipt["pid"].as_i64().unwrap();
    assert_eq!(process_alive_for_pid(detached_pid), Some(true));

    let signal_ok = unsafe { GenerateConsoleCtrlEvent(CTRL_BREAK_EVENT, launcher.id()) };
    let mut launcher_exit = (signal_ok != 0)
        .then(|| wait_for_exit(&mut launcher, Duration::from_secs(10)))
        .flatten();
    let detached_child_survived = process_alive_for_pid(detached_pid) == Some(true);

    fs::write(&stop_file, b"test cleanup\n").unwrap();
    wait_for_process_dead(detached_pid, Duration::from_secs(10));
    if launcher_exit.is_none() {
        launcher_exit = wait_for_exit(&mut launcher, Duration::from_secs(3));
    }
    if launcher_exit.is_none() {
        launcher.kill().unwrap();
        launcher_exit = wait_for_exit(&mut launcher, Duration::from_secs(3));
    }
    fs::remove_dir_all(root).unwrap();

    assert_ne!(
        signal_ok, 0,
        "CTRL_BREAK must reach the launcher process group"
    );
    assert!(
        launcher_exit.is_some(),
        "launcher did not exit after CTRL_BREAK"
    );
    assert!(
        detached_child_survived,
        "the child spawned by the production reconcile seam inherited the launcher control-event failure domain"
    );
}

fn wait_for_file(path: &Path, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if path.is_file() {
            return;
        }
        thread::sleep(Duration::from_millis(25));
    }
    panic!("timed out waiting for {}", path.display());
}

fn wait_for_exit(
    child: &mut std::process::Child,
    timeout: Duration,
) -> Option<std::process::ExitStatus> {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if let Some(status) = child.try_wait().unwrap() {
            return Some(status);
        }
        thread::sleep(Duration::from_millis(25));
    }
    None
}

fn wait_for_process_dead(process_id: i64, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if process_alive_for_pid(process_id) == Some(false) {
            return;
        }
        thread::sleep(Duration::from_millis(25));
    }
    panic!("test-created detached process {process_id} did not stop");
}

fn repo_target_fixture(name: &str) -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .join("target")
        .join("process-boundary-tests")
        .join(format!("{name}-{}-{nonce}", std::process::id()))
}

fn ps_quote(path: &Path) -> String {
    format!("'{}'", path.display().to_string().replace('\'', "''"))
}
