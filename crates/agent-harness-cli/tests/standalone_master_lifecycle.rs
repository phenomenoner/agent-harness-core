#![cfg(windows)]

use std::fs;
use std::mem::{size_of, zeroed};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use agent_harness_core::loop_health::process_alive_for_pid;
use agent_harness_core::process_launch::configure_noninteractive_command;
use agent_harness_core::{
    WorkerEnqueueOptions, WorkerJobKind, WorkerStatusOptions, collect_worker_status,
    enqueue_worker_job, write_json_atomic,
};
use serde_json::Value;
use windows_sys::Win32::Foundation::{CloseHandle, INVALID_HANDLE_VALUE};
use windows_sys::Win32::System::Diagnostics::ToolHelp::{
    CreateToolhelp32Snapshot, PROCESSENTRY32W, Process32FirstW, Process32NextW, TH32CS_SNAPPROCESS,
    TH32CS_SNAPTHREAD, THREADENTRY32, Thread32First, Thread32Next,
};
use windows_sys::Win32::System::Threading::{
    OpenProcess, OpenThread, PROCESS_TERMINATE, SuspendThread, THREAD_SUSPEND_RESUME,
    TerminateProcess,
};
use windows_sys::Win32::UI::WindowsAndMessaging::{
    EnumWindows, GetWindowThreadProcessId, IsWindowVisible,
};

#[test]
fn clean_start_has_one_master_and_direct_child_parents() {
    let fixture = MasterFixture::new("clean-start-direct-parent", "direct-owner");
    let mut master = fixture.spawn_master("direct-owner", "master.stderr.log");
    let service_state = fixture.wait_for_service_state(&mut master);
    let running_state = wait_for_json_matching(
        &fixture.service_state_file,
        Duration::from_secs(15),
        || master.assert_running("waiting for direct child heartbeat admission"),
        |value| {
            value["actualState"] == "running"
                && value["detail"] == "direct child heartbeat matches master attempt and is fresh"
        },
    );
    let master_state = wait_for_json(&fixture.master_state_file, Duration::from_secs(5), || {});
    let child_pid = service_state["processId"]
        .as_u64()
        .and_then(|pid| u32::try_from(pid).ok())
        .expect("direct child pid");
    assert_eq!(
        running_state["processId"], service_state["processId"],
        "heartbeat admission must apply to the retained direct child, not a replacement attempt"
    );

    assert_eq!(
        master_state["schema"],
        "agent-harness.supervisor-master-state.v1"
    );
    assert_eq!(master_state["mode"], "direct-owner");
    assert_eq!(master_state["pid"], u64::from(master.id()));
    assert_eq!(master_state["desiredServiceCount"], 1);
    assert_eq!(
        service_state["launchOwner"], "rust-standalone-master",
        "direct mode must never report a supervisor-run owner"
    );
    assert_eq!(service_state["supervisorPid"], u64::from(master.id()));
    assert_eq!(service_state["parentPid"], u64::from(master.id()));
    assert_eq!(
        parent_pid_for_process(child_pid),
        Some(master.id()),
        "independent Windows process enumeration must agree with master-authored state"
    );
    let direct_children = agent_harness_child_process_ids(master.id());
    assert_eq!(
        direct_children,
        vec![child_pid],
        "the master must have exactly one direct child and no supervisor-run intermediary; observed={:?}",
        child_process_details(master.id())
    );
    for (process_id, executable) in std::iter::once((master.id(), "agent-harness.exe".to_string()))
        .chain(child_process_details(master.id()))
    {
        assert!(
            !process_has_visible_top_level_window(process_id),
            "noninteractive master topology process {executable} pid={process_id} exposed a visible top-level window"
        );
    }

    fixture.stop_master_cleanly(&mut master);
}

#[test]
fn second_master_is_rejected_without_child_mutation() {
    let fixture = MasterFixture::new("duplicate-master", "direct-owner");
    let mut first = fixture.spawn_master("direct-owner", "first.stderr.log");
    let first_state = fixture.wait_for_service_state(&mut first);
    let child_pid = first_state["processId"]
        .as_u64()
        .and_then(|pid| u32::try_from(pid).ok())
        .expect("first direct child pid");

    let output = fixture
        .master_command("direct-owner", "second.stderr.log")
        .output()
        .expect("run competing master");
    assert!(
        !output.status.success(),
        "a second master must fail before any child mutation"
    );
    let second_error = fs::read_to_string(fixture.root.join("second.stderr.log"))
        .unwrap_or_else(|_| String::from_utf8_lossy(&output.stderr).into_owned());
    assert!(
        second_error.contains("compatibility import is blocked")
            || second_error.contains("ownership lease DirectExclusive is already held"),
        "unexpected duplicate-master refusal: {second_error}"
    );
    let state_after = wait_for_json_matching(
        &fixture.service_state_file,
        Duration::from_secs(2),
        || {},
        |value| value["processId"].as_u64() == Some(u64::from(child_pid)),
    );
    assert_eq!(state_after["attemptOrdinal"], first_state["attemptOrdinal"]);
    assert_eq!(parent_pid_for_process(child_pid), Some(first.id()));
    assert_eq!(agent_harness_child_process_ids(first.id()), vec![child_pid]);

    fixture.stop_master_cleanly(&mut first);
}

#[test]
fn child_crash_backoff_restart_keeps_one_attempt() {
    let fixture = MasterFixture::new("child-crash-restart", "direct-owner");
    let mut master = fixture.spawn_master("direct-owner", "master.stderr.log");
    let first_state = fixture.wait_for_service_state(&mut master);
    let first_pid = first_state["processId"]
        .as_u64()
        .and_then(|pid| u32::try_from(pid).ok())
        .expect("first child pid");
    terminate_process(first_pid);

    let restarted = wait_for_json_matching(
        &fixture.service_state_file,
        Duration::from_secs(10),
        || master.assert_running("waiting for restarted child"),
        |value| {
            value["attemptOrdinal"].as_u64().unwrap_or_default() >= 2
                && value["processId"]
                    .as_u64()
                    .is_some_and(|pid| pid > 0 && pid != u64::from(first_pid))
        },
    );
    let restarted_pid = restarted["processId"]
        .as_u64()
        .and_then(|pid| u32::try_from(pid).ok())
        .expect("replacement child pid");
    assert_eq!(restarted["restartCount"], 1);
    assert_eq!(restarted["launchOwner"], "rust-standalone-master");
    assert_eq!(parent_pid_for_process(restarted_pid), Some(master.id()));
    assert_eq!(
        agent_harness_child_process_ids(master.id()),
        vec![restarted_pid],
        "restart must never overlap two direct attempts; observed={:?}",
        child_process_details(master.id())
    );
    assert_eq!(process_alive_for_pid(i64::from(first_pid)), Some(false));

    fixture.stop_master_cleanly(&mut master);
}

#[test]
fn repeated_child_crashes_open_breaker_without_an_overlapping_attempt() {
    let fixture = MasterFixture::new("child-crash-breaker", "direct-owner");
    let mut command = fixture.master_command("direct-owner", "master.stderr.log");
    command.args([
        "--crash-loop-threshold",
        "2",
        "--crash-loop-window-ms",
        "10000",
    ]);
    let mut master = RunningMaster {
        child: Some(command.spawn().expect("spawn breaker-policy master")),
        stderr_file: fixture.root.join("master.stderr.log"),
    };
    let first_state = fixture.wait_for_service_state(&mut master);
    let first_pid = first_state["processId"]
        .as_u64()
        .and_then(|pid| u32::try_from(pid).ok())
        .expect("first breaker child pid");
    terminate_process(first_pid);
    let restarted = wait_for_json_matching(
        &fixture.service_state_file,
        Duration::from_secs(10),
        || master.assert_running("waiting for the breaker test restart"),
        |value| {
            value["attemptOrdinal"].as_u64().unwrap_or_default() >= 2
                && value["processId"]
                    .as_u64()
                    .is_some_and(|pid| pid > 0 && pid != u64::from(first_pid))
        },
    );
    let restarted_pid = restarted["processId"]
        .as_u64()
        .and_then(|pid| u32::try_from(pid).ok())
        .expect("second breaker child pid");
    terminate_process(restarted_pid);

    let breaker = wait_for_json_matching(
        &fixture.service_state_file,
        Duration::from_secs(10),
        || master.assert_running("waiting for crash-loop breaker"),
        |value| {
            value["actualState"] == "breaker-open"
                && value["restartCount"].as_u64() == Some(2)
                && value["processId"].is_null()
        },
    );
    assert_eq!(breaker["attemptOrdinal"], 2);
    assert_eq!(
        agent_harness_child_process_ids(master.id()),
        Vec::<u32>::new()
    );
    thread::sleep(Duration::from_millis(300));
    let stable_breaker = wait_for_json_matching(
        &fixture.service_state_file,
        Duration::from_secs(2),
        || master.assert_running("checking breaker stability"),
        |value| value["actualState"] == "breaker-open",
    );
    assert_eq!(stable_breaker["attemptOrdinal"], 2);
    assert_eq!(
        agent_harness_child_process_ids(master.id()),
        Vec::<u32>::new()
    );

    fixture.stop_master_cleanly(&mut master);
}

#[test]
fn abrupt_master_death_reaps_direct_child() {
    let fixture = MasterFixture::new("abrupt-master-death", "direct-owner");
    let mut master = fixture.spawn_master("direct-owner", "master.stderr.log");
    let state = wait_for_json_matching(
        &fixture.service_state_file,
        Duration::from_secs(15),
        || master.assert_running("waiting for first direct-owner heartbeat admission"),
        |value| value["actualState"] == "running",
    );
    let child_pid = state["processId"]
        .as_u64()
        .and_then(|pid| u32::try_from(pid).ok())
        .expect("direct child pid");
    assert_eq!(parent_pid_for_process(child_pid), Some(master.id()));

    master.kill().expect("terminate master abruptly");
    let _ = master.wait().expect("reap terminated master");
    master.mark_reaped();
    wait_until(Duration::from_secs(5), || {
        process_alive_for_pid(i64::from(child_pid)) == Some(false)
    });
    assert_eq!(
        process_alive_for_pid(i64::from(child_pid)),
        Some(false),
        "kill-on-close Job must reap a direct child when the master dies"
    );

    let mut recovered = fixture.spawn_master("direct-owner", "recovered-master.stderr.log");
    let recovered_state = wait_for_json_matching(
        &fixture.service_state_file,
        Duration::from_secs(15),
        || recovered.assert_running("waiting for direct-owner crash recovery"),
        |value| {
            value["actualState"] == "running"
                && value["masterGenerationId"] != state["masterGenerationId"]
        },
    );
    assert_ne!(
        recovered_state["processId"], state["processId"],
        "host recovery must spawn a new child attempt rather than adopt the dead PID"
    );
    fixture.stop_master_cleanly(&mut recovered);
}

#[test]
fn partial_startup_failure_keeps_live_sibling_owned_and_drains_it_cleanly() {
    let fixture = MasterFixture::new("partial-startup-failure", "direct-owner");
    let failed_service_state = fixture.write_partial_start_services();
    let mut command = fixture.master_command("direct-owner", "master.stderr.log");
    command.args(["--crash-loop-threshold", "1"]);
    let mut master = RunningMaster {
        child: Some(command.spawn().expect("spawn partial-startup master")),
        stderr_file: fixture.root.join("master.stderr.log"),
    };
    let running = fixture.wait_for_service_state(&mut master);
    let running_pid = running["processId"]
        .as_u64()
        .and_then(|pid| u32::try_from(pid).ok())
        .expect("live sibling pid");
    let failed = wait_for_json_matching(
        &failed_service_state,
        Duration::from_secs(10),
        || master.assert_running("waiting for isolated spawn failure"),
        |value| value["actualState"] == "breaker-open" && value["processId"].is_null(),
    );
    assert!(
        failed["detail"]
            .as_str()
            .is_some_and(|detail| detail.contains("failed to start direct child")),
        "unexpected partial-start detail: {}",
        failed["detail"]
    );
    assert_eq!(
        agent_harness_child_process_ids(master.id()),
        vec![running_pid],
        "one failed service must not orphan, duplicate, or hide the still-owned sibling"
    );

    fixture.stop_master_cleanly(&mut master);
    wait_until(Duration::from_secs(5), || {
        process_alive_for_pid(i64::from(running_pid)) == Some(false)
    });
    assert_eq!(
        process_alive_for_pid(i64::from(running_pid)),
        Some(false),
        "clean drain after partial startup must reap the live sibling"
    );
}

#[test]
fn abrupt_master_death_reaps_gateway_descendant_tree() {
    let fixture = MasterFixture::new("gateway-descendant-reap", "direct-owner");
    let descendant_pid_file = fixture.write_discord_gateway_service();
    let service_state_file = fixture
        .state_root
        .join("services")
        .join("discord-gateway-loop.json");
    let mut master = fixture.spawn_master("direct-owner", "master.stderr.log");
    let service_state = wait_for_json_matching(
        &service_state_file,
        Duration::from_secs(15),
        || master.assert_running("waiting for direct gateway loop"),
        |value| {
            value["launchOwner"] == "rust-standalone-master"
                && value["processId"].as_u64().is_some_and(|pid| pid > 0)
        },
    );
    let loop_pid = service_state["processId"]
        .as_u64()
        .and_then(|pid| u32::try_from(pid).ok())
        .expect("direct gateway loop pid");
    let descendant_pid = wait_for_u32_file(&descendant_pid_file, Duration::from_secs(15));
    wait_until(Duration::from_secs(5), || {
        process_alive_for_pid(i64::from(descendant_pid)) == Some(true)
    });
    let gateway_pid = parent_pid_for_process(descendant_pid).expect("gateway process parent");
    assert_eq!(parent_pid_for_process(gateway_pid), Some(loop_pid));
    assert_eq!(parent_pid_for_process(loop_pid), Some(master.id()));
    for process_id in [loop_pid, gateway_pid, descendant_pid] {
        assert!(
            !process_has_visible_top_level_window(process_id),
            "contained gateway process pid={process_id} exposed a visible top-level window"
        );
    }

    master
        .kill()
        .expect("terminate gateway-owning master abruptly");
    let _ = master.wait().expect("reap gateway-owning master");
    master.mark_reaped();
    for process_id in [loop_pid, gateway_pid, descendant_pid] {
        wait_until(Duration::from_secs(8), || {
            process_alive_for_pid(i64::from(process_id)) == Some(false)
        });
        assert_eq!(
            process_alive_for_pid(i64::from(process_id)),
            Some(false),
            "kill-on-close Job did not reap gateway tree pid={process_id}"
        );
    }
}

#[test]
fn long_worker_job_keeps_one_fresh_direct_owner_attempt() {
    let fixture = MasterFixture::new("long-worker-heartbeat", "direct-owner");
    fixture.write_worker_service(800, 600);
    let script_dir = fixture
        .harness_home
        .join("state")
        .join("workers")
        .join("scripts");
    fs::create_dir_all(&script_dir).expect("create worker scripts");
    let script = script_dir.join("long-worker.ps1");
    fs::write(&script, "Start-Sleep -Milliseconds 2600\r\n").expect("write long worker");

    let mut master = fixture.spawn_master("direct-owner", "master.stderr.log");
    let service_state_file = fixture.state_root.join("services").join("worker-loop.json");
    let initial = wait_for_json_matching(
        &service_state_file,
        Duration::from_secs(15),
        || master.assert_running("waiting for worker direct-owner admission"),
        |value| value["actualState"] == "running",
    );
    let initial_pid = initial["processId"].as_u64().expect("worker pid");
    let initial_attempt = initial["attemptOrdinal"].clone();
    let initial_restart_count = initial["restartCount"].clone();
    enqueue_shell_fixture(
        &fixture.harness_home,
        &script,
        &script_dir,
        "long-worker",
        1,
    );
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        master.assert_running("waiting for long worker completion");
        let status = collect_worker_status(WorkerStatusOptions {
            harness_home: fixture.harness_home.clone(),
        })
        .expect("collect worker status");
        if status.totals.succeeded == 1 {
            break;
        }
        assert!(Instant::now() < deadline, "long worker did not complete");
        thread::sleep(Duration::from_millis(100));
    }
    let final_state = wait_for_json_matching(
        &service_state_file,
        Duration::from_secs(5),
        || master.assert_running("waiting for stable worker attempt"),
        |value| value["actualState"] == "running",
    );
    assert_eq!(final_state["attemptOrdinal"], initial_attempt);
    assert_eq!(final_state["restartCount"], initial_restart_count);
    assert_eq!(final_state["processId"].as_u64(), Some(initial_pid));
    fixture.stop_master_cleanly(&mut master);
}

#[test]
fn abrupt_worker_death_reaps_descendant_and_expires_owned_job() {
    let fixture = MasterFixture::new("abrupt-worker-descendant", "direct-owner");
    fixture.write_worker_service(800, 600);
    let script_dir = fixture
        .harness_home
        .join("state")
        .join("workers")
        .join("scripts");
    fs::create_dir_all(&script_dir).expect("create worker scripts");
    let descendant_pid_file = fixture.root.join("worker-descendant.pid");
    let script = script_dir.join("spawn-worker-descendant.ps1");
    fs::write(
        &script,
        format!(
            "$child = Start-Process -FilePath powershell.exe -ArgumentList '-NoProfile','-Command','Start-Sleep -Seconds 30' -WindowStyle Hidden -PassThru\r\nSet-Content -LiteralPath '{}' -Value $child.Id -NoNewline\r\nWait-Process -Id $child.Id\r\n",
            descendant_pid_file.display()
        ),
    )
    .expect("write descendant worker");
    enqueue_shell_fixture(
        &fixture.harness_home,
        &script,
        &script_dir,
        "abrupt-worker",
        1,
    );

    let mut master = fixture.spawn_master("direct-owner", "master.stderr.log");
    let service_state_file = fixture.state_root.join("services").join("worker-loop.json");
    let initial = wait_for_json_matching(
        &service_state_file,
        Duration::from_secs(15),
        || master.assert_running("waiting for worker direct-owner admission"),
        |value| value["actualState"] == "running",
    );
    let worker_pid = initial["processId"]
        .as_u64()
        .and_then(|pid| u32::try_from(pid).ok())
        .expect("worker pid");
    let descendant_pid = wait_for_u32_file(&descendant_pid_file, Duration::from_secs(15));
    assert_eq!(process_alive_for_pid(i64::from(descendant_pid)), Some(true));

    terminate_process(worker_pid);
    wait_for_process_absent(worker_pid, Duration::from_secs(5));
    wait_for_process_absent(descendant_pid, Duration::from_secs(5));
    let replacement = wait_for_json_matching(
        &service_state_file,
        Duration::from_secs(15),
        || master.assert_running("waiting for replacement worker"),
        |value| {
            value["attemptOrdinal"].as_u64().unwrap_or_default() >= 2
                && value["actualState"] == "running"
        },
    );
    assert_ne!(
        replacement["processId"].as_u64(),
        Some(u64::from(worker_pid))
    );

    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        master.assert_running("waiting for expired old-generation worker job");
        let status = collect_worker_status(WorkerStatusOptions {
            harness_home: fixture.harness_home.clone(),
        })
        .expect("collect worker status");
        if status.totals.expired == 1 && status.totals.running == 0 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "replacement did not expire old-generation worker job: {:?}",
            status.totals
        );
        thread::sleep(Duration::from_millis(100));
    }
    fixture.stop_master_cleanly(&mut master);
}

#[test]
fn observe_mode_has_no_process_or_canonical_state_side_effects() {
    let fixture = MasterFixture::new("observe-no-side-effects", "observe");
    let output = fixture
        .master_command("observe", "observe.stderr.log")
        .output()
        .expect("run observe mode");
    assert!(
        output.status.success(),
        "observe mode failed: {}",
        fs::read_to_string(fixture.root.join("observe.stderr.log")).unwrap_or_default()
    );
    assert!(
        fixture
            .state_root
            .join("master")
            .join("observations")
            .read_dir()
            .expect("observation directory")
            .next()
            .is_some(),
        "observe mode must write a separate observation receipt"
    );
    assert!(!fixture.master_state_file.exists());
    assert!(
        !fixture
            .state_root
            .join("master")
            .join("master.lock")
            .exists()
    );
    assert!(!fixture.state_root.join("services").exists());
}

#[test]
fn direct_cli_rejects_config_mode_mismatch() {
    let fixture = MasterFixture::new("mode-mismatch", "compatibility");
    let output = fixture
        .master_command("direct-owner", "mismatch.stderr.log")
        .output()
        .expect("run mismatched direct owner");
    assert!(!output.status.success());
    let error = fs::read_to_string(fixture.root.join("mismatch.stderr.log")).unwrap_or_default();
    assert!(error.contains("does not match supervisor.ownershipMode=compatibility"));
    assert!(!fixture.master_state_file.exists());
    assert!(!fixture.state_root.join("services").exists());
}

#[test]
fn stale_matching_heartbeat_is_reaped_and_restarted() {
    let fixture = MasterFixture::new("stale-heartbeat-restart", "direct-owner");
    fixture.write_progress_service(60_000, 3_000, 75, &[]);
    let mut master = fixture.spawn_master("direct-owner", "master.stderr.log");
    let first = fixture.wait_for_service_state(&mut master);
    let first_pid = first["processId"]
        .as_u64()
        .and_then(|pid| u32::try_from(pid).ok())
        .expect("first child pid");
    let heartbeat = wait_for_json_matching(
        &fixture.heartbeat_file,
        Duration::from_secs(15),
        || master.assert_running("waiting for first fresh heartbeat"),
        |value| value["processId"] == u64::from(first_pid),
    );
    assert_eq!(heartbeat["generationId"], first["generationId"]);
    suspend_process(first_pid);

    let restarted = wait_for_json_matching(
        &fixture.service_state_file,
        Duration::from_secs(15),
        || master.assert_running("waiting for stale child replacement"),
        |value| {
            value["attemptOrdinal"].as_u64().unwrap_or_default() >= 2
                && value["processId"]
                    .as_u64()
                    .is_some_and(|pid| pid > 0 && pid != u64::from(first_pid))
        },
    );
    let restarted_pid = restarted["processId"]
        .as_u64()
        .and_then(|pid| u32::try_from(pid).ok())
        .expect("replacement child pid");
    assert_eq!(process_alive_for_pid(i64::from(first_pid)), Some(false));
    assert_eq!(parent_pid_for_process(restarted_pid), Some(master.id()));
    assert_eq!(
        agent_harness_child_process_ids(master.id()),
        vec![restarted_pid]
    );

    fixture.stop_master_cleanly(&mut master);
}

#[test]
fn clean_stop_removes_ephemeral_holds_and_allows_second_generation() {
    let fixture = MasterFixture::new("clean-stop-restart", "direct-owner");
    let mut first = fixture.spawn_master("direct-owner", "first.stderr.log");
    fixture.wait_for_service_state(&mut first);
    let first_master_state =
        wait_for_json(&fixture.master_state_file, Duration::from_secs(5), || {});
    fixture.stop_master_cleanly(&mut first);
    assert!(
        !fixture.child_stop_file.exists(),
        "master-created ephemeral child stop must be removed after proven reap"
    );

    fs::remove_file(&fixture.master_stop_file).expect("clear explicit persistent master hold");
    let mut second = fixture.spawn_master("direct-owner", "second.stderr.log");
    let second_service_state = fixture.wait_for_service_state(&mut second);
    let second_master_state =
        wait_for_json(&fixture.master_state_file, Duration::from_secs(5), || {});
    assert_ne!(
        first_master_state["generationId"], second_master_state["generationId"],
        "a clean restart must establish a new master generation"
    );
    assert_eq!(second_service_state["attemptOrdinal"], 1);

    fixture.stop_master_cleanly(&mut second);
    assert!(!fixture.child_stop_file.exists());
}

#[test]
fn ownership_mode_flip_drains_direct_owner_before_releasing_lease() {
    let fixture = MasterFixture::new("mode-flip-drain", "direct-owner");
    let mut master = fixture.spawn_master("direct-owner", "master.stderr.log");
    let service_state = fixture.wait_for_service_state(&mut master);
    let child_pid = service_state["processId"]
        .as_u64()
        .and_then(|pid| u32::try_from(pid).ok())
        .expect("direct child pid");

    fixture.write_ownership_mode("compatibility");
    let status = master.wait_for_exit(Duration::from_secs(10));
    assert!(
        !status.success(),
        "a runtime ownership-mode flip must terminate direct ownership with a typed failure"
    );
    wait_until(Duration::from_secs(5), || {
        process_alive_for_pid(i64::from(child_pid)) == Some(false)
    });
    let final_state = wait_for_json(&fixture.master_state_file, Duration::from_secs(2), || {});
    assert_eq!(final_state["phase"], "failed");
    assert!(
        final_state["detail"]
            .as_str()
            .is_some_and(|detail| detail.contains("direct-owner runtime fence"))
    );
    assert!(!fixture.child_stop_file.exists());
}

#[test]
fn hostile_desired_identity_args_fail_before_master_state_mutation() {
    let fixture = MasterFixture::new("hostile-desired-identity", "direct-owner");
    let foreign_home = fixture.root.join("foreign-harness");
    fixture.write_progress_service(
        50,
        5_000,
        100,
        &[
            "--harness-home".to_string(),
            foreign_home.display().to_string(),
        ],
    );
    let output = fixture
        .master_command("direct-owner", "hostile.stderr.log")
        .output()
        .expect("run hostile desired-service candidate");
    assert!(!output.status.success());
    let error = fs::read_to_string(fixture.root.join("hostile.stderr.log")).unwrap_or_default();
    assert!(
        error.contains("refused reserved option --harness-home"),
        "unexpected hostile-args refusal: {error}"
    );
    assert!(!fixture.master_state_file.exists());
    assert!(!fixture.state_root.join("services").exists());
    assert!(!foreign_home.exists());
}

struct MasterFixture {
    root: PathBuf,
    harness_home: PathBuf,
    state_root: PathBuf,
    desired_file: PathBuf,
    harness_cli: PathBuf,
    master_state_file: PathBuf,
    service_state_file: PathBuf,
    heartbeat_file: PathBuf,
    master_stop_file: PathBuf,
    child_stop_file: PathBuf,
}

impl MasterFixture {
    fn new(label: &str, ownership_mode: &str) -> Self {
        let root = temp_root(label);
        let harness_home = root.join(".agent-harness");
        let state_root = harness_home.join("state").join("supervisor");
        let stop_root = state_root.join("stop");
        fs::create_dir_all(&stop_root).expect("create isolated supervisor state");
        fs::write(
            harness_home.join("harness-config.json"),
            format!(
                r#"{{"schema":"agent-harness.config.v1","supervisor":{{"enabled":true,"manageAllLoops":false,"autoReconcile":false,"ownershipMode":"{ownership_mode}"}}}}"#
            ),
        )
        .expect("write isolated ownership config");

        let harness_cli = PathBuf::from(env!("CARGO_BIN_EXE_agent-harness"));
        let child_stop = stop_root.join("progress-delivery-loop.stop");
        let desired_services = serde_json::json!([
            {
                "enabled": true,
                "serviceId": "progress-delivery-loop",
                "serviceKind": "loop",
                "args": [
                    "--harness-cli", harness_cli,
                    "--child-iterations", "0",
                    "--idle-ms", "50",
                    "--max-consecutive-errors", "5",
                    "--stop-file", child_stop
                ],
                "priority": "telemetry",
                "restartDelayMs": 100,
                "heartbeatTimeoutMs": 5_000
            }
        ]);
        let desired_file = root.join("desired-services.json");
        fs::write(
            &desired_file,
            serde_json::to_vec_pretty(&desired_services).expect("serialize desired services"),
        )
        .expect("write desired services");
        let master_state_file = state_root.join("master").join("master-state.json");
        let service_state_file = state_root
            .join("services")
            .join("progress-delivery-loop.json");
        let heartbeat_file = state_root
            .join("loop-heartbeats")
            .join("progress-delivery-loop.json");
        let master_stop_file = stop_root.join("supervisor-master.stop");
        Self {
            root,
            harness_home,
            state_root,
            desired_file,
            harness_cli,
            master_state_file,
            service_state_file,
            heartbeat_file,
            master_stop_file,
            child_stop_file: child_stop,
        }
    }

    fn write_ownership_mode(&self, ownership_mode: &str) {
        write_json_atomic(
            &self.harness_home.join("harness-config.json"),
            &serde_json::json!({
                "schema": "agent-harness.config.v1",
                "supervisor": {
                    "enabled": true,
                    "manageAllLoops": false,
                    "autoReconcile": false,
                    "ownershipMode": ownership_mode,
                },
            }),
        )
        .expect("atomically write isolated ownership config");
    }

    fn write_progress_service(
        &self,
        idle_ms: u64,
        heartbeat_timeout_ms: i64,
        restart_delay_ms: i64,
        extra_args: &[String],
    ) {
        let mut service_args = vec![
            "--harness-cli".to_string(),
            self.harness_cli.display().to_string(),
            "--child-iterations".to_string(),
            "0".to_string(),
            "--idle-ms".to_string(),
            idle_ms.to_string(),
            "--max-consecutive-errors".to_string(),
            "5".to_string(),
            "--stop-file".to_string(),
            self.child_stop_file.display().to_string(),
        ];
        service_args.extend(extra_args.iter().cloned());
        let desired_services = serde_json::json!([{
            "enabled": true,
            "serviceId": "progress-delivery-loop",
            "serviceKind": "loop",
            "args": service_args,
            "priority": "telemetry",
            "restartDelayMs": restart_delay_ms,
            "heartbeatTimeoutMs": heartbeat_timeout_ms
        }]);
        fs::write(
            &self.desired_file,
            serde_json::to_vec_pretty(&desired_services).expect("serialize desired services"),
        )
        .expect("write desired services");
    }

    fn write_worker_service(&self, heartbeat_timeout_ms: i64, lease_ms: i64) {
        let worker_stop_file = self.state_root.join("stop").join("worker-loop.stop");
        let desired_services = serde_json::json!([{
            "enabled": true,
            "serviceId": "worker-loop",
            "serviceKind": "worker",
            "args": [
                "--harness-cli", self.harness_cli,
                "--child-iterations", "0",
                "--idle-ms", "50",
                "--max-consecutive-errors", "5",
                "--stop-file", worker_stop_file,
                "--lease-ms", lease_ms.to_string(),
                "--lane", "shell"
            ],
            "priority": "worker",
            "restartDelayMs": 100,
            "heartbeatTimeoutMs": heartbeat_timeout_ms
        }]);
        fs::write(
            &self.desired_file,
            serde_json::to_vec_pretty(&desired_services).expect("serialize worker service"),
        )
        .expect("write worker service");
    }

    fn write_partial_start_services(&self) -> PathBuf {
        let failed_stop_file = self
            .state_root
            .join("stop")
            .join("ledger-maintenance-loop.stop");
        let missing_runtime_workspace = self.root.join("missing-runtime-workspace");
        let desired_services = serde_json::json!([
            {
                "enabled": true,
                "serviceId": "progress-delivery-loop",
                "serviceKind": "progress-delivery",
                "args": [
                    "--harness-cli", self.harness_cli,
                    "--child-iterations", "0",
                    "--idle-ms", "50",
                    "--max-consecutive-errors", "5",
                    "--stop-file", self.child_stop_file,
                    "--runtime-workspace", self.root
                ],
                "priority": "telemetry",
                "restartDelayMs": 100,
                "heartbeatTimeoutMs": 5_000
            },
            {
                "enabled": true,
                "serviceId": "ledger-maintenance-loop",
                "serviceKind": "maintenance",
                "args": [
                    "--source-home", self.root,
                    "--harness-cli", self.harness_cli,
                    "--child-iterations", "0",
                    "--idle-ms", "50",
                    "--max-consecutive-errors", "5",
                    "--stop-file", failed_stop_file,
                    "--runtime-workspace", missing_runtime_workspace
                ],
                "priority": "maintenance",
                "restartDelayMs": 100,
                "heartbeatTimeoutMs": 5_000
            }
        ]);
        fs::write(
            &self.desired_file,
            serde_json::to_vec_pretty(&desired_services).expect("serialize partial-start services"),
        )
        .expect("write partial-start services");
        self.state_root
            .join("services")
            .join("ledger-maintenance-loop.json")
    }

    fn write_discord_gateway_service(&self) -> PathBuf {
        let system_root =
            PathBuf::from(std::env::var_os("SystemRoot").expect("Windows SystemRoot"));
        let powershell = system_root.join("System32/WindowsPowerShell/v1.0/powershell.exe");
        assert!(
            powershell.is_file(),
            "Windows PowerShell fixture is missing"
        );
        let descendant_script = self.root.join("gateway-descendant.ps1");
        fs::write(
            &descendant_script,
            "while ($true) { Start-Sleep -Milliseconds 200 }\r\n",
        )
        .expect("write descendant fixture script");
        let descendant_pid_file = self.root.join("gateway-descendant.pid");
        let quote = |path: &Path| path.display().to_string().replace('\'', "''");
        let gateway_script = self.root.join("gateway-fixture.ps1");
        fs::write(
            &gateway_script,
            format!(
                "$ErrorActionPreference = 'Stop'\r\n\
                 $child = Start-Process -FilePath '{}' -ArgumentList @('-NoProfile','-NonInteractive','-ExecutionPolicy','Bypass','-File','{}') -WindowStyle Hidden -PassThru\r\n\
                 Set-Content -LiteralPath '{}' -Value $child.Id -NoNewline\r\n\
                 Wait-Process -Id $child.Id\r\n",
                quote(&powershell),
                quote(&descendant_script),
                quote(&descendant_pid_file),
            ),
        )
        .expect("write gateway fixture script");
        let gateway_stop_file = self
            .state_root
            .join("stop")
            .join("discord-gateway-loop.stop");
        let desired_services = serde_json::json!([{
            "enabled": true,
            "serviceId": "discord-gateway-loop",
            "serviceKind": "gateway",
            "args": [
                "--source-home", self.root,
                "--harness-cli", self.harness_cli,
                "--node-exe", powershell,
                "--gateway-script", gateway_script,
                "--child-iterations", "0",
                "--idle-ms", "50",
                "--max-consecutive-errors", "5",
                "--stop-file", gateway_stop_file
            ],
            "priority": "gateway",
            "restartDelayMs": 100,
            "heartbeatTimeoutMs": 5_000
        }]);
        fs::write(
            &self.desired_file,
            serde_json::to_vec_pretty(&desired_services).expect("serialize gateway service"),
        )
        .expect("write gateway service");
        descendant_pid_file
    }

    fn master_command(&self, mode: &str, stderr_name: &str) -> Command {
        let mut command = Command::new(&self.harness_cli);
        command
            .args(["supervisor-master", "--mode", mode, "--harness-home"])
            .arg(&self.harness_home)
            .args(["--desired-services-file"])
            .arg(&self.desired_file)
            .args(["--harness-cli"])
            .arg(&self.harness_cli)
            .args([
                "--iterations",
                "0",
                "--idle-ms",
                "50",
                "--drain-timeout-ms",
                "5000",
            ])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::from(
                fs::File::create(self.root.join(stderr_name)).expect("create master stderr"),
            ));
        configure_noninteractive_command(&mut command);
        command
    }

    fn spawn_master(&self, mode: &str, stderr_name: &str) -> RunningMaster {
        RunningMaster {
            child: Some(
                self.master_command(mode, stderr_name)
                    .spawn()
                    .expect("spawn standalone master candidate"),
            ),
            stderr_file: self.root.join(stderr_name),
        }
    }

    fn wait_for_service_state(&self, master: &mut RunningMaster) -> Value {
        wait_for_json_matching(
            &self.service_state_file,
            Duration::from_secs(15),
            || master.assert_running("waiting for direct child readiness"),
            |value| {
                value["launchOwner"] == "rust-standalone-master"
                    && value["processId"].as_u64().is_some_and(|pid| pid > 0)
            },
        )
    }

    fn stop_master_cleanly(&self, master: &mut RunningMaster) {
        fs::write(
            &self.master_stop_file,
            br#"{"schema":"agent-harness.supervisor-stop-file.v1","serviceId":"supervisor-master","persistent":true}"#,
        )
        .expect("request master stop");
        let status = master.wait_for_exit(Duration::from_secs(10));
        assert!(
            status.success(),
            "standalone master did not drain cleanly: {status}; stderr={}",
            fs::read_to_string(&master.stderr_file).unwrap_or_default()
        );
        let stopped_state = wait_for_json(&self.master_state_file, Duration::from_secs(2), || {});
        assert_eq!(stopped_state["phase"], "stopped");
    }
}

impl Drop for MasterFixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}

struct RunningMaster {
    child: Option<Child>,
    stderr_file: PathBuf,
}

impl RunningMaster {
    fn id(&self) -> u32 {
        self.child.as_ref().expect("master still owned").id()
    }

    fn assert_running(&mut self, context: &str) {
        if let Some(status) = self
            .child
            .as_mut()
            .expect("master still owned")
            .try_wait()
            .expect("poll master")
        {
            let stderr = fs::read_to_string(&self.stderr_file).unwrap_or_default();
            panic!("{context}: master exited early: {status}; stderr={stderr}");
        }
    }

    fn kill(&mut self) -> std::io::Result<()> {
        self.child.as_mut().expect("master still owned").kill()
    }

    fn wait(&mut self) -> std::io::Result<std::process::ExitStatus> {
        self.child.as_mut().expect("master still owned").wait()
    }

    fn mark_reaped(&mut self) {
        self.child.take();
    }

    fn wait_for_exit(&mut self, timeout: Duration) -> std::process::ExitStatus {
        let deadline = Instant::now() + timeout;
        loop {
            if let Some(status) = self
                .child
                .as_mut()
                .expect("master still owned")
                .try_wait()
                .expect("poll child exit")
            {
                self.child.take();
                return status;
            }
            assert!(
                Instant::now() < deadline,
                "timed out waiting for process exit"
            );
            thread::sleep(Duration::from_millis(25));
        }
    }
}

impl Drop for RunningMaster {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

fn wait_for_json(path: &Path, timeout: Duration, heartbeat: impl FnMut()) -> Value {
    wait_for_json_matching(path, timeout, heartbeat, |_| true)
}

fn wait_for_json_matching(
    path: &Path,
    timeout: Duration,
    mut heartbeat: impl FnMut(),
    predicate: impl Fn(&Value) -> bool,
) -> Value {
    let deadline = Instant::now() + timeout;
    loop {
        heartbeat();
        if let Ok(text) = fs::read_to_string(path) {
            if let Ok(value) = serde_json::from_str::<Value>(&text) {
                if predicate(&value) {
                    return value;
                }
            }
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for matching JSON at {}",
            path.display()
        );
        thread::sleep(Duration::from_millis(25));
    }
}

fn wait_until(timeout: Duration, predicate: impl Fn() -> bool) {
    let deadline = Instant::now() + timeout;
    while !predicate() {
        assert!(Instant::now() < deadline, "timed out waiting for condition");
        thread::sleep(Duration::from_millis(25));
    }
}

fn wait_for_u32_file(path: &Path, timeout: Duration) -> u32 {
    let deadline = Instant::now() + timeout;
    loop {
        if let Ok(text) = fs::read_to_string(path)
            && let Ok(value) = text.trim().parse::<u32>()
        {
            return value;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for readable numeric file {}",
            path.display()
        );
        thread::sleep(Duration::from_millis(25));
    }
}

fn parent_pid_for_process(process_id: u32) -> Option<u32> {
    process_snapshot()
        .into_iter()
        .find(|entry| entry.th32ProcessID == process_id)
        .map(|entry| entry.th32ParentProcessID)
}

fn child_process_details(parent_pid: u32) -> Vec<(u32, String)> {
    let mut details = process_snapshot()
        .into_iter()
        .filter(|entry| entry.th32ParentProcessID == parent_pid)
        .map(|entry| {
            let length = entry
                .szExeFile
                .iter()
                .position(|code_unit| *code_unit == 0)
                .unwrap_or(entry.szExeFile.len());
            (
                entry.th32ProcessID,
                String::from_utf16_lossy(&entry.szExeFile[..length]),
            )
        })
        .collect::<Vec<_>>();
    details.sort_by_key(|entry| entry.0);
    details
}

fn agent_harness_child_process_ids(parent_pid: u32) -> Vec<u32> {
    child_process_details(parent_pid)
        .into_iter()
        .filter(|(_, executable)| executable.eq_ignore_ascii_case("agent-harness.exe"))
        .map(|(process_id, _)| process_id)
        .collect()
}

fn process_snapshot() -> Vec<PROCESSENTRY32W> {
    unsafe {
        let snapshot = CreateToolhelp32Snapshot(TH32CS_SNAPPROCESS, 0);
        assert_ne!(
            snapshot, INVALID_HANDLE_VALUE,
            "create Windows process snapshot"
        );
        let mut entries = Vec::new();
        let mut entry: PROCESSENTRY32W = zeroed();
        entry.dwSize = u32::try_from(size_of::<PROCESSENTRY32W>()).expect("entry size fits u32");
        if Process32FirstW(snapshot, &mut entry) != 0 {
            loop {
                entries.push(entry);
                entry = zeroed();
                entry.dwSize =
                    u32::try_from(size_of::<PROCESSENTRY32W>()).expect("entry size fits u32");
                if Process32NextW(snapshot, &mut entry) == 0 {
                    break;
                }
            }
        }
        let _ = CloseHandle(snapshot);
        entries
    }
}

fn terminate_process(process_id: u32) {
    unsafe {
        let handle = OpenProcess(PROCESS_TERMINATE, 0, process_id);
        assert_ne!(handle, 0, "open child pid {process_id} for termination");
        assert_ne!(
            TerminateProcess(handle, 91),
            0,
            "terminate child pid {process_id}"
        );
        let _ = CloseHandle(handle);
    }
}

fn suspend_process(process_id: u32) {
    unsafe {
        let snapshot = CreateToolhelp32Snapshot(TH32CS_SNAPTHREAD, 0);
        assert_ne!(
            snapshot, INVALID_HANDLE_VALUE,
            "create Windows thread snapshot"
        );
        let mut entry: THREADENTRY32 = zeroed();
        entry.dwSize = u32::try_from(size_of::<THREADENTRY32>()).expect("thread entry size");
        let mut suspended = 0usize;
        if Thread32First(snapshot, &mut entry) != 0 {
            loop {
                if entry.th32OwnerProcessID == process_id {
                    let thread_handle = OpenThread(THREAD_SUSPEND_RESUME, 0, entry.th32ThreadID);
                    if thread_handle != 0 {
                        if SuspendThread(thread_handle) != u32::MAX {
                            suspended += 1;
                        }
                        let _ = CloseHandle(thread_handle);
                    }
                }
                entry = zeroed();
                entry.dwSize =
                    u32::try_from(size_of::<THREADENTRY32>()).expect("thread entry size");
                if Thread32Next(snapshot, &mut entry) == 0 {
                    break;
                }
            }
        }
        let _ = CloseHandle(snapshot);
        assert!(
            suspended > 0,
            "suspend at least one thread for pid={process_id}"
        );
    }
}

fn wait_for_process_absent(process_id: u32, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    while process_alive_for_pid(i64::from(process_id)) != Some(false) {
        assert!(
            Instant::now() < deadline,
            "process {process_id} remained alive after bounded termination wait"
        );
        thread::sleep(Duration::from_millis(50));
    }
}

fn enqueue_shell_fixture(
    harness_home: &Path,
    script: &Path,
    script_dir: &Path,
    idempotency_key: &str,
    max_attempts: i64,
) {
    let now_ms = i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock after epoch")
            .as_millis(),
    )
    .expect("clock fits i64");
    enqueue_worker_job(WorkerEnqueueOptions {
        harness_home: harness_home.to_path_buf(),
        kind: WorkerJobKind::DeterministicShell,
        lane: Some("shell".to_string()),
        payload: serde_json::json!({
            "scriptPath": script,
            "cwd": script_dir,
        }),
        idempotency_key: Some(idempotency_key.to_string()),
        parent_job_id: None,
        job_group_id: None,
        master_agent_id: None,
        master_session_key: None,
        wake_policy: None,
        source: Some("standalone-master-lifecycle-test".to_string()),
        priority: 0,
        available_at_ms: Some(now_ms),
        max_attempts,
        timeout_ms: Some(60_000),
        cascade_timeout_ms: None,
        rate_key: None,
        concurrency_group_key: None,
        now_ms,
    })
    .expect("enqueue shell fixture");
}

fn process_has_visible_top_level_window(process_id: u32) -> bool {
    struct Probe {
        process_id: u32,
        found: bool,
    }

    unsafe extern "system" fn inspect_window(window: isize, parameter: isize) -> i32 {
        let probe = unsafe { &mut *(parameter as *mut Probe) };
        let mut owner_process_id = 0u32;
        unsafe {
            GetWindowThreadProcessId(window, &mut owner_process_id);
        }
        if owner_process_id == probe.process_id && unsafe { IsWindowVisible(window) } != 0 {
            probe.found = true;
            return 0;
        }
        1
    }

    let mut probe = Probe {
        process_id,
        found: false,
    };
    unsafe {
        EnumWindows(Some(inspect_window), std::ptr::addr_of_mut!(probe) as isize);
    }
    probe.found
}

fn temp_root(label: &str) -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock after epoch")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "agent-harness-standalone-master-{label}-{}-{nonce}",
        std::process::id()
    ))
}
