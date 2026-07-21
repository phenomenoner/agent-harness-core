use std::fs;
use std::path::PathBuf;
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

fn temp_root(test_name: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock must follow the Unix epoch")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "agent-harness-trace-cli-{test_name}-{}-{nanos}",
        std::process::id()
    ))
}

fn harness_command() -> Command {
    Command::new(env!("CARGO_BIN_EXE_agent-harness"))
}

#[test]
fn trace_queue_id_alias_matches_legacy_id_and_keeps_output_bounded() {
    let root = temp_root("queue-id-alias");
    let harness_home = root.join(".agent-harness");
    let queue_dir = harness_home.join("state").join("runtime-queue");
    fs::create_dir_all(&queue_dir).unwrap();
    fs::write(
        queue_dir.join("run-once-receipts.jsonl"),
        concat!(
            "{\"queueId\":\"guide-queue\",\"status\":\"completed\",",
            "\"reason\":\"bounded guide trace\",\"liveControlToken\":\"ahx1_not_serialized\"}\n",
            "{\"queueId\":\"unrelated-queue\",\"status\":\"completed\",",
            "\"reason\":\"UNRELATED_PRIVATE_MARKER\"}\n"
        ),
    )
    .unwrap();

    let alias = harness_command()
        .args([
            "trace",
            "--target-home",
            harness_home.to_str().unwrap(),
            "--queue-id",
            "guide-queue",
        ])
        .output()
        .unwrap();
    assert!(
        alias.status.success(),
        "documented --queue-id form failed: {}",
        String::from_utf8_lossy(&alias.stderr)
    );
    let alias_report: serde_json::Value = serde_json::from_slice(&alias.stdout).unwrap();
    assert_eq!(alias_report["schema"], "agent-harness.trace.v1");
    assert_eq!(alias_report["id"], "guide-queue");
    assert_eq!(alias_report["terminal"], true);
    assert_eq!(alias_report["records"].as_array().unwrap().len(), 1);
    let alias_text = String::from_utf8(alias.stdout).unwrap();
    assert!(!alias_text.contains("UNRELATED_PRIVATE_MARKER"));
    assert!(!alias_text.contains("ahx1_not_serialized"));

    let legacy = harness_command()
        .args([
            "trace",
            "--target-home",
            harness_home.to_str().unwrap(),
            "--id",
            "guide-queue",
        ])
        .output()
        .unwrap();
    assert!(legacy.status.success());
    let legacy_report: serde_json::Value = serde_json::from_slice(&legacy.stdout).unwrap();
    assert_eq!(legacy_report["id"], alias_report["id"]);
    assert_eq!(legacy_report["records"], alias_report["records"]);

    let _ = fs::remove_dir_all(root);
}

#[test]
fn trace_rejects_ambiguous_dual_selector() {
    let root = temp_root("dual-selector");
    let harness_home = root.join(".agent-harness");
    fs::create_dir_all(&harness_home).unwrap();

    let output = harness_command()
        .args([
            "trace",
            "--target-home",
            harness_home.to_str().unwrap(),
            "--queue-id",
            "queue-a",
            "--id",
            "queue-b",
        ])
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr)
            .contains("trace: --queue-id and --id are mutually exclusive")
    );

    let _ = fs::remove_dir_all(root);
}

#[test]
fn trace_requires_canonical_selector_or_legacy_alias() {
    let root = temp_root("missing-selector");
    let harness_home = root.join(".agent-harness");
    fs::create_dir_all(&harness_home).unwrap();

    let output = harness_command()
        .args(["trace", "--target-home", harness_home.to_str().unwrap()])
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr)
            .contains("trace: --queue-id is required (legacy --id is also accepted)")
    );

    let _ = fs::remove_dir_all(root);
}

#[test]
fn general_help_names_trace_queue_selector_contract() {
    let output = harness_command().output().unwrap();
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains(
        "--queue-id <id>         Canonical queue selector for trace and runtime queue commands"
    ));
    assert!(stdout.contains("--id <id>               Legacy trace selector; prefer --queue-id"));
}
