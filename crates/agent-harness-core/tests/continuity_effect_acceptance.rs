use std::fs;
use std::path::PathBuf;

use ring::digest::{SHA256, digest};
use serde_json::Value;

const FIXTURES: [(&str, &str); 5] = [
    (
        "parent-subagent-completion-ownership-replay.json",
        "a0b1db48d61e3663788a646e8ed941e485ddaed220d8d2b4d20e2487b25f5ef6",
    ),
    (
        "fresh-turn-after-goal-final-replay.json",
        "4e49db850d464293e5ebf7af2f925779b83ea5df15d7dfb9445b292926511e0b",
    ),
    (
        "historical-goal-closure-replay.json",
        "025a663ab2b3ead33dcea2a93f6ae2aadae3a949119d4026cfd2d870586390ee",
    ),
    (
        "channel-command-active-goal-cancel-replay.json",
        "dbb11dd305f0d88e10eaf9e247c3762b8bb373100bc3f15717fb48cd8d46125a",
    ),
    (
        "approval-waiting-channel-actions-replay.json",
        "0e03fa3ff5e1abcab0bd6ee0bc2c8d54293b1081fb33581b0b326c2628fd7112",
    ),
];

fn fixture_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("continuity-effects")
}

fn read_fixture(name: &str) -> Value {
    serde_json::from_slice(&fs::read(fixture_dir().join(name)).unwrap()).unwrap()
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

#[test]
fn continuity_effect_fixtures_are_checksum_bound_and_sanitized() {
    for (name, expected_sha256) in FIXTURES {
        let bytes = fs::read(fixture_dir().join(name)).unwrap();
        let text = String::from_utf8(bytes).unwrap();
        let canonical_text = text.replace("\r\n", "\n");
        assert_eq!(
            hex(digest(&SHA256, canonical_text.as_bytes()).as_ref()),
            expected_sha256,
            "{name}"
        );
        for forbidden in [
            ".agent-harness/",
            ".agent-harness\\",
            "D:\\Warehouse",
            "https://",
            "http://",
            "ghp_",
            "github_pat_",
            "Bearer ",
            "ahx1_",
        ] {
            assert!(
                !text.contains(forbidden),
                "fixture {name} contains forbidden private marker {forbidden}"
            );
        }
        let parsed: Value = serde_json::from_str(&text).unwrap();
        assert!(
            parsed
                .get("schema")
                .and_then(Value::as_str)
                .is_some_and(|schema| schema.starts_with("agent-harness.acceptance-fixture.")),
            "fixture {name} has an unexpected schema"
        );
    }
}

#[test]
fn parent_completion_fixture_covers_live_recovery_and_two_children() {
    let fixture = read_fixture("parent-subagent-completion-ownership-replay.json");
    let events = fixture["events"].as_array().unwrap();
    assert!(
        events
            .iter()
            .any(|event| event["path"] == "stdout-recovery")
    );
    let child_turns = events
        .iter()
        .filter_map(|event| event["turnId"].as_str())
        .filter(|turn_id| turn_id.starts_with("turn-child-"))
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(child_turns.len(), 2);
    assert_eq!(fixture["expected"]["parentCompletionCount"], 1);
    assert_eq!(fixture["expected"]["foreignFinalCanCompleteParent"], false);
}

#[test]
fn goal_authority_fixtures_cover_fresh_final_closure_and_command_boundaries() {
    let fresh = read_fixture("fresh-turn-after-goal-final-replay.json");
    assert_eq!(fresh["expected"]["queueAFinalCount"], 1);
    assert_eq!(fresh["expected"]["queueBFinalCount"], 1);
    assert_eq!(fresh["expected"]["queueBReusesQueueAText"], false);

    let closure = read_fixture("historical-goal-closure-replay.json");
    assert_eq!(closure["expected"]["backendUpdateCount"], 1);
    assert_eq!(closure["expected"]["runnableLineages"], 0);
    assert_eq!(closure["expected"]["newBindingCount"], 0);
    assert!(
        closure["rejections"]
            .as_array()
            .unwrap()
            .iter()
            .any(|case| case == "stale-checksum")
    );

    let commands = read_fixture("channel-command-active-goal-cancel-replay.json");
    assert_eq!(commands["providers"].as_array().unwrap().len(), 2);
    assert_eq!(commands["expected"]["newSessionBeforeGoalTerminal"], false);
    assert_eq!(commands["expected"]["duplicateTargetSessionCount"], 1);
}

#[test]
fn approval_fixture_covers_parked_non_blocking_and_restart_authority() {
    let fixture = read_fixture("approval-waiting-channel-actions-replay.json");
    assert_eq!(fixture["expected"]["waitingLeaseCount"], 0);
    assert_eq!(fixture["expected"]["laterQueueRuns"], true);
    assert_eq!(fixture["expected"]["modelTurnFromActionCount"], 0);
    assert_eq!(fixture["expected"]["effectCount"], 1);
    let variants = fixture["variants"].as_array().unwrap();
    for required in [
        "wrong-account",
        "wrong-session",
        "expired",
        "same-decision-replay",
        "restart-after-decision-before-continuation",
    ] {
        assert!(
            variants.iter().any(|variant| variant == required),
            "missing approval replay variant {required}"
        );
    }
}
