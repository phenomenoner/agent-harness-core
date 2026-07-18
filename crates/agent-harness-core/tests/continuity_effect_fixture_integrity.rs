use std::fs;
use std::path::PathBuf;

use ring::digest::{SHA256, digest};

const FIXTURES: [(&str, &str); 6] = [
    (
        "account-continuation-double-bind-replay.json",
        "15306aac954c66d0e13b0f4f9ad6cf630a9988c2cf37cce9b20854bca0204f69",
    ),
    (
        "queued-before-lease-progress-replay.json",
        "112934bc32a24595c8a358d3425af2732742516140c3e4849311864fbdc42968",
    ),
    (
        "server-overloaded-protocol-replay.json",
        "4af06f426df3481a877164f44ff28dff3c463f23a3fcb141df4b9ad5ed976959",
    ),
    (
        "timeout-continuation-handoff-replay.json",
        "ffbf16f7223f50fd54bd5fd0d6595d28cb4b3bd12d1c6a586966aa1224a7bbd4",
    ),
    (
        "deadline-drain-operation-plan-replay.json",
        "fc7e1f40c80b88b516d7c8bd80238f2ac25f39b20dff7bd4cd1a2b44fda2437a",
    ),
    (
        "mcp-elicitation-external-effect-replay.json",
        "5e784d44652996c1e62af50c189f274c8693b983fc1922cf609572a4d7653386",
    ),
];

fn fixture_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("continuity-effects")
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

#[test]
fn continuity_and_effect_replay_fixtures_are_checksum_bound_and_sanitized() {
    for (name, expected_sha256) in FIXTURES {
        let bytes = fs::read(fixture_dir().join(name)).unwrap();
        assert_eq!(
            hex(digest(&SHA256, &bytes).as_ref()),
            expected_sha256,
            "{name}"
        );
        let text = String::from_utf8(bytes).unwrap();
        for forbidden in [
            ".agent-harness/",
            ".agent-harness\\",
            "D:\\Warehouse",
            "https://",
            "http://",
            "sk-",
            "ghp_",
            "github_pat_",
            "Bearer ",
        ] {
            assert!(
                !text.contains(forbidden),
                "fixture {name} contains forbidden private marker {forbidden}"
            );
        }
        let parsed: serde_json::Value = serde_json::from_str(&text).unwrap();
        assert!(
            parsed.get("schema").is_some(),
            "fixture {name} lacks schema"
        );
    }
}
