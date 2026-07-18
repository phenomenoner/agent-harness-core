use std::fs;
use std::path::PathBuf;

use ring::digest::{SHA256, digest};

const FIXTURES: [(&str, &str); 6] = [
    (
        "account-continuation-double-bind-replay.json",
        "442871f7079563a8ef4f5bffa20c5b67241253318c0148818afcbd3d9100f075",
    ),
    (
        "queued-before-lease-progress-replay.json",
        "050142909fa689a3d05afd72182427d037a34fbdb886041831b75ddec6db4c04",
    ),
    (
        "server-overloaded-protocol-replay.json",
        "16647c4e59eaa14401d8b21cf2113430994862de52b86229efc000fdb7207eb8",
    ),
    (
        "timeout-continuation-handoff-replay.json",
        "1174156f28293cf0c08e7f7d9e733a7c1877ce433a4515252c2cc43a1157571c",
    ),
    (
        "deadline-drain-operation-plan-replay.json",
        "2baffd905d20ddc9fa349a748c30dc2fb69cdd1d24ff805530ae5d23c4846fa3",
    ),
    (
        "mcp-elicitation-external-effect-replay.json",
        "3e016e60d44baecfa868446c5f28e4d7ac9b14fac17b6ed4401ffba0645add74",
    ),
];

fn fixture_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("round22")
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

#[test]
fn round22_replay_fixtures_are_checksum_bound_and_sanitized() {
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
