use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use serde::Serialize;
use serde_json::Value;

const HARNESS_CONFIG_VALIDATION_SCHEMA: &str = "agent-harness.config-validation.v1";
pub const HARNESS_CONFIG_FILE_NAME: &str = "harness-config.json";

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HarnessConfigValidationReport {
    pub schema: &'static str,
    pub harness_home: PathBuf,
    pub config_file: Option<PathBuf>,
    pub status: HarnessConfigValidationStatus,
    pub errors: Vec<String>,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum HarnessConfigValidationStatus {
    Missing,
    Valid,
    Invalid,
}

impl HarnessConfigValidationReport {
    pub fn is_valid(&self) -> bool {
        matches!(
            self.status,
            HarnessConfigValidationStatus::Missing | HarnessConfigValidationStatus::Valid
        )
    }
}

pub fn harness_config_candidates(harness_home: impl AsRef<Path>) -> [PathBuf; 2] {
    let harness_home = harness_home.as_ref();
    [
        harness_home.join(HARNESS_CONFIG_FILE_NAME),
        harness_home.join("config").join(HARNESS_CONFIG_FILE_NAME),
    ]
}

pub fn validate_harness_config(
    harness_home: impl AsRef<Path>,
) -> io::Result<HarnessConfigValidationReport> {
    let harness_home = harness_home.as_ref();
    let mut report = HarnessConfigValidationReport {
        schema: HARNESS_CONFIG_VALIDATION_SCHEMA,
        harness_home: harness_home.to_path_buf(),
        config_file: None,
        status: HarnessConfigValidationStatus::Missing,
        errors: Vec::new(),
        warnings: Vec::new(),
    };

    let Some(config_file) = harness_config_candidates(harness_home)
        .into_iter()
        .find(|path| path.is_file())
    else {
        return Ok(report);
    };

    report.config_file = Some(config_file.clone());
    let text = fs::read_to_string(&config_file)?;
    let value = match serde_json::from_str::<Value>(&text) {
        Ok(value) => value,
        Err(error) => {
            report.status = HarnessConfigValidationStatus::Invalid;
            report.errors.push(format!(
                "invalid JSON in {}: {error}",
                config_file.display()
            ));
            return Ok(report);
        }
    };

    validate_config_value(&value, &mut report.errors, &mut report.warnings);
    report.status = if report.errors.is_empty() {
        HarnessConfigValidationStatus::Valid
    } else {
        HarnessConfigValidationStatus::Invalid
    };
    Ok(report)
}

fn validate_config_value(value: &Value, errors: &mut Vec<String>, warnings: &mut Vec<String>) {
    let Some(object) = value.as_object() else {
        errors.push("harness-config root must be a JSON object".to_string());
        return;
    };

    let known_top_level = [
        "schema",
        "response",
        "security",
        "workerDispatch",
        "learning",
        "staging",
        "codex",
        "runtime",
    ];
    let has_known_section = object
        .keys()
        .any(|key| known_top_level.contains(&key.as_str()));
    if !has_known_section && object.keys().any(|key| is_worker_dispatch_key(key)) {
        validate_worker_dispatch_object("$", value, errors);
        return;
    }

    for (key, child) in object {
        match key.as_str() {
            "schema" => expect_string("$.schema", child, errors),
            "response" => validate_response_object("$.response", child, errors),
            "security" => validate_security_object("$.security", child, errors),
            "workerDispatch" => validate_worker_dispatch_object("$.workerDispatch", child, errors),
            "learning" => validate_learning_object("$.learning", child, errors),
            "staging" => validate_staging_object("$.staging", child, errors),
            "codex" => validate_codex_object("$.codex", child, errors),
            "runtime" => validate_runtime_object("$.runtime", child, errors),
            other => errors.push(format!("unknown harness-config key `{other}` at $")),
        }
    }

    if object.contains_key("codex") || object.contains_key("runtime") {
        warnings.push(
            "codex/runtime security aliases are accepted for compatibility; prefer security.* keys"
                .to_string(),
        );
    }
}

fn validate_response_object(path: &str, value: &Value, errors: &mut Vec<String>) {
    let Some(object) = expect_object(path, value, errors) else {
        return;
    };
    for (key, child) in object {
        match key.as_str() {
            "assistantNarrationMode" | "assistant_narration_mode" => expect_enum(
                path_key(path, key),
                child,
                &["off", "progress_panel", "inline_preface"],
                errors,
            ),
            "assistantNarrationMaxChars" | "assistant_narration_max_chars" => {
                expect_positive_u64(path_key(path, key), child, errors)
            }
            "assistantNarrationProgressMinUpdateMs"
            | "assistant_narration_progress_min_update_ms" => {
                expect_positive_i64(path_key(path, key), child, errors)
            }
            "assistantNarrationFinalPrefix" | "assistant_narration_final_prefix" => {
                expect_string(path_key(path, key), child, errors)
            }
            "emojiAccentMode" | "emoji_accent_mode" => {
                expect_emoji_accent_mode(path_key(path, key), child, errors)
            }
            "emojiAccentAgentModes" | "emoji_accent_agent_modes" => {
                validate_emoji_accent_mode_map(path_key(path, key), child, errors)
            }
            "emojiAccentChannelModes" | "emoji_accent_channel_modes" => {
                validate_emoji_accent_mode_map(path_key(path, key), child, errors)
            }
            other => errors.push(format!("unknown response config key `{other}` at {path}")),
        }
    }
}

fn validate_emoji_accent_mode_map(path: String, value: &Value, errors: &mut Vec<String>) {
    let Some(object) = expect_object(&path, value, errors) else {
        return;
    };
    for (key, child) in object {
        expect_emoji_accent_mode(path_key(&path, key), child, errors);
    }
}

fn expect_emoji_accent_mode(path: impl Into<String>, value: &Value, errors: &mut Vec<String>) {
    expect_enum(
        path,
        value,
        &[
            "off", "none", "disabled", "disable", "false", "subtle", "on", "enabled", "enable",
            "true",
        ],
        errors,
    );
}

fn validate_security_object(path: &str, value: &Value, errors: &mut Vec<String>) {
    let Some(object) = expect_object(path, value, errors) else {
        return;
    };
    for (key, child) in object {
        match key.as_str() {
            "codexApprovalPolicy" | "codexApprovals" => expect_string_bool_or_enum(
                path_key(path, key),
                child,
                &["deny", "accept", "on-request", "on-failure", "never"],
                errors,
            ),
            "codexSandbox"
            | "codexSandboxMode"
            | "codexSandboxPolicy"
            | "codexFilesystemSandbox" => expect_string_bool_or_enum(
                path_key(path, key),
                child,
                &[
                    "elevated",
                    "read-only",
                    "workspace-write",
                    "workspaceWrite",
                    "danger-full-access",
                    "dangerFullAccess",
                ],
                errors,
            ),
            other => errors.push(format!("unknown security config key `{other}` at {path}")),
        }
    }
}

fn validate_codex_object(path: &str, value: &Value, errors: &mut Vec<String>) {
    let Some(object) = expect_object(path, value, errors) else {
        return;
    };
    for (key, child) in object {
        match key.as_str() {
            "approvalPolicy" | "approvals" => expect_string_bool_or_enum(
                path_key(path, key),
                child,
                &["deny", "accept", "on-request", "on-failure", "never"],
                errors,
            ),
            "sandbox" | "sandboxMode" | "sandboxPolicy" | "filesystemSandbox" => {
                expect_string_bool_or_enum(
                    path_key(path, key),
                    child,
                    &[
                        "elevated",
                        "read-only",
                        "workspace-write",
                        "workspaceWrite",
                        "danger-full-access",
                        "dangerFullAccess",
                    ],
                    errors,
                )
            }
            other => errors.push(format!("unknown codex config key `{other}` at {path}")),
        }
    }
}

fn validate_runtime_object(path: &str, value: &Value, errors: &mut Vec<String>) {
    let Some(object) = expect_object(path, value, errors) else {
        return;
    };
    for (key, child) in object {
        match key.as_str() {
            "codexApprovalPolicy" | "codexSandbox" | "codexSandboxPolicy" => {
                expect_string_bool_or_enum(
                    path_key(path, key),
                    child,
                    &[
                        "deny",
                        "accept",
                        "on-request",
                        "on-failure",
                        "never",
                        "elevated",
                        "read-only",
                        "workspace-write",
                        "workspaceWrite",
                        "danger-full-access",
                        "dangerFullAccess",
                    ],
                    errors,
                )
            }
            other => errors.push(format!("unknown runtime config key `{other}` at {path}")),
        }
    }
}

fn validate_worker_dispatch_object(path: &str, value: &Value, errors: &mut Vec<String>) {
    let Some(object) = expect_object(path, value, errors) else {
        return;
    };
    let mut global_limit = None;
    let mut group_limit = None;
    let mut channel_limit = None;
    for (key, child) in object {
        match key.as_str() {
            "globalConcurrencyLimit" | "groupConcurrencyLimit" | "channelConcurrencyLimit" => {
                expect_positive_u64(path_key(path, key), child, errors);
                match key.as_str() {
                    "globalConcurrencyLimit" => global_limit = child.as_u64(),
                    "groupConcurrencyLimit" => group_limit = child.as_u64(),
                    "channelConcurrencyLimit" => channel_limit = child.as_u64(),
                    _ => {}
                }
            }
            "laneConcurrencyLimits" => validate_lane_limits(path_key(path, key), child, errors),
            "rateLeaseLimit" => expect_u64(path_key(path, key), child, errors),
            "rateLeaseWindowMs" => expect_positive_i64(path_key(path, key), child, errors),
            "allowedScriptRoots" => validate_string_array(path_key(path, key), child, errors),
            other => errors.push(format!(
                "unknown workerDispatch config key `{other}` at {path}"
            )),
        }
    }
    if let (Some(global), Some(group)) = (global_limit, group_limit)
        && global < group
    {
        errors.push(format!(
            "{path}.globalConcurrencyLimit ({global}) must be >= groupConcurrencyLimit ({group})"
        ));
    }
    if let (Some(group), Some(channel)) = (group_limit, channel_limit)
        && group < channel
    {
        errors.push(format!(
            "{path}.groupConcurrencyLimit ({group}) must be >= channelConcurrencyLimit ({channel})"
        ));
    }
}

fn validate_learning_object(path: &str, value: &Value, errors: &mut Vec<String>) {
    let Some(object) = expect_object(path, value, errors) else {
        return;
    };
    for (key, child) in object {
        match key.as_str() {
            "skillLearning" | "memoryNudge" | "backgroundReview" | "curator" | "sessionSearch"
            | "userModel" => validate_learning_section(path_key(path, key), child, errors),
            other => errors.push(format!("unknown learning config key `{other}` at {path}")),
        }
    }
}

fn validate_learning_section(path: String, value: &Value, errors: &mut Vec<String>) {
    let Some(object) = expect_object(&path, value, errors) else {
        return;
    };
    for (key, child) in object {
        match key.as_str() {
            "enabled" | "usageWeighted" => expect_bool(path_key(&path, key), child, errors),
            "applyMode" => expect_enum(
                path_key(&path, key),
                child,
                &["propose", "auto", "off", "quarantine"],
                errors,
            ),
            "trigger" => expect_enum(
                path_key(&path, key),
                child,
                &["signal", "interval", "manual"],
                errors,
            ),
            "tokenizer" => expect_enum(path_key(&path, key), child, &["trigram"], errors),
            "provider" => expect_string(path_key(&path, key), child, errors),
            "turnInterval" | "dailyJobCap" | "intervalHours" => {
                expect_positive_u64(path_key(&path, key), child, errors)
            }
            other => errors.push(format!("unknown learning section key `{other}` at {path}")),
        }
    }
}

fn validate_staging_object(path: &str, value: &Value, errors: &mut Vec<String>) {
    let Some(object) = expect_object(path, value, errors) else {
        return;
    };
    for (key, child) in object {
        match key.as_str() {
            "enabled" | "fakeCodexDefault" | "allowLiveTelegram" | "allowLiveDiscord" => {
                expect_bool(path_key(path, key), child, errors)
            }
            "harnessHome" | "buildTargetDir" | "runtimeWorkspace" => {
                expect_string(path_key(path, key), child, errors)
            }
            other => errors.push(format!("unknown staging config key `{other}` at {path}")),
        }
    }
}

fn validate_lane_limits(path: String, value: &Value, errors: &mut Vec<String>) {
    let Some(object) = expect_object(&path, value, errors) else {
        return;
    };
    for (lane, child) in object {
        expect_positive_u64(format!("{path}.{lane}"), child, errors);
    }
}

fn validate_string_array(path: String, value: &Value, errors: &mut Vec<String>) {
    let Some(values) = value.as_array() else {
        errors.push(format!("{path} must be an array of strings"));
        return;
    };
    for (index, child) in values.iter().enumerate() {
        expect_string(format!("{path}[{index}]"), child, errors);
    }
}

fn expect_object<'a>(
    path: &str,
    value: &'a Value,
    errors: &mut Vec<String>,
) -> Option<&'a serde_json::Map<String, Value>> {
    let object = value.as_object();
    if object.is_none() {
        errors.push(format!("{path} must be a JSON object"));
    }
    object
}

fn expect_string(path: impl Into<String>, value: &Value, errors: &mut Vec<String>) {
    if !value.is_string() {
        errors.push(format!("{} must be a string", path.into()));
    }
}

fn expect_bool(path: impl Into<String>, value: &Value, errors: &mut Vec<String>) {
    if !value.is_boolean() {
        errors.push(format!("{} must be a boolean", path.into()));
    }
}

fn expect_enum(path: impl Into<String>, value: &Value, allowed: &[&str], errors: &mut Vec<String>) {
    let path = path.into();
    let Some(raw) = value.as_str() else {
        errors.push(format!("{path} must be a string"));
        return;
    };
    if !allowed.contains(&raw) {
        errors.push(format!(
            "{path} has unsupported value `{raw}`; expected one of: {}",
            allowed.join(", ")
        ));
    }
}

fn expect_string_bool_or_enum(
    path: impl Into<String>,
    value: &Value,
    allowed: &[&str],
    errors: &mut Vec<String>,
) {
    if value.is_boolean() {
        return;
    }
    expect_enum(path, value, allowed, errors);
}

fn expect_u64(path: impl Into<String>, value: &Value, errors: &mut Vec<String>) {
    if value.as_u64().is_none() {
        errors.push(format!("{} must be a non-negative integer", path.into()));
    }
}

fn expect_positive_u64(path: impl Into<String>, value: &Value, errors: &mut Vec<String>) {
    match value.as_u64() {
        Some(value) if value > 0 => {}
        _ => errors.push(format!("{} must be a positive integer", path.into())),
    }
}

fn expect_positive_i64(path: impl Into<String>, value: &Value, errors: &mut Vec<String>) {
    match value.as_i64() {
        Some(value) if value > 0 => {}
        _ => errors.push(format!("{} must be a positive integer", path.into())),
    }
}

fn path_key(path: &str, key: &str) -> String {
    format!("{path}.{key}")
}

fn is_worker_dispatch_key(key: &str) -> bool {
    matches!(
        key,
        "globalConcurrencyLimit"
            | "groupConcurrencyLimit"
            | "channelConcurrencyLimit"
            | "laneConcurrencyLimits"
            | "rateLeaseLimit"
            | "rateLeaseWindowMs"
            | "allowedScriptRoots"
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn validates_current_config_shape() {
        let root = temp_root("validates_current_config_shape");
        let harness_home = root.join(".agent-harness");
        fs::create_dir_all(&harness_home).unwrap();
        fs::write(
            harness_home.join(HARNESS_CONFIG_FILE_NAME),
            r#"{
              "schema": "agent-harness.config.v1",
              "response": {
                "assistantNarrationMode": "progress_panel",
                "assistantNarrationMaxChars": 1200,
                "assistantNarrationProgressMinUpdateMs": 2500,
                "assistantNarrationFinalPrefix": "Work log",
                "emojiAccentMode": "subtle",
                "emojiAccentAgentModes": { "main": "on", "ops": "off" },
                "emojiAccentChannelModes": { "telegram:dm-42": "enabled" }
              },
              "security": {
                "codexApprovalPolicy": "accept",
                "codexSandbox": "elevated",
                "codexSandboxPolicy": "dangerFullAccess"
              },
              "workerDispatch": {
                "globalConcurrencyLimit": 12,
                "groupConcurrencyLimit": 6,
                "channelConcurrencyLimit": 3,
                "laneConcurrencyLimits": { "llm": 6, "shell": 6 },
                "rateLeaseLimit": 0,
                "rateLeaseWindowMs": 60000
              },
              "learning": {
                "skillLearning": { "enabled": true, "applyMode": "propose" },
                "memoryNudge": { "enabled": true, "turnInterval": 6 },
                "backgroundReview": { "enabled": true, "trigger": "signal", "dailyJobCap": 24 },
                "curator": { "enabled": true, "intervalHours": 168, "usageWeighted": true },
                "sessionSearch": { "enabled": true, "tokenizer": "trigram" },
                "userModel": { "enabled": true, "provider": "local", "applyMode": "propose" }
              },
              "staging": {
                "enabled": true,
                "harnessHome": ".agent-harness-staging",
                "buildTargetDir": "target/staging-build",
                "runtimeWorkspace": ".tmp/staging-workspace",
                "fakeCodexDefault": true,
                "allowLiveTelegram": false,
                "allowLiveDiscord": false
              }
            }"#,
        )
        .unwrap();

        let report = validate_harness_config(&harness_home).unwrap();

        assert_eq!(report.status, HarnessConfigValidationStatus::Valid);
        assert!(report.errors.is_empty());

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn rejects_unknown_keys_and_wrong_types() {
        let root = temp_root("rejects_unknown_keys_and_wrong_types");
        let harness_home = root.join(".agent-harness");
        fs::create_dir_all(&harness_home).unwrap();
        fs::write(
            harness_home.join(HARNESS_CONFIG_FILE_NAME),
            r#"{
              "workerDispatch": {
                "globalConcurrencyLimit": "12",
                "typoLimit": 3
              },
              "unknownRoot": true
            }"#,
        )
        .unwrap();

        let report = validate_harness_config(&harness_home).unwrap();

        assert_eq!(report.status, HarnessConfigValidationStatus::Invalid);
        assert!(
            report
                .errors
                .iter()
                .any(|error| error.contains("globalConcurrencyLimit"))
        );
        assert!(
            report
                .errors
                .iter()
                .any(|error| error.contains("typoLimit"))
        );
        assert!(
            report
                .errors
                .iter()
                .any(|error| error.contains("unknownRoot"))
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn rejects_invalid_enums_and_concurrency_invariants() {
        let root = temp_root("rejects_invalid_enums_and_concurrency_invariants");
        let harness_home = root.join(".agent-harness");
        fs::create_dir_all(&harness_home).unwrap();
        fs::write(
            harness_home.join(HARNESS_CONFIG_FILE_NAME),
            r#"{
              "response": {
                "assistantNarrationMode": "chatty",
                "emojiAccentMode": "loud"
              },
              "security": { "codexApprovalPolicy": "YOLO" },
              "workerDispatch": {
                "globalConcurrencyLimit": 2,
                "groupConcurrencyLimit": 3,
                "channelConcurrencyLimit": 4
              },
              "learning": {
                "skillLearning": { "enabled": true, "applyMode": "always" }
              }
            }"#,
        )
        .unwrap();

        let report = validate_harness_config(&harness_home).unwrap();

        assert_eq!(report.status, HarnessConfigValidationStatus::Invalid);
        assert!(
            report
                .errors
                .iter()
                .any(|error| error.contains("assistantNarrationMode"))
        );
        assert!(
            report
                .errors
                .iter()
                .any(|error| error.contains("emojiAccentMode"))
        );
        assert!(
            report
                .errors
                .iter()
                .any(|error| error.contains("codexApprovalPolicy"))
        );
        assert!(
            report
                .errors
                .iter()
                .any(|error| error.contains("globalConcurrencyLimit"))
        );
        assert!(
            report
                .errors
                .iter()
                .any(|error| error.contains("applyMode"))
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn accepts_legacy_bare_worker_dispatch_shape() {
        let root = temp_root("accepts_legacy_bare_worker_dispatch_shape");
        let harness_home = root.join(".agent-harness");
        fs::create_dir_all(&harness_home).unwrap();
        fs::write(
            harness_home.join(HARNESS_CONFIG_FILE_NAME),
            r#"{
              "globalConcurrencyLimit": 3,
              "groupConcurrencyLimit": 2,
              "laneConcurrencyLimits": { "shell": 1 }
            }"#,
        )
        .unwrap();

        let report = validate_harness_config(&harness_home).unwrap();

        assert_eq!(report.status, HarnessConfigValidationStatus::Valid);

        let _ = fs::remove_dir_all(root);
    }

    fn temp_root(test_name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "agent-harness-config-{test_name}-{}-{nanos}",
            std::process::id()
        ))
    }
}
