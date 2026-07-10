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
        "orchestration",
        "staging",
        "codex",
        "codexContext",
        "runtime",
        "runtimeDispatch",
        "runtimeBackoff",
        "cronScheduler",
        "memory",
        "media",
        "supervisor",
        "channelIdentity",
        "liveControlGuard",
        "skills",
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
            "orchestration" => validate_orchestration_object("$.orchestration", child, errors),
            "staging" => validate_staging_object("$.staging", child, errors),
            "codex" => validate_codex_object("$.codex", child, errors),
            "codexContext" => validate_codex_context_object("$.codexContext", child, errors),
            "runtime" => validate_runtime_object("$.runtime", child, errors),
            "runtimeDispatch" => {
                validate_runtime_dispatch_object("$.runtimeDispatch", child, errors)
            }
            "runtimeBackoff" => validate_runtime_backoff_object("$.runtimeBackoff", child, errors),
            "cronScheduler" => validate_cron_scheduler_object("$.cronScheduler", child, errors),
            "memory" => validate_memory_object("$.memory", child, errors),
            "media" => validate_media_object("$.media", child, errors),
            "supervisor" => validate_supervisor_object("$.supervisor", child, errors),
            "channelIdentity" => {
                validate_channel_identity_object("$.channelIdentity", child, errors)
            }
            "liveControlGuard" => {
                validate_live_control_guard_object("$.liveControlGuard", child, errors)
            }
            "skills" => validate_skills_object("$.skills", child, errors),
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

fn validate_orchestration_object(path: &str, value: &Value, errors: &mut Vec<String>) {
    let Some(object) = expect_object(path, value, errors) else {
        return;
    };
    for (key, child) in object {
        match key.as_str() {
            "features" => {
                validate_orchestration_features_object(&path_key(path, key), child, errors)
            }
            other => errors.push(format!(
                "unknown orchestration config key `{other}` at {path}"
            )),
        }
    }
}

fn validate_orchestration_features_object(path: &str, value: &Value, errors: &mut Vec<String>) {
    let Some(object) = expect_object(path, value, errors) else {
        return;
    };
    for (key, child) in object {
        match key.as_str() {
            "modelCatalogV2" => {
                validate_model_catalog_v2_object(&path_key(path, key), child, errors)
            }
            other => errors.push(format!("unknown orchestration feature `{other}` at {path}")),
        }
    }
}

fn validate_model_catalog_v2_object(path: &str, value: &Value, errors: &mut Vec<String>) {
    let Some(object) = expect_object(path, value, errors) else {
        return;
    };
    for (key, child) in object {
        match key.as_str() {
            "mode" => expect_enum(
                path_key(path, key),
                child,
                &["off", "shadow", "authoritative"],
                errors,
            ),
            other => errors.push(format!(
                "unknown modelCatalogV2 config key `{other}` at {path}"
            )),
        }
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
            "telegramFormattingMode" | "telegram_formatting_mode" => {
                expect_telegram_formatting_mode(path_key(path, key), child, errors)
            }
            "telegramFormattingAgentModes" | "telegram_formatting_agent_modes" => {
                validate_telegram_formatting_mode_map(path_key(path, key), child, errors)
            }
            "telegramFormattingAccountModes" | "telegram_formatting_account_modes" => {
                validate_telegram_formatting_mode_map(path_key(path, key), child, errors)
            }
            "telegramFormattingChannelModes" | "telegram_formatting_channel_modes" => {
                validate_telegram_formatting_mode_map(path_key(path, key), child, errors)
            }
            "progressDeliveryMode" | "progress_delivery_mode" => {
                expect_progress_delivery_mode(path_key(path, key), child, errors)
            }
            "progressDeliveryMaxNonterminalUpdatesPerLane"
            | "progress_delivery_max_nonterminal_updates_per_lane"
            | "progressDeliveryMaxNonterminalBodyUpdatesPerQueue"
            | "progress_delivery_max_nonterminal_body_updates_per_queue"
            | "progressDeliveryStatusHeartbeatAfterBodyCapMs"
            | "progress_delivery_status_heartbeat_after_body_cap_ms" => {
                expect_u64(path_key(path, key), child, errors)
            }
            "progressDeliveryAgentModes" | "progress_delivery_agent_modes" => {
                validate_progress_delivery_mode_map(path_key(path, key), child, errors)
            }
            "progressDeliveryChannelModes" | "progress_delivery_channel_modes" => {
                validate_progress_delivery_mode_map(path_key(path, key), child, errors)
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

fn validate_telegram_formatting_mode_map(path: String, value: &Value, errors: &mut Vec<String>) {
    let Some(object) = expect_object(&path, value, errors) else {
        return;
    };
    for (key, child) in object {
        expect_telegram_formatting_mode(path_key(&path, key), child, errors);
    }
}

fn expect_telegram_formatting_mode(
    path: impl Into<String>,
    value: &Value,
    errors: &mut Vec<String>,
) {
    expect_enum(
        path,
        value,
        &[
            "plain",
            "text",
            "off",
            "disabled",
            "false",
            "html",
            "telegram-html",
            "on",
            "enabled",
            "true",
        ],
        errors,
    );
}

fn validate_progress_delivery_mode_map(path: String, value: &Value, errors: &mut Vec<String>) {
    let Some(object) = expect_object(&path, value, errors) else {
        return;
    };
    for (key, child) in object {
        expect_progress_delivery_mode(path_key(&path, key), child, errors);
    }
}

fn expect_progress_delivery_mode(path: impl Into<String>, value: &Value, errors: &mut Vec<String>) {
    expect_enum(
        path,
        value,
        &[
            "on",
            "enabled",
            "enable",
            "true",
            "progress_panel",
            "progress-panel",
            "off",
            "none",
            "hidden",
            "disabled",
            "disable",
            "false",
            "mute",
            "muted",
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
            "codexSandbox" | "codexSandboxMode" => {
                expect_codex_windows_sandbox_mode(path_key(path, key), child, errors)
            }
            "codexSandboxPolicy" | "codexFilesystemSandbox" => {
                expect_codex_sandbox_policy(path_key(path, key), child, errors)
            }
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
            "sandbox" | "sandboxMode" => {
                expect_codex_windows_sandbox_mode(path_key(path, key), child, errors)
            }
            "sandboxPolicy" | "filesystemSandbox" => {
                expect_codex_sandbox_policy(path_key(path, key), child, errors)
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
            "codexApprovalPolicy" => expect_string_bool_or_enum(
                path_key(path, key),
                child,
                &["deny", "accept", "on-request", "on-failure", "never"],
                errors,
            ),
            "codexSandbox" => expect_codex_windows_sandbox_mode(path_key(path, key), child, errors),
            "codexSandboxPolicy" => expect_codex_sandbox_policy(path_key(path, key), child, errors),
            "backoff" => validate_runtime_backoff_object(&path_key(path, key), child, errors),
            other => errors.push(format!("unknown runtime config key `{other}` at {path}")),
        }
    }
}

fn validate_runtime_dispatch_object(path: &str, value: &Value, errors: &mut Vec<String>) {
    let Some(object) = expect_object(path, value, errors) else {
        return;
    };
    for (key, child) in object {
        match key.as_str() {
            "globalConcurrencyLimit" => expect_positive_u64(path_key(path, key), child, errors),
            "interactiveReserve" | "interactiveReserved" => {
                expect_u64(path_key(path, key), child, errors)
            }
            "classes" => validate_runtime_dispatch_classes(path_key(path, key), child, errors),
            other => errors.push(format!(
                "unknown runtimeDispatch config key `{other}` at {path}"
            )),
        }
    }
}

fn validate_runtime_dispatch_classes(path: String, value: &Value, errors: &mut Vec<String>) {
    let Some(object) = expect_object(&path, value, errors) else {
        return;
    };
    for (class_name, child) in object {
        validate_runtime_dispatch_class(path_key(&path, class_name), child, errors);
    }
}

fn validate_runtime_dispatch_class(path: String, value: &Value, errors: &mut Vec<String>) {
    let Some(object) = expect_object(&path, value, errors) else {
        return;
    };
    for (key, child) in object {
        match key.as_str() {
            "maxActive"
            | "perAgentMaxActive"
            | "perChannelMaxActive"
            | "perAgentChannelMaxActive"
            | "perSessionMaxActive"
            | "perSessionLaneMaxActive"
            | "perJobMaxActive"
            | "maxQueuedPerAgent" => expect_positive_u64(path_key(&path, key), child, errors),
            "sessionFifo" | "sameSessionMainAgentSerialization" => {
                expect_bool(path_key(&path, key), child, errors)
            }
            other => errors.push(format!(
                "unknown runtimeDispatch class config key `{other}` at {path}"
            )),
        }
    }
}

fn validate_codex_context_object(path: &str, value: &Value, errors: &mut Vec<String>) {
    let Some(object) = expect_object(path, value, errors) else {
        return;
    };
    for (key, child) in object {
        match key.as_str() {
            "enabled"
            | "preferOfficialCompact"
            | "prefer_official_compact"
            | "autoCompactBeforeTurn"
            | "auto_compact_before_turn"
            | "retryOnceAfterCompact"
            | "retry_once_after_compact"
            | "manualRecoveryAllowed"
            | "manual_recovery_allowed"
            | "cooperativeMidTurnDrain"
            | "cooperative_mid_turn_drain" => expect_bool(path_key(path, key), child, errors),
            "fallbackOnCompactFailure" | "fallback_on_compact_failure" => expect_enum(
                path_key(path, key),
                child,
                &["checkpoint-and-new-thread", "manual", "disabled"],
                errors,
            ),
            "rolloverMode" | "rollover_mode" => expect_enum(
                path_key(path, key),
                child,
                &["working-set-memory", "disabled"],
                errors,
            ),
            "warnAtActiveContextRatio"
            | "warn_at_active_context_ratio"
            | "compactAtActiveContextRatio"
            | "compact_at_active_context_ratio" => expect_ratio(path_key(path, key), child, errors),
            "modelContextWindow"
            | "model_context_window"
            | "modelAutoCompactTokenLimit"
            | "model_auto_compact_token_limit"
            | "maxSuccessfulCompactsBeforeRollover"
            | "max_successful_compacts_before_rollover"
            | "maxContinuationDepth"
            | "max_continuation_depth"
            | "streamUnstableContinuationMinAttempts"
            | "stream_unstable_continuation_min_attempts"
            | "streamUnstableContinuationTokenLimit"
            | "stream_unstable_continuation_token_limit"
            | "toolOutputTokenLimit"
            | "tool_output_token_limit" => expect_positive_u64(path_key(path, key), child, errors),
            "modelAutoCompactTokenLimitScope"
            | "model_auto_compact_token_limit_scope"
            | "compactPrompt"
            | "compact_prompt"
            | "experimentalCompactPromptFile"
            | "experimental_compact_prompt_file" => {
                expect_string(path_key(path, key), child, errors)
            }
            other => errors.push(format!(
                "unknown codexContext config key `{other}` at {path}"
            )),
        }
    }
}

fn validate_runtime_backoff_object(path: &str, value: &Value, errors: &mut Vec<String>) {
    let Some(object) = expect_object(path, value, errors) else {
        return;
    };
    for (key, child) in object {
        match key.as_str() {
            "maxFailureAttempts" => expect_positive_u64(path_key(path, key), child, errors),
            "baseDelayMs" | "maxDelayMs" => expect_positive_i64(path_key(path, key), child, errors),
            "operatorHints" => expect_bool(path_key(path, key), child, errors),
            "providerFallbacks" => validate_runtime_fallbacks(path_key(path, key), child, errors),
            other => errors.push(format!(
                "unknown runtimeBackoff config key `{other}` at {path}"
            )),
        }
    }
}

fn validate_runtime_fallbacks(path: String, value: &Value, errors: &mut Vec<String>) {
    let Some(array) = value.as_array() else {
        errors.push(format!("{path} must be an array"));
        return;
    };
    for (index, child) in array.iter().enumerate() {
        let item_path = format!("{path}[{index}]");
        let Some(object) = expect_object(&item_path, child, errors) else {
            continue;
        };
        for (key, value) in object {
            match key.as_str() {
                "fromProvider" | "toProvider" | "toModel" | "reason" => {
                    expect_string(path_key(&item_path, key), value, errors)
                }
                "fromModel" => expect_string(path_key(&item_path, key), value, errors),
                other => errors.push(format!(
                    "unknown runtimeBackoff providerFallbacks key `{other}` at {item_path}"
                )),
            }
        }
    }
}

fn validate_cron_scheduler_object(path: &str, value: &Value, errors: &mut Vec<String>) {
    let Some(object) = expect_object(path, value, errors) else {
        return;
    };
    for (key, child) in object {
        match key.as_str() {
            "enabled" => expect_bool(path_key(path, key), child, errors),
            "intervalMs" => expect_positive_i64(path_key(path, key), child, errors),
            "maxCatchupPerTick"
            | "maxEnqueuePerTick"
            | "maxActiveRunsPerJob"
            | "maxActiveRunsPerAgent"
            | "maxQueuedPerAgent" => expect_positive_u64(path_key(path, key), child, errors),
            "nativeCron" => validate_cron_scheduler_native(path_key(path, key), child, errors),
            "deterministicCron" => {
                validate_cron_scheduler_deterministic(path_key(path, key), child, errors)
            }
            other => errors.push(format!(
                "unknown cronScheduler config key `{other}` at {path}"
            )),
        }
    }
}

fn validate_cron_scheduler_native(path: String, value: &Value, errors: &mut Vec<String>) {
    let Some(object) = expect_object(&path, value, errors) else {
        return;
    };
    for (key, child) in object {
        match key.as_str() {
            "enabled" | "resumeCron" | "includeRegisteredCron" => {
                expect_bool(path_key(&path, key), child, errors)
            }
            other => errors.push(format!(
                "unknown cronScheduler.nativeCron config key `{other}` at {path}"
            )),
        }
    }
}

fn validate_cron_scheduler_deterministic(path: String, value: &Value, errors: &mut Vec<String>) {
    let Some(object) = expect_object(&path, value, errors) else {
        return;
    };
    for (key, child) in object {
        match key.as_str() {
            "enabled" | "allowDeterministicRun" | "executeShell" => {
                expect_bool(path_key(&path, key), child, errors)
            }
            other => errors.push(format!(
                "unknown cronScheduler.deterministicCron config key `{other}` at {path}"
            )),
        }
    }
}

fn validate_supervisor_object(path: &str, value: &Value, errors: &mut Vec<String>) {
    let Some(object) = expect_object(path, value, errors) else {
        return;
    };
    for (key, child) in object {
        match key.as_str() {
            "enabled" | "manageAllLoops" | "autoReconcile" => {
                expect_bool(path_key(path, key), child, errors)
            }
            "defaultHeartbeatTimeoutMs" | "restartDelayMs" | "idleMs" => {
                expect_positive_i64(path_key(path, key), child, errors)
            }
            "runtimeLoop"
            | "workerLoop"
            | "cronSchedulerLoop"
            | "progressDeliveryLoop"
            | "telegramLoop"
            | "discordOutboxLoop"
            | "discordGatewayLoop" => {
                validate_supervisor_loop_object(path_key(path, key), child, errors)
            }
            "telegramLoops" => {
                validate_supervisor_telegram_loops(path_key(path, key), child, errors)
            }
            "services" => validate_supervisor_services(path_key(path, key), child, errors),
            other => errors.push(format!("unknown supervisor config key `{other}` at {path}")),
        }
    }
}

fn validate_memory_object(path: &str, value: &Value, errors: &mut Vec<String>) {
    let Some(object) = expect_object(path, value, errors) else {
        return;
    };
    for (key, child) in object {
        match key.as_str() {
            "openclawMemBridgeCommand"
            | "openclaw_mem_bridge_command"
            | "openclawMemBridgeBin"
            | "openclaw_mem_bridge_bin" => expect_string(path_key(path, key), child, errors),
            other => errors.push(format!("unknown memory config key `{other}` at {path}")),
        }
    }
}

fn validate_media_object(path: &str, value: &Value, errors: &mut Vec<String>) {
    let Some(object) = expect_object(path, value, errors) else {
        return;
    };
    for (key, child) in object {
        match key.as_str() {
            "maxMbPerAttachment" | "max_mb_per_attachment" => {
                expect_positive_u64(path_key(path, key), child, errors)
            }
            "allowDirs" | "allow_dirs" => validate_string_array(path_key(path, key), child, errors),
            "trustRecentSeconds" | "trust_recent_seconds" => {
                if !child.is_null() {
                    expect_u64(path_key(path, key), child, errors);
                }
            }
            "strict" | "lintFailClosed" | "lint_fail_closed" | "nativeImageInput"
            | "native_image_input" => expect_bool(path_key(path, key), child, errors),
            other => errors.push(format!("unknown media config key `{other}` at {path}")),
        }
    }
}

fn validate_supervisor_loop_object(path: String, value: &Value, errors: &mut Vec<String>) {
    let Some(object) = expect_object(&path, value, errors) else {
        return;
    };
    for (key, child) in object {
        match key.as_str() {
            "enabled" => expect_bool(path_key(&path, key), child, errors),
            "serviceId" | "serviceKind" | "account" | "telegramAccount" | "discordAccount"
            | "agent" | "agentId" | "lane" | "workerId" => {
                expect_string(path_key(&path, key), child, errors)
            }
            "restartDelayMs" | "heartbeatTimeoutMs" | "idleMs" | "timeoutMs" | "idleTimeoutMs"
            | "leaseMs" => expect_positive_i64(path_key(&path, key), child, errors),
            "runtimeConcurrency"
            | "maxConsecutiveErrors"
            | "pollTimeoutSeconds"
            | "maxUpdates"
            | "outboxLimit" => expect_positive_u64(path_key(&path, key), child, errors),
            "childIterations" => expect_u64(path_key(&path, key), child, errors),
            "args" => validate_string_array(path_key(&path, key), child, errors),
            other => errors.push(format!(
                "unknown supervisor loop config key `{other}` at {path}"
            )),
        }
    }
}

fn validate_supervisor_telegram_loops(path: String, value: &Value, errors: &mut Vec<String>) {
    let Some(array) = value.as_array() else {
        errors.push(format!("{path} must be an array"));
        return;
    };
    for (index, child) in array.iter().enumerate() {
        validate_supervisor_loop_object(format!("{path}[{index}]"), child, errors);
    }
}

fn validate_supervisor_services(path: String, value: &Value, errors: &mut Vec<String>) {
    let Some(array) = value.as_array() else {
        errors.push(format!("{path} must be an array"));
        return;
    };
    for (index, child) in array.iter().enumerate() {
        let item_path = format!("{path}[{index}]");
        let Some(object) = expect_object(&item_path, child, errors) else {
            continue;
        };
        for (key, value) in object {
            match key.as_str() {
                "enabled" => expect_bool(path_key(&item_path, key), value, errors),
                "serviceId" | "serviceKind" | "priority" => {
                    expect_string(path_key(&item_path, key), value, errors)
                }
                "restartDelayMs" | "heartbeatTimeoutMs" => {
                    expect_positive_i64(path_key(&item_path, key), value, errors)
                }
                "args" => validate_string_array(path_key(&item_path, key), value, errors),
                other => errors.push(format!(
                    "unknown supervisor service config key `{other}` at {item_path}"
                )),
            }
        }
    }
}

fn validate_channel_identity_object(path: &str, value: &Value, errors: &mut Vec<String>) {
    let Some(object) = expect_object(path, value, errors) else {
        return;
    };
    for (key, child) in object {
        match key.as_str() {
            "schema" => expect_string(path_key(path, key), child, errors),
            "bindings" => validate_channel_identity_bindings(path_key(path, key), child, errors),
            other => errors.push(format!(
                "unknown channelIdentity config key `{other}` at {path}"
            )),
        }
    }
}

fn validate_live_control_guard_object(path: &str, value: &Value, errors: &mut Vec<String>) {
    let Some(object) = expect_object(path, value, errors) else {
        return;
    };
    for (key, child) in object {
        match key.as_str() {
            "enabled" | "allowStatusCommands" => expect_bool(path_key(path, key), child, errors),
            "liveHarnessHome" | "protectedTaskPrefix" => {
                expect_string(path_key(path, key), child, errors)
            }
            "approvalTtlSeconds" => expect_positive_u64(path_key(path, key), child, errors),
            "protectedProcessNames" | "protectedPaths" | "stagingHomePrefixes" => {
                validate_string_array(path_key(path, key), child, errors)
            }
            other => errors.push(format!(
                "unknown liveControlGuard config key `{other}` at {path}"
            )),
        }
    }
}

fn validate_channel_identity_bindings(path: String, value: &Value, errors: &mut Vec<String>) {
    let Some(array) = value.as_array() else {
        errors.push(format!("{path} must be an array"));
        return;
    };
    for (index, child) in array.iter().enumerate() {
        let item_path = format!("{path}[{index}]");
        let Some(object) = expect_object(&item_path, child, errors) else {
            continue;
        };
        for (key, value) in object {
            match key.as_str() {
                "platform" | "accountId" | "chatId" | "threadId" | "agentId" | "secretRef" => {
                    expect_string(path_key(&item_path, key), value, errors)
                }
                "enabled" => expect_bool(path_key(&item_path, key), value, errors),
                other => errors.push(format!(
                    "unknown channelIdentity binding key `{other}` at {item_path}"
                )),
            }
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

fn validate_skills_object(path: &str, value: &Value, errors: &mut Vec<String>) {
    let Some(object) = expect_object(path, value, errors) else {
        return;
    };
    for (key, child) in object {
        match key.as_str() {
            "matcher" => validate_skill_matcher_object(path_key(path, key), child, errors),
            "catalog" => validate_skill_catalog_object(path_key(path, key), child, errors),
            "taxonomy" => validate_skill_taxonomy_object(path_key(path, key), child, errors),
            "guard" => validate_skill_guard_object(path_key(path, key), child, errors),
            "lint" => validate_skill_lint_object(path_key(path, key), child, errors),
            other => errors.push(format!("unknown skills config key `{other}` at {path}")),
        }
    }
}

fn validate_skill_matcher_object(path: String, value: &Value, errors: &mut Vec<String>) {
    let Some(object) = expect_object(&path, value, errors) else {
        return;
    };
    for (key, child) in object {
        match key.as_str() {
            "ftsEnabled" | "usagePriorEnabled" => expect_bool(path_key(&path, key), child, errors),
            "minScore" => expect_u64(path_key(&path, key), child, errors),
            other => errors.push(format!("unknown skills.matcher key `{other}` at {path}")),
        }
    }
}

fn validate_skill_catalog_object(path: String, value: &Value, errors: &mut Vec<String>) {
    let Some(object) = expect_object(&path, value, errors) else {
        return;
    };
    for (key, child) in object {
        match key.as_str() {
            "enabled" => expect_bool(path_key(&path, key), child, errors),
            "limit" => expect_positive_u64(path_key(&path, key), child, errors),
            other => errors.push(format!("unknown skills.catalog key `{other}` at {path}")),
        }
    }
}

fn validate_skill_taxonomy_object(path: String, value: &Value, errors: &mut Vec<String>) {
    let Some(object) = expect_object(&path, value, errors) else {
        return;
    };
    for (key, child) in object {
        match key.as_str() {
            "categories" => validate_string_array(path_key(&path, key), child, errors),
            other => errors.push(format!("unknown skills.taxonomy key `{other}` at {path}")),
        }
    }
}

fn validate_skill_guard_object(path: String, value: &Value, errors: &mut Vec<String>) {
    let Some(object) = expect_object(&path, value, errors) else {
        return;
    };
    for (key, child) in object {
        match key.as_str() {
            "agentCreated" => expect_bool(path_key(&path, key), child, errors),
            "packPolicy" => expect_string(path_key(&path, key), child, errors),
            other => errors.push(format!("unknown skills.guard key `{other}` at {path}")),
        }
    }
}

fn validate_skill_lint_object(path: String, value: &Value, errors: &mut Vec<String>) {
    let Some(object) = expect_object(&path, value, errors) else {
        return;
    };
    for (key, child) in object {
        match key.as_str() {
            "enforceOnApply" => expect_bool(path_key(&path, key), child, errors),
            other => errors.push(format!("unknown skills.lint key `{other}` at {path}")),
        }
    }
}

fn validate_learning_object(path: &str, value: &Value, errors: &mut Vec<String>) {
    let Some(object) = expect_object(path, value, errors) else {
        return;
    };
    for (key, child) in object {
        match key.as_str() {
            "skillLearning"
            | "skillSynthesis"
            | "skillNudge"
            | "memoryNudge"
            | "backgroundReview"
            | "selfImprovementReview"
            | "curator"
            | "sessionSearch"
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
            "enabled" | "usageWeighted" | "notify" | "consolidate" => {
                expect_bool(path_key(&path, key), child, errors)
            }
            "mode" => expect_enum(
                path_key(&path, key),
                child,
                &[
                    "propose-only",
                    "propose",
                    "propose-record-only",
                    "record-only",
                    "dry-run",
                    "dry_run",
                    "dispatch-and-replace",
                    "dispatch-and-replacement",
                    "auto",
                    "apply",
                    "off",
                ],
                errors,
            ),
            "applyMode" => expect_enum(
                path_key(&path, key),
                child,
                &[
                    "propose",
                    "auto",
                    "off",
                    "quarantine",
                    "propose-only",
                    "propose-record-only",
                    "record-only",
                    "dispatch-and-replace",
                    "dispatch-and-replacement",
                    "apply",
                ],
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
            "turnInterval" | "dailyJobCap" | "dailyCap" | "intervalHours" | "maxSelectedSkills"
            | "minToolCalls" | "minAssistantChars" | "staleAfterDays" | "archiveAfterDays"
            | "minClusterSize" => expect_positive_u64(path_key(&path, key), child, errors),
            "includeNamespaces" => validate_string_array(path_key(&path, key), child, errors),
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

fn expect_codex_windows_sandbox_mode(
    path: impl Into<String>,
    value: &Value,
    errors: &mut Vec<String>,
) {
    expect_string_bool_or_enum(
        path,
        value,
        &[
            "default",
            "elevated",
            "windows-elevated",
            "unelevated",
            "windows-unelevated",
            "disabled",
            "off",
            "none",
            "false",
        ],
        errors,
    );
}

fn expect_codex_sandbox_policy(path: impl Into<String>, value: &Value, errors: &mut Vec<String>) {
    expect_string_bool_or_enum(
        path,
        value,
        &[
            "default",
            "workspace",
            "workspace-write",
            "workspaceWrite",
            "workspacewrite",
            "readonly",
            "read-only",
            "read",
            "readOnly",
            "dangerfullaccess",
            "danger-full-access",
            "dangerFullAccess",
            "full-access",
            "full",
            "none",
            "off",
            "disabled",
            "false",
        ],
        errors,
    );
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

fn expect_ratio(path: impl Into<String>, value: &Value, errors: &mut Vec<String>) {
    let path = path.into();
    match value.as_f64() {
        Some(value) if value > 0.0 && value <= 1.0 => {}
        _ => errors.push(format!(
            "{path} must be a number greater than 0 and at most 1"
        )),
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
                "emojiAccentChannelModes": { "telegram:dm-42": "enabled" },
                "telegramFormattingMode": "plain",
                "telegramFormattingAgentModes": { "main": "html" },
                "telegramFormattingAccountModes": { "xiaoxiaoli": "html" },
                "telegramFormattingChannelModes": { "telegram:dm-42": "plain" },
                "progressDeliveryMode": "on",
                "progressDeliveryAgentModes": { "xiaoxiaoli": "off" },
                "progressDeliveryChannelModes": { "telegram:group-alpha": "muted" }
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
                "supervisor": {
                "enabled": true,
                "manageAllLoops": true,
                "autoReconcile": false,
                "defaultHeartbeatTimeoutMs": 120000,
                "restartDelayMs": 60000,
                "runtimeLoop": { "enabled": true, "runtimeConcurrency": 1, "childIterations": 0 },
                "workerLoop": { "enabled": true, "leaseMs": 120000 },
                "cronSchedulerLoop": { "enabled": true, "idleMs": 60000 },
                "progressDeliveryLoop": { "enabled": true },
                "telegramLoop": { "enabled": true },
                "telegramLoops": [
                  { "enabled": true, "serviceId": "telegram-loop-xiaoxiaoli", "telegramAccount": "xiaoxiaoli", "agent": "xiaoxiaoli" }
                ],
                "discordOutboxLoop": { "enabled": true, "outboxLimit": 20 },
                "discordGatewayLoop": { "enabled": true },
                "services": [
                  {
                    "enabled": true,
                    "serviceId": "custom-loop",
                    "serviceKind": "loop",
                    "priority": "standard",
                    "args": ["--source-home", "."],
                    "restartDelayMs": 60000,
                    "heartbeatTimeoutMs": 120000
                  }
                ]
              },
              "learning": {
                "skillLearning": { "enabled": true, "applyMode": "propose" },
                "selfImprovementReview": { "enabled": true, "mode": "dispatch-and-replace", "notify": true, "dailyCap": 24, "maxSelectedSkills": 1 },
                "memoryNudge": { "enabled": true, "turnInterval": 6 },
                "backgroundReview": { "enabled": true, "trigger": "signal", "dailyJobCap": 24 },
                "curator": { "enabled": true, "intervalHours": 168, "usageWeighted": true },
                "sessionSearch": { "enabled": true, "tokenizer": "trigram" },
                "userModel": { "enabled": true, "provider": "local", "applyMode": "propose" }
              },
              "memory": {
                "openclawMemBridgeCommand": "openclaw-mem-bridge-dispatch .agent-harness",
                "openclawMemBridgeBin": "openclaw-mem"
              },
              "staging": {
                "enabled": true,
                "harnessHome": ".agent-harness-staging",
                "buildTargetDir": "target/staging-build",
                "runtimeWorkspace": ".tmp/staging-workspace",
                "fakeCodexDefault": true,
                "allowLiveTelegram": false,
                "allowLiveDiscord": false
              },
              "liveControlGuard": {
                "enabled": true,
                "liveHarnessHome": ".agent-harness",
                "allowStatusCommands": true,
                "protectedTaskPrefix": "AgentHarness",
                "protectedProcessNames": ["agent-harness.exe"],
                "protectedPaths": [".agent-harness/state/supervisor/windows-scheduled-tasks"],
                "stagingHomePrefixes": [".agent-harness-staging", ".debug", "target/staging"],
                "approvalTtlSeconds": 900
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
                "emojiAccentMode": "loud",
                "progressDeliveryMode": "chatty"
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
                .any(|error| error.contains("progressDeliveryMode"))
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
    fn validates_codex_sandbox_mode_and_policy_separately() {
        let root = temp_root("validates_codex_sandbox_mode_and_policy_separately");
        let harness_home = root.join(".agent-harness");
        fs::create_dir_all(&harness_home).unwrap();
        fs::write(
            harness_home.join(HARNESS_CONFIG_FILE_NAME),
            r#"{
              "security": {
                "codexSandboxMode": "disabled",
                "codexSandboxPolicy": "dangerFullAccess"
              }
            }"#,
        )
        .unwrap();

        let report = validate_harness_config(&harness_home).unwrap();
        assert_eq!(report.status, HarnessConfigValidationStatus::Valid);
        assert!(report.errors.is_empty());

        fs::write(
            harness_home.join(HARNESS_CONFIG_FILE_NAME),
            r#"{
              "security": {
                "codexSandbox": "read-only",
                "codexSandboxPolicy": "readOnly"
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
                .any(|error| { error.contains("codexSandbox") && error.contains("read-only") })
        );
        assert!(
            !report
                .errors
                .iter()
                .any(|error| error.contains("codexSandboxPolicy"))
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn validates_codex_context_rollover_config_keys() {
        let root = temp_root("validates_codex_context_rollover_config_keys");
        let harness_home = root.join(".agent-harness");
        fs::create_dir_all(&harness_home).unwrap();
        fs::write(
            harness_home.join(HARNESS_CONFIG_FILE_NAME),
            r#"{
              "codexContext": {
                "maxSuccessfulCompactsBeforeRollover": 2,
                "rolloverMode": "working-set-memory",
                "cooperativeMidTurnDrain": false
              }
            }"#,
        )
        .unwrap();

        let report = validate_harness_config(&harness_home).unwrap();
        assert_eq!(report.status, HarnessConfigValidationStatus::Valid);
        assert!(report.errors.is_empty());

        fs::write(
            harness_home.join(HARNESS_CONFIG_FILE_NAME),
            r#"{
              "codexContext": {
                "maxSuccessfulCompactsBeforeRollover": 0,
                "rolloverMode": "fresh-thread"
              }
            }"#,
        )
        .unwrap();

        let report = validate_harness_config(&harness_home).unwrap();
        assert_eq!(report.status, HarnessConfigValidationStatus::Invalid);
        assert!(report.errors.iter().any(|error| {
            error.contains("maxSuccessfulCompactsBeforeRollover")
                && error.contains("positive integer")
        }));
        assert!(
            report
                .errors
                .iter()
                .any(|error| { error.contains("rolloverMode") && error.contains("fresh-thread") })
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn validate_harness_config_accepts_cron_scheduler_run_caps() {
        let root = temp_root("validate_harness_config_accepts_cron_scheduler_run_caps");
        let harness_home = root.join(".agent-harness");
        fs::create_dir_all(&harness_home).unwrap();
        fs::write(
            harness_home.join(HARNESS_CONFIG_FILE_NAME),
            r#"{
              "schema": "agent-harness.config.v1",
              "cronScheduler": {
                "enabled": true,
                "intervalMs": 60000,
                "maxCatchupPerTick": 3,
                "maxEnqueuePerTick": 10,
                "maxActiveRunsPerJob": 1,
                "maxActiveRunsPerAgent": 4,
                "maxQueuedPerAgent": 20,
                "nativeCron": { "enabled": true, "resumeCron": true, "includeRegisteredCron": false },
                "deterministicCron": { "enabled": true, "allowDeterministicRun": true, "executeShell": false }
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
    fn validate_harness_config_accepts_skill_ecosystem_sections() {
        let root = temp_root("validate_harness_config_accepts_skill_ecosystem_sections");
        let harness_home = root.join(".agent-harness");
        fs::create_dir_all(&harness_home).unwrap();
        fs::write(
            harness_home.join(HARNESS_CONFIG_FILE_NAME),
            r#"{
              "schema": "agent-harness.config.v1",
              "skills": {
                "matcher": { "ftsEnabled": true, "usagePriorEnabled": true, "minScore": 0 },
                "catalog": { "enabled": true, "limit": 8 },
                "taxonomy": { "categories": ["operations", "channels", "memory", "runtime", "trading", "research", "media", "development", "self-improvement", "general"] },
                "guard": { "agentCreated": true, "packPolicy": "default" },
                "lint": { "enforceOnApply": true }
              },
              "learning": {
                "skillSynthesis": { "enabled": true, "mode": "auto", "dailyCap": 3, "minToolCalls": 5, "minAssistantChars": 600 },
                "skillNudge": { "enabled": true, "turnInterval": 8 },
                "memoryNudge": { "enabled": true, "turnInterval": 6 },
                "curator": {
                  "enabled": true,
                  "mode": "propose",
                  "intervalHours": 168,
                  "staleAfterDays": 30,
                  "archiveAfterDays": 90,
                  "consolidate": true,
                  "minClusterSize": 2,
                  "includeNamespaces": ["agent-created"]
                }
              }
            }"#,
        )
        .unwrap();

        let report = validate_harness_config(&harness_home).unwrap();

        assert_eq!(report.status, HarnessConfigValidationStatus::Valid);
        assert!(report.errors.is_empty(), "{:?}", report.errors);

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

    #[test]
    fn model_catalog_config_accepts_validated_v2_rollout_namespace() {
        let root = temp_root("model_catalog_config_accepts_validated_v2_rollout_namespace");
        let harness_home = root.join(".agent-harness");
        fs::create_dir_all(&harness_home).unwrap();
        fs::write(
            harness_home.join(HARNESS_CONFIG_FILE_NAME),
            r#"{
              "orchestration": {
                "features": {
                  "modelCatalogV2": { "mode": "authoritative" }
                }
              }
            }"#,
        )
        .unwrap();

        let report = validate_harness_config(&harness_home).unwrap();
        assert_eq!(report.status, HarnessConfigValidationStatus::Valid);
        assert!(report.errors.is_empty(), "{:?}", report.errors);

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn model_catalog_config_rejects_unknown_mode_at_exact_path() {
        let root = temp_root("model_catalog_config_rejects_unknown_mode_at_exact_path");
        let harness_home = root.join(".agent-harness");
        fs::create_dir_all(&harness_home).unwrap();
        fs::write(
            harness_home.join(HARNESS_CONFIG_FILE_NAME),
            r#"{
              "orchestration": {
                "features": {
                  "modelCatalogV2": { "mode": "turbo" }
                }
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
                .any(|error| error.contains("$.orchestration.features.modelCatalogV2.mode")),
            "{:?}",
            report.errors
        );

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
