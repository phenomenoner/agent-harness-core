use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::fs;
use std::io::{self, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

use serde::Serialize;
use serde_json::Value;

use crate::{
    WorkerStatusOptions,
    codex_runtime::{
        CodexApprovalPolicy, inspect_codex_approval_policy, inspect_codex_sandbox,
        inspect_codex_sandbox_policy,
    },
    collect_worker_status,
    config::{HarnessConfigValidationStatus, harness_config_candidates, validate_harness_config},
    logging::current_log_time_ms,
    loop_health::{process_alive_for_pid, read_supervisor_stop_file},
    probe_harness_log_writable,
};

const ACTIVATION_READINESS_SCHEMA: &str = "agent-harness.activation-readiness.v1";
const LOOP_HEARTBEAT_STALE_MS: i64 = 120_000;
const ACTIVATION_JSONL_SAMPLE_BYTES: u64 = 4 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActivationReadinessOptions {
    pub harness_home: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ActivationReadinessReport {
    pub schema: &'static str,
    pub harness_home: PathBuf,
    pub ready: bool,
    pub summary: ActivationReadinessSummary,
    pub checks: Vec<ActivationReadinessCheck>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ActivationReadinessSummary {
    pub passed: usize,
    pub warnings: usize,
    pub failed: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ActivationReadinessCheck {
    pub name: String,
    pub status: ActivationReadinessStatus,
    pub detail: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum ActivationReadinessStatus {
    Pass,
    Warn,
    Fail,
}

pub fn check_activation_readiness(
    options: ActivationReadinessOptions,
) -> io::Result<ActivationReadinessReport> {
    let mut checks = Vec::new();
    check_harness_config(&options.harness_home, &mut checks);
    let registry_file = options
        .harness_home
        .join("state")
        .join("harness-registry.json");
    let registry = match fs::read_to_string(&registry_file) {
        Ok(text) => match serde_json::from_str::<Value>(&text) {
            Ok(value) => {
                checks.push(pass(
                    "harness-registry",
                    format!("found {}", registry_file.display()),
                ));
                Some(value)
            }
            Err(error) => {
                checks.push(fail(
                    "harness-registry",
                    format!(
                        "registry JSON is invalid at {}: {error}",
                        registry_file.display()
                    ),
                ));
                None
            }
        },
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            checks.push(fail(
                "harness-registry",
                format!(
                    "missing {}; run registry-export after import",
                    registry_file.display()
                ),
            ));
            None
        }
        Err(error) => return Err(error),
    };

    if let Some(registry) = &registry {
        check_agents(registry, &mut checks);
        check_channels(registry, &options.harness_home, &mut checks);
        check_providers(registry, &mut checks);
        check_plugins(registry, &options.harness_home, &mut checks);
    }
    check_activation_plan_doc(&mut checks);
    check_harness_skills(&options.harness_home, &mut checks);
    check_runtime_queue(&options.harness_home, &mut checks);
    check_worker_dispatch(&options.harness_home, &mut checks);
    check_supervisor_plan(&options.harness_home, &mut checks);
    check_channel_state(&options.harness_home, &mut checks);
    check_logging(&options.harness_home, &mut checks);
    check_memory_import(&options.harness_home, registry.as_ref(), &mut checks);
    check_codex_auth(&mut checks);
    check_codex_config(&options.harness_home, &mut checks);
    check_codex_approval_policy(&options.harness_home, &mut checks);
    check_codex_sandbox(&options.harness_home, &mut checks);
    check_codex_filesystem_sandbox(&options.harness_home, &mut checks);

    let summary = summarize(&checks);
    Ok(ActivationReadinessReport {
        schema: ACTIVATION_READINESS_SCHEMA,
        harness_home: options.harness_home,
        ready: summary.failed == 0,
        summary,
        checks,
    })
}

fn check_harness_config(harness_home: &Path, checks: &mut Vec<ActivationReadinessCheck>) {
    match validate_harness_config(harness_home) {
        Ok(report) => match report.status {
            HarnessConfigValidationStatus::Missing => checks.push(pass(
                "harness-config",
                "harness-config.json is absent; built-in defaults apply",
            )),
            HarnessConfigValidationStatus::Valid if report.warnings.is_empty() => {
                let detail = report
                    .config_file
                    .as_ref()
                    .map(|path| format!("validated {}", path.display()))
                    .unwrap_or_else(|| {
                        "harness-config.json is absent; built-in defaults apply".to_string()
                    });
                checks.push(pass("harness-config", detail));
            }
            HarnessConfigValidationStatus::Valid => {
                let path = report
                    .config_file
                    .as_ref()
                    .map(|path| path.display().to_string())
                    .unwrap_or_else(|| "harness-config.json".to_string());
                checks.push(warn(
                    "harness-config",
                    format!("validated {path}; {}", report.warnings.join("; ")),
                ));
            }
            HarnessConfigValidationStatus::Invalid => {
                let path = report
                    .config_file
                    .as_ref()
                    .map(|path| path.display().to_string())
                    .unwrap_or_else(|| "harness-config.json".to_string());
                checks.push(fail(
                    "harness-config",
                    format!("invalid {path}: {}", report.errors.join("; ")),
                ));
            }
        },
        Err(error) => checks.push(fail(
            "harness-config",
            format!("failed to validate harness-config.json: {error}"),
        )),
    }
}

fn check_agents(registry: &Value, checks: &mut Vec<ActivationReadinessCheck>) {
    let enabled = registry
        .get("agents")
        .and_then(Value::as_array)
        .map(|agents| {
            agents
                .iter()
                .filter(|agent| {
                    agent
                        .get("enabled")
                        .and_then(Value::as_bool)
                        .unwrap_or(true)
                })
                .count()
        })
        .unwrap_or(0);
    if enabled == 0 {
        checks.push(fail(
            "agents",
            "no enabled agents found in harness registry",
        ));
    } else {
        checks.push(pass(
            "agents",
            format!("{enabled} enabled agent(s) available"),
        ));
    }
}

fn check_channels(
    registry: &Value,
    harness_home: &Path,
    checks: &mut Vec<ActivationReadinessCheck>,
) {
    let channels = registry.get("channels").unwrap_or(&Value::Null);
    let telegram = channels
        .get("telegram")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let discord = channels
        .get("discord")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    if telegram || discord {
        checks.push(pass(
            "channels",
            format!("configured: telegram={telegram}, discord={discord}"),
        ));
    } else {
        checks.push(warn(
            "channels",
            "no Telegram or Discord channel is enabled in registry",
        ));
    }
    if telegram {
        check_channel_token(
            checks,
            harness_home,
            "telegram-token",
            "TELEGRAM_BOT_TOKEN",
            "Telegram channel is enabled",
        );
        check_channel_access_policy(
            checks,
            harness_home,
            "telegram-access-policy",
            "Telegram",
            &[
                "AGENT_HARNESS_TELEGRAM_ADMIN_USER_IDS",
                "AGENT_HARNESS_TELEGRAM_ALLOWED_USER_IDS",
                "AGENT_HARNESS_TELEGRAM_GROUP_ADMIN_USER_IDS",
                "AGENT_HARNESS_TELEGRAM_GROUP_ALLOWED_USER_IDS",
                "AGENT_HARNESS_TELEGRAM_DIRECT_CHAT_IDS",
                "AGENT_HARNESS_TELEGRAM_GROUP_CHAT_IDS",
                "AGENT_HARNESS_TELEGRAM_GROUP_OPEN",
            ],
        );
    }
    if discord {
        check_channel_token(
            checks,
            harness_home,
            "discord-token",
            "DISCORD_BOT_TOKEN",
            "Discord channel is enabled",
        );
        check_channel_access_policy(
            checks,
            harness_home,
            "discord-access-policy",
            "Discord",
            &[
                "AGENT_HARNESS_DISCORD_ADMIN_USER_IDS",
                "AGENT_HARNESS_DISCORD_ALLOWED_USER_IDS",
                "AGENT_HARNESS_DISCORD_GROUP_ALLOWED_USER_IDS",
                "AGENT_HARNESS_DISCORD_CHANNEL_IDS",
                "AGENT_HARNESS_DISCORD_GUILD_IDS",
                "AGENT_HARNESS_DISCORD_GROUP_OPEN",
            ],
        );
    }
}

fn check_providers(registry: &Value, checks: &mut Vec<ActivationReadinessCheck>) {
    let providers = registry
        .get("providers")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    if providers.is_empty() {
        checks.push(warn(
            "providers",
            "no providers found in harness registry; Codex OAuth may still be usable",
        ));
        return;
    }
    if providers.iter().any(|provider| {
        provider
            .get("id")
            .and_then(Value::as_str)
            .is_some_and(|id| id.to_ascii_lowercase().contains("openrouter"))
    }) {
        check_env_token(
            checks,
            "openrouter-token",
            "OPENROUTER_API_KEY",
            "OpenRouter provider is present",
        );
    }
}

fn check_plugins(
    registry: &Value,
    harness_home: &Path,
    checks: &mut Vec<ActivationReadinessCheck>,
) {
    let sidecar_required = registry
        .get("plugins")
        .and_then(Value::as_array)
        .map(|plugins| {
            plugins
                .iter()
                .filter(|plugin| {
                    plugin
                        .get("sidecarRequired")
                        .and_then(Value::as_bool)
                        .unwrap_or(false)
                })
                .count()
        })
        .unwrap_or(0);
    if sidecar_required > 0 {
        check_plugin_sidecar_execution(harness_home, sidecar_required, checks);
        check_plugin_sidecar_probe(harness_home, checks);
        check_plugin_sidecar_bridge(harness_home, checks);
        check_plugin_hook_receipts(harness_home, checks);
        check_plugin_memory_slot_receipts(harness_home, checks);
    } else {
        checks.push(pass(
            "plugin-sidecar",
            "no sidecar-required plugins reported by registry",
        ));
    }
}

fn check_plugin_hook_receipts(harness_home: &Path, checks: &mut Vec<ActivationReadinessCheck>) {
    let path = harness_home
        .join("state")
        .join("plugin-sidecar")
        .join("hook-receipts.jsonl");
    match latest_jsonl_value(&path) {
        Ok(Some(value)) => {
            let status = value
                .get("status")
                .and_then(Value::as_str)
                .unwrap_or("unknown");
            let hook = value
                .get("hook")
                .and_then(Value::as_str)
                .unwrap_or("unknown");
            if status == "recorded" {
                checks.push(pass(
                    "plugin-hooks",
                    format!(
                        "OpenClaw-compatible hook receipt recorded for {hook} at {}",
                        path.display()
                    ),
                ));
            } else {
                checks.push(warn(
                    "plugin-hooks",
                    format!(
                        "latest hook receipt status={status} hook={hook} at {}",
                        path.display()
                    ),
                ));
            }
        }
        Ok(None) => checks.push(warn(
            "plugin-hooks",
            format!("no plugin hook receipts found at {}", path.display()),
        )),
        Err(error) if error.kind() == io::ErrorKind::NotFound => checks.push(warn(
            "plugin-hooks",
            format!(
                "not found yet: {}; run plugin-sidecar-call --method hooks.invoke",
                path.display()
            ),
        )),
        Err(error) => checks.push(warn(
            "plugin-hooks",
            format!("could not read {}: {error}", path.display()),
        )),
    }
}

fn check_plugin_memory_slot_receipts(
    harness_home: &Path,
    checks: &mut Vec<ActivationReadinessCheck>,
) {
    let path = harness_home
        .join("state")
        .join("plugin-sidecar")
        .join("memory-slot-receipts.jsonl");
    match latest_jsonl_value(&path) {
        Ok(Some(value)) => {
            let status = value
                .get("status")
                .and_then(Value::as_str)
                .unwrap_or("unknown");
            let operation = value
                .get("operation")
                .and_then(Value::as_str)
                .unwrap_or("unknown");
            if status == "recorded" {
                checks.push(pass(
                    "plugin-memory-slots",
                    format!(
                        "OpenClaw-compatible memory-slot receipt recorded for {operation} at {}",
                        path.display()
                    ),
                ));
            } else {
                checks.push(warn(
                    "plugin-memory-slots",
                    format!(
                        "latest memory-slot receipt status={status} operation={operation} at {}",
                        path.display()
                    ),
                ));
            }
        }
        Ok(None) => checks.push(warn(
            "plugin-memory-slots",
            format!("no plugin memory-slot receipts found at {}", path.display()),
        )),
        Err(error) if error.kind() == io::ErrorKind::NotFound => checks.push(warn(
            "plugin-memory-slots",
            format!(
                "not found yet: {}; run plugin-sidecar-call --method memory.slot",
                path.display()
            ),
        )),
        Err(error) => checks.push(warn(
            "plugin-memory-slots",
            format!("could not read {}: {error}", path.display()),
        )),
    }
}

fn check_plugin_sidecar_execution(
    harness_home: &Path,
    sidecar_required: usize,
    checks: &mut Vec<ActivationReadinessCheck>,
) {
    let path = harness_home
        .join("state")
        .join("plugin-sidecar")
        .join("execution-receipts.jsonl");
    match latest_jsonl_value(&path) {
        Ok(Some(value)) => {
            let status = value
                .get("status")
                .and_then(Value::as_str)
                .unwrap_or("unknown");
            let method = value
                .get("method")
                .and_then(Value::as_str)
                .unwrap_or("unknown");
            let resolved = value
                .get("resolvedManifests")
                .and_then(Value::as_u64)
                .unwrap_or(0);
            let unresolved = value
                .get("unresolvedSidecarRequired")
                .and_then(Value::as_u64)
                .unwrap_or(0);
            let tools = value.get("tools").and_then(Value::as_u64).unwrap_or(0);
            let reason = value
                .get("reason")
                .and_then(Value::as_str)
                .unwrap_or("no reason recorded");
            if status == "ready" {
                checks.push(pass(
                    "plugin-sidecar",
                    format!(
                        "sidecar execution catalog ready via method={method}: sidecarRequired={sidecar_required}, resolvedManifests={resolved}, tools={tools} at {}",
                        path.display()
                    ),
                ));
            } else {
                checks.push(fail(
                    "plugin-sidecar",
                    format!(
                        "sidecar execution catalog status={status} via method={method}: sidecarRequired={sidecar_required}, resolvedManifests={resolved}, unresolvedSidecarRequired={unresolved}, tools={tools} at {}: {reason}",
                        path.display()
                    ),
                ));
            }
        }
        Ok(None) => checks.push(fail(
            "plugin-sidecar",
            format!(
                "no sidecar execution receipt lines found at {}; run plugin-sidecar-call --method tools.probe",
                path.display()
            ),
        )),
        Err(error) if error.kind() == io::ErrorKind::NotFound => checks.push(fail(
            "plugin-sidecar",
            format!(
                "not found yet: {}; run plugin-sidecar-call --method tools.probe before plugin handoff",
                path.display()
            ),
        )),
        Err(error) => checks.push(fail(
            "plugin-sidecar",
            format!("could not read {}: {error}", path.display()),
        )),
    }
}

fn check_plugin_sidecar_bridge(harness_home: &Path, checks: &mut Vec<ActivationReadinessCheck>) {
    let path = harness_home
        .join("state")
        .join("plugin-sidecar")
        .join("bridge-receipts.jsonl");
    match latest_jsonl_value(&path) {
        Ok(Some(value)) => {
            let status = value
                .get("status")
                .and_then(Value::as_str)
                .unwrap_or("unknown");
            let method = value
                .get("method")
                .and_then(Value::as_str)
                .unwrap_or("unknown");
            let reason = value
                .get("reason")
                .and_then(Value::as_str)
                .unwrap_or("no reason recorded");
            if status == "ok" {
                checks.push(pass(
                    "plugin-sidecar-bridge",
                    format!(
                        "latest sidecar JSON-RPC call method={method} passed at {}",
                        path.display()
                    ),
                ));
            } else {
                checks.push(fail(
                    "plugin-sidecar-bridge",
                    format!(
                        "latest sidecar JSON-RPC call method={method} status={status} at {}: {reason}",
                        path.display()
                    ),
                ));
            }
        }
        Ok(None) => checks.push(warn(
            "plugin-sidecar-bridge",
            format!(
                "no sidecar bridge receipt lines found at {}",
                path.display()
            ),
        )),
        Err(error) if error.kind() == io::ErrorKind::NotFound => checks.push(warn(
            "plugin-sidecar-bridge",
            format!(
                "not found yet: {}; run plugin-sidecar-call before plugin handoff",
                path.display()
            ),
        )),
        Err(error) => checks.push(warn(
            "plugin-sidecar-bridge",
            format!("could not read {}: {error}", path.display()),
        )),
    }
}

fn check_plugin_sidecar_probe(harness_home: &Path, checks: &mut Vec<ActivationReadinessCheck>) {
    let path = harness_home
        .join("state")
        .join("plugin-sidecar")
        .join("probe-receipts.jsonl");
    match latest_jsonl_value(&path) {
        Ok(Some(value)) => {
            let status = value
                .get("status")
                .and_then(Value::as_str)
                .unwrap_or("unknown");
            let sidecar_required = value
                .get("sidecarRequired")
                .and_then(Value::as_u64)
                .unwrap_or(0);
            let reason = value
                .get("reason")
                .and_then(Value::as_str)
                .unwrap_or("no reason recorded");
            if status == "contract-ready" {
                checks.push(pass(
                    "plugin-sidecar-probe",
                    format!(
                        "sidecar probe contract-ready for {sidecar_required} plugin(s) at {}",
                        path.display()
                    ),
                ));
            } else {
                checks.push(fail(
                    "plugin-sidecar-probe",
                    format!(
                        "sidecar probe status={status} at {}: {reason}",
                        path.display()
                    ),
                ));
            }
        }
        Ok(None) => checks.push(warn(
            "plugin-sidecar-probe",
            format!("no sidecar probe receipt lines found at {}", path.display()),
        )),
        Err(error) if error.kind() == io::ErrorKind::NotFound => checks.push(warn(
            "plugin-sidecar-probe",
            format!(
                "not found yet: {}; run plugin-sidecar-probe before plugin handoff",
                path.display()
            ),
        )),
        Err(error) => checks.push(warn(
            "plugin-sidecar-probe",
            format!("could not read {}: {error}", path.display()),
        )),
    }
}

fn check_runtime_queue(harness_home: &Path, checks: &mut Vec<ActivationReadinessCheck>) {
    let queue_dir = harness_home.join("state").join("runtime-queue");
    if queue_dir.is_dir() {
        checks.push(pass(
            "runtime-queue",
            format!("runtime queue directory exists at {}", queue_dir.display()),
        ));
    } else {
        checks.push(warn(
            "runtime-queue",
            format!(
                "runtime queue directory is not present yet at {}",
                queue_dir.display()
            ),
        ));
    }
    for (name, file) in [
        ("prepared-executions", "execution-receipts.jsonl"),
        ("runtime-run-once", "run-once-receipts.jsonl"),
        ("codex-runtime-plans", "codex-runtime-receipts.jsonl"),
        ("codex-runtime-runs", "codex-runtime-run-receipts.jsonl"),
        (
            "codex-runtime-completions",
            "codex-runtime-completion-receipts.jsonl",
        ),
    ] {
        let path = queue_dir.join(file);
        if path.is_file() {
            checks.push(pass(name, format!("found {}", path.display())));
        } else {
            checks.push(warn(name, format!("not found yet: {}", path.display())));
        }
    }
    check_runtime_loop(harness_home, checks);
    check_codex_launch_probe(harness_home, checks);
}

fn check_runtime_loop(harness_home: &Path, checks: &mut Vec<ActivationReadinessCheck>) {
    let path = harness_home
        .join("state")
        .join("runtime-queue")
        .join("loop-last.json");
    match read_json_value(&path) {
        Ok(value) => {
            let errors = value.get("errors").and_then(Value::as_u64);
            let stop_reason = value
                .get("stopReason")
                .and_then(Value::as_str)
                .unwrap_or("no stop reason recorded");
            match errors {
                Some(0) => checks.push(pass(
                    "runtime-loop",
                    format!(
                        "latest runtime loop stopped cleanly at {}: {stop_reason}",
                        path.display()
                    ),
                )),
                Some(errors) => {
                    if let Some(heartbeat_detail) =
                        live_loop_heartbeat_detail(harness_home, "runtime-loop")
                    {
                        let detail = if stop_reason.contains("stop file") {
                            format!(
                                "latest runtime loop report at {} was a prior stop-file shutdown with errors={errors}; {heartbeat_detail}",
                                path.display()
                            )
                        } else {
                            format!(
                                "latest runtime loop report at {} recorded errors={errors}: {stop_reason}; superseded by live heartbeat; {heartbeat_detail}",
                                path.display()
                            )
                        };
                        if stop_reason.contains("stop file") {
                            checks.push(pass("runtime-loop", detail));
                        } else {
                            checks.push(warn("runtime-loop", detail));
                        }
                    } else if stop_reason.contains("stop file") {
                        checks.push(fail(
                            "runtime-loop",
                            format!(
                                "latest runtime loop recorded errors={errors} at {}: {stop_reason}",
                                path.display()
                            ),
                        ));
                    } else {
                        checks.push(fail(
                            "runtime-loop",
                            format!(
                                "latest runtime loop recorded errors={errors} at {}: {stop_reason}",
                                path.display()
                            ),
                        ));
                    }
                }
                None => checks.push(warn(
                    "runtime-loop",
                    format!(
                        "runtime loop report has no errors field at {}",
                        path.display()
                    ),
                )),
            }
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => checks.push(warn(
            "runtime-loop",
            format!(
                "not found yet: {}; run runtime-loop --stop-when-idle before runtime handoff",
                path.display()
            ),
        )),
        Err(error) => checks.push(fail(
            "runtime-loop",
            format!(
                "could not read runtime loop report {}: {error}",
                path.display()
            ),
        )),
    }
}

fn live_loop_heartbeat_detail(harness_home: &Path, name: &str) -> Option<String> {
    let stop_file = read_supervisor_stop_file(harness_home, name).ok()?;
    if stop_file.present {
        return None;
    }
    let path = harness_home
        .join("state")
        .join("supervisor")
        .join("loop-heartbeats")
        .join(format!("{name}.json"));
    let value = read_json_value(&path).ok()?;
    let status = value
        .get("status")
        .and_then(Value::as_str)
        .unwrap_or("unknown");
    if !matches!(
        status,
        "running"
            | "ok"
            | "no-work"
            | "ready"
            | "heartbeat"
            | "connected"
            | "safe-mode"
            | "lease-busy"
    ) {
        return None;
    }
    let age_ms = value
        .get("atMs")
        .and_then(Value::as_i64)
        .and_then(|at_ms| current_log_time_ms().ok().map(|now_ms| now_ms - at_ms));
    if age_ms.is_some_and(|age_ms| age_ms > LOOP_HEARTBEAT_STALE_MS) {
        return None;
    }
    if value
        .get("processId")
        .and_then(Value::as_i64)
        .and_then(process_alive_for_pid)
        == Some(false)
    {
        return None;
    }
    let detail = value
        .get("detail")
        .and_then(Value::as_str)
        .unwrap_or("no detail recorded");
    let age_detail = age_ms
        .map(|age_ms| format!("ageMs={age_ms}"))
        .unwrap_or_else(|| "ageMs=-".to_string());
    Some(format!(
        "{name} heartbeat is live at {}: status={status}, {age_detail}, detail={detail}",
        path.display()
    ))
}

fn check_codex_launch_probe(harness_home: &Path, checks: &mut Vec<ActivationReadinessCheck>) {
    let path = harness_home
        .join("state")
        .join("runtime-queue")
        .join("codex-runtime-launch-receipts.jsonl");
    match latest_jsonl_value(&path) {
        Ok(Some(value)) => {
            let status = value
                .get("status")
                .and_then(Value::as_str)
                .unwrap_or("unknown");
            let reason = value
                .get("reason")
                .and_then(Value::as_str)
                .unwrap_or("no reason recorded");
            if status == "started-and-stopped" {
                checks.push(pass(
                    "codex-runtime-launch-probe",
                    format!("latest launch probe passed in {}", path.display()),
                ));
            } else {
                checks.push(fail(
                    "codex-runtime-launch-probe",
                    format!(
                        "latest launch probe status={status} at {}: {reason}",
                        path.display()
                    ),
                ));
            }
        }
        Ok(None) => checks.push(warn(
            "codex-runtime-launch-probe",
            format!("no launch probe receipt lines found at {}", path.display()),
        )),
        Err(error) if error.kind() == io::ErrorKind::NotFound => checks.push(warn(
            "codex-runtime-launch-probe",
            format!(
                "not found yet: {}; run codex-launch-probe before runtime handoff",
                path.display()
            ),
        )),
        Err(error) => checks.push(warn(
            "codex-runtime-launch-probe",
            format!("could not read {}: {error}", path.display()),
        )),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SupervisorReadinessMode {
    ReconcileManaged,
    ScheduledTask,
}

fn configured_loop_enabled(supervisor: &Value, key: &str, all: bool) -> bool {
    supervisor
        .get(key)
        .and_then(|value| value.get("enabled"))
        .and_then(Value::as_bool)
        .unwrap_or(all)
}

fn supervisor_loop_service_kind(service_id: &str) -> &'static str {
    match service_id {
        "runtime-loop" => "runtime",
        "worker-loop" => "worker",
        "cron-scheduler-loop" => "cron",
        "progress-delivery-loop" => "progress-delivery",
        "ledger-maintenance-loop" => "ledger-maintenance",
        "telegram-loop" => "telegram-ingress",
        "discord-outbox-loop" => "final-outbox",
        "discord-gateway-loop" => "discord-gateway",
        service_id if service_id.starts_with("telegram-loop") => "telegram-ingress",
        _ => "loop",
    }
}

fn supervisor_service_id_suffix(value: &str) -> String {
    let mut suffix = value
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || character == '-' || character == '_' {
                character.to_ascii_lowercase()
            } else {
                '-'
            }
        })
        .collect::<String>();
    while suffix.contains("--") {
        suffix = suffix.replace("--", "-");
    }
    suffix.trim_matches('-').to_string()
}

fn reconcile_managed_expected_services(config: &Value) -> io::Result<BTreeMap<String, String>> {
    let supervisor = config.get("supervisor").unwrap_or(&Value::Null);
    let all = supervisor
        .get("manageAllLoops")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let mut expected = BTreeMap::new();

    if let Some(raw_services) = supervisor.get("services") {
        let services = serde_json::from_value::<
            Vec<crate::supervisor_inventory::SupervisorInventoryServiceConfig>,
        >(raw_services.clone())
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
        for service in services {
            if service.enabled {
                expected.insert(service.service_id, service.service_kind);
            }
        }
    }

    let mut insert_default = |service_id: &str| {
        expected.insert(
            service_id.to_string(),
            supervisor_loop_service_kind(service_id).to_string(),
        );
    };
    for (key, service_id) in [
        ("runtimeLoop", "runtime-loop"),
        ("workerLoop", "worker-loop"),
    ] {
        if configured_loop_enabled(supervisor, key, all) {
            insert_default(service_id);
        }
    }
    let cron_enabled = supervisor
        .get("cronSchedulerLoop")
        .and_then(|value| value.get("enabled"))
        .and_then(Value::as_bool)
        .or_else(|| {
            config
                .get("cronScheduler")
                .and_then(|value| value.get("enabled"))
                .and_then(Value::as_bool)
        })
        .unwrap_or(all);
    if cron_enabled {
        insert_default("cron-scheduler-loop");
    }
    for (key, service_id) in [
        ("progressDeliveryLoop", "progress-delivery-loop"),
        ("ledgerMaintenanceLoop", "ledger-maintenance-loop"),
        ("telegramLoop", "telegram-loop"),
    ] {
        if configured_loop_enabled(supervisor, key, all) {
            insert_default(service_id);
        }
    }
    if let Some(telegram_loops) = supervisor.get("telegramLoops").and_then(Value::as_array) {
        for entry in telegram_loops {
            if !entry
                .get("enabled")
                .and_then(Value::as_bool)
                .unwrap_or(true)
            {
                continue;
            }
            let service_id = entry
                .get("serviceId")
                .and_then(Value::as_str)
                .map(ToString::to_string)
                .or_else(|| {
                    entry
                        .get("account")
                        .or_else(|| entry.get("telegramAccount"))
                        .and_then(Value::as_str)
                        .map(supervisor_service_id_suffix)
                        .map(|suffix| format!("telegram-loop-{suffix}"))
                })
                .unwrap_or_else(|| "telegram-loop".to_string());
            insert_default(&service_id);
        }
    }
    for (key, service_id) in [
        ("discordOutboxLoop", "discord-outbox-loop"),
        ("discordGatewayLoop", "discord-gateway-loop"),
    ] {
        if configured_loop_enabled(supervisor, key, all) {
            insert_default(service_id);
        }
    }
    Ok(expected)
}

fn supervisor_readiness_mode(
    harness_home: &Path,
) -> io::Result<(SupervisorReadinessMode, BTreeMap<String, String>)> {
    let Some(config_file) = harness_config_candidates(harness_home)
        .into_iter()
        .find(|path| path.is_file())
    else {
        return Ok((SupervisorReadinessMode::ScheduledTask, BTreeMap::new()));
    };
    let config = read_json_value(&config_file)?;
    let supervisor = config.get("supervisor");
    let manage_all_loops = supervisor
        .and_then(|value| value.get("manageAllLoops"))
        .and_then(Value::as_bool)
        .unwrap_or(false);
    Ok(if manage_all_loops {
        (
            SupervisorReadinessMode::ReconcileManaged,
            reconcile_managed_expected_services(&config)?,
        )
    } else {
        (SupervisorReadinessMode::ScheduledTask, BTreeMap::new())
    })
}

fn check_reconcile_managed_supervisor(
    harness_home: &Path,
    expected_services: &BTreeMap<String, String>,
    checks: &mut Vec<ActivationReadinessCheck>,
) {
    let services_dir = harness_home
        .join("state")
        .join("supervisor")
        .join("services");
    let entries = match fs::read_dir(&services_dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            checks.push(warn(
                "supervisor-plan",
                format!(
                    "reconcile-managed supervisor has no desired-service inventory at {}",
                    services_dir.display()
                ),
            ));
            return;
        }
        Err(error) => {
            checks.push(warn(
                "supervisor-plan",
                format!(
                    "could not inspect reconcile-managed supervisor inventory {}: {error}",
                    services_dir.display()
                ),
            ));
            return;
        }
    };

    let mut services = BTreeMap::new();
    let mut invalid = Vec::new();
    for entry in entries {
        let entry = match entry {
            Ok(entry) => entry,
            Err(error) => {
                invalid.push(format!("directory entry could not be read: {error}"));
                continue;
            }
        };
        let path = entry.path();
        if path.extension().and_then(|extension| extension.to_str()) != Some("json") {
            continue;
        }
        let value = match read_json_value(&path) {
            Ok(value) => value,
            Err(error) => {
                invalid.push(format!("{}: {error}", path.display()));
                continue;
            }
        };
        let service_id = value
            .get("serviceId")
            .and_then(Value::as_str)
            .filter(|service_id| !service_id.is_empty());
        let service_kind = value
            .get("serviceKind")
            .and_then(Value::as_str)
            .filter(|service_kind| !service_kind.is_empty());
        let expected_file_name = service_id.map(|service_id| format!("{service_id}.json"));
        let valid = value.get("schema").and_then(Value::as_str)
            == Some("agent-harness.supervisor-service-state.v1")
            && expected_file_name.as_deref() == path.file_name().and_then(|name| name.to_str())
            && service_kind.is_some()
            && value.get("desiredState").and_then(Value::as_str) == Some("running")
            && value.get("launchOwner").and_then(Value::as_str) == Some("rust-supervisor-run")
            && value.get("observedOnly").and_then(Value::as_bool) == Some(false);
        if !valid {
            invalid.push(format!(
                "{} is not a deployment-owned desired-running supervisor service",
                path.display()
            ));
            continue;
        }
        let service_id = service_id.expect("validated serviceId must be present");
        let service_kind = service_kind.expect("validated serviceKind must be present");
        if services
            .insert(service_id.to_string(), service_kind.to_string())
            .is_some()
        {
            invalid.push(format!("duplicate serviceId `{service_id}`"));
        }
    }

    let actual_service_ids = services.keys().cloned().collect::<BTreeSet<_>>();
    let expected_service_ids = expected_services.keys().cloned().collect::<BTreeSet<_>>();
    let missing = expected_service_ids
        .difference(&actual_service_ids)
        .cloned()
        .collect::<Vec<_>>();
    if !missing.is_empty() {
        invalid.push(format!(
            "missing configured desired service(s): {}",
            missing.join(", ")
        ));
    }
    let unexpected = actual_service_ids
        .difference(&expected_service_ids)
        .cloned()
        .collect::<Vec<_>>();
    if !unexpected.is_empty() {
        invalid.push(format!(
            "unexpected desired service(s): {}",
            unexpected.join(", ")
        ));
    }
    for (service_id, expected_kind) in expected_services {
        if let Some(actual_kind) = services.get(service_id)
            && actual_kind != expected_kind
        {
            invalid.push(format!(
                "service `{service_id}` kind mismatch: expected `{expected_kind}`, found `{actual_kind}`"
            ));
        }
    }

    if expected_services.is_empty() || !invalid.is_empty() {
        let detail = if invalid.is_empty() {
            "configured desired-service inventory is empty".to_string()
        } else {
            invalid.join("; ")
        };
        checks.push(warn(
            "supervisor-plan",
            format!(
                "reconcile-managed supervisor inventory is not ready at {}: {detail}",
                services_dir.display()
            ),
        ));
    } else {
        checks.push(pass(
            "supervisor-plan",
            format!(
                "reconcile-managed supervisor inventory has {} deployment-owned desired service(s) at {}; scheduled-task plan is not applicable",
                expected_services.len(),
                services_dir.display()
            ),
        ));
    }
}

fn check_supervisor_plan(harness_home: &Path, checks: &mut Vec<ActivationReadinessCheck>) {
    let path = harness_home
        .join("state")
        .join("supervisor")
        .join("windows-scheduled-tasks")
        .join("supervisor-plan.json");
    match supervisor_readiness_mode(harness_home) {
        Ok((SupervisorReadinessMode::ReconcileManaged, expected_service_ids)) => {
            check_reconcile_managed_supervisor(harness_home, &expected_service_ids, checks)
        }
        Ok((SupervisorReadinessMode::ScheduledTask, _)) => match read_json_value(&path) {
            Ok(value) => {
                let task_count = value
                    .get("tasks")
                    .and_then(Value::as_array)
                    .map(Vec::len)
                    .unwrap_or(0);
                if task_count > 0 {
                    checks.push(pass(
                        "supervisor-plan",
                        format!(
                            "found {task_count} scheduled task plan(s) at {}",
                            path.display()
                        ),
                    ));
                } else {
                    checks.push(warn(
                        "supervisor-plan",
                        format!("supervisor plan has no tasks at {}", path.display()),
                    ));
                }
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => checks.push(warn(
                "supervisor-plan",
                format!(
                    "not found yet: {}; run supervisor-plan before service handoff",
                    path.display()
                ),
            )),
            Err(error) => checks.push(fail(
                "supervisor-plan",
                format!("could not read supervisor plan {}: {error}", path.display()),
            )),
        },
        Err(error) => checks.push(fail(
            "supervisor-plan",
            format!("could not resolve supervisor readiness mode from harness config: {error}"),
        )),
    }
    check_loop_heartbeats(harness_home, checks);
}

fn check_loop_heartbeats(harness_home: &Path, checks: &mut Vec<ActivationReadinessCheck>) {
    for name in [
        "runtime-loop",
        "progress-delivery-loop",
        "telegram-loop",
        "discord-outbox-loop",
        "discord-gateway-loop",
        "worker-loop",
    ] {
        check_loop_heartbeat(harness_home, checks, name);
    }
}

fn check_worker_dispatch(harness_home: &Path, checks: &mut Vec<ActivationReadinessCheck>) {
    match collect_worker_status(WorkerStatusOptions {
        harness_home: harness_home.to_path_buf(),
    }) {
        Ok(report) => {
            if report.config.global_concurrency_limit < report.config.group_concurrency_limit {
                checks.push(fail(
                    "worker-dispatch",
                    "workerDispatch global concurrency limit is lower than group limit",
                ));
            } else if !report.config.warnings.is_empty() {
                checks.push(warn(
                    "worker-dispatch",
                    format!(
                        "worker dispatch config warning(s): {}",
                        report.config.warnings.join("; ")
                    ),
                ));
            } else if report.totals.failed_terminal > 0 || report.totals.expired > 0 {
                checks.push(warn(
                    "worker-dispatch",
                    format!(
                        "worker store ready at {} but has failedTerminal={} expired={} job(s)",
                        report.database.display(),
                        report.totals.failed_terminal,
                        report.totals.expired
                    ),
                ));
            } else {
                checks.push(pass(
                    "worker-dispatch",
                    format!(
                        "worker store ready at {}; pending={} running={} runtimeQueued={} succeeded={}",
                        report.database.display(),
                        report.totals.pending,
                        report.totals.running,
                        report.totals.runtime_queued,
                        report.totals.succeeded
                    ),
                ));
            }
        }
        Err(error) => checks.push(fail(
            "worker-dispatch",
            format!("worker store is not available: {error}"),
        )),
    }
}

fn check_loop_heartbeat(
    harness_home: &Path,
    checks: &mut Vec<ActivationReadinessCheck>,
    name: &str,
) {
    let path = harness_home
        .join("state")
        .join("supervisor")
        .join("loop-heartbeats")
        .join(format!("{name}.json"));
    let stop_file = match read_supervisor_stop_file(harness_home, name) {
        Ok(stop_file) => stop_file,
        Err(error) => {
            checks.push(warn(
                format!("{name}-heartbeat"),
                format!("could not read {name} stop file state: {error}"),
            ));
            return;
        }
    };
    if stop_file.present {
        let reason = stop_file
            .reason
            .as_deref()
            .filter(|reason| !reason.is_empty())
            .unwrap_or("no reason recorded");
        let detail = format!(
            "{name} is disabled by stop file at {}: {reason}",
            stop_file.path.display()
        );
        if name == "runtime-loop" {
            checks.push(fail(format!("{name}-heartbeat"), detail));
        } else {
            checks.push(warn(format!("{name}-heartbeat"), detail));
        }
        return;
    }
    match read_json_value(&path) {
        Ok(value) => {
            let status = value
                .get("status")
                .and_then(Value::as_str)
                .unwrap_or("unknown");
            let detail = value
                .get("detail")
                .and_then(Value::as_str)
                .unwrap_or("no detail recorded");
            let age_ms = value
                .get("atMs")
                .and_then(Value::as_i64)
                .and_then(|at_ms| current_log_time_ms().ok().map(|now_ms| now_ms - at_ms));
            let age_detail = age_ms
                .map(|age_ms| format!("ageMs={age_ms}"))
                .unwrap_or_else(|| "ageMs=-".to_string());
            let check_name = format!("{name}-heartbeat");
            let process_id = value.get("processId").and_then(Value::as_i64);
            let process_alive = process_id.and_then(process_alive_for_pid);
            if matches!(
                status,
                "running" | "ok" | "no-work" | "ready" | "heartbeat" | "connected"
            ) {
                if age_ms.is_some_and(|age_ms| age_ms > LOOP_HEARTBEAT_STALE_MS) {
                    let detail = format!(
                        "{name} heartbeat is stale at {}: status={status}, {age_detail}, detail={detail}",
                        path.display()
                    );
                    if name == "runtime-loop" {
                        checks.push(fail(check_name, detail));
                    } else {
                        checks.push(warn(check_name, detail));
                    }
                } else if process_alive == Some(false) {
                    let detail = loop_process_dead_detail(
                        name,
                        &path,
                        process_id,
                        status,
                        &age_detail,
                        detail,
                    );
                    if name == "runtime-loop" {
                        checks.push(fail(check_name, detail));
                    } else {
                        checks.push(warn(check_name, detail));
                    }
                } else {
                    checks.push(pass(
                        check_name,
                        format!(
                            "{name} heartbeat is live at {}: status={status}, {age_detail}, detail={detail}",
                            path.display()
                        ),
                    ));
                }
            } else if status == "safe-mode" || status == "lease-busy" {
                let base_detail = if age_ms.is_some_and(|age_ms| age_ms > LOOP_HEARTBEAT_STALE_MS) {
                    format!(
                        "{name} heartbeat is stale in degraded mode at {}: status={status}, {age_detail}, detail={detail}",
                        path.display()
                    )
                } else {
                    format!(
                        "{name} heartbeat is in degraded mode at {}: status={status}, {age_detail}, detail={detail}",
                        path.display()
                    )
                };
                let detail = if process_alive == Some(false) {
                    let process_detail = loop_process_dead_detail(
                        name,
                        &path,
                        process_id,
                        status,
                        &age_detail,
                        detail,
                    );
                    format!("{}; {}", base_detail, process_detail)
                } else {
                    base_detail
                };
                if name == "runtime-loop"
                    && age_ms.is_some_and(|age_ms| age_ms > LOOP_HEARTBEAT_STALE_MS)
                {
                    checks.push(fail(check_name, detail));
                } else {
                    checks.push(warn(check_name, detail));
                }
            } else if status == "stopped"
                || status.contains("error")
                || status.contains("fail")
                || status == "closed"
            {
                checks.push(fail(
                    check_name,
                    format!(
                        "{name} heartbeat is not live at {}: status={status}, {age_detail}, detail={detail}",
                        path.display()
                    ),
                ));
            } else {
                checks.push(warn(
                    check_name,
                    format!(
                        "{name} heartbeat has unrecognized status at {}: status={status}, {age_detail}, detail={detail}",
                        path.display()
                    ),
                ));
            }
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            let check_name = format!("{name}-heartbeat");
            let detail = format!("no live {name} heartbeat found yet at {}", path.display());
            if name == "runtime-loop" {
                checks.push(fail(check_name, detail));
            } else {
                checks.push(warn(check_name, detail));
            }
        }
        Err(error) => checks.push(warn(
            format!("{name}-heartbeat"),
            format!("could not read {}: {error}", path.display()),
        )),
    }
}

fn loop_process_dead_detail(
    name: &str,
    path: &Path,
    process_id: Option<i64>,
    status: &str,
    age_detail: &str,
    detail: &str,
) -> String {
    let process_id = process_id
        .map(|process_id| process_id.to_string())
        .unwrap_or_else(|| "-".to_string());
    format!(
        "{name} heartbeat references processId={process_id} but that process is not running at {}: status={status}, {age_detail}, detail={detail}",
        path.display()
    )
}

fn check_channel_state(harness_home: &Path, checks: &mut Vec<ActivationReadinessCheck>) {
    let channels_dir = harness_home.join("state").join("channels");
    if channels_dir.is_dir() {
        checks.push(pass(
            "channel-state",
            format!(
                "channel state directory exists at {}",
                channels_dir.display()
            ),
        ));
    } else {
        checks.push(warn(
            "channel-state",
            format!(
                "no channel state directory yet at {}",
                channels_dir.display()
            ),
        ));
    }
    let outbox = channels_dir.join("outbox.jsonl");
    if outbox.is_file() {
        checks.push(pass(
            "channel-outbox",
            format!("found {}", outbox.display()),
        ));
    } else {
        checks.push(warn(
            "channel-outbox",
            format!(
                "no outbound channel messages queued at {}",
                outbox.display()
            ),
        ));
    }
    let delivery_receipts = channels_dir.join("delivery-receipts.jsonl");
    if delivery_receipts.is_file() {
        checks.push(pass(
            "channel-delivery",
            format!("found {}", delivery_receipts.display()),
        ));
    } else {
        checks.push(warn(
            "channel-delivery",
            format!(
                "no channel delivery receipts yet at {}",
                delivery_receipts.display()
            ),
        ));
    }
    check_telegram_probe(harness_home, checks);
    let telegram_offset = channels_dir.join("telegram-offset.json");
    if telegram_offset.is_file() {
        checks.push(pass(
            "telegram-offset",
            format!("found {}", telegram_offset.display()),
        ));
    } else {
        checks.push(warn(
            "telegram-offset",
            format!(
                "Telegram update offset not found at {}; run telegram-poll-once or telegram-loop before Telegram handoff",
                telegram_offset.display()
            ),
        ));
    }
    check_discord_gateway_probe(harness_home, checks);
    check_discord_dm_probe(harness_home, checks);
}

fn check_telegram_probe(harness_home: &Path, checks: &mut Vec<ActivationReadinessCheck>) {
    let path = harness_home
        .join("state")
        .join("channels")
        .join("telegram-probe-receipts.jsonl");
    match latest_jsonl_value(&path) {
        Ok(Some(value)) => {
            let status = value
                .get("status")
                .and_then(Value::as_str)
                .unwrap_or("unknown");
            let reason = value
                .get("reason")
                .and_then(Value::as_str)
                .unwrap_or("no reason recorded");
            match status {
                "ready" => checks.push(pass(
                    "telegram-probe",
                    format!("Telegram Bot API getMe probe ready at {}", path.display()),
                )),
                "token-missing" => checks.push(warn(
                    "telegram-probe",
                    format!(
                        "Telegram probe needs TELEGRAM_BOT_TOKEN at {}: {reason}",
                        path.display()
                    ),
                )),
                _ => checks.push(fail(
                    "telegram-probe",
                    format!(
                        "Telegram probe status={status} at {}: {reason}",
                        path.display()
                    ),
                )),
            }
        }
        Ok(None) => checks.push(warn(
            "telegram-probe",
            format!(
                "no Telegram probe receipt lines found at {}",
                path.display()
            ),
        )),
        Err(error) if error.kind() == io::ErrorKind::NotFound => checks.push(warn(
            "telegram-probe",
            format!(
                "not found yet: {}; run telegram-probe before Telegram handoff",
                path.display()
            ),
        )),
        Err(error) => checks.push(warn(
            "telegram-probe",
            format!("could not read {}: {error}", path.display()),
        )),
    }
}

fn check_discord_gateway_probe(harness_home: &Path, checks: &mut Vec<ActivationReadinessCheck>) {
    let path = harness_home
        .join("state")
        .join("channels")
        .join("discord-gateway-probe-receipts.jsonl");
    match latest_jsonl_value(&path) {
        Ok(Some(value)) => {
            let status = value
                .get("status")
                .and_then(Value::as_str)
                .unwrap_or("unknown");
            let reason = value
                .get("reason")
                .and_then(Value::as_str)
                .unwrap_or("no reason recorded");
            match status {
                "ready" => checks.push(pass(
                    "discord-gateway-probe",
                    format!("Discord gateway probe ready at {}", path.display()),
                )),
                "token-missing" => checks.push(warn(
                    "discord-gateway-probe",
                    format!(
                        "Discord gateway probe needs DISCORD_BOT_TOKEN at {}: {reason}",
                        path.display()
                    ),
                )),
                _ => checks.push(fail(
                    "discord-gateway-probe",
                    format!(
                        "Discord gateway probe status={status} at {}: {reason}",
                        path.display()
                    ),
                )),
            }
        }
        Ok(None) => checks.push(warn(
            "discord-gateway-probe",
            format!(
                "no Discord gateway probe receipt lines found at {}",
                path.display()
            ),
        )),
        Err(error) if error.kind() == io::ErrorKind::NotFound => checks.push(warn(
            "discord-gateway-probe",
            format!(
                "not found yet: {}; run discord-gateway-probe before Discord gateway handoff",
                path.display()
            ),
        )),
        Err(error) => checks.push(warn(
            "discord-gateway-probe",
            format!("could not read {}: {error}", path.display()),
        )),
    }
}

fn check_discord_dm_probe(harness_home: &Path, checks: &mut Vec<ActivationReadinessCheck>) {
    let path = harness_home
        .join("state")
        .join("channels")
        .join("discord-dm-probe-receipts.jsonl");
    match latest_jsonl_value(&path) {
        Ok(Some(value)) => {
            let status = value
                .get("status")
                .and_then(Value::as_str)
                .unwrap_or("unknown");
            let reason = value
                .get("reason")
                .and_then(Value::as_str)
                .unwrap_or("no reason recorded");
            let channel_id = value
                .get("channelId")
                .and_then(Value::as_str)
                .unwrap_or("-");
            let provider_message_id = value
                .get("providerMessageId")
                .and_then(Value::as_str)
                .unwrap_or("-");
            match status {
                "ready" => checks.push(pass(
                    "discord-dm-probe",
                    format!(
                        "Discord DM probe ready at {}; channelId={channel_id}, providerMessageId={provider_message_id}",
                        path.display()
                    ),
                )),
                _ => checks.push(fail(
                    "discord-dm-probe",
                    format!(
                        "Discord DM probe status={status} at {}: {reason}",
                        path.display()
                    ),
                )),
            }
        }
        Ok(None) => checks.push(warn(
            "discord-dm-probe",
            format!(
                "no Discord DM probe receipt lines found at {}",
                path.display()
            ),
        )),
        Err(error) if error.kind() == io::ErrorKind::NotFound => checks.push(warn(
            "discord-dm-probe",
            format!(
                "not found yet: {}; run discord-dm-probe --user-id <id> before Discord DM handoff",
                path.display()
            ),
        )),
        Err(error) => checks.push(warn(
            "discord-dm-probe",
            format!("could not read {}: {error}", path.display()),
        )),
    }
}

fn check_activation_plan_doc(checks: &mut Vec<ActivationReadinessCheck>) {
    let plan = PathBuf::from("docs").join("activation-readiness-plan.md");
    if plan.is_file() {
        checks.push(pass(
            "activation-plan",
            format!("activation plan exists at {}", plan.display()),
        ));
    } else {
        checks.push(warn(
            "activation-plan",
            format!("activation plan doc not found at {}", plan.display()),
        ));
    }
}

fn check_harness_skills(harness_home: &Path, checks: &mut Vec<ActivationReadinessCheck>) {
    let manifest = harness_home
        .join("skills")
        .join(".agent-harness-builtins.json");
    if manifest.is_file() {
        checks.push(pass(
            "harness-skills",
            format!(
                "builtin harness skill manifest found at {}",
                manifest.display()
            ),
        ));
    } else {
        checks.push(warn(
            "harness-skills",
            format!(
                "builtin harness skill manifest not found at {}; run harness-skills-sync",
                manifest.display()
            ),
        ));
    }
}

fn check_logging(harness_home: &Path, checks: &mut Vec<ActivationReadinessCheck>) {
    match probe_harness_log_writable(harness_home) {
        Ok(log_file) => checks.push(pass(
            "operational-log",
            format!(
                "harness operational log is writable at {}",
                log_file.display()
            ),
        )),
        Err(error) => checks.push(fail(
            "operational-log",
            format!("harness operational log is not writable: {error}"),
        )),
    }
    check_log_event(
        harness_home,
        checks,
        "telegram-poll-log",
        "telegram.poll-once",
        "no Telegram poll summary found yet; run telegram-poll-once or telegram-loop",
    );
    check_log_event(
        harness_home,
        checks,
        "discord-send-log",
        "discord.outbox-send-once",
        "no Discord outbound summary found yet; run discord-outbox-send-once",
    );
    check_log_event(
        harness_home,
        checks,
        "discord-event-log",
        "discord.event-run-once",
        "no Discord inbound event summary found yet; run discord-event-run-once",
    );
    check_discord_dm_poll_fallback(harness_home, checks);
    check_discord_real_inbound(harness_home, checks);
}

fn check_log_event(
    harness_home: &Path,
    checks: &mut Vec<ActivationReadinessCheck>,
    name: &str,
    event: &str,
    missing_detail: &str,
) {
    let log_file = harness_home
        .join("state")
        .join("logs")
        .join("harness.jsonl");
    match read_tail_text(&log_file, ACTIVATION_JSONL_SAMPLE_BYTES) {
        Ok(Some(text)) if text.contains(&format!(r#""event":"{event}""#)) => checks.push(pass(
            name,
            format!("found event {event} in {}", log_file.display()),
        )),
        Ok(Some(_)) | Ok(None) => checks.push(warn(name, missing_detail)),
        Err(error) => checks.push(warn(
            name,
            format!("could not read {}: {error}", log_file.display()),
        )),
    }
}

fn check_discord_real_inbound(harness_home: &Path, checks: &mut Vec<ActivationReadinessCheck>) {
    let path = harness_home
        .join("state")
        .join("channels")
        .join("discord-gateway-events.jsonl");
    match read_tail_text(&path, ACTIVATION_JSONL_SAMPLE_BYTES) {
        Ok(Some(text)) if text.contains(r#""event":"message-create""#) => checks.push(pass(
            "discord-real-inbound",
            format!(
                "Discord Gateway has received a real MESSAGE_CREATE event at {}",
                path.display()
            ),
        )),
        Ok(Some(_)) | Ok(None) => checks.push(warn(
            "discord-real-inbound",
            format!(
                "Discord Gateway is connected but no real MESSAGE_CREATE event is recorded at {}; ask an allowed Discord user to DM the bot",
                path.display()
            ),
        )),
        Err(error) => checks.push(warn(
            "discord-real-inbound",
            format!("could not read {}: {error}", path.display()),
        )),
    }
}

fn check_discord_dm_poll_fallback(harness_home: &Path, checks: &mut Vec<ActivationReadinessCheck>) {
    let path = harness_home
        .join("state")
        .join("channels")
        .join("discord-dm-poll-cursors.json");
    match fs::read_to_string(&path) {
        Ok(text) => {
            let initialized_targets = serde_json::from_str::<serde_json::Value>(&text)
                .ok()
                .and_then(|value| {
                    value.as_object().map(|targets| {
                        targets
                            .values()
                            .filter(|target| {
                                target
                                    .get("initialized")
                                    .and_then(serde_json::Value::as_bool)
                                    .unwrap_or(false)
                            })
                            .count()
                    })
                })
                .unwrap_or(0);
            if initialized_targets > 0 {
                checks.push(pass(
                    "discord-dm-poll-fallback",
                    format!(
                        "Discord DM HTTP poll fallback initialized for {initialized_targets} target(s) at {}",
                        path.display()
                    ),
                ));
            } else {
                checks.push(warn(
                    "discord-dm-poll-fallback",
                    format!(
                        "Discord DM HTTP poll fallback cursor file exists but has no initialized targets at {}",
                        path.display()
                    ),
                ));
            }
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => checks.push(warn(
            "discord-dm-poll-fallback",
            format!(
                "Discord DM HTTP poll fallback has not initialized at {}; start discord-gateway-loop",
                path.display()
            ),
        )),
        Err(error) => checks.push(warn(
            "discord-dm-poll-fallback",
            format!("could not read {}: {error}", path.display()),
        )),
    }
}

fn check_memory_import(
    harness_home: &Path,
    registry: Option<&Value>,
    checks: &mut Vec<ActivationReadinessCheck>,
) {
    let memory_dir = harness_home.join("memory");
    if memory_dir.is_dir() {
        checks.push(pass(
            "memory-files",
            format!("memory files are imported at {}", memory_dir.display()),
        ));
    } else {
        checks.push(warn(
            "memory-files",
            "no imported memory directory detected; memory recall will be unavailable",
        ));
        return;
    }
    check_memory_search_probe(harness_home, checks);
    check_memory_credentials(harness_home, checks);
    check_memory_vector_recall_probe(harness_home, checks);
    check_memory_prompt_context_probe(harness_home, checks);
    check_memory_lifecycle_probe(harness_home, checks);
    check_memory_canvas_probe(harness_home, checks);
    check_memory_hook_adapter_probe(harness_home, checks);

    let qdrant_edge = memory_dir.join("qdrant-edge");
    if qdrant_edge.is_dir() {
        checks.push(pass(
            "memory-qdrant-edge",
            format!(
                "Qdrant edge memory snapshot found at {}; native Qdrant recall parity is tracked separately from active recall backend status",
                qdrant_edge.display()
            ),
        ));
    } else {
        checks.push(warn(
            "memory-qdrant-edge",
            format!(
                "Qdrant edge memory snapshot not found at {}; memory recall may fall back to SQLite/LanceDB or be unavailable",
                qdrant_edge.display()
            ),
        ));
    }

    let sqlite = memory_dir.join("openclaw-mem.sqlite");
    if sqlite.is_file() {
        checks.push(pass(
            "memory-legacy-mem-sqlite",
            format!(
                "legacy memory SQLite snapshot found at {}",
                sqlite.display()
            ),
        ));
    } else {
        checks.push(warn(
            "memory-legacy-mem-sqlite",
            format!(
                "legacy memory SQLite snapshot not found at {}",
                sqlite.display()
            ),
        ));
    }

    if let Some(reason) = source_config_selects_lancedb(harness_home, registry) {
        let lancedb = memory_dir.join("lancedb");
        if lancedb.is_dir() {
            checks.push(pass(
                "memory-lancedb",
                format!(
                    "LanceDB memory backend is explicitly selected ({reason}) and found at {}",
                    lancedb.display()
                ),
            ));
        } else {
            checks.push(warn(
                "memory-lancedb",
                format!(
                    "LanceDB memory backend is explicitly selected ({reason}) but not found at {}",
                    lancedb.display()
                ),
            ));
        }
    }
}

fn source_config_selects_lancedb(harness_home: &Path, registry: Option<&Value>) -> Option<String> {
    let registry = registry?;
    if registry
        .get("plugins")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .any(|plugin| {
            plugin
                .get("id")
                .and_then(Value::as_str)
                .is_some_and(|id| id.eq_ignore_ascii_case("memory-lancedb"))
                && plugin
                    .get("enabled")
                    .and_then(Value::as_bool)
                    .unwrap_or(false)
        })
    {
        return Some("harness registry plugin memory-lancedb is enabled".to_string());
    }

    let config = source_config_from_registry(harness_home, registry)?;
    if string_at(&config, "/plugins/slots/memory")
        .is_some_and(|value| value.eq_ignore_ascii_case("memory-lancedb"))
    {
        return Some("source plugins.slots.memory=memory-lancedb".to_string());
    }
    if config
        .pointer("/plugins/entries/memory-lancedb/enabled")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        return Some("source plugins.entries.memory-lancedb.enabled=true".to_string());
    }

    for pointer in [
        "/plugins/entries/openclaw-mem-engine/config/retrievalBackend/backend",
        "/plugins/entries/openclaw-mem-engine/config/backend",
        "/plugins/entries/openclaw-mem-engine/config/backendMode",
        "/memory/retrievalBackend/backend",
        "/memory/backend",
        "/agents/defaults/memorySearch/backend",
    ] {
        if string_at(&config, pointer).is_some_and(|value| value.eq_ignore_ascii_case("lancedb")) {
            return Some(format!("source {pointer}=lancedb"));
        }
    }

    None
}

fn source_config_from_registry(harness_home: &Path, registry: &Value) -> Option<Value> {
    let source_home = registry.get("sourceHome").and_then(Value::as_str)?;
    for source_home in source_home_candidates(harness_home, source_home) {
        let config = source_home.join("openclaw.json");
        if let Ok(value) = read_json_value(&config) {
            return Some(value);
        }
    }
    None
}

fn source_home_candidates(harness_home: &Path, source_home: &str) -> Vec<PathBuf> {
    let raw = PathBuf::from(source_home);
    if raw.is_absolute() {
        return vec![raw];
    }

    let mut candidates = Vec::new();
    candidates.push(raw.clone());
    if let Ok(cwd) = env::current_dir() {
        candidates.push(cwd.join(&raw));
    }
    if let Some(parent) = harness_home.parent() {
        candidates.push(parent.join(&raw));
        if let Some(grandparent) = parent.parent() {
            candidates.push(grandparent.join(&raw));
        }
    }
    candidates
}

fn string_at<'a>(value: &'a Value, pointer: &str) -> Option<&'a str> {
    value.pointer(pointer).and_then(Value::as_str)
}

fn check_memory_search_probe(harness_home: &Path, checks: &mut Vec<ActivationReadinessCheck>) {
    let path = harness_home
        .join("state")
        .join("memory")
        .join("search-receipts.jsonl");
    match latest_jsonl_value(&path) {
        Ok(Some(value)) => {
            let status = value
                .get("status")
                .and_then(Value::as_str)
                .unwrap_or("unknown");
            let reason = value
                .get("reason")
                .and_then(Value::as_str)
                .unwrap_or("no reason recorded");
            let hit_count = value.get("hitCount").and_then(Value::as_u64).unwrap_or(0);
            let searched_files = value
                .get("searchedFiles")
                .and_then(Value::as_u64)
                .unwrap_or(0);
            match status {
                "ready" if hit_count > 0 => checks.push(pass(
                    "memory-search",
                    format!(
                        "read-only imported memory search probe returned {hit_count} hit(s) across {searched_files} searched file(s) at {}",
                        path.display()
                    ),
                )),
                "ready" => checks.push(warn(
                    "memory-search",
                    format!(
                        "read-only imported memory search ran but returned no hits across {searched_files} searched file(s) at {}",
                        path.display()
                    ),
                )),
                _ => checks.push(fail(
                    "memory-search",
                    format!("memory search probe status={status} at {}: {reason}", path.display()),
                )),
            }
        }
        Ok(None) => checks.push(warn(
            "memory-search",
            format!(
                "no memory search probe receipt lines found at {}",
                path.display()
            ),
        )),
        Err(error) if error.kind() == io::ErrorKind::NotFound => checks.push(warn(
            "memory-search",
            format!(
                "not found yet: {}; run memory-search --query <text> before claiming memory recall",
                path.display()
            ),
        )),
        Err(error) => checks.push(warn(
            "memory-search",
            format!("could not read {}: {error}", path.display()),
        )),
    }
}

fn check_memory_credentials(harness_home: &Path, checks: &mut Vec<ActivationReadinessCheck>) {
    const MEMORY_EMBEDDING_API_KEY_ENV: &str = "AGENT_HARNESS_MEMORY_EMBEDDING_API_KEY";
    let env_file = harness_home.join("secrets").join("memory-credentials.env");
    match fs::read_to_string(&env_file) {
        Ok(text) if env_file_has_nonempty_key(&text, MEMORY_EMBEDDING_API_KEY_ENV) => {
            checks.push(pass(
                "memory-embedding-secrets",
                format!(
                    "embedding key is available in harness memory secrets at {}; value is not disclosed",
                    env_file.display()
                ),
            ));
        }
        Ok(_) => checks.push(warn(
            "memory-embedding-secrets",
            format!(
                "memory credentials env exists but does not include a non-empty {MEMORY_EMBEDDING_API_KEY_ENV} at {}",
                env_file.display()
            ),
        )),
        Err(error) if error.kind() == io::ErrorKind::NotFound => checks.push(warn(
            "memory-embedding-secrets",
            format!(
                "embedding key may exist in imported snapshot but has not been moved into harness secrets at {}; run memory-credentials-export --include-sensitive",
                env_file.display()
            ),
        )),
        Err(error) => checks.push(warn(
            "memory-embedding-secrets",
            format!("could not read {}: {error}", env_file.display()),
        )),
    }

    let receipt = harness_home
        .join("secrets")
        .join("memory-credentials-receipt.json");
    if receipt.is_file() {
        checks.push(pass(
            "memory-credentials-receipt",
            format!(
                "memory credential migration receipt exists at {}; sensitive values are redacted",
                receipt.display()
            ),
        ));
    } else {
        checks.push(warn(
            "memory-credentials-receipt",
            format!(
                "not found yet: {}; run memory-credentials-export before memory handoff",
                receipt.display()
            ),
        ));
    }
}

fn check_memory_vector_recall_probe(
    harness_home: &Path,
    checks: &mut Vec<ActivationReadinessCheck>,
) {
    let path = harness_home
        .join("state")
        .join("memory")
        .join("vector-recall-receipts.jsonl");
    match latest_jsonl_value(&path) {
        Ok(Some(value)) => {
            let status = value
                .get("status")
                .and_then(Value::as_str)
                .unwrap_or("unknown");
            let reason = value
                .get("reason")
                .and_then(Value::as_str)
                .unwrap_or("no reason recorded");
            let hit_count = value.get("hitCount").and_then(Value::as_u64).unwrap_or(0);
            let backend = value
                .get("backend")
                .and_then(Value::as_str)
                .unwrap_or("unknown");
            match status {
                "ready" if hit_count > 0 => checks.push(pass(
                    "memory-vector-recall",
                    format!(
                        "imported vector memory recall returned {hit_count} hit(s) via {backend} at {}",
                        path.display()
                    ),
                )),
                "no-hits" | "ready" => checks.push(warn(
                    "memory-vector-recall",
                    format!(
                        "vector memory recall ran via {backend} but returned no hits at {}: {reason}",
                        path.display()
                    ),
                )),
                "skipped" => checks.push(warn(
                    "memory-vector-recall",
                    format!(
                        "vector memory recall skipped at {}: {reason}",
                        path.display()
                    ),
                )),
                _ => checks.push(fail(
                    "memory-vector-recall",
                    format!(
                        "vector memory recall status={status} at {}: {reason}",
                        path.display()
                    ),
                )),
            }
        }
        Ok(None) => checks.push(warn(
            "memory-vector-recall",
            format!(
                "no vector recall receipt lines found at {}; run memory-vector-search --query <text>",
                path.display()
            ),
        )),
        Err(error) if error.kind() == io::ErrorKind::NotFound => checks.push(warn(
            "memory-vector-recall",
            format!(
                "not found yet: {}; run memory-vector-search --query <text> before claiming vector memory recall",
                path.display()
            ),
        )),
        Err(error) => checks.push(warn(
            "memory-vector-recall",
            format!("could not read {}: {error}", path.display()),
        )),
    }
}

fn check_memory_prompt_context_probe(
    harness_home: &Path,
    checks: &mut Vec<ActivationReadinessCheck>,
) {
    let path = harness_home
        .join("state")
        .join("memory")
        .join("prompt-context-receipts.jsonl");
    match latest_jsonl_value(&path) {
        Ok(Some(value)) => {
            let status = value
                .get("status")
                .and_then(Value::as_str)
                .unwrap_or("unknown");
            let reason = value
                .get("reason")
                .and_then(Value::as_str)
                .unwrap_or("no reason recorded");
            let hit_count = value.get("hitCount").and_then(Value::as_u64).unwrap_or(0);
            match status {
                "ready" if hit_count > 0 => checks.push(pass(
                    "memory-prompt-context",
                    format!(
                        "pre-turn imported memory context prepared {hit_count} hit(s) at {}",
                        path.display()
                    ),
                )),
                "no-hits" | "ready" => checks.push(warn(
                    "memory-prompt-context",
                    format!(
                        "pre-turn memory context ran but no hit was injected at {}: {reason}",
                        path.display()
                    ),
                )),
                _ => checks.push(warn(
                    "memory-prompt-context",
                    format!(
                        "pre-turn memory context status={status} at {}: {reason}",
                        path.display()
                    ),
                )),
            }
        }
        Ok(None) => checks.push(warn(
            "memory-prompt-context",
            format!(
                "no memory prompt-context receipt lines found at {}",
                path.display()
            ),
        )),
        Err(error) if error.kind() == io::ErrorKind::NotFound => checks.push(warn(
            "memory-prompt-context",
            format!(
                "not found yet: {}; run an agent turn before claiming pre-turn memory recall",
                path.display()
            ),
        )),
        Err(error) => checks.push(warn(
            "memory-prompt-context",
            format!("could not read {}: {error}", path.display()),
        )),
    }
}

fn check_memory_lifecycle_probe(harness_home: &Path, checks: &mut Vec<ActivationReadinessCheck>) {
    let path = harness_home
        .join("state")
        .join("memory")
        .join("lifecycle-receipts.jsonl");
    match latest_jsonl_value(&path) {
        Ok(Some(value)) => {
            let status = value
                .get("status")
                .and_then(Value::as_str)
                .unwrap_or("unknown");
            let reason = value
                .get("reason")
                .and_then(Value::as_str)
                .unwrap_or("no reason recorded");
            let episodes = value
                .get("episodesAppended")
                .and_then(Value::as_u64)
                .unwrap_or(0);
            let candidates = value
                .get("captureCandidates")
                .and_then(Value::as_u64)
                .unwrap_or(0);
            match status {
                "recorded" => checks.push(pass(
                    "memory-lifecycle",
                    format!(
                        "post-turn memory lifecycle recorded episodes={episodes} captureCandidates={candidates} at {}",
                        path.display()
                    ),
                )),
                "skipped" => checks.push(warn(
                    "memory-lifecycle",
                    format!(
                        "post-turn memory lifecycle skipped at {}: {reason}",
                        path.display()
                    ),
                )),
                _ => checks.push(warn(
                    "memory-lifecycle",
                    format!(
                        "post-turn memory lifecycle status={status} at {}: {reason}",
                        path.display()
                    ),
                )),
            }
        }
        Ok(None) => checks.push(warn(
            "memory-lifecycle",
            format!(
                "no memory lifecycle receipt lines found at {}",
                path.display()
            ),
        )),
        Err(error) if error.kind() == io::ErrorKind::NotFound => checks.push(warn(
            "memory-lifecycle",
            format!(
                "not found yet: {}; run a completed agent turn before claiming post-turn capture",
                path.display()
            ),
        )),
        Err(error) => checks.push(warn(
            "memory-lifecycle",
            format!("could not read {}: {error}", path.display()),
        )),
    }
}

fn check_memory_canvas_probe(harness_home: &Path, checks: &mut Vec<ActivationReadinessCheck>) {
    let path = harness_home
        .join("state")
        .join("memory")
        .join("canvas-receipts.jsonl");
    match latest_jsonl_value(&path) {
        Ok(Some(value)) => {
            let status = value
                .get("status")
                .and_then(Value::as_str)
                .unwrap_or("unknown");
            let reason = value
                .get("reason")
                .and_then(Value::as_str)
                .unwrap_or("no reason recorded");
            let candidates = value
                .get("candidatesRead")
                .and_then(Value::as_u64)
                .unwrap_or(0);
            let episodes = value
                .get("episodesRead")
                .and_then(Value::as_u64)
                .unwrap_or(0);
            match status {
                "written" => checks.push(pass(
                    "memory-canvas",
                    format!(
                        "symbolic canvas worker wrote candidates={candidates} episodes={episodes} at {}",
                        path.display()
                    ),
                )),
                "skipped" => checks.push(warn(
                    "memory-canvas",
                    format!(
                        "symbolic canvas worker skipped at {}: {reason}",
                        path.display()
                    ),
                )),
                _ => checks.push(warn(
                    "memory-canvas",
                    format!(
                        "symbolic canvas worker status={status} at {}: {reason}",
                        path.display()
                    ),
                )),
            }
        }
        Ok(None) => checks.push(warn(
            "memory-canvas",
            format!("no canvas receipt lines found at {}", path.display()),
        )),
        Err(error) if error.kind() == io::ErrorKind::NotFound => checks.push(warn(
            "memory-canvas",
            format!(
                "not found yet: {}; run memory-canvas-run or a successful turn with symbolic canvas enabled",
                path.display()
            ),
        )),
        Err(error) => checks.push(warn(
            "memory-canvas",
            format!("could not read {}: {error}", path.display()),
        )),
    }
}

fn check_memory_hook_adapter_probe(
    harness_home: &Path,
    checks: &mut Vec<ActivationReadinessCheck>,
) {
    let path = harness_home
        .join("state")
        .join("memory")
        .join("hook-receipts.jsonl");
    match latest_jsonl_value(&path) {
        Ok(Some(value)) => {
            let status = value
                .get("status")
                .and_then(Value::as_str)
                .unwrap_or("unknown");
            let hook = value
                .get("hook")
                .and_then(Value::as_str)
                .unwrap_or("unknown");
            let reason = value
                .get("reason")
                .and_then(Value::as_str)
                .unwrap_or("no reason recorded");
            match status {
                "recorded" => checks.push(pass(
                    "memory-hook-adapter",
                    format!(
                        "OpenClaw-compatible memory hook `{hook}` recorded at {}",
                        path.display()
                    ),
                )),
                "skipped" => checks.push(warn(
                    "memory-hook-adapter",
                    format!(
                        "memory hook `{hook}` skipped at {}: {reason}",
                        path.display()
                    ),
                )),
                _ => checks.push(warn(
                    "memory-hook-adapter",
                    format!(
                        "memory hook `{hook}` status={status} at {}: {reason}",
                        path.display()
                    ),
                )),
            }
        }
        Ok(None) => checks.push(warn(
            "memory-hook-adapter",
            format!(
                "no memory hook adapter receipt lines found at {}",
                path.display()
            ),
        )),
        Err(error) if error.kind() == io::ErrorKind::NotFound => checks.push(warn(
            "memory-hook-adapter",
            format!(
                "not found yet: {}; run memory-hook --hook before-prompt-build or agent-end",
                path.display()
            ),
        )),
        Err(error) => checks.push(warn(
            "memory-hook-adapter",
            format!("could not read {}: {error}", path.display()),
        )),
    }
}

fn env_file_has_nonempty_key(text: &str, key: &str) -> bool {
    text.lines().any(|line| {
        let trimmed = line.trim();
        if trimmed.starts_with('#') {
            return false;
        }
        let Some((name, value)) = trimmed.split_once('=') else {
            return false;
        };
        name.trim() == key && !value.trim().is_empty()
    })
}

fn check_codex_auth(checks: &mut Vec<ActivationReadinessCheck>) {
    if env::var_os("OPENAI_API_KEY").is_some() {
        checks.push(pass("codex-auth", "OPENAI_API_KEY is present"));
        return;
    }
    if codex_auth_candidates().iter().any(|path| path.is_file()) {
        checks.push(pass("codex-auth", "Codex OAuth auth state file is present"));
    } else {
        checks.push(fail(
            "codex-auth",
            "neither OPENAI_API_KEY nor Codex OAuth auth state was found",
        ));
    }
}

fn check_codex_config(harness_home: &Path, checks: &mut Vec<ActivationReadinessCheck>) {
    let codex_home = harness_home.join("codex-home");
    let config = codex_home.join("config.toml");
    if config.is_file() {
        if let Ok(config_text) = fs::read_to_string(&config) {
            if config_text.contains("model_provider = \"openrouter\"")
                || config_text.contains("[model_providers.openrouter]")
            {
                checks.push(fail(
                    "codex-config",
                    format!(
                        "shared Codex config at {} contains OpenRouter provider config; OpenRouter routes must use codex-home-providers/openrouter",
                        config.display()
                    ),
                ));
                return;
            }
        }
        checks.push(pass(
            "codex-config",
            format!(
                "default harness-local Codex config is present at {}",
                config.display()
            ),
        ));
        return;
    }

    let has_harness_auth = [codex_home.join("auth.json"), codex_home.join("auth.toml")]
        .iter()
        .any(|path| path.is_file());
    if has_harness_auth {
        checks.push(warn(
            "codex-config",
            format!(
                "default harness-local Codex auth exists but config is missing at {}; codex-plan will create a minimal runtime config before app-server launch",
                config.display()
            ),
        ));
    } else {
        checks.push(warn(
            "codex-config",
            format!(
                "default harness-local Codex config is not present at {}; using global Codex config/auth if available",
                config.display()
            ),
        ));
    }
}

fn check_codex_approval_policy(harness_home: &Path, checks: &mut Vec<ActivationReadinessCheck>) {
    let inspection = inspect_codex_approval_policy(harness_home);
    if !inspection.warnings.is_empty() {
        checks.push(warn(
            "codex-approval-policy",
            format!(
                "{}; policy={} source={}",
                inspection.warnings.join("; "),
                inspection.policy.as_str(),
                inspection.source
            ),
        ));
        return;
    }

    match inspection.policy {
        CodexApprovalPolicy::Accept => checks.push(pass(
            "codex-approval-policy",
            format!(
                "Codex approval requests are auto-accepted for unattended channel runtime; source={}",
                inspection.source
            ),
        )),
        CodexApprovalPolicy::Deny => checks.push(warn(
            "codex-approval-policy",
            format!(
                "Codex approval requests are cancelled; agent can chat but tool execution will be blocked until {}=accept or {} sets security.codexApprovalPolicy=\"accept\"; source={}",
                super::codex_runtime::CODEX_APPROVAL_POLICY_ENV,
                inspection.config_file.display(),
                inspection.source
            ),
        )),
    }
}

fn check_codex_sandbox(harness_home: &Path, checks: &mut Vec<ActivationReadinessCheck>) {
    let inspection = inspect_codex_sandbox(harness_home);
    if !inspection.warnings.is_empty() {
        checks.push(warn(
            "codex-sandbox",
            format!(
                "{}; sandbox={} source={}",
                inspection.warnings.join("; "),
                inspection.sandbox,
                inspection.source
            ),
        ));
        return;
    }

    checks.push(pass(
        "codex-sandbox",
        format!(
            "Codex Windows sandbox is {}; source={}",
            inspection.sandbox, inspection.source
        ),
    ));
}

fn check_codex_filesystem_sandbox(harness_home: &Path, checks: &mut Vec<ActivationReadinessCheck>) {
    let inspection = inspect_codex_sandbox_policy(harness_home);
    if !inspection.warnings.is_empty() {
        checks.push(warn(
            "codex-filesystem-sandbox",
            format!(
                "{}; sandboxPolicy={} source={}",
                inspection.warnings.join("; "),
                inspection.sandbox,
                inspection.source
            ),
        ));
        return;
    }

    checks.push(pass(
        "codex-filesystem-sandbox",
        format!(
            "Codex app-server filesystem sandbox policy is {}; source={}",
            inspection.sandbox, inspection.source
        ),
    ));
}

fn check_env_token(
    checks: &mut Vec<ActivationReadinessCheck>,
    name: &str,
    env_name: &str,
    reason: &str,
) {
    if env::var_os(env_name).is_some() {
        checks.push(pass(name, format!("{env_name} is present")));
    } else {
        checks.push(fail(name, format!("{env_name} is missing: {reason}")));
    }
}

fn check_channel_token(
    checks: &mut Vec<ActivationReadinessCheck>,
    harness_home: &Path,
    name: &str,
    env_name: &str,
    reason: &str,
) {
    if env::var_os(env_name).is_some() {
        checks.push(pass(name, format!("{env_name} is present")));
    } else if harness_secret_env_has(harness_home, env_name) {
        checks.push(pass(
            name,
            format!(
                "{env_name} is present in {}",
                harness_home
                    .join("secrets")
                    .join("channel-credentials.env")
                    .display()
            ),
        ));
    } else {
        checks.push(fail(
            name,
            format!("{env_name} is missing from env and harness secrets: {reason}"),
        ));
    }
}

fn check_channel_access_policy(
    checks: &mut Vec<ActivationReadinessCheck>,
    harness_home: &Path,
    name: &str,
    platform: &str,
    env_names: &[&str],
) {
    let present = env_names
        .iter()
        .filter(|env_name| channel_config_has(harness_home, env_name))
        .count();
    if present == 0 {
        checks.push(warn(
            name,
            format!(
                "{platform} access allow-list is not configured; live adapters will accept any inbound chat/user permitted by the platform token"
            ),
        ));
    } else {
        checks.push(pass(
            name,
            format!("{platform} imported access allow-list has {present} configured id list(s)"),
        ));
    }
}

fn channel_config_has(harness_home: &Path, env_name: &str) -> bool {
    env::var_os(env_name).is_some_and(|value| !value.is_empty())
        || harness_secret_env_has(harness_home, env_name)
}

fn harness_secret_env_has(harness_home: &Path, env_name: &str) -> bool {
    let path = harness_home.join("secrets").join("channel-credentials.env");
    let Ok(text) = fs::read_to_string(path) else {
        return false;
    };
    text.lines().any(|line| {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            return false;
        }
        let Some((key, value)) = line.split_once('=') else {
            return false;
        };
        key.trim() == env_name && !value.trim().is_empty()
    })
}

fn codex_auth_candidates() -> Vec<PathBuf> {
    let mut roots = Vec::new();
    if let Some(home) = env::var_os("CODEX_HOME") {
        roots.push(PathBuf::from(home));
    }
    if let Some(profile) = env::var_os("USERPROFILE") {
        roots.push(PathBuf::from(profile).join(".codex"));
    }
    if let Some(home) = env::var_os("HOME") {
        roots.push(PathBuf::from(home).join(".codex"));
    }
    roots.dedup();
    roots
        .into_iter()
        .flat_map(|root| [root.join("auth.json"), root.join("auth.toml")])
        .collect()
}

fn latest_jsonl_value(path: &Path) -> io::Result<Option<Value>> {
    let Some(text) = read_tail_text(path, ACTIVATION_JSONL_SAMPLE_BYTES)? else {
        return Ok(None);
    };
    for line in text.lines().rev() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        return serde_json::from_str(line)
            .map(Some)
            .map_err(io::Error::other);
    }
    Ok(None)
}

fn read_tail_text(path: &Path, max_bytes: u64) -> io::Result<Option<String>> {
    let mut file = match fs::File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    let bytes = file.metadata()?.len();
    if bytes <= max_bytes {
        let mut text = String::new();
        file.read_to_string(&mut text)?;
        return Ok(Some(text));
    }
    file.seek(SeekFrom::Start(bytes.saturating_sub(max_bytes)))?;
    let mut buffer = Vec::new();
    file.read_to_end(&mut buffer)?;
    let mut text = String::from_utf8_lossy(&buffer).into_owned();
    if let Some(newline) = text.find('\n') {
        text = text[newline + 1..].to_string();
    }
    Ok(Some(text))
}

fn read_json_value(path: &Path) -> io::Result<Value> {
    let text = fs::read_to_string(path)?;
    serde_json::from_str(&text).map_err(io::Error::other)
}

fn summarize(checks: &[ActivationReadinessCheck]) -> ActivationReadinessSummary {
    let mut summary = ActivationReadinessSummary::default();
    for check in checks {
        match check.status {
            ActivationReadinessStatus::Pass => summary.passed += 1,
            ActivationReadinessStatus::Warn => summary.warnings += 1,
            ActivationReadinessStatus::Fail => summary.failed += 1,
        }
    }
    summary
}

fn pass(name: impl Into<String>, detail: impl Into<String>) -> ActivationReadinessCheck {
    ActivationReadinessCheck {
        name: name.into(),
        status: ActivationReadinessStatus::Pass,
        detail: detail.into(),
    }
}

fn warn(name: impl Into<String>, detail: impl Into<String>) -> ActivationReadinessCheck {
    ActivationReadinessCheck {
        name: name.into(),
        status: ActivationReadinessStatus::Warn,
        detail: detail.into(),
    }
}

fn fail(name: impl Into<String>, detail: impl Into<String>) -> ActivationReadinessCheck {
    ActivationReadinessCheck {
        name: name.into(),
        status: ActivationReadinessStatus::Fail,
        detail: detail.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn readiness_fails_without_registry() {
        let root = temp_root("readiness_fails_without_registry");
        let report = check_activation_readiness(ActivationReadinessOptions {
            harness_home: root.join(".agent-harness"),
        })
        .unwrap();

        assert!(!report.ready);
        assert!(report.summary.failed >= 1);
        assert!(report.checks.iter().any(|check| {
            check.name == "harness-registry" && check.status == ActivationReadinessStatus::Fail
        }));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn readiness_fails_shared_codex_home_openrouter_override() {
        let root = temp_root("readiness_fails_shared_codex_home_openrouter_override");
        let harness_home = root.join(".agent-harness");
        let codex_home = harness_home.join("codex-home");
        fs::create_dir_all(&codex_home).unwrap();
        fs::write(
            codex_home.join("config.toml"),
            "# Generated by agent-harness. Contains no secrets.\n\
             model_provider = \"openrouter\"\n\
             [model_providers.openrouter]\n\
             name = \"OpenRouter\"\n",
        )
        .unwrap();

        let report =
            check_activation_readiness(ActivationReadinessOptions { harness_home }).unwrap();

        assert!(report.checks.iter().any(|check| {
            check.name == "codex-config"
                && check.status == ActivationReadinessStatus::Fail
                && check.detail.contains("codex-home-providers/openrouter")
        }));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn readiness_reports_registry_channels_and_plugin_blockers() {
        let root = temp_root("readiness_reports_registry_channels_and_plugin_blockers");
        let harness_home = root.join(".agent-harness");
        let state = harness_home.join("state");
        fs::create_dir_all(&state).unwrap();
        fs::write(
            state.join("harness-registry.json"),
            r#"{
              "schema": "agent-harness.target-registry.v1",
              "agents": [
                { "id": "main", "enabled": true }
              ],
              "providers": [
                { "id": "openrouter", "credentialStatus": "not-detected" }
              ],
              "plugins": [
                { "id": "memory", "sidecarRequired": true }
              ],
              "channels": { "telegram": true, "discord": false }
            }"#,
        )
        .unwrap();

        let report =
            check_activation_readiness(ActivationReadinessOptions { harness_home }).unwrap();

        assert!(report.checks.iter().any(|check| {
            check.name == "agents" && check.status == ActivationReadinessStatus::Pass
        }));
        assert!(report.checks.iter().any(|check| {
            check.name == "channels" && check.status == ActivationReadinessStatus::Pass
        }));
        assert!(report.checks.iter().any(|check| {
            check.name == "plugin-sidecar" && check.status == ActivationReadinessStatus::Fail
        }));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn readiness_reports_imported_channel_access_policy() {
        let root = temp_root("readiness_reports_imported_channel_access_policy");
        let harness_home = root.join(".agent-harness");
        let state = harness_home.join("state");
        let secrets = harness_home.join("secrets");
        fs::create_dir_all(&state).unwrap();
        fs::create_dir_all(&secrets).unwrap();
        fs::write(
            state.join("harness-registry.json"),
            r#"{
              "schema": "agent-harness.target-registry.v1",
              "agents": [
                { "id": "main", "enabled": true }
              ],
              "providers": [],
              "plugins": [],
              "channels": { "telegram": true, "discord": true }
            }"#,
        )
        .unwrap();
        fs::write(
            secrets.join("channel-credentials.env"),
            "\
TELEGRAM_BOT_TOKEN=\"test-telegram-token\"
DISCORD_BOT_TOKEN=\"test-discord-token\"
AGENT_HARNESS_TELEGRAM_ALLOWED_USER_IDS=\"user-1\"
AGENT_HARNESS_TELEGRAM_DIRECT_CHAT_IDS=\"chat-1\"
AGENT_HARNESS_DISCORD_ALLOWED_USER_IDS=\"discord-user-1\"
AGENT_HARNESS_DISCORD_CHANNEL_IDS=\"discord-channel-1\"
",
        )
        .unwrap();

        let report =
            check_activation_readiness(ActivationReadinessOptions { harness_home }).unwrap();

        assert!(report.checks.iter().any(|check| {
            check.name == "telegram-access-policy"
                && check.status == ActivationReadinessStatus::Pass
        }));
        assert!(report.checks.iter().any(|check| {
            check.name == "discord-access-policy" && check.status == ActivationReadinessStatus::Pass
        }));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn readiness_fails_failed_codex_launch_probe_receipt() {
        let root = temp_root("readiness_fails_failed_codex_launch_probe_receipt");
        let harness_home = root.join(".agent-harness");
        let state = harness_home.join("state");
        let runtime_queue = state.join("runtime-queue");
        fs::create_dir_all(&runtime_queue).unwrap();
        fs::write(
            state.join("harness-registry.json"),
            r#"{
              "schema": "agent-harness.target-registry.v1",
              "agents": [
                { "id": "main", "enabled": true }
              ],
              "providers": [],
              "plugins": [],
              "channels": { "telegram": false, "discord": false }
            }"#,
        )
        .unwrap();
        fs::write(
            runtime_queue.join("codex-runtime-launch-receipts.jsonl"),
            r#"{"status":"spawn-failed","reason":"failed to spawn codex app-server process"}"#,
        )
        .unwrap();

        let report =
            check_activation_readiness(ActivationReadinessOptions { harness_home }).unwrap();

        assert!(report.checks.iter().any(|check| {
            check.name == "codex-runtime-launch-probe"
                && check.status == ActivationReadinessStatus::Fail
                && check.detail.contains("spawn-failed")
        }));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn readiness_checks_runtime_loop_last_report() {
        let root = temp_root("readiness_checks_runtime_loop_last_report");
        let harness_home = root.join(".agent-harness");
        let state = harness_home.join("state");
        let runtime_queue = state.join("runtime-queue");
        fs::create_dir_all(&runtime_queue).unwrap();
        fs::write(
            state.join("harness-registry.json"),
            r#"{
              "schema": "agent-harness.target-registry.v1",
              "agents": [
                { "id": "main", "enabled": true }
              ],
              "providers": [],
              "plugins": [],
              "channels": { "telegram": false, "discord": false }
            }"#,
        )
        .unwrap();
        fs::write(
            runtime_queue.join("loop-last.json"),
            r#"{
              "schema": "agent-harness.runtime-loop.v1",
              "errors": 0,
              "stopReason": "stopped after idle runtime result completed"
            }"#,
        )
        .unwrap();

        let clean_report = check_activation_readiness(ActivationReadinessOptions {
            harness_home: harness_home.clone(),
        })
        .unwrap();

        assert!(clean_report.checks.iter().any(|check| {
            check.name == "runtime-loop" && check.status == ActivationReadinessStatus::Pass
        }));

        fs::write(
            runtime_queue.join("loop-last.json"),
            r#"{
              "schema": "agent-harness.runtime-loop.v1",
              "errors": 2,
              "stopReason": "stopped after 2 consecutive runtime errors"
            }"#,
        )
        .unwrap();

        let failed_report = check_activation_readiness(ActivationReadinessOptions {
            harness_home: harness_home.clone(),
        })
        .unwrap();

        assert!(failed_report.checks.iter().any(|check| {
            check.name == "runtime-loop"
                && check.status == ActivationReadinessStatus::Fail
                && check.detail.contains("errors=2")
        }));

        let heartbeat_dir = state.join("supervisor").join("loop-heartbeats");
        fs::create_dir_all(&heartbeat_dir).unwrap();
        let now_ms = current_log_time_ms().unwrap();
        let process_id = std::process::id();
        fs::write(
            heartbeat_dir.join("runtime-loop.json"),
            format!(
                r#"{{"status":"safe-mode","iteration":4,"processId":{process_id},"atMs":{now_ms},"detail":"safeModeRestart=1 after consecutiveErrors=5/5; runtimeConcurrency=1"}}"#
            ),
        )
        .unwrap();

        let live_safe_mode_report = check_activation_readiness(ActivationReadinessOptions {
            harness_home: harness_home.clone(),
        })
        .unwrap();

        assert!(live_safe_mode_report.checks.iter().any(|check| {
            check.name == "runtime-loop"
                && check.status == ActivationReadinessStatus::Warn
                && check.detail.contains("superseded by live heartbeat")
                && check.detail.contains("status=safe-mode")
        }));
        assert!(live_safe_mode_report.checks.iter().any(|check| {
            check.name == "runtime-loop-heartbeat"
                && check.status == ActivationReadinessStatus::Warn
                && check.detail.contains("safe-mode")
        }));

        fs::write(
            heartbeat_dir.join("runtime-loop.json"),
            format!(
                r#"{{"status":"lease-busy","iteration":5,"processId":{process_id},"atMs":{now_ms},"detail":"runtime queue lease lock is busy during capacity inspection"}}"#
            ),
        )
        .unwrap();

        let live_lease_busy_report = check_activation_readiness(ActivationReadinessOptions {
            harness_home: harness_home.clone(),
        })
        .unwrap();

        assert!(live_lease_busy_report.checks.iter().any(|check| {
            check.name == "runtime-loop"
                && check.status == ActivationReadinessStatus::Warn
                && check.detail.contains("superseded by live heartbeat")
                && check.detail.contains("status=lease-busy")
        }));
        assert!(live_lease_busy_report.checks.iter().any(|check| {
            check.name == "runtime-loop-heartbeat"
                && check.status == ActivationReadinessStatus::Warn
                && check.detail.contains("lease-busy")
        }));

        fs::write(
            runtime_queue.join("loop-last.json"),
            r#"{
              "schema": "agent-harness.runtime-loop.v1",
              "errors": 6,
              "stopReason": "stopped after stop file request"
            }"#,
        )
        .unwrap();
        fs::write(
            heartbeat_dir.join("runtime-loop.json"),
            format!(
                r#"{{"status":"running","iteration":3,"processId":{process_id},"atMs":{now_ms},"detail":"checking runtime queue active=0/12"}}"#
            ),
        )
        .unwrap();

        let live_after_stop_report =
            check_activation_readiness(ActivationReadinessOptions { harness_home }).unwrap();

        assert!(live_after_stop_report.checks.iter().any(|check| {
            check.name == "runtime-loop"
                && check.status == ActivationReadinessStatus::Pass
                && check.detail.contains("prior stop-file shutdown")
                && check.detail.contains("active=0/12")
        }));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn readiness_reports_supervisor_plan() {
        let root = temp_root("readiness_reports_supervisor_plan");
        let harness_home = root.join(".agent-harness");
        let state = harness_home.join("state");
        let supervisor_dir = state.join("supervisor").join("windows-scheduled-tasks");
        fs::create_dir_all(&supervisor_dir).unwrap();
        fs::write(
            state.join("harness-registry.json"),
            r#"{
              "schema": "agent-harness.target-registry.v1",
              "agents": [
                { "id": "main", "enabled": true }
              ],
              "providers": [],
              "plugins": [],
              "channels": { "telegram": false, "discord": false }
            }"#,
        )
        .unwrap();
        fs::write(
            supervisor_dir.join("supervisor-plan.json"),
            r#"{
              "schema": "agent-harness.windows-supervisor-plan.v1",
              "tasks": [
                { "name": "AgentHarness-runtime-loop" }
              ]
            }"#,
        )
        .unwrap();

        let report =
            check_activation_readiness(ActivationReadinessOptions { harness_home }).unwrap();

        assert!(report.checks.iter().any(|check| {
            check.name == "supervisor-plan" && check.status == ActivationReadinessStatus::Pass
        }));

        let _ = fs::remove_dir_all(root);
    }

    fn write_reconcile_service_state(services_dir: &Path, service_id: &str, service_kind: &str) {
        fs::write(
            services_dir.join(format!("{service_id}.json")),
            serde_json::to_vec_pretty(&serde_json::json!({
                "schema": "agent-harness.supervisor-service-state.v1",
                "serviceId": service_id,
                "serviceKind": service_kind,
                "desiredState": "running",
                "actualState": "running",
                "launchOwner": "rust-supervisor-run",
                "observedOnly": false
            }))
            .unwrap(),
        )
        .unwrap();
    }

    fn write_default_reconcile_service_states(services_dir: &Path) {
        for service_id in [
            "runtime-loop",
            "worker-loop",
            "cron-scheduler-loop",
            "progress-delivery-loop",
            "ledger-maintenance-loop",
            "telegram-loop",
            "discord-outbox-loop",
            "discord-gateway-loop",
        ] {
            write_reconcile_service_state(
                services_dir,
                service_id,
                supervisor_loop_service_kind(service_id),
            );
        }
    }

    #[test]
    fn readiness_accepts_reconcile_managed_inventory_without_scheduled_task_plan() {
        let root =
            temp_root("readiness_accepts_reconcile_managed_inventory_without_scheduled_task_plan");
        let harness_home = root.join(".agent-harness");
        let services_dir = harness_home
            .join("state")
            .join("supervisor")
            .join("services");
        fs::create_dir_all(&services_dir).unwrap();
        fs::write(
            harness_home.join("harness-config.json"),
            r#"{
              "supervisor": {
                "enabled": true,
                "manageAllLoops": true,
                "telegramLoops": [
                  {
                    "enabled": true,
                    "serviceId": "telegram-loop-secondary",
                    "telegramAccount": "secondary",
                    "agent": "secondary"
                  }
                ]
              }
            }"#,
        )
        .unwrap();

        for service_id in [
            "runtime-loop",
            "worker-loop",
            "cron-scheduler-loop",
            "progress-delivery-loop",
            "ledger-maintenance-loop",
            "telegram-loop",
            "telegram-loop-secondary",
            "discord-outbox-loop",
            "discord-gateway-loop",
        ] {
            write_reconcile_service_state(
                &services_dir,
                service_id,
                supervisor_loop_service_kind(service_id),
            );
        }

        let report =
            check_activation_readiness(ActivationReadinessOptions { harness_home }).unwrap();

        assert!(report.checks.iter().any(|check| {
            check.name == "supervisor-plan"
                && check.status == ActivationReadinessStatus::Pass
                && check.detail.contains("reconcile-managed")
                && check
                    .detail
                    .contains("9 deployment-owned desired service(s)")
                && check
                    .detail
                    .contains("scheduled-task plan is not applicable")
        }));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn readiness_warns_when_reconcile_managed_inventory_is_absent() {
        let root = temp_root("readiness_warns_when_reconcile_managed_inventory_is_absent");
        let harness_home = root.join(".agent-harness");
        fs::create_dir_all(&harness_home).unwrap();
        fs::write(
            harness_home.join("harness-config.json"),
            r#"{
              "supervisor": {
                "enabled": true,
                "manageAllLoops": true
              }
            }"#,
        )
        .unwrap();

        let report =
            check_activation_readiness(ActivationReadinessOptions { harness_home }).unwrap();

        assert!(report.checks.iter().any(|check| {
            check.name == "supervisor-plan"
                && check.status == ActivationReadinessStatus::Warn
                && check
                    .detail
                    .contains("reconcile-managed supervisor has no desired-service inventory")
        }));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn readiness_warns_when_reconcile_managed_inventory_omits_configured_service() {
        let root =
            temp_root("readiness_warns_when_reconcile_managed_inventory_omits_configured_service");
        let harness_home = root.join(".agent-harness");
        let services_dir = harness_home
            .join("state")
            .join("supervisor")
            .join("services");
        fs::create_dir_all(&services_dir).unwrap();
        fs::write(
            harness_home.join("harness-config.json"),
            r#"{
              "supervisor": {
                "enabled": true,
                "manageAllLoops": true
              }
            }"#,
        )
        .unwrap();
        for service_id in [
            "runtime-loop",
            "worker-loop",
            "cron-scheduler-loop",
            "progress-delivery-loop",
            "ledger-maintenance-loop",
            "telegram-loop",
            "discord-outbox-loop",
        ] {
            write_reconcile_service_state(
                &services_dir,
                service_id,
                supervisor_loop_service_kind(service_id),
            );
        }

        let report =
            check_activation_readiness(ActivationReadinessOptions { harness_home }).unwrap();

        assert!(report.checks.iter().any(|check| {
            check.name == "supervisor-plan"
                && check.status == ActivationReadinessStatus::Warn
                && check
                    .detail
                    .contains("missing configured desired service(s): discord-gateway-loop")
        }));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn readiness_uses_reconcile_mode_when_manage_all_loops_true_and_supervisor_disabled() {
        let root = temp_root(
            "readiness_uses_reconcile_mode_when_manage_all_loops_true_and_supervisor_disabled",
        );
        let harness_home = root.join(".agent-harness");
        let services_dir = harness_home
            .join("state")
            .join("supervisor")
            .join("services");
        fs::create_dir_all(&services_dir).unwrap();
        fs::write(
            harness_home.join("harness-config.json"),
            r#"{
              "supervisor": {
                "enabled": false,
                "manageAllLoops": true
              }
            }"#,
        )
        .unwrap();
        write_default_reconcile_service_states(&services_dir);

        let report =
            check_activation_readiness(ActivationReadinessOptions { harness_home }).unwrap();

        assert!(report.checks.iter().any(|check| {
            check.name == "supervisor-plan"
                && check.status == ActivationReadinessStatus::Pass
                && check.detail.contains("reconcile-managed")
        }));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn readiness_matches_reconcile_config_overrides_and_service_kinds() {
        let root = temp_root("readiness_matches_reconcile_config_overrides_and_service_kinds");
        let harness_home = root.join(".agent-harness");
        let services_dir = harness_home
            .join("state")
            .join("supervisor")
            .join("services");
        fs::create_dir_all(&services_dir).unwrap();
        fs::write(
            harness_home.join("harness-config.json"),
            r#"{
              "cronScheduler": { "enabled": false },
              "supervisor": {
                "manageAllLoops": true,
                "runtimeLoop": { "enabled": false },
                "telegramLoop": { "enabled": false },
                "discordGatewayLoop": { "enabled": false },
                "telegramLoops": [
                  { "enabled": true, "telegramAccount": "Team A" },
                  { "enabled": false, "serviceId": "telegram-loop-disabled" }
                ],
                "services": [
                  {
                    "enabled": true,
                    "serviceId": "custom-loop",
                    "serviceKind": "custom-kind",
                    "args": [],
                    "priority": "latency",
                    "restartDelayMs": 1000,
                    "heartbeatTimeoutMs": 120000
                  },
                  {
                    "enabled": false,
                    "serviceId": "disabled-custom-loop",
                    "serviceKind": "custom-kind",
                    "args": [],
                    "priority": "latency",
                    "restartDelayMs": 1000,
                    "heartbeatTimeoutMs": 120000
                  }
                ]
              }
            }"#,
        )
        .unwrap();
        for (service_id, service_kind) in [
            ("worker-loop", "worker"),
            ("progress-delivery-loop", "progress-delivery"),
            ("ledger-maintenance-loop", "ledger-maintenance"),
            ("telegram-loop-team-a", "telegram-ingress"),
            ("discord-outbox-loop", "final-outbox"),
            ("custom-loop", "custom-kind"),
        ] {
            write_reconcile_service_state(&services_dir, service_id, service_kind);
        }

        let report =
            check_activation_readiness(ActivationReadinessOptions { harness_home }).unwrap();

        assert!(report.checks.iter().any(|check| {
            check.name == "supervisor-plan"
                && check.status == ActivationReadinessStatus::Pass
                && check
                    .detail
                    .contains("6 deployment-owned desired service(s)")
        }));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn readiness_fails_closed_for_invalid_reconcile_service_records() {
        for case in [
            "wrong-kind",
            "missing-kind",
            "observed-only",
            "missing-observed-only",
            "wrong-owner",
            "unexpected",
        ] {
            let root = temp_root(&format!(
                "readiness_fails_closed_for_invalid_reconcile_service_records-{case}"
            ));
            let harness_home = root.join(".agent-harness");
            let services_dir = harness_home
                .join("state")
                .join("supervisor")
                .join("services");
            fs::create_dir_all(&services_dir).unwrap();
            fs::write(
                harness_home.join("harness-config.json"),
                r#"{ "supervisor": { "manageAllLoops": true } }"#,
            )
            .unwrap();
            write_default_reconcile_service_states(&services_dir);

            if case == "unexpected" {
                write_reconcile_service_state(&services_dir, "unexpected-loop", "loop");
            } else {
                let path = services_dir.join("runtime-loop.json");
                let mut value = read_json_value(&path).unwrap();
                match case {
                    "wrong-kind" => value["serviceKind"] = Value::String("loop".to_string()),
                    "missing-kind" => {
                        value.as_object_mut().unwrap().remove("serviceKind");
                    }
                    "observed-only" => value["observedOnly"] = Value::Bool(true),
                    "missing-observed-only" => {
                        value.as_object_mut().unwrap().remove("observedOnly");
                    }
                    "wrong-owner" => {
                        value["launchOwner"] = Value::String("external-owner".to_string())
                    }
                    _ => unreachable!(),
                }
                fs::write(path, serde_json::to_vec_pretty(&value).unwrap()).unwrap();
            }

            let report = check_activation_readiness(ActivationReadinessOptions {
                harness_home: harness_home.clone(),
            })
            .unwrap();

            assert!(
                report.checks.iter().any(|check| {
                    check.name == "supervisor-plan"
                        && check.status == ActivationReadinessStatus::Warn
                        && check.detail.contains("inventory is not ready")
                }),
                "case {case} must fail closed: {:?}",
                report.checks
            );

            let _ = fs::remove_dir_all(root);
        }
    }

    #[test]
    fn readiness_keeps_missing_plan_warning_for_scheduled_task_mode() {
        let root = temp_root("readiness_keeps_missing_plan_warning_for_scheduled_task_mode");
        let harness_home = root.join(".agent-harness");
        fs::create_dir_all(&harness_home).unwrap();
        fs::write(
            harness_home.join("harness-config.json"),
            r#"{
              "supervisor": {
                "enabled": true,
                "manageAllLoops": false
              }
            }"#,
        )
        .unwrap();

        let mut checks = Vec::new();
        check_supervisor_plan(&harness_home, &mut checks);

        assert!(checks.iter().any(|check| {
            check.name == "supervisor-plan"
                && check.status == ActivationReadinessStatus::Warn
                && check
                    .detail
                    .contains("run supervisor-plan before service handoff")
        }));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn readiness_reports_live_loop_heartbeats() {
        let root = temp_root("readiness_reports_live_loop_heartbeats");
        let harness_home = root.join(".agent-harness");
        let state = harness_home.join("state");
        let supervisor_dir = state.join("supervisor").join("windows-scheduled-tasks");
        let heartbeat_dir = state.join("supervisor").join("loop-heartbeats");
        fs::create_dir_all(&supervisor_dir).unwrap();
        fs::create_dir_all(&heartbeat_dir).unwrap();
        fs::write(
            state.join("harness-registry.json"),
            r#"{
              "schema": "agent-harness.target-registry.v1",
              "agents": [
                { "id": "main", "enabled": true }
              ],
              "providers": [],
              "plugins": [],
              "channels": { "telegram": false, "discord": false }
            }"#,
        )
        .unwrap();
        fs::write(
            supervisor_dir.join("supervisor-plan.json"),
            r#"{
              "schema": "agent-harness.windows-supervisor-plan.v1",
              "tasks": [
                { "name": "AgentHarness-runtime-loop" }
              ]
            }"#,
        )
        .unwrap();
        let now_ms = current_log_time_ms().unwrap();
        let process_id = std::process::id();
        fs::write(
            heartbeat_dir.join("runtime-loop.json"),
            format!(
                r#"{{"status":"no-work","iteration":7,"processId":{process_id},"atMs":{now_ms},"detail":"idle"}}"#
            ),
        )
        .unwrap();
        fs::write(
            heartbeat_dir.join("telegram-loop.json"),
            format!(
                r#"{{"status":"stopped","iteration":8,"processId":43,"atMs":{now_ms},"detail":"stop file requested"}}"#
            ),
        )
        .unwrap();

        let report =
            check_activation_readiness(ActivationReadinessOptions { harness_home }).unwrap();

        assert!(report.checks.iter().any(|check| {
            check.name == "runtime-loop-heartbeat"
                && check.status == ActivationReadinessStatus::Pass
                && check.detail.contains("status=no-work")
        }));
        assert!(report.checks.iter().any(|check| {
            check.name == "telegram-loop-heartbeat"
                && check.status == ActivationReadinessStatus::Fail
                && check.detail.contains("status=stopped")
        }));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn readiness_reports_progress_delivery_stop_file_as_disabled() {
        let root = temp_root("readiness_reports_progress_delivery_stop_file_as_disabled");
        let harness_home = root.join(".agent-harness");
        let state = harness_home.join("state");
        let stop_dir = state.join("supervisor").join("stop");
        fs::create_dir_all(&stop_dir).unwrap();
        fs::write(
            stop_dir.join("progress-delivery-loop.stop"),
            "stop for current-step cutover 2026-06-13T17:37:07.7114321+08:00",
        )
        .unwrap();

        let report =
            check_activation_readiness(ActivationReadinessOptions { harness_home }).unwrap();

        assert!(report.checks.iter().any(|check| {
            check.name == "progress-delivery-loop-heartbeat"
                && check.status == ActivationReadinessStatus::Warn
                && check.detail.contains("disabled by stop file")
        }));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn readiness_reports_telegram_probe() {
        let root = temp_root("readiness_reports_telegram_probe");
        let harness_home = root.join(".agent-harness");
        let state = harness_home.join("state");
        let channels = state.join("channels");
        fs::create_dir_all(&channels).unwrap();
        fs::write(
            state.join("harness-registry.json"),
            r#"{
              "schema": "agent-harness.target-registry.v1",
              "agents": [
                { "id": "main", "enabled": true }
              ],
              "providers": [],
              "plugins": [],
              "channels": { "telegram": false, "discord": false }
            }"#,
        )
        .unwrap();
        fs::write(
            channels.join("telegram-probe-receipts.jsonl"),
            r#"{"status":"ready","reason":"Telegram Bot API getMe succeeded without consuming updates"}"#,
        )
        .unwrap();

        let report =
            check_activation_readiness(ActivationReadinessOptions { harness_home }).unwrap();

        assert!(report.checks.iter().any(|check| {
            check.name == "telegram-probe" && check.status == ActivationReadinessStatus::Pass
        }));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn readiness_reports_discord_real_inbound_message_create() {
        let root = temp_root("readiness_reports_discord_real_inbound_message_create");
        let harness_home = root.join(".agent-harness");
        let state = harness_home.join("state");
        let channels = state.join("channels");
        fs::create_dir_all(&channels).unwrap();
        fs::write(
            state.join("harness-registry.json"),
            r#"{
              "schema": "agent-harness.target-registry.v1",
              "agents": [
                { "id": "main", "enabled": true }
              ],
              "providers": [],
              "plugins": [],
              "channels": { "telegram": false, "discord": false }
            }"#,
        )
        .unwrap();
        fs::write(
            channels.join("discord-gateway-events.jsonl"),
            r#"{"event":"message-create","messageId":"m1","channelId":"c1","guildId":null,"contentLength":5,"status":0}"#,
        )
        .unwrap();

        let report =
            check_activation_readiness(ActivationReadinessOptions { harness_home }).unwrap();

        assert!(report.checks.iter().any(|check| {
            check.name == "discord-real-inbound" && check.status == ActivationReadinessStatus::Pass
        }));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn readiness_reports_plugin_sidecar_probe_contract() {
        let root = temp_root("readiness_reports_plugin_sidecar_probe_contract");
        let harness_home = root.join(".agent-harness");
        let state = harness_home.join("state");
        let sidecar = state.join("plugin-sidecar");
        fs::create_dir_all(&sidecar).unwrap();
        fs::write(
            state.join("harness-registry.json"),
            r#"{
              "schema": "agent-harness.target-registry.v1",
              "agents": [
                { "id": "main", "enabled": true }
              ],
              "providers": [],
              "plugins": [
                { "id": "openclaw-mem-engine", "sidecarRequired": true }
              ],
              "channels": { "telegram": false, "discord": false }
            }"#,
        )
        .unwrap();
        fs::write(
            sidecar.join("probe-receipts.jsonl"),
            r#"{"status":"contract-ready","sidecarRequired":1,"reason":"plugin sidecar probe loaded harness registry"}"#,
        )
        .unwrap();

        let report =
            check_activation_readiness(ActivationReadinessOptions { harness_home }).unwrap();

        assert!(report.checks.iter().any(|check| {
            check.name == "plugin-sidecar-probe" && check.status == ActivationReadinessStatus::Pass
        }));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn readiness_reports_plugin_sidecar_bridge_receipt() {
        let root = temp_root("readiness_reports_plugin_sidecar_bridge_receipt");
        let harness_home = root.join(".agent-harness");
        let state = harness_home.join("state");
        let sidecar = state.join("plugin-sidecar");
        fs::create_dir_all(&sidecar).unwrap();
        fs::write(
            state.join("harness-registry.json"),
            r#"{
              "schema": "agent-harness.target-registry.v1",
              "agents": [
                { "id": "main", "enabled": true }
              ],
              "providers": [],
              "plugins": [
                { "id": "openclaw-mem-engine", "sidecarRequired": true }
              ],
              "channels": { "telegram": false, "discord": false }
            }"#,
        )
        .unwrap();
        fs::write(
            sidecar.join("bridge-receipts.jsonl"),
            r#"{"status":"ok","method":"sidecar.status","reason":"plugin sidecar JSON-RPC call completed"}"#,
        )
        .unwrap();

        let report =
            check_activation_readiness(ActivationReadinessOptions { harness_home }).unwrap();

        assert!(report.checks.iter().any(|check| {
            check.name == "plugin-sidecar-bridge" && check.status == ActivationReadinessStatus::Pass
        }));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn readiness_passes_plugin_sidecar_with_ready_execution_receipt() {
        let root = temp_root("readiness_passes_plugin_sidecar_with_ready_execution_receipt");
        let harness_home = root.join(".agent-harness");
        let state = harness_home.join("state");
        let sidecar = state.join("plugin-sidecar");
        fs::create_dir_all(&sidecar).unwrap();
        fs::write(
            state.join("harness-registry.json"),
            r#"{
              "schema": "agent-harness.target-registry.v1",
              "agents": [
                { "id": "main", "enabled": true }
              ],
              "providers": [],
              "plugins": [
                { "id": "openclaw-mem-engine", "sidecarRequired": true }
              ],
              "channels": { "telegram": false, "discord": false }
            }"#,
        )
        .unwrap();
        fs::write(
            sidecar.join("execution-receipts.jsonl"),
            r#"{"status":"ready","method":"tools.probe","sidecarRequired":1,"resolvedManifests":1,"unresolvedSidecarRequired":0,"tools":2,"reason":"plugin sidecar manifest catalog is ready"}"#,
        )
        .unwrap();

        let report =
            check_activation_readiness(ActivationReadinessOptions { harness_home }).unwrap();

        assert!(report.checks.iter().any(|check| {
            check.name == "plugin-sidecar" && check.status == ActivationReadinessStatus::Pass
        }));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn readiness_hides_lancedb_when_not_explicitly_selected() {
        let root = temp_root("readiness_hides_lancedb_when_not_explicitly_selected");
        let harness_home = root.join(".agent-harness");
        let state = harness_home.join("state");
        let memory = harness_home.join("memory");
        fs::create_dir_all(&state).unwrap();
        fs::create_dir_all(&memory).unwrap();
        fs::write(
            state.join("harness-registry.json"),
            r#"{
              "schema": "agent-harness.target-registry.v1",
              "agents": [
                { "id": "main", "enabled": true }
              ],
              "providers": [],
              "plugins": [
                { "id": "openclaw-mem-engine", "enabled": true },
                { "id": "memory-lancedb", "enabled": false }
              ],
              "channels": { "telegram": false, "discord": false }
            }"#,
        )
        .unwrap();

        let report =
            check_activation_readiness(ActivationReadinessOptions { harness_home }).unwrap();

        assert!(
            !report
                .checks
                .iter()
                .any(|check| check.name == "memory-lancedb")
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn readiness_warns_when_source_explicitly_selects_lancedb() {
        let root = temp_root("readiness_warns_when_source_explicitly_selects_lancedb");
        let harness_home = root.join(".agent-harness");
        let source_home = root.join("source");
        let state = harness_home.join("state");
        let memory = harness_home.join("memory");
        fs::create_dir_all(&state).unwrap();
        fs::create_dir_all(&memory).unwrap();
        fs::create_dir_all(&source_home).unwrap();
        fs::write(
            source_home.join("openclaw.json"),
            r#"{
              "plugins": {
                "entries": {
                  "openclaw-mem-engine": {
                    "enabled": true,
                    "config": {
                      "retrievalBackend": { "backend": "lancedb" }
                    }
                  }
                }
              }
            }"#,
        )
        .unwrap();
        let registry = serde_json::json!({
            "schema": "agent-harness.target-registry.v1",
            "sourceHome": source_home.to_string_lossy(),
            "agents": [
                { "id": "main", "enabled": true }
            ],
            "providers": [],
            "plugins": [
                { "id": "openclaw-mem-engine", "enabled": true }
            ],
            "channels": { "telegram": false, "discord": false }
        });
        fs::write(
            state.join("harness-registry.json"),
            serde_json::to_string(&registry).unwrap(),
        )
        .unwrap();

        let report =
            check_activation_readiness(ActivationReadinessOptions { harness_home }).unwrap();

        assert!(report.checks.iter().any(|check| {
            check.name == "memory-lancedb"
                && check.status == ActivationReadinessStatus::Warn
                && check.detail.contains("explicitly selected")
        }));

        let _ = fs::remove_dir_all(root);
    }

    fn temp_root(test_name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "agent-harness-activation-{test_name}-{}-{nanos}",
            std::process::id()
        ))
    }
}
