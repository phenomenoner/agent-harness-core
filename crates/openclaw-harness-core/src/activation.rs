use std::env;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use serde::Serialize;
use serde_json::Value;

use crate::{
    codex_runtime::{CodexApprovalPolicy, inspect_codex_approval_policy, inspect_codex_sandbox},
    logging::current_log_time_ms,
    probe_harness_log_writable,
};

const ACTIVATION_READINESS_SCHEMA: &str = "openclaw-harness.activation-readiness.v1";
const LOOP_HEARTBEAT_STALE_MS: i64 = 120_000;

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
    check_supervisor_plan(&options.harness_home, &mut checks);
    check_channel_state(&options.harness_home, &mut checks);
    check_logging(&options.harness_home, &mut checks);
    check_memory_import(&options.harness_home, &mut checks);
    check_codex_auth(&mut checks);
    check_codex_config(&options.harness_home, &mut checks);
    check_codex_approval_policy(&options.harness_home, &mut checks);
    check_codex_sandbox(&options.harness_home, &mut checks);

    let summary = summarize(&checks);
    Ok(ActivationReadinessReport {
        schema: ACTIVATION_READINESS_SCHEMA,
        harness_home: options.harness_home,
        ready: summary.failed == 0,
        summary,
        checks,
    })
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
                "OPENCLAW_TELEGRAM_ALLOWED_USER_IDS",
                "OPENCLAW_TELEGRAM_GROUP_ALLOWED_USER_IDS",
                "OPENCLAW_TELEGRAM_DIRECT_CHAT_IDS",
                "OPENCLAW_TELEGRAM_GROUP_CHAT_IDS",
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
                "OPENCLAW_DISCORD_ALLOWED_USER_IDS",
                "OPENCLAW_DISCORD_CHANNEL_IDS",
                "OPENCLAW_DISCORD_GUILD_IDS",
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
    } else {
        checks.push(pass(
            "plugin-sidecar",
            "no sidecar-required plugins reported by registry",
        ));
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
                Some(errors) => checks.push(fail(
                    "runtime-loop",
                    format!(
                        "latest runtime loop recorded errors={errors} at {}: {stop_reason}",
                        path.display()
                    ),
                )),
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

fn check_supervisor_plan(harness_home: &Path, checks: &mut Vec<ActivationReadinessCheck>) {
    let path = harness_home
        .join("state")
        .join("supervisor")
        .join("windows-scheduled-tasks")
        .join("supervisor-plan.json");
    match read_json_value(&path) {
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
    }
    check_loop_heartbeats(harness_home, checks);
}

fn check_loop_heartbeats(harness_home: &Path, checks: &mut Vec<ActivationReadinessCheck>) {
    for name in [
        "runtime-loop",
        "telegram-loop",
        "discord-outbox-loop",
        "discord-gateway-loop",
    ] {
        check_loop_heartbeat(harness_home, checks, name);
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
            if matches!(
                status,
                "running" | "ok" | "no-work" | "ready" | "heartbeat" | "connected"
            ) {
                if age_ms.is_some_and(|age_ms| age_ms > LOOP_HEARTBEAT_STALE_MS) {
                    checks.push(warn(
                        check_name,
                        format!(
                            "{name} heartbeat is stale at {}: status={status}, {age_detail}, detail={detail}",
                            path.display()
                        ),
                    ));
                } else {
                    checks.push(pass(
                        check_name,
                        format!(
                            "{name} heartbeat is live at {}: status={status}, {age_detail}, detail={detail}",
                            path.display()
                        ),
                    ));
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
        Err(error) if error.kind() == io::ErrorKind::NotFound => checks.push(warn(
            format!("{name}-heartbeat"),
            format!("no live {name} heartbeat found yet at {}", path.display()),
        )),
        Err(error) => checks.push(warn(
            format!("{name}-heartbeat"),
            format!("could not read {}: {error}", path.display()),
        )),
    }
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
        .join(".openclaw-harness-builtins.json");
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
    match fs::read_to_string(&log_file) {
        Ok(text) if text.contains(&format!(r#""event":"{event}""#)) => checks.push(pass(
            name,
            format!("found event {event} in {}", log_file.display()),
        )),
        Ok(_) => checks.push(warn(name, missing_detail)),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            checks.push(warn(name, missing_detail));
        }
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
    match fs::read_to_string(&path) {
        Ok(text) if text.contains(r#""event":"message-create""#) => checks.push(pass(
            "discord-real-inbound",
            format!(
                "Discord Gateway has received a real MESSAGE_CREATE event at {}",
                path.display()
            ),
        )),
        Ok(_) => checks.push(warn(
            "discord-real-inbound",
            format!(
                "Discord Gateway is connected but no real MESSAGE_CREATE event is recorded at {}; ask an allowed Discord user to DM the bot",
                path.display()
            ),
        )),
        Err(error) if error.kind() == io::ErrorKind::NotFound => checks.push(warn(
            "discord-real-inbound",
            format!(
                "Discord gateway event log is not present at {}; start discord-gateway-loop and DM the bot",
                path.display()
            ),
        )),
        Err(error) => checks.push(warn(
            "discord-real-inbound",
            format!("could not read {}: {error}", path.display()),
        )),
    }
}

fn check_memory_import(harness_home: &Path, checks: &mut Vec<ActivationReadinessCheck>) {
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

    let qdrant_edge = memory_dir.join("qdrant-edge");
    if qdrant_edge.is_dir() {
        checks.push(pass(
            "memory-qdrant-edge",
            format!(
                "primary Qdrant edge memory backend found at {}",
                qdrant_edge.display()
            ),
        ));
    } else {
        checks.push(warn(
            "memory-qdrant-edge",
            format!(
                "primary Qdrant edge memory backend not found at {}; memory recall may fall back to SQLite/LanceDB or be unavailable",
                qdrant_edge.display()
            ),
        ));
    }

    let sqlite = memory_dir.join("openclaw-mem.sqlite");
    if sqlite.is_file() {
        checks.push(pass(
            "memory-openclaw-mem-sqlite",
            format!(
                "OpenClaw memory SQLite snapshot found at {}",
                sqlite.display()
            ),
        ));
    } else {
        checks.push(warn(
            "memory-openclaw-mem-sqlite",
            format!(
                "OpenClaw memory SQLite snapshot not found at {}",
                sqlite.display()
            ),
        ));
    }

    let lancedb = memory_dir.join("lancedb");
    if lancedb.is_dir() {
        checks.push(pass(
            "memory-lancedb",
            format!(
                "LanceDB backup memory backend found at {}",
                lancedb.display()
            ),
        ));
    } else {
        checks.push(warn(
            "memory-lancedb",
            format!(
                "LanceDB backup backend not found at {}; acceptable when Qdrant edge is primary",
                lancedb.display()
            ),
        ));
    }
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
        checks.push(pass(
            "codex-config",
            format!(
                "harness-local Codex config is present at {}",
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
                "harness-local Codex auth exists but config is missing at {}; codex-plan will create a minimal runtime config before app-server launch",
                config.display()
            ),
        ));
    } else {
        checks.push(warn(
            "codex-config",
            format!(
                "harness-local Codex config is not present at {}; using global Codex config/auth if available",
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
    let text = fs::read_to_string(path)?;
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
            harness_home: root.join(".openclaw-harness"),
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
    fn readiness_reports_registry_channels_and_plugin_blockers() {
        let root = temp_root("readiness_reports_registry_channels_and_plugin_blockers");
        let harness_home = root.join(".openclaw-harness");
        let state = harness_home.join("state");
        fs::create_dir_all(&state).unwrap();
        fs::write(
            state.join("harness-registry.json"),
            r#"{
              "schema": "openclaw-harness.target-registry.v1",
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
        let harness_home = root.join(".openclaw-harness");
        let state = harness_home.join("state");
        let secrets = harness_home.join("secrets");
        fs::create_dir_all(&state).unwrap();
        fs::create_dir_all(&secrets).unwrap();
        fs::write(
            state.join("harness-registry.json"),
            r#"{
              "schema": "openclaw-harness.target-registry.v1",
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
OPENCLAW_TELEGRAM_ALLOWED_USER_IDS=\"user-1\"
OPENCLAW_TELEGRAM_DIRECT_CHAT_IDS=\"chat-1\"
OPENCLAW_DISCORD_ALLOWED_USER_IDS=\"discord-user-1\"
OPENCLAW_DISCORD_CHANNEL_IDS=\"discord-channel-1\"
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
        let harness_home = root.join(".openclaw-harness");
        let state = harness_home.join("state");
        let runtime_queue = state.join("runtime-queue");
        fs::create_dir_all(&runtime_queue).unwrap();
        fs::write(
            state.join("harness-registry.json"),
            r#"{
              "schema": "openclaw-harness.target-registry.v1",
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
        let harness_home = root.join(".openclaw-harness");
        let state = harness_home.join("state");
        let runtime_queue = state.join("runtime-queue");
        fs::create_dir_all(&runtime_queue).unwrap();
        fs::write(
            state.join("harness-registry.json"),
            r#"{
              "schema": "openclaw-harness.target-registry.v1",
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
              "schema": "openclaw-harness.runtime-loop.v1",
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
              "schema": "openclaw-harness.runtime-loop.v1",
              "errors": 2,
              "stopReason": "stopped after 2 consecutive runtime errors"
            }"#,
        )
        .unwrap();

        let failed_report =
            check_activation_readiness(ActivationReadinessOptions { harness_home }).unwrap();

        assert!(failed_report.checks.iter().any(|check| {
            check.name == "runtime-loop"
                && check.status == ActivationReadinessStatus::Fail
                && check.detail.contains("errors=2")
        }));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn readiness_reports_supervisor_plan() {
        let root = temp_root("readiness_reports_supervisor_plan");
        let harness_home = root.join(".openclaw-harness");
        let state = harness_home.join("state");
        let supervisor_dir = state.join("supervisor").join("windows-scheduled-tasks");
        fs::create_dir_all(&supervisor_dir).unwrap();
        fs::write(
            state.join("harness-registry.json"),
            r#"{
              "schema": "openclaw-harness.target-registry.v1",
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
              "schema": "openclaw-harness.windows-supervisor-plan.v1",
              "tasks": [
                { "name": "OpenClawHarness-runtime-loop" }
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

    #[test]
    fn readiness_reports_live_loop_heartbeats() {
        let root = temp_root("readiness_reports_live_loop_heartbeats");
        let harness_home = root.join(".openclaw-harness");
        let state = harness_home.join("state");
        let supervisor_dir = state.join("supervisor").join("windows-scheduled-tasks");
        let heartbeat_dir = state.join("supervisor").join("loop-heartbeats");
        fs::create_dir_all(&supervisor_dir).unwrap();
        fs::create_dir_all(&heartbeat_dir).unwrap();
        fs::write(
            state.join("harness-registry.json"),
            r#"{
              "schema": "openclaw-harness.target-registry.v1",
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
              "schema": "openclaw-harness.windows-supervisor-plan.v1",
              "tasks": [
                { "name": "OpenClawHarness-runtime-loop" }
              ]
            }"#,
        )
        .unwrap();
        let now_ms = current_log_time_ms().unwrap();
        fs::write(
            heartbeat_dir.join("runtime-loop.json"),
            format!(
                r#"{{"status":"no-work","iteration":7,"processId":42,"atMs":{now_ms},"detail":"idle"}}"#
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
    fn readiness_reports_telegram_probe() {
        let root = temp_root("readiness_reports_telegram_probe");
        let harness_home = root.join(".openclaw-harness");
        let state = harness_home.join("state");
        let channels = state.join("channels");
        fs::create_dir_all(&channels).unwrap();
        fs::write(
            state.join("harness-registry.json"),
            r#"{
              "schema": "openclaw-harness.target-registry.v1",
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
        let harness_home = root.join(".openclaw-harness");
        let state = harness_home.join("state");
        let channels = state.join("channels");
        fs::create_dir_all(&channels).unwrap();
        fs::write(
            state.join("harness-registry.json"),
            r#"{
              "schema": "openclaw-harness.target-registry.v1",
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
        let harness_home = root.join(".openclaw-harness");
        let state = harness_home.join("state");
        let sidecar = state.join("plugin-sidecar");
        fs::create_dir_all(&sidecar).unwrap();
        fs::write(
            state.join("harness-registry.json"),
            r#"{
              "schema": "openclaw-harness.target-registry.v1",
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
        let harness_home = root.join(".openclaw-harness");
        let state = harness_home.join("state");
        let sidecar = state.join("plugin-sidecar");
        fs::create_dir_all(&sidecar).unwrap();
        fs::write(
            state.join("harness-registry.json"),
            r#"{
              "schema": "openclaw-harness.target-registry.v1",
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
        let harness_home = root.join(".openclaw-harness");
        let state = harness_home.join("state");
        let sidecar = state.join("plugin-sidecar");
        fs::create_dir_all(&sidecar).unwrap();
        fs::write(
            state.join("harness-registry.json"),
            r#"{
              "schema": "openclaw-harness.target-registry.v1",
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

    fn temp_root(test_name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "openclaw-harness-activation-{test_name}-{}-{nanos}",
            std::process::id()
        ))
    }
}
