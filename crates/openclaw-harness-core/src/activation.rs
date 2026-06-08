use std::env;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use serde::Serialize;
use serde_json::Value;

use crate::probe_harness_log_writable;

const ACTIVATION_READINESS_SCHEMA: &str = "openclaw-harness.activation-readiness.v1";

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
        check_channels(registry, &mut checks);
        check_providers(registry, &mut checks);
        check_plugins(registry, &mut checks);
    }
    check_activation_plan_doc(&mut checks);
    check_harness_skills(&options.harness_home, &mut checks);
    check_runtime_queue(&options.harness_home, &mut checks);
    check_channel_state(&options.harness_home, &mut checks);
    check_logging(&options.harness_home, &mut checks);
    check_memory_import(&options.harness_home, &mut checks);
    check_codex_auth(&mut checks);

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

fn check_channels(registry: &Value, checks: &mut Vec<ActivationReadinessCheck>) {
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
        check_env_token(
            checks,
            "telegram-token",
            "TELEGRAM_BOT_TOKEN",
            "Telegram channel is enabled",
        );
    }
    if discord {
        check_env_token(
            checks,
            "discord-token",
            "DISCORD_BOT_TOKEN",
            "Discord channel is enabled",
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

fn check_plugins(registry: &Value, checks: &mut Vec<ActivationReadinessCheck>) {
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
        checks.push(fail(
            "plugin-sidecar",
            format!("{sidecar_required} imported plugin(s) require the Node sidecar, which is not enabled yet"),
        ));
    } else {
        checks.push(pass(
            "plugin-sidecar",
            "no sidecar-required plugins reported by registry",
        ));
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
}

fn check_memory_import(harness_home: &Path, checks: &mut Vec<ActivationReadinessCheck>) {
    let memory_dir = harness_home.join("memory");
    if memory_dir.is_dir() {
        checks.push(warn(
            "memory-adapter",
            format!(
                "memory files are imported at {}, but native memory query adapter is not enabled yet",
                memory_dir.display()
            ),
        ));
    } else {
        checks.push(warn(
            "memory-adapter",
            "no imported memory directory detected; memory recall will be unavailable",
        ));
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
