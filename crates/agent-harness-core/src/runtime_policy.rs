use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::config::harness_config_candidates;

const DEFAULT_MAX_FAILURE_ATTEMPTS: usize = 3;
const DEFAULT_BASE_DELAY_MS: i64 = 15_000;
const DEFAULT_MAX_DELAY_MS: i64 = 5 * 60 * 1000;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RuntimeBackoffPolicyInspection {
    pub schema: &'static str,
    pub harness_home: PathBuf,
    pub config_file: Option<PathBuf>,
    pub policy: RuntimeBackoffPolicy,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RuntimeBackoffPolicy {
    #[serde(default = "default_max_failure_attempts")]
    pub max_failure_attempts: usize,
    #[serde(default = "default_base_delay_ms")]
    pub base_delay_ms: i64,
    #[serde(default = "default_max_delay_ms")]
    pub max_delay_ms: i64,
    #[serde(default)]
    pub provider_fallbacks: Vec<RuntimeProviderFallbackRule>,
    #[serde(default = "default_operator_hints")]
    pub operator_hints: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RuntimeProviderFallbackRule {
    pub from_provider: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub from_model: Option<String>,
    pub to_provider: String,
    pub to_model: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

impl Default for RuntimeBackoffPolicy {
    fn default() -> Self {
        Self {
            max_failure_attempts: DEFAULT_MAX_FAILURE_ATTEMPTS,
            base_delay_ms: DEFAULT_BASE_DELAY_MS,
            max_delay_ms: DEFAULT_MAX_DELAY_MS,
            provider_fallbacks: Vec::new(),
            operator_hints: true,
        }
    }
}

impl RuntimeBackoffPolicy {
    pub fn retry_delay_ms(&self, failure_attempts: usize) -> i64 {
        let exponent = failure_attempts.saturating_sub(1).min(20);
        let factor = 1_i64.checked_shl(exponent as u32).unwrap_or(i64::MAX);
        self.base_delay_ms
            .saturating_mul(factor)
            .clamp(0, self.max_delay_ms.max(0))
    }

    pub fn fallback_hint(
        &self,
        provider: Option<&str>,
        model: Option<&str>,
        reason: &str,
    ) -> Option<String> {
        let provider = provider?;
        let rule = self.provider_fallbacks.iter().find(|rule| {
            rule.from_provider == provider
                && rule
                    .from_model
                    .as_deref()
                    .is_none_or(|from_model| model == Some(from_model))
        })?;
        Some(format!(
            "configured fallback candidate for {}/{}: use {}/{}{}; last failure: {}",
            provider,
            model.unwrap_or("*"),
            rule.to_provider,
            rule.to_model,
            rule.reason
                .as_deref()
                .map(|reason| format!(" ({reason})"))
                .unwrap_or_default(),
            truncate(reason, 240)
        ))
    }
}

pub fn inspect_runtime_backoff_policy(
    harness_home: impl AsRef<Path>,
) -> io::Result<RuntimeBackoffPolicyInspection> {
    let harness_home = harness_home.as_ref();
    let mut warnings = Vec::new();
    let mut policy = RuntimeBackoffPolicy::default();
    let mut config_file = None;

    if let Some(candidate) = harness_config_candidates(harness_home)
        .into_iter()
        .find(|path| path.is_file())
    {
        config_file = Some(candidate.clone());
        let text = fs::read_to_string(&candidate)?;
        match serde_json::from_str::<Value>(&text) {
            Ok(value) => {
                let section = value.get("runtimeBackoff").or_else(|| {
                    value
                        .get("runtime")
                        .and_then(|runtime| runtime.get("backoff"))
                });
                if let Some(section) = section {
                    match serde_json::from_value::<RuntimeBackoffPolicy>(section.clone()) {
                        Ok(mut loaded) => {
                            if loaded.max_failure_attempts == 0 {
                                warnings.push(
                                    "runtimeBackoff.maxFailureAttempts was zero; using default"
                                        .to_string(),
                                );
                                loaded.max_failure_attempts = DEFAULT_MAX_FAILURE_ATTEMPTS;
                            }
                            if loaded.base_delay_ms <= 0 {
                                warnings.push(
                                    "runtimeBackoff.baseDelayMs must be positive; using default"
                                        .to_string(),
                                );
                                loaded.base_delay_ms = DEFAULT_BASE_DELAY_MS;
                            }
                            if loaded.max_delay_ms < loaded.base_delay_ms {
                                warnings.push(
                                    "runtimeBackoff.maxDelayMs was below baseDelayMs; using baseDelayMs"
                                        .to_string(),
                                );
                                loaded.max_delay_ms = loaded.base_delay_ms;
                            }
                            policy = loaded;
                        }
                        Err(error) => warnings.push(format!(
                            "runtimeBackoff section in {} is invalid: {error}; using defaults",
                            candidate.display()
                        )),
                    }
                }
            }
            Err(error) => warnings.push(format!(
                "harness-config {} is not valid JSON while loading runtimeBackoff: {error}",
                candidate.display()
            )),
        }
    }

    Ok(RuntimeBackoffPolicyInspection {
        schema: "agent-harness.runtime-backoff-policy.v1",
        harness_home: harness_home.to_path_buf(),
        config_file,
        policy,
        warnings,
    })
}

fn truncate(value: &str, max_chars: usize) -> String {
    if value.chars().count() <= max_chars {
        return value.to_string();
    }
    let mut truncated = value
        .chars()
        .take(max_chars.saturating_sub(3))
        .collect::<String>();
    truncated.push_str("...");
    truncated
}

fn default_max_failure_attempts() -> usize {
    DEFAULT_MAX_FAILURE_ATTEMPTS
}

fn default_base_delay_ms() -> i64 {
    DEFAULT_BASE_DELAY_MS
}

fn default_max_delay_ms() -> i64 {
    DEFAULT_MAX_DELAY_MS
}

fn default_operator_hints() -> bool {
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn loads_runtime_backoff_policy_from_harness_config() {
        let root = temp_root("loads_runtime_backoff_policy_from_harness_config");
        let harness_home = root.join(".agent-harness");
        fs::create_dir_all(&harness_home).unwrap();
        fs::write(
            harness_home.join("harness-config.json"),
            r#"{
              "runtimeBackoff": {
                "maxFailureAttempts": 5,
                "baseDelayMs": 1000,
                "maxDelayMs": 8000,
                "providerFallbacks": [
                  {
                    "fromProvider": "openrouter",
                    "fromModel": "a",
                    "toProvider": "openai",
                    "toModel": "gpt-5"
                  }
                ]
              }
            }"#,
        )
        .unwrap();

        let report = inspect_runtime_backoff_policy(&harness_home).unwrap();
        assert_eq!(report.policy.max_failure_attempts, 5);
        assert_eq!(report.policy.retry_delay_ms(4), 8000);
        assert!(
            report
                .policy
                .fallback_hint(Some("openrouter"), Some("a"), "stream disconnected")
                .unwrap()
                .contains("openai/gpt-5")
        );

        let _ = fs::remove_dir_all(root);
    }

    fn temp_root(test_name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "agent-harness-runtime-policy-{test_name}-{}-{nanos}",
            std::process::id()
        ))
    }
}
