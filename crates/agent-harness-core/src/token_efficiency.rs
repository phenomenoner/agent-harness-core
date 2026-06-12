use std::collections::BTreeMap;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use serde::Serialize;
use serde_json::Value;

const TOKEN_REPORT_SCHEMA: &str = "agent-harness.token-efficiency.v1";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TokenEfficiencyOptions {
    pub harness_home: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PromptReductionOptions {
    pub first_prompt: String,
    pub second_prompt: String,
    pub min_reduction_percent: u8,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TokenEfficiencyReport {
    pub schema: &'static str,
    pub harness_home: PathBuf,
    pub receipt_files: Vec<PathBuf>,
    pub total_input_tokens: Option<u64>,
    pub total_output_tokens: Option<u64>,
    pub total_tokens: Option<u64>,
    pub status_counts: BTreeMap<String, usize>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PromptReductionReport {
    pub first_estimated_tokens: u64,
    pub second_estimated_tokens: u64,
    pub reduction_percent: u8,
    pub passed: bool,
    pub stable_prefix_pure: bool,
    pub warnings: Vec<String>,
}

pub fn collect_token_efficiency(
    options: TokenEfficiencyOptions,
) -> io::Result<TokenEfficiencyReport> {
    let receipt_files = vec![
        options
            .harness_home
            .join("state")
            .join("runtime-queue")
            .join("codex-runtime-run-receipts.jsonl"),
        options
            .harness_home
            .join("state")
            .join("runtime-queue")
            .join("codex-runtime-completion-receipts.jsonl"),
    ];
    let mut totals = TokenTotals::default();
    let mut status_counts = BTreeMap::new();
    for path in &receipt_files {
        accumulate_tokens(path, &mut totals, &mut status_counts)?;
    }
    Ok(TokenEfficiencyReport {
        schema: TOKEN_REPORT_SCHEMA,
        harness_home: options.harness_home,
        receipt_files,
        total_input_tokens: totals.input,
        total_output_tokens: totals.output,
        total_tokens: totals.total,
        status_counts,
    })
}

pub fn evaluate_prompt_reduction(options: PromptReductionOptions) -> PromptReductionReport {
    let first = estimate_tokens(&options.first_prompt);
    let second = estimate_tokens(&options.second_prompt);
    let reduction_percent = if first == 0 || second >= first {
        0
    } else {
        (((first - second) * 100) / first).min(100) as u8
    };
    let mut warnings = Vec::new();
    let stable_prefix_pure = stable_prefix_is_pure(&options.second_prompt, &mut warnings);
    PromptReductionReport {
        first_estimated_tokens: first,
        second_estimated_tokens: second,
        reduction_percent,
        passed: reduction_percent >= options.min_reduction_percent && stable_prefix_pure,
        stable_prefix_pure,
        warnings,
    }
}

fn accumulate_tokens(
    path: &Path,
    totals: &mut TokenTotals,
    status_counts: &mut BTreeMap<String, usize>,
) -> io::Result<()> {
    let text = match fs::read_to_string(path) {
        Ok(text) => text,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error),
    };
    for line in text.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let Ok(value) = serde_json::from_str::<Value>(trimmed) else {
            continue;
        };
        if let Some(status) = value.get("status").and_then(Value::as_str) {
            *status_counts.entry(status.to_string()).or_insert(0) += 1;
        }
        let token_value = value.get("tokenUsage").unwrap_or(&value);
        totals.add_input(u64_field(
            token_value,
            &["inputTokens", "input_tokens", "promptTokens"],
        ));
        totals.add_output(u64_field(
            token_value,
            &["outputTokens", "output_tokens", "completionTokens"],
        ));
        totals.add_total(u64_field(token_value, &["totalTokens", "total_tokens"]));
    }
    Ok(())
}

fn estimate_tokens(text: &str) -> u64 {
    let chars = text.chars().count() as u64;
    chars.div_ceil(4)
}

fn stable_prefix_is_pure(prompt: &str, warnings: &mut Vec<String>) -> bool {
    let prefix = prompt
        .split("\n## User Message")
        .next()
        .unwrap_or(prompt)
        .to_ascii_lowercase();
    let volatile = [
        "traceid",
        "trace id",
        "queueid",
        "queue id",
        "timestamp",
        "atms",
    ];
    for marker in volatile {
        if prefix.contains(marker) {
            warnings.push(format!(
                "stable prompt prefix contains volatile marker `{marker}`"
            ));
        }
    }
    warnings.is_empty()
}

fn u64_field(value: &Value, keys: &[&str]) -> Option<u64> {
    keys.iter()
        .find_map(|key| value.get(*key).and_then(Value::as_u64))
}

#[derive(Default)]
struct TokenTotals {
    input: Option<u64>,
    output: Option<u64>,
    total: Option<u64>,
}

impl TokenTotals {
    fn add_input(&mut self, value: Option<u64>) {
        self.input = add_optional(self.input, value);
    }

    fn add_output(&mut self, value: Option<u64>) {
        self.output = add_optional(self.output, value);
    }

    fn add_total(&mut self, value: Option<u64>) {
        self.total = add_optional(self.total, value);
    }
}

fn add_optional(left: Option<u64>, right: Option<u64>) -> Option<u64> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.saturating_add(right)),
        (Some(left), None) => Some(left),
        (None, Some(right)) => Some(right),
        (None, None) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn token_report_aggregates_receipt_usage_and_prompt_reduction() {
        let root = temp_root("token_report_aggregates_receipt_usage_and_prompt_reduction");
        let harness_home = root.join(".agent-harness");
        let queue = harness_home.join("state").join("runtime-queue");
        fs::create_dir_all(&queue).unwrap();
        fs::write(
            queue.join("codex-runtime-run-receipts.jsonl"),
            r#"{"status":"completed","tokenUsage":{"inputTokens":100,"outputTokens":20,"totalTokens":120}}"#,
        )
        .unwrap();

        let report = collect_token_efficiency(TokenEfficiencyOptions { harness_home }).unwrap();

        assert_eq!(report.total_input_tokens, Some(100));
        assert_eq!(report.total_output_tokens, Some(20));
        assert_eq!(report.status_counts["completed"], 1);

        let reduction = evaluate_prompt_reduction(PromptReductionOptions {
            first_prompt: "stable identity ".repeat(80) + "\n## User Message\nhello",
            second_prompt: "stable identity ".repeat(40) + "\n## User Message\nhello",
            min_reduction_percent: 30,
        });
        assert!(reduction.passed);
        assert!(reduction.reduction_percent >= 30);
    }

    fn temp_root(test_name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "agent-harness-token-{test_name}-{}-{nanos}",
            std::process::id()
        ))
    }
}
