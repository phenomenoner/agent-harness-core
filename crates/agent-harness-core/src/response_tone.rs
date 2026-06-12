use std::collections::BTreeMap;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
    HARNESS_CONFIG_FILE_NAME, HarnessConfigValidationStatus, harness_config_candidates,
    validate_harness_config,
};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EmojiAccentMode {
    Off,
    #[default]
    Subtle,
}

impl EmojiAccentMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Off => "off",
            Self::Subtle => "subtle",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ResponseToneConfig {
    pub emoji_accent_mode: EmojiAccentMode,
    pub emoji_accent: String,
    pub emoji_accent_agent_modes: BTreeMap<String, EmojiAccentMode>,
    pub emoji_accent_channel_modes: BTreeMap<String, EmojiAccentMode>,
    pub source: String,
    pub configured: bool,
    pub config_file: PathBuf,
    pub warnings: Vec<String>,
}

impl Default for ResponseToneConfig {
    fn default() -> Self {
        Self {
            emoji_accent_mode: EmojiAccentMode::Subtle,
            emoji_accent: "✨".to_string(),
            emoji_accent_agent_modes: BTreeMap::new(),
            emoji_accent_channel_modes: BTreeMap::new(),
            source: "default".to_string(),
            configured: false,
            config_file: PathBuf::new(),
            warnings: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResponseToneContext<'a> {
    pub agent_id: Option<&'a str>,
    pub platform: &'a str,
    pub channel_id: &'a str,
    pub user_id: &'a str,
}

pub fn load_response_tone_config(harness_home: impl AsRef<Path>) -> io::Result<ResponseToneConfig> {
    let harness_home = harness_home.as_ref();
    ensure_harness_config_valid(harness_home)?;
    let mut config = ResponseToneConfig {
        config_file: harness_home.join(HARNESS_CONFIG_FILE_NAME),
        ..ResponseToneConfig::default()
    };

    for path in harness_config_candidates(harness_home) {
        if !path.is_file() {
            continue;
        }
        config.config_file = path.clone();
        let text = fs::read_to_string(&path)?;
        let value = serde_json::from_str::<Value>(&text).map_err(io::Error::other)?;
        let response = value.get("response").unwrap_or(&value);
        let mut configured = false;

        if let Some(mode) = response
            .get("emojiAccentMode")
            .or_else(|| response.get("emoji_accent_mode"))
            .and_then(Value::as_str)
        {
            configured = true;
            match parse_emoji_accent_mode(mode) {
                Some(mode) => config.emoji_accent_mode = mode,
                None => config.warnings.push(format!(
                    "unknown response.emojiAccentMode `{mode}`; using {}",
                    config.emoji_accent_mode.as_str()
                )),
            }
        }
        if let Some(modes) = response
            .get("emojiAccentAgentModes")
            .or_else(|| response.get("emoji_accent_agent_modes"))
        {
            configured = true;
            load_emoji_accent_mode_map(
                "response.emojiAccentAgentModes",
                modes,
                &mut config.emoji_accent_agent_modes,
                &mut config.warnings,
            );
        }
        if let Some(modes) = response
            .get("emojiAccentChannelModes")
            .or_else(|| response.get("emoji_accent_channel_modes"))
        {
            configured = true;
            load_emoji_accent_mode_map(
                "response.emojiAccentChannelModes",
                modes,
                &mut config.emoji_accent_channel_modes,
                &mut config.warnings,
            );
        }
        config.configured = configured;
        config.source = if configured {
            format!("config:{}", path.display())
        } else {
            "default".to_string()
        };
        break;
    }

    Ok(config)
}

pub fn apply_response_tone(
    text: &str,
    context: ResponseToneContext<'_>,
    config: &ResponseToneConfig,
) -> String {
    if config.emoji_accent_mode_for(context) != EmojiAccentMode::Subtle {
        return text.to_string();
    }
    let trimmed = text.trim_end();
    if !should_add_subtle_emoji_accent(trimmed) {
        return text.to_string();
    }
    format!("{trimmed} {}", config.emoji_accent)
}

impl ResponseToneConfig {
    pub fn emoji_accent_mode_for(&self, context: ResponseToneContext<'_>) -> EmojiAccentMode {
        for key in channel_selector_candidates(context) {
            if let Some(mode) = self.emoji_accent_channel_modes.get(&key) {
                return *mode;
            }
        }
        if let Some(agent_id) = context.agent_id
            && let Some(mode) = self.emoji_accent_agent_modes.get(&selector_key(agent_id))
        {
            return *mode;
        }
        self.emoji_accent_mode
    }
}

pub fn parse_emoji_accent_mode(value: &str) -> Option<EmojiAccentMode> {
    let normalized = value.trim().to_ascii_lowercase().replace(['-', ' '], "_");
    match normalized.as_str() {
        "off" | "none" | "disabled" | "disable" | "false" => Some(EmojiAccentMode::Off),
        "subtle" | "on" | "enabled" | "enable" | "true" => Some(EmojiAccentMode::Subtle),
        _ => None,
    }
}

fn load_emoji_accent_mode_map(
    label: &str,
    value: &Value,
    target: &mut BTreeMap<String, EmojiAccentMode>,
    warnings: &mut Vec<String>,
) {
    let Some(object) = value.as_object() else {
        warnings.push(format!("{label} must be an object; ignoring overrides"));
        return;
    };
    for (key, value) in object {
        let Some(raw) = value.as_str() else {
            warnings.push(format!("{label}.{key} must be a string; ignoring override"));
            continue;
        };
        match parse_emoji_accent_mode(raw) {
            Some(mode) => {
                target.insert(selector_key(key), mode);
            }
            None => warnings.push(format!(
                "unknown {label}.{key} value `{raw}`; ignoring override"
            )),
        }
    }
}

fn ensure_harness_config_valid(harness_home: &Path) -> io::Result<()> {
    let validation = validate_harness_config(harness_home)?;
    if validation.status == HarnessConfigValidationStatus::Invalid {
        let path = validation
            .config_file
            .as_ref()
            .map(|path| path.display().to_string())
            .unwrap_or_else(|| "harness-config.json".to_string());
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("invalid {path}: {}", validation.errors.join("; ")),
        ));
    }
    Ok(())
}

fn should_add_subtle_emoji_accent(text: &str) -> bool {
    if text.trim().is_empty() || text.contains("```") || ends_with_emoji(text) {
        return false;
    }
    if looks_like_status_or_risk_reply(text) || looks_like_code_heavy_reply(text) {
        return false;
    }
    true
}

fn looks_like_status_or_risk_reply(text: &str) -> bool {
    let first_line = text
        .lines()
        .map(str::trim)
        .find(|line| !line.is_empty())
        .unwrap_or("");
    let first = first_line.to_ascii_lowercase();
    let status_prefixes = [
        "error:",
        "warning:",
        "blocked:",
        "failed:",
        "failure:",
        "risk:",
        "security:",
        "cannot ",
        "can't ",
        "unable ",
        "stopped.",
        "agent harness runtime error",
        "agent harness could not",
    ];
    if status_prefixes
        .iter()
        .any(|prefix| first.starts_with(prefix))
    {
        return true;
    }
    let lower = text.to_ascii_lowercase();
    lower.contains("\nreason:") && lower.contains("failed")
}

fn looks_like_code_heavy_reply(text: &str) -> bool {
    let mut code_like_lines = 0usize;
    let mut non_empty_lines = 0usize;
    for line in text.lines().map(str::trim).filter(|line| !line.is_empty()) {
        non_empty_lines += 1;
        if line.starts_with("use ")
            || line.starts_with("fn ")
            || line.starts_with("let ")
            || line.starts_with("pub ")
            || line.starts_with("class ")
            || line.starts_with("def ")
            || line.starts_with("import ")
            || line.starts_with("const ")
            || line.starts_with("cargo ")
            || line.starts_with("git ")
            || line.contains(" = ")
            || line.ends_with(';')
            || line.ends_with('{')
            || line.ends_with('}')
        {
            code_like_lines += 1;
        }
    }
    non_empty_lines >= 3 && code_like_lines.saturating_mul(2) >= non_empty_lines
}

fn ends_with_emoji(text: &str) -> bool {
    text.trim_end()
        .chars()
        .rev()
        .find(|ch| !ch.is_whitespace())
        .is_some_and(is_emoji_like)
}

fn is_emoji_like(ch: char) -> bool {
    let value = ch as u32;
    matches!(
        value,
        0x1F000..=0x1FAFF | 0x2600..=0x27BF | 0xFE00..=0xFE0F
    )
}

fn channel_selector_candidates(context: ResponseToneContext<'_>) -> Vec<String> {
    vec![
        selector_key(&format!(
            "{}:{}:{}",
            context.platform, context.channel_id, context.user_id
        )),
        selector_key(&format!("{}:{}", context.platform, context.channel_id)),
        selector_key(context.channel_id),
        selector_key(context.platform),
    ]
}

fn selector_key(value: &str) -> String {
    value.trim().to_ascii_lowercase()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn subtle_mode_adds_single_accent_to_agent_reply_text() {
        let config = ResponseToneConfig::default();

        let text = apply_response_tone("Done.", context(Some("main")), &config);

        assert_eq!(text, "Done. ✨");
    }

    #[test]
    fn subtle_mode_skips_code_status_risk_and_existing_emoji() {
        let config = ResponseToneConfig::default();

        assert_eq!(
            apply_response_tone(
                "Here:\n```rust\nfn main() {}\n```",
                context(Some("main")),
                &config
            ),
            "Here:\n```rust\nfn main() {}\n```"
        );
        assert_eq!(
            apply_response_tone(
                "Warning: approval is required.",
                context(Some("main")),
                &config
            ),
            "Warning: approval is required."
        );
        assert_eq!(
            apply_response_tone("Already lively ✨", context(Some("main")), &config),
            "Already lively ✨"
        );
    }

    #[test]
    fn agent_and_channel_overrides_disable_accent() {
        let mut config = ResponseToneConfig::default();
        config
            .emoji_accent_agent_modes
            .insert("main".to_string(), EmojiAccentMode::Off);
        assert_eq!(
            apply_response_tone("Done.", context(Some("main")), &config),
            "Done."
        );

        config
            .emoji_accent_channel_modes
            .insert("telegram:dm-42".to_string(), EmojiAccentMode::Subtle);
        assert_eq!(
            apply_response_tone("Done.", context(Some("main")), &config),
            "Done. ✨"
        );
    }

    #[test]
    fn load_response_tone_config_reads_modes_from_harness_config() {
        let root = temp_root("load_response_tone_config_reads_modes_from_harness_config");
        let harness_home = root.join(".agent-harness");
        fs::create_dir_all(&harness_home).unwrap();
        fs::write(
            harness_home.join(HARNESS_CONFIG_FILE_NAME),
            r#"{
              "response": {
                "emojiAccentMode": "subtle",
                "emojiAccentAgentModes": { "main": "off" },
                "emojiAccentChannelModes": { "telegram:dm-42": "on" }
              }
            }"#,
        )
        .unwrap();

        let config = load_response_tone_config(&harness_home).unwrap();

        assert_eq!(config.emoji_accent_mode, EmojiAccentMode::Subtle);
        assert_eq!(
            config.emoji_accent_agent_modes.get("main"),
            Some(&EmojiAccentMode::Off)
        );
        assert_eq!(
            config.emoji_accent_channel_modes.get("telegram:dm-42"),
            Some(&EmojiAccentMode::Subtle)
        );

        let _ = fs::remove_dir_all(root);
    }

    fn context(agent_id: Option<&str>) -> ResponseToneContext<'_> {
        ResponseToneContext {
            agent_id,
            platform: "telegram",
            channel_id: "dm-42",
            user_id: "user-7",
        }
    }

    fn temp_root(test_name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "agent-harness-response-tone-{test_name}-{}-{nanos}",
            std::process::id()
        ))
    }
}
