use serde::Serialize;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum ChannelCommand {
    New {
        topic: Option<String>,
    },
    Think {
        level: Option<String>,
        global: bool,
    },
    Stop {
        reason: Option<String>,
    },
    Restart {
        target: Option<String>,
        reason: Option<String>,
    },
    Steer {
        instruction: String,
    },
    Btw {
        note: String,
    },
    Model {
        target: Option<String>,
        global: bool,
    },
    Status {
        scope: Option<String>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum ChannelCommandIntent {
    StartNewSession {
        topic: Option<String>,
    },
    Think {
        level: Option<String>,
        global: bool,
    },
    StopCurrentRun {
        reason: Option<String>,
    },
    RestartGateway {
        reason: Option<String>,
    },
    RestartChannel {
        target: Option<String>,
        reason: Option<String>,
    },
    AddSteering {
        instruction: String,
    },
    AddBtwNote {
        note: String,
    },
    Model {
        target: Option<String>,
        global: bool,
    },
    ShowStatus {
        scope: Option<String>,
    },
}

pub const DEFAULT_THINKING_LEVEL: &str = "medium";
pub const THINKING_LEVELS: &[&str] = &["minimal", "low", "medium", "high"];
pub const XHIGH_THINKING_LEVEL: &str = "xhigh";

impl ChannelCommand {
    pub fn name(&self) -> &'static str {
        match self {
            ChannelCommand::New { .. } => "new",
            ChannelCommand::Think { .. } => "think",
            ChannelCommand::Stop { .. } => "stop",
            ChannelCommand::Restart { .. } => "restart",
            ChannelCommand::Steer { .. } => "steer",
            ChannelCommand::Btw { .. } => "btw",
            ChannelCommand::Model { .. } => "model",
            ChannelCommand::Status { .. } => "status",
        }
    }

    pub fn into_intent(self) -> ChannelCommandIntent {
        match self {
            ChannelCommand::New { topic } => ChannelCommandIntent::StartNewSession { topic },
            ChannelCommand::Think { level, global } => {
                ChannelCommandIntent::Think { level, global }
            }
            ChannelCommand::Stop { reason } => ChannelCommandIntent::StopCurrentRun { reason },
            ChannelCommand::Restart { target, reason } => match target.as_deref() {
                Some("channel") | Some("tg") | Some("telegram") | Some("discord")
                | Some("current") => ChannelCommandIntent::RestartChannel { target, reason },
                Some("gateway") | None => ChannelCommandIntent::RestartGateway { reason },
                Some(_) => ChannelCommandIntent::RestartGateway { reason },
            },
            ChannelCommand::Steer { instruction } => {
                ChannelCommandIntent::AddSteering { instruction }
            }
            ChannelCommand::Btw { note } => ChannelCommandIntent::AddBtwNote { note },
            ChannelCommand::Model { target, global } => {
                ChannelCommandIntent::Model { target, global }
            }
            ChannelCommand::Status { scope } => ChannelCommandIntent::ShowStatus { scope },
        }
    }
}

pub fn parse_channel_command(input: &str) -> Option<ChannelCommand> {
    let trimmed = input.trim();
    let command_text = trimmed.strip_prefix('/')?.trim_start();
    let (name, rest) = split_command(command_text);

    match name.to_ascii_lowercase().as_str() {
        "new" => Some(ChannelCommand::New {
            topic: optional_text(rest),
        }),
        "think" => {
            let (level, global) = optional_text_with_global_flag(rest);
            Some(ChannelCommand::Think { level, global })
        }
        "stop" => Some(ChannelCommand::Stop {
            reason: optional_text(rest),
        }),
        "restart" => {
            let (target, reason) = optional_restart_target(rest);
            Some(ChannelCommand::Restart { target, reason })
        }
        "steer" => required_text(rest).map(|instruction| ChannelCommand::Steer { instruction }),
        "btw" => required_text(rest).map(|note| ChannelCommand::Btw { note }),
        "model" => {
            let (target, global) = optional_text_with_global_flag(rest);
            Some(ChannelCommand::Model { target, global })
        }
        "status" => Some(ChannelCommand::Status {
            scope: optional_text(rest),
        }),
        _ => None,
    }
}

pub fn parse_channel_command_intent(input: &str) -> Option<ChannelCommandIntent> {
    parse_channel_command(input).map(ChannelCommand::into_intent)
}

fn split_command(value: &str) -> (&str, &str) {
    value
        .split_once(char::is_whitespace)
        .map_or((value, ""), |(name, rest)| (name, rest.trim()))
}

fn optional_text(value: &str) -> Option<String> {
    required_text(value)
}

fn optional_text_with_global_flag(value: &str) -> (Option<String>, bool) {
    let mut parts: Vec<&str> = value.split_whitespace().collect();
    let mut global = false;
    if parts.last().is_some_and(|part| *part == "--global") {
        parts.pop();
        global = true;
    }
    let text = parts.join(" ");
    (required_text(&text), global)
}

fn optional_restart_target(value: &str) -> (Option<String>, Option<String>) {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return (None, None);
    }
    let (first, rest) = split_command(trimmed);
    match first.to_ascii_lowercase().as_str() {
        "gateway" => (Some(first.to_ascii_lowercase()), optional_text(rest)),
        "current" | "channel" | "tg" | "telegram" | "discord" => {
            (Some(first.to_ascii_lowercase()), optional_text(rest))
        }
        _ => (None, optional_text(trimmed)),
    }
}

fn required_text(value: &str) -> Option<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}

pub fn normalize_thinking_level(level: &str) -> Option<String> {
    let normalized = level.trim().to_ascii_lowercase();
    if THINKING_LEVELS
        .iter()
        .any(|candidate| *candidate == normalized)
    {
        return Some(normalized);
    }
    match normalized.as_str() {
        "xhigh" | "x-high" | "x_high" | "extra-high" | "extra_high" | "very-high" | "very_high"
        | "ultra-high" | "ultra_high" | "max" | "maximum" | "超高" | "最高" => {
            Some(XHIGH_THINKING_LEVEL.to_string())
        }
        "最小" | "最低" => Some("minimal".to_string()),
        "低" => Some("low".to_string()),
        "中" | "中等" | "普通" | "標準" => Some("medium".to_string()),
        "高" => Some("high".to_string()),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_session_and_runtime_commands() {
        assert_eq!(
            parse_channel_command("/new weekly review"),
            Some(ChannelCommand::New {
                topic: Some("weekly review".to_string())
            })
        );
        assert_eq!(
            parse_channel_command(" /think use compact reasoning "),
            Some(ChannelCommand::Think {
                level: Some("use compact reasoning".to_string()),
                global: false
            })
        );
        assert_eq!(
            parse_channel_command(" / think 超高 "),
            Some(ChannelCommand::Think {
                level: Some("超高".to_string()),
                global: false
            })
        );
        assert_eq!(
            parse_channel_command("/stop"),
            Some(ChannelCommand::Stop { reason: None })
        );
        assert_eq!(
            parse_channel_command("/restart"),
            Some(ChannelCommand::Restart {
                target: None,
                reason: None
            })
        );
        assert_eq!(
            parse_channel_command("/restart telegram reconnect websocket"),
            Some(ChannelCommand::Restart {
                target: Some("telegram".to_string()),
                reason: Some("reconnect websocket".to_string())
            })
        );
    }

    #[test]
    fn parses_steer_and_btw_with_required_body() {
        assert_eq!(
            parse_channel_command("/steer keep replies short"),
            Some(ChannelCommand::Steer {
                instruction: "keep replies short".to_string()
            })
        );
        assert_eq!(
            parse_channel_command("/btw this belongs to current thread"),
            Some(ChannelCommand::Btw {
                note: "this belongs to current thread".to_string()
            })
        );
        assert_eq!(parse_channel_command("/steer"), None);
        assert_eq!(parse_channel_command("/btw"), None);
    }

    #[test]
    fn parses_model_and_status_commands() {
        assert_eq!(
            parse_channel_command("/model openrouter/anthropic/claude-sonnet-4"),
            Some(ChannelCommand::Model {
                target: Some("openrouter/anthropic/claude-sonnet-4".to_string()),
                global: false
            })
        );
        assert_eq!(
            parse_channel_command("/model openai/gpt-5.5 --global"),
            Some(ChannelCommand::Model {
                target: Some("openai/gpt-5.5".to_string()),
                global: true
            })
        );
        assert_eq!(
            parse_channel_command("/model"),
            Some(ChannelCommand::Model {
                target: None,
                global: false
            })
        );
        assert_eq!(
            parse_channel_command("/status cron"),
            Some(ChannelCommand::Status {
                scope: Some("cron".to_string())
            })
        );
        assert_eq!(
            parse_channel_command("/status"),
            Some(ChannelCommand::Status { scope: None })
        );
    }

    #[test]
    fn maps_commands_to_runtime_intents() {
        assert_eq!(
            parse_channel_command_intent("/new weekly review"),
            Some(ChannelCommandIntent::StartNewSession {
                topic: Some("weekly review".to_string())
            })
        );
        assert_eq!(
            parse_channel_command_intent("/think"),
            Some(ChannelCommandIntent::Think {
                level: None,
                global: false
            })
        );
        assert_eq!(
            parse_channel_command_intent("/stop user canceled"),
            Some(ChannelCommandIntent::StopCurrentRun {
                reason: Some("user canceled".to_string())
            })
        );
        assert_eq!(
            parse_channel_command_intent("/restart recycle adapter"),
            Some(ChannelCommandIntent::RestartGateway {
                reason: Some("recycle adapter".to_string())
            })
        );
        assert_eq!(
            parse_channel_command_intent("/restart gateway"),
            Some(ChannelCommandIntent::RestartGateway { reason: None })
        );
        assert_eq!(
            parse_channel_command_intent("/restart channel reconnect websocket"),
            Some(ChannelCommandIntent::RestartChannel {
                target: Some("channel".to_string()),
                reason: Some("reconnect websocket".to_string())
            })
        );
        assert_eq!(
            parse_channel_command_intent("/steer use agent main"),
            Some(ChannelCommandIntent::AddSteering {
                instruction: "use agent main".to_string()
            })
        );
        assert_eq!(
            parse_channel_command_intent("/btw check cron state"),
            Some(ChannelCommandIntent::AddBtwNote {
                note: "check cron state".to_string()
            })
        );
    }

    #[test]
    fn maps_model_and_status_to_runtime_intents() {
        assert_eq!(
            parse_channel_command_intent("/model"),
            Some(ChannelCommandIntent::Model {
                target: None,
                global: false
            })
        );
        assert_eq!(
            parse_channel_command_intent("/model openrouter/anthropic/claude-sonnet-4"),
            Some(ChannelCommandIntent::Model {
                target: Some("openrouter/anthropic/claude-sonnet-4".to_string()),
                global: false
            })
        );
        assert_eq!(
            parse_channel_command_intent("/think high --global"),
            Some(ChannelCommandIntent::Think {
                level: Some("high".to_string()),
                global: true
            })
        );
        assert_eq!(
            parse_channel_command_intent("/status"),
            Some(ChannelCommandIntent::ShowStatus { scope: None })
        );
        assert_eq!(
            parse_channel_command_intent("/status cron"),
            Some(ChannelCommandIntent::ShowStatus {
                scope: Some("cron".to_string())
            })
        );
    }

    #[test]
    fn exposes_stable_command_names_for_channel_adapters() {
        let command = parse_channel_command("/model openai/gpt-5").unwrap();
        assert_eq!(command.name(), "model");
        let command = parse_channel_command("/status agents").unwrap();
        assert_eq!(command.name(), "status");
    }

    #[test]
    fn ignores_plain_messages_and_unknown_commands() {
        assert_eq!(parse_channel_command("hello"), None);
        assert_eq!(parse_channel_command("/unknown value"), None);
    }

    #[test]
    fn normalizes_supported_thinking_levels() {
        assert_eq!(
            normalize_thinking_level("Medium"),
            Some("medium".to_string())
        );
        assert_eq!(normalize_thinking_level("XHIGH"), Some("xhigh".to_string()));
        assert_eq!(normalize_thinking_level("超高"), Some("xhigh".to_string()));
        assert_eq!(normalize_thinking_level("turbo"), None);
    }
}
