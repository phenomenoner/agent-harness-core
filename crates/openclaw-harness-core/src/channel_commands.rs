use serde::Serialize;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum ChannelCommand {
    New { topic: Option<String> },
    Think { instruction: Option<String> },
    Stop { reason: Option<String> },
    Steer { instruction: String },
    Btw { note: String },
    Model { target: Option<String> },
    Status { scope: Option<String> },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum ChannelCommandIntent {
    StartNewSession { topic: Option<String> },
    SetThinkingMode { instruction: Option<String> },
    StopCurrentRun { reason: Option<String> },
    AddSteering { instruction: String },
    AddBtwNote { note: String },
    ShowModel,
    SwitchModel { target: String },
    ShowStatus { scope: Option<String> },
}

impl ChannelCommand {
    pub fn name(&self) -> &'static str {
        match self {
            ChannelCommand::New { .. } => "new",
            ChannelCommand::Think { .. } => "think",
            ChannelCommand::Stop { .. } => "stop",
            ChannelCommand::Steer { .. } => "steer",
            ChannelCommand::Btw { .. } => "btw",
            ChannelCommand::Model { .. } => "model",
            ChannelCommand::Status { .. } => "status",
        }
    }

    pub fn into_intent(self) -> ChannelCommandIntent {
        match self {
            ChannelCommand::New { topic } => ChannelCommandIntent::StartNewSession { topic },
            ChannelCommand::Think { instruction } => {
                ChannelCommandIntent::SetThinkingMode { instruction }
            }
            ChannelCommand::Stop { reason } => ChannelCommandIntent::StopCurrentRun { reason },
            ChannelCommand::Steer { instruction } => {
                ChannelCommandIntent::AddSteering { instruction }
            }
            ChannelCommand::Btw { note } => ChannelCommandIntent::AddBtwNote { note },
            ChannelCommand::Model { target } => match target {
                Some(target) => ChannelCommandIntent::SwitchModel { target },
                None => ChannelCommandIntent::ShowModel,
            },
            ChannelCommand::Status { scope } => ChannelCommandIntent::ShowStatus { scope },
        }
    }
}

pub fn parse_channel_command(input: &str) -> Option<ChannelCommand> {
    let trimmed = input.trim();
    let command_text = trimmed.strip_prefix('/')?;
    let (name, rest) = split_command(command_text);

    match name.to_ascii_lowercase().as_str() {
        "new" => Some(ChannelCommand::New {
            topic: optional_text(rest),
        }),
        "think" => Some(ChannelCommand::Think {
            instruction: optional_text(rest),
        }),
        "stop" => Some(ChannelCommand::Stop {
            reason: optional_text(rest),
        }),
        "steer" => required_text(rest).map(|instruction| ChannelCommand::Steer { instruction }),
        "btw" => required_text(rest).map(|note| ChannelCommand::Btw { note }),
        "model" => Some(ChannelCommand::Model {
            target: optional_text(rest),
        }),
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

fn required_text(value: &str) -> Option<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
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
                instruction: Some("use compact reasoning".to_string())
            })
        );
        assert_eq!(
            parse_channel_command("/stop"),
            Some(ChannelCommand::Stop { reason: None })
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
                target: Some("openrouter/anthropic/claude-sonnet-4".to_string())
            })
        );
        assert_eq!(
            parse_channel_command("/model"),
            Some(ChannelCommand::Model { target: None })
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
            Some(ChannelCommandIntent::SetThinkingMode { instruction: None })
        );
        assert_eq!(
            parse_channel_command_intent("/stop user canceled"),
            Some(ChannelCommandIntent::StopCurrentRun {
                reason: Some("user canceled".to_string())
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
            Some(ChannelCommandIntent::ShowModel)
        );
        assert_eq!(
            parse_channel_command_intent("/model openrouter/anthropic/claude-sonnet-4"),
            Some(ChannelCommandIntent::SwitchModel {
                target: "openrouter/anthropic/claude-sonnet-4".to_string()
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
}
