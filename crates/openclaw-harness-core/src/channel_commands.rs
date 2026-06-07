#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChannelCommand {
    New { topic: Option<String> },
    Think { instruction: Option<String> },
    Stop { reason: Option<String> },
    Steer { instruction: String },
    Btw { note: String },
    Model { target: Option<String> },
    Status { scope: Option<String> },
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
    fn ignores_plain_messages_and_unknown_commands() {
        assert_eq!(parse_channel_command("hello"), None);
        assert_eq!(parse_channel_command("/unknown value"), None);
    }
}
