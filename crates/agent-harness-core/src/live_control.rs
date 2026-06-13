use std::env;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use base64::{Engine as _, engine::general_purpose::STANDARD};
use ring::digest;
use serde::{Deserialize, Serialize};
use serde_json::Value;

pub const LIVE_CONTROL_TOKEN_SCHEMA: &str = "agent-harness.live-control-token.v1";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum LiveControlAction {
    Stop,
    Start,
    Restart,
    Uninstall,
    KillProcess,
    ReplaceBinary,
    Status,
    Cutover,
}

impl LiveControlAction {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Stop => "stop",
            Self::Start => "start",
            Self::Restart => "restart",
            Self::Uninstall => "uninstall",
            Self::KillProcess => "kill-process",
            Self::ReplaceBinary => "replace-binary",
            Self::Status => "status",
            Self::Cutover => "cutover",
        }
    }

    pub fn destructive(self) -> bool {
        !matches!(self, Self::Status)
    }
}

impl std::str::FromStr for LiveControlAction {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match normalize_token(value).as_str() {
            "stop" => Ok(Self::Stop),
            "start" => Ok(Self::Start),
            "restart" => Ok(Self::Restart),
            "uninstall" => Ok(Self::Uninstall),
            "killprocess" | "kill" | "taskkill" | "stopprocess" => Ok(Self::KillProcess),
            "replacebinary" | "replace" | "deploy" | "buildlive" => Ok(Self::ReplaceBinary),
            "status" | "check" => Ok(Self::Status),
            "cutover" | "apply" => Ok(Self::Cutover),
            other => Err(format!("unsupported live-control action: {other}")),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LiveControlIntent {
    pub action: LiveControlAction,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LiveControlTokenRecord {
    pub schema: String,
    pub token_hash: String,
    pub ticket_id: Option<String>,
    pub action: LiveControlAction,
    pub issued_to: Option<String>,
    pub issued_at_ms: i64,
    pub expires_at_ms: i64,
    #[serde(default)]
    pub revoked: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum LiveControlTokenStatus {
    Valid,
    Missing,
    Invalid,
    Expired,
    Revoked,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LiveControlTokenValidation {
    pub status: LiveControlTokenStatus,
    pub reason: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ticket_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expires_at_ms: Option<i64>,
}

pub fn is_live_agent_session_env() -> bool {
    env_flag("AGENT_HARNESS_LIVE_SESSION")
}

pub fn env_live_control_token() -> Option<String> {
    env::var("AGENT_HARNESS_LIVE_CONTROL_TOKEN")
        .ok()
        .filter(|value| !value.trim().is_empty())
}

pub fn is_live_harness_home(path: &Path) -> bool {
    path.file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| name.eq_ignore_ascii_case(".agent-harness"))
}

pub fn live_control_tokens_file(harness_home: &Path) -> PathBuf {
    harness_home
        .join("state")
        .join("cutover")
        .join("live-control-tokens.jsonl")
}

pub fn hash_live_control_token(token: &str) -> String {
    STANDARD.encode(digest::digest(&digest::SHA256, token.as_bytes()).as_ref())
}

pub fn validate_live_control_token(
    harness_home: &Path,
    token: Option<&str>,
    action: LiveControlAction,
    now_ms: i64,
) -> io::Result<LiveControlTokenValidation> {
    let Some(token) = token.map(str::trim).filter(|token| !token.is_empty()) else {
        return Ok(LiveControlTokenValidation {
            status: LiveControlTokenStatus::Missing,
            reason: "live-control token is required".to_string(),
            ticket_id: None,
            expires_at_ms: None,
        });
    };
    let token_hash = hash_live_control_token(token);
    let mut last_match = None;
    let file = live_control_tokens_file(harness_home);
    match fs::read_to_string(&file) {
        Ok(text) => {
            for line in text.lines() {
                let trimmed = line.trim();
                if trimmed.is_empty() {
                    continue;
                }
                let Ok(record) = serde_json::from_str::<LiveControlTokenRecord>(trimmed) else {
                    continue;
                };
                if record.token_hash == token_hash
                    && (record.action == action || record.action == LiveControlAction::Cutover)
                {
                    last_match = Some(record);
                }
            }
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }

    let Some(record) = last_match else {
        return Ok(LiveControlTokenValidation {
            status: LiveControlTokenStatus::Invalid,
            reason: "live-control token was not found for this action".to_string(),
            ticket_id: None,
            expires_at_ms: None,
        });
    };
    if record.revoked {
        return Ok(LiveControlTokenValidation {
            status: LiveControlTokenStatus::Revoked,
            reason: "live-control token has been revoked".to_string(),
            ticket_id: record.ticket_id,
            expires_at_ms: Some(record.expires_at_ms),
        });
    }
    if now_ms > record.expires_at_ms {
        return Ok(LiveControlTokenValidation {
            status: LiveControlTokenStatus::Expired,
            reason: "live-control token has expired".to_string(),
            ticket_id: record.ticket_id,
            expires_at_ms: Some(record.expires_at_ms),
        });
    }
    Ok(LiveControlTokenValidation {
        status: LiveControlTokenStatus::Valid,
        reason: "live-control token is valid".to_string(),
        ticket_id: record.ticket_id,
        expires_at_ms: Some(record.expires_at_ms),
    })
}

pub fn classify_live_control_command(text: &str) -> Option<LiveControlIntent> {
    let normalized = normalize_text(text);
    if normalized.is_empty() {
        return None;
    }
    if normalized.contains("gateway status")
        || normalized.contains("gateway ps")
        || normalized.contains("gateway logs")
        || normalized.contains("gateway tail")
        || normalized.contains("ops-control status")
        || normalized.contains("ops-cutover-status")
    {
        return None;
    }
    if normalized.contains("harness.ps1")
        && (normalized.contains("gateway stop")
            || normalized.contains("gateway start")
            || normalized.contains("gateway restart"))
    {
        let action = if normalized.contains("gateway restart") {
            LiveControlAction::Restart
        } else if normalized.contains("gateway start") {
            LiveControlAction::Start
        } else {
            LiveControlAction::Stop
        };
        return Some(LiveControlIntent {
            action,
            reason: "harness.ps1 live gateway stop/restart".to_string(),
        });
    }
    if normalized.contains("ops-cutover-approve") {
        return Some(LiveControlIntent {
            action: LiveControlAction::Cutover,
            reason: "live-control token approval from agent session".to_string(),
        });
    }
    if normalized.contains("start-scheduled-tasks.ps1") {
        return Some(LiveControlIntent {
            action: LiveControlAction::Start,
            reason: "live supervisor start script".to_string(),
        });
    }
    if normalized.contains("stop-scheduled-tasks.ps1") {
        return Some(LiveControlIntent {
            action: LiveControlAction::Stop,
            reason: "live supervisor stop script".to_string(),
        });
    }
    if normalized.contains("uninstall-scheduled-tasks.ps1") {
        return Some(LiveControlIntent {
            action: LiveControlAction::Uninstall,
            reason: "live supervisor uninstall script".to_string(),
        });
    }
    if normalized.contains("ops-control stop") || normalized.contains("ops-control start") {
        let action = if normalized.contains("ops-control start") {
            LiveControlAction::Start
        } else {
            LiveControlAction::Stop
        };
        return Some(LiveControlIntent {
            action,
            reason: "ops-control live stop/start".to_string(),
        });
    }
    if normalized.contains("start-scheduledtask") {
        return Some(LiveControlIntent {
            action: LiveControlAction::Start,
            reason: "Windows scheduled task start".to_string(),
        });
    }
    if normalized.contains("stop-scheduledtask") {
        return Some(LiveControlIntent {
            action: LiveControlAction::Stop,
            reason: "Windows scheduled task stop".to_string(),
        });
    }
    if normalized.contains("register-scheduledtask") {
        let action = if normalized.contains("unregister-scheduledtask") {
            LiveControlAction::Uninstall
        } else {
            LiveControlAction::Start
        };
        return Some(LiveControlIntent {
            action,
            reason: "Windows scheduled task registration change".to_string(),
        });
    }
    if (normalized.contains("stop-process") || normalized.contains("taskkill"))
        && normalized.contains("agent-harness")
    {
        return Some(LiveControlIntent {
            action: LiveControlAction::KillProcess,
            reason: "live agent-harness process termination".to_string(),
        });
    }
    if normalized.contains("windows-scheduled-tasks")
        && normalized.contains(".stop")
        && (normalized.contains("new-item")
            || normalized.contains("set-content")
            || normalized.contains("out-file")
            || normalized.contains("writealltext")
            || normalized.contains("remove-item"))
    {
        let action = if normalized.contains("remove-item") {
            LiveControlAction::Start
        } else {
            LiveControlAction::Stop
        };
        return Some(LiveControlIntent {
            action,
            reason: "direct live supervisor stop-file mutation".to_string(),
        });
    }
    if normalized.contains("target/debug/agent-harness.exe")
        && (normalized.contains("copy-item")
            || normalized.contains("move-item")
            || normalized.contains("remove-item")
            || normalized.contains("set-content")
            || normalized.contains("writeallbytes"))
    {
        return Some(LiveControlIntent {
            action: LiveControlAction::ReplaceBinary,
            reason: "live agent-harness binary replacement".to_string(),
        });
    }
    None
}

pub fn classify_approval_request(value: &Value) -> Option<LiveControlIntent> {
    classify_live_control_command(&value.to_string())
}

fn env_flag(name: &str) -> bool {
    env::var(name).ok().is_some_and(|value| {
        matches!(
            normalize_token(&value).as_str(),
            "1" | "true" | "yes" | "on" | "live"
        )
    })
}

fn normalize_text(value: &str) -> String {
    value
        .replace('\\', "/")
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_ascii_lowercase()
}

fn normalize_token(value: &str) -> String {
    value
        .trim()
        .chars()
        .filter(|ch| ch.is_ascii_alphanumeric())
        .flat_map(char::to_lowercase)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn classifies_live_gateway_restart_but_not_status() {
        assert!(classify_live_control_command(".\\harness.ps1 gateway restart").is_some());
        assert!(classify_live_control_command(".\\harness.ps1 gateway stop").is_some());
        assert!(classify_live_control_command(".\\harness.ps1 gateway start").is_some());
        assert!(classify_live_control_command(".\\harness.ps1 gateway status").is_none());
    }

    #[test]
    fn classifies_process_and_stop_file_controls() {
        assert!(classify_live_control_command("Stop-Process -Name agent-harness -Force").is_some());
        assert!(
            classify_live_control_command(
                "New-Item -Path .agent-harness/state/supervisor/windows-scheduled-tasks/stop/runtime-loop.stop"
            )
            .is_some()
        );
        assert!(
            classify_live_control_command(
                "Remove-Item -LiteralPath .agent-harness/state/supervisor/windows-scheduled-tasks/stop/runtime-loop.stop"
            )
            .is_some()
        );
    }

    #[test]
    fn classifies_scheduler_control_and_token_mint() {
        assert!(
            classify_live_control_command(
                "Start-ScheduledTask -TaskName AgentHarness-runtime-loop"
            )
            .is_some()
        );
        assert!(
            classify_live_control_command(
                "Unregister-ScheduledTask -TaskName AgentHarness-runtime-loop"
            )
            .is_some()
        );
        assert!(
            classify_live_control_command(
                "agent-harness.exe ops-cutover-approve --ticket-id cutover-1"
            )
            .is_some()
        );
        assert!(
            classify_live_control_command("agent-harness.exe ops-cutover-status --action cutover")
                .is_none()
        );
    }

    #[test]
    fn classifies_live_binary_replacement() {
        assert!(
            classify_live_control_command(
                "Copy-Item .\\target\\staging-build\\debug\\agent-harness.exe .\\target\\debug\\agent-harness.exe"
            )
            .is_some()
        );
    }

    #[test]
    fn validates_live_control_token_record() {
        let root = temp_root("validates_live_control_token_record");
        let home = root.join(".agent-harness");
        fs::create_dir_all(home.join("state").join("cutover")).unwrap();
        let token = "test-token";
        let record = LiveControlTokenRecord {
            schema: LIVE_CONTROL_TOKEN_SCHEMA.to_string(),
            token_hash: hash_live_control_token(token),
            ticket_id: Some("ticket-1".to_string()),
            action: LiveControlAction::Stop,
            issued_to: Some("operator".to_string()),
            issued_at_ms: 1000,
            expires_at_ms: 2000,
            revoked: false,
            reason: None,
        };
        fs::write(
            live_control_tokens_file(&home),
            format!("{}\n", serde_json::to_string(&record).unwrap()),
        )
        .unwrap();

        let valid =
            validate_live_control_token(&home, Some(token), LiveControlAction::Stop, 1500).unwrap();
        assert_eq!(valid.status, LiveControlTokenStatus::Valid);

        let expired =
            validate_live_control_token(&home, Some(token), LiveControlAction::Stop, 2500).unwrap();
        assert_eq!(expired.status, LiveControlTokenStatus::Expired);

        let _ = fs::remove_dir_all(root);
    }

    fn temp_root(test_name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "agent-harness-live-control-{test_name}-{}-{nanos}",
            std::process::id()
        ))
    }
}
