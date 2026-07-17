use std::collections::BTreeSet;
use std::io;

use serde::Serialize;
use serde_json::Value;

use crate::REQUIRED_CODEX_BACKEND_VERSION;

pub const CODEX_PROTOCOL_COMPATIBILITY_SCHEMA: &str =
    "agent-harness.codex-protocol-compatibility.v1";
const CODEX_0_144_5_FIXTURE: &str =
    include_str!("../tests/fixtures/round20/codex-0.144.5-protocol.json");

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CodexProtocolCompatibilityReportV1 {
    pub schema: String,
    pub codex_version: String,
    pub request_count: usize,
    pub response_count: usize,
    pub notification_count: usize,
    pub observed_model_context_window: u64,
    pub compact_resume_race_owned_by_harness: bool,
}

pub fn validate_codex_0_144_5_protocol_fixture() -> io::Result<CodexProtocolCompatibilityReportV1> {
    let fixture: Value = serde_json::from_str(CODEX_0_144_5_FIXTURE).map_err(io::Error::other)?;
    require_string(&fixture, "/schema", CODEX_PROTOCOL_COMPATIBILITY_SCHEMA)?;
    require_string(&fixture, "/codexVersion", REQUIRED_CODEX_BACKEND_VERSION)?;
    let requests = require_array(&fixture, "/requests")?;
    let responses = require_array(&fixture, "/responses")?;
    let notifications = require_array(&fixture, "/notifications")?;

    let request_methods = requests
        .iter()
        .filter_map(|value| value.get("method").and_then(Value::as_str))
        .collect::<BTreeSet<_>>();
    for method in [
        "initialize",
        "initialized",
        "thread/start",
        "thread/resume",
        "turn/start",
        "thread/compact/start",
        "thread/goal/set",
        "thread/goal/get",
        "account/read",
        "account/login/start",
        "account/login/cancel",
        "account/logout",
        "model/list",
        "config/read",
        "modelProvider/capabilities/read",
    ] {
        if !request_methods.contains(method) {
            return Err(io::Error::other(format!(
                "0.144.5 fixture is missing request method {method}"
            )));
        }
    }

    let initialize = responses
        .iter()
        .find(|value| value.get("id").and_then(Value::as_i64) == Some(0))
        .ok_or_else(|| io::Error::other("0.144.5 fixture has no initialize response"))?;
    if !initialize
        .pointer("/result/userAgent")
        .and_then(Value::as_str)
        .is_some_and(|value| value.contains(REQUIRED_CODEX_BACKEND_VERSION))
        || initialize
            .pointer("/result/codexHome")
            .and_then(Value::as_str)
            != Some("<CODEX_HOME>")
    {
        return Err(io::Error::other(
            "initialize fixture is not exact-version and privacy-sanitized",
        ));
    }
    let account_read = responses
        .iter()
        .find(|value| value.get("id").and_then(Value::as_i64) == Some(7))
        .ok_or_else(|| io::Error::other("0.144.5 fixture has no account/read response"))?;
    if account_read.pointer("/result/account") != Some(&Value::Null)
        || account_read
            .pointer("/result/requiresOpenaiAuth")
            .and_then(Value::as_bool)
            != Some(true)
    {
        return Err(io::Error::other(
            "account/read unauthenticated fixture shape drifted",
        ));
    }
    let provider_capability = responses
        .iter()
        .find(|value| value.get("id").and_then(Value::as_i64) == Some(15))
        .ok_or_else(|| {
            io::Error::other("0.144.5 fixture has no model provider capability response")
        })?;
    if provider_capability
        .pointer("/result/webSearch")
        .and_then(Value::as_bool)
        != Some(true)
    {
        return Err(io::Error::other(
            "model provider capability fixture omitted webSearch=true",
        ));
    }

    let usage_event = notifications
        .iter()
        .find(|value| {
            value.get("method").and_then(Value::as_str) == Some("thread/tokenUsage/updated")
        })
        .ok_or_else(|| io::Error::other("0.144.5 fixture has no token-usage notification"))?;
    let observed_model_context_window = model_context_window_from_protocol_event(usage_event)
        .ok_or_else(|| io::Error::other("modelContextWindow was lost from token-usage fixture"))?;
    if observed_model_context_window != 258_400 {
        return Err(io::Error::other(format!(
            "expected modelContextWindow 258400, observed {observed_model_context_window}"
        )));
    }

    for method in [
        "thread/goal/updated",
        "item/completed",
        "thread/compacted",
        "turn/completed",
        "item/started",
    ] {
        if !notifications
            .iter()
            .any(|value| value.get("method").and_then(Value::as_str) == Some(method))
        {
            return Err(io::Error::other(format!(
                "0.144.5 fixture is missing notification method {method}"
            )));
        }
    }
    let web_item = notifications.iter().find(|value| {
        value.pointer("/params/item/type").and_then(Value::as_str) == Some("webSearch")
    });
    if web_item.is_none() {
        return Err(io::Error::other("0.144.5 fixture has no webSearch item"));
    }

    let compact_resume_race_owned_by_harness = fixture
        .pointer("/raceExpectation/harnessCorrelationStillRequired")
        .and_then(Value::as_bool)
        == Some(true)
        && fixture
            .pointer("/raceExpectation/unattributedInterruptedNullMustNotCompleteCompact")
            .and_then(Value::as_bool)
            == Some(true);
    if !compact_resume_race_owned_by_harness {
        return Err(io::Error::other(
            "compact/resume fixture incorrectly delegates correlation to Codex",
        ));
    }

    if CODEX_0_144_5_FIXTURE.contains("sk-")
        || CODEX_0_144_5_FIXTURE.contains("accessToken")
        || CODEX_0_144_5_FIXTURE.contains("authUrl")
        || CODEX_0_144_5_FIXTURE.contains("verificationUrl")
        || CODEX_0_144_5_FIXTURE.contains("discord")
    {
        return Err(io::Error::other(
            "protocol compatibility fixture contains forbidden credential/private material",
        ));
    }

    Ok(CodexProtocolCompatibilityReportV1 {
        schema: CODEX_PROTOCOL_COMPATIBILITY_SCHEMA.to_string(),
        codex_version: REQUIRED_CODEX_BACKEND_VERSION.to_string(),
        request_count: requests.len(),
        response_count: responses.len(),
        notification_count: notifications.len(),
        observed_model_context_window,
        compact_resume_race_owned_by_harness,
    })
}

pub fn model_context_window_from_protocol_event(value: &Value) -> Option<u64> {
    [
        "/params/tokenUsage/modelContextWindow",
        "/params/token_usage/model_context_window",
        "/params/usage/modelContextWindow",
        "/params/turn/usage/modelContextWindow",
        "/result/modelContextWindow",
    ]
    .iter()
    .find_map(|pointer| value.pointer(pointer).and_then(json_u64))
}

fn json_u64(value: &Value) -> Option<u64> {
    value
        .as_u64()
        .or_else(|| value.as_i64().and_then(|number| number.try_into().ok()))
}

fn require_array<'a>(value: &'a Value, pointer: &str) -> io::Result<&'a Vec<Value>> {
    value
        .pointer(pointer)
        .and_then(Value::as_array)
        .ok_or_else(|| io::Error::other(format!("fixture field {pointer} must be an array")))
}

fn require_string(value: &Value, pointer: &str, expected: &str) -> io::Result<()> {
    if value.pointer(pointer).and_then(Value::as_str) == Some(expected) {
        Ok(())
    } else {
        Err(io::Error::other(format!(
            "fixture field {pointer} must equal {expected}"
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_0_144_5_protocol_fixture_covers_cross_cutting_contracts() {
        let report = validate_codex_0_144_5_protocol_fixture().unwrap();
        assert_eq!(report.codex_version, "0.144.5");
        assert_eq!(report.observed_model_context_window, 258_400);
        assert!(report.compact_resume_race_owned_by_harness);
        assert!(report.request_count >= 16);
        assert!(report.response_count >= 11);
        assert!(report.notification_count >= 6);
    }

    #[test]
    fn model_context_window_parser_accepts_backend_camel_case_and_legacy_snake_case() {
        assert_eq!(
            model_context_window_from_protocol_event(&serde_json::json!({
                "params": {"tokenUsage": {"modelContextWindow": 258400}}
            })),
            Some(258_400)
        );
        assert_eq!(
            model_context_window_from_protocol_event(&serde_json::json!({
                "params": {"token_usage": {"model_context_window": 120000}}
            })),
            Some(120_000)
        );
    }
}
