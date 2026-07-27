use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io;
use std::path::Path;

use ring::digest;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::config::harness_config_candidates;

pub const EXTERNAL_CAPABILITY_DECLARATION_SCHEMA: &str =
    "agent-harness.external-capability-declaration.v1";
pub const CODEX_EXTERNAL_CAPABILITY_READINESS_SCHEMA: &str =
    "agent-harness.codex-external-capability-readiness.v1";
const MAX_MCP_STATUS_PAGES: usize = 16;
const MAX_MCP_SERVERS: usize = 1_024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ExternalCapabilityTransportV1 {
    Stdio,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ExternalCapabilityRequirementV1 {
    Required,
    Optional,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ExternalMcpCapabilityDeclarationV1 {
    pub server_id: String,
    pub transport: ExternalCapabilityTransportV1,
    pub expected_tool_schema_digest: String,
    #[serde(default)]
    pub expected_server_info_digest: Option<String>,
    pub requirement: ExternalCapabilityRequirementV1,
    pub agent_id: String,
    pub provider: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ExternalCapabilitiesConfigV1 {
    #[serde(default)]
    mcp_servers: Vec<ExternalMcpCapabilityDeclarationV1>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum CodexExternalCapabilityStatusV1 {
    Ready,
    AuthenticationRequired,
    Missing,
    ServerInfoMismatch,
    SchemaMismatch,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CodexExternalCapabilityReadinessEntryV1 {
    pub server_id: String,
    pub requirement: ExternalCapabilityRequirementV1,
    pub status: CodexExternalCapabilityStatusV1,
    pub expected_tool_schema_digest: String,
    pub observed_tool_schema_digest: Option<String>,
    pub expected_server_info_digest: Option<String>,
    pub observed_server_info_digest: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CodexExternalCapabilityReadinessReceiptV1 {
    pub schema: String,
    pub admitted: bool,
    pub agent_id_digest: String,
    pub provider_digest: String,
    pub codex_home_digest: String,
    pub config_digest: String,
    pub backend_executable_digest: String,
    pub declaration_digest: String,
    pub process_generation: String,
    pub thread_id_digest: String,
    pub page_count: usize,
    pub entries: Vec<CodexExternalCapabilityReadinessEntryV1>,
    pub warnings: Vec<String>,
    pub observed_at_ms: i64,
}

#[derive(Debug, Clone)]
pub struct CodexExternalCapabilityReadinessInput<'a> {
    pub declarations: &'a [ExternalMcpCapabilityDeclarationV1],
    pub agent_id: &'a str,
    pub provider: &'a str,
    pub codex_home: &'a Path,
    pub config_digest: &'a str,
    pub backend_executable_digest: &'a str,
    pub process_generation: &'a str,
    pub thread_id: &'a str,
    pub pages: &'a [Value],
    pub observed_at_ms: i64,
}

pub fn load_external_mcp_capability_declarations(
    harness_home: &Path,
) -> io::Result<Vec<ExternalMcpCapabilityDeclarationV1>> {
    let Some(config_file) = harness_config_candidates(harness_home)
        .into_iter()
        .find(|path| path.is_file())
    else {
        return Ok(Vec::new());
    };
    let root: Value =
        serde_json::from_slice(&fs::read(config_file)?).map_err(invalid_data_error)?;
    let Some(section) = root.get("externalCapabilities") else {
        return Ok(Vec::new());
    };
    let section: ExternalCapabilitiesConfigV1 =
        serde_json::from_value(section.clone()).map_err(invalid_data_error)?;
    validate_external_mcp_capability_declarations(&section.mcp_servers)?;
    Ok(section.mcp_servers)
}

pub fn validate_external_mcp_capability_declarations(
    declarations: &[ExternalMcpCapabilityDeclarationV1],
) -> io::Result<()> {
    let mut identities = BTreeSet::new();
    for declaration in declarations {
        for (label, value) in [
            ("serverId", declaration.server_id.as_str()),
            ("agentId", declaration.agent_id.as_str()),
            ("provider", declaration.provider.as_str()),
        ] {
            if value.is_empty()
                || value.len() > 128
                || !value
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("{label} must be a bounded safe capability identity"),
                ));
            }
        }
        require_sha256_digest(&declaration.expected_tool_schema_digest)?;
        if let Some(digest) = declaration.expected_server_info_digest.as_deref() {
            require_sha256_digest(digest)?;
        }
        let identity = (
            declaration.agent_id.to_ascii_lowercase(),
            declaration.provider.to_ascii_lowercase(),
            declaration.server_id.clone(),
        );
        if !identities.insert(identity) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "external MCP capability declarations contain a duplicate agent/provider/server identity",
            ));
        }
    }
    Ok(())
}

pub fn declaration_digest(
    declarations: &[ExternalMcpCapabilityDeclarationV1],
) -> io::Result<String> {
    validate_external_mcp_capability_declarations(declarations)?;
    let mut declarations = declarations.to_vec();
    declarations.sort_by(|left, right| {
        (
            left.agent_id.as_str(),
            left.provider.as_str(),
            left.server_id.as_str(),
        )
            .cmp(&(
                right.agent_id.as_str(),
                right.provider.as_str(),
                right.server_id.as_str(),
            ))
    });
    serde_json::to_vec(&declarations)
        .map(|bytes| sha256_prefixed(&bytes))
        .map_err(io::Error::other)
}

pub fn canonical_mcp_tool_schema_digest(server_status: &Value) -> io::Result<String> {
    let server_name = server_status
        .get("name")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "MCP status entry has no server name",
            )
        })?;
    let tools = server_status
        .get("tools")
        .and_then(Value::as_object)
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "MCP status entry has no tool catalog",
            )
        })?;
    let mut projected_tools = BTreeMap::new();
    for (catalog_key, tool) in tools {
        let name = tool
            .get("name")
            .and_then(Value::as_str)
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("MCP tool {catalog_key} has no name"),
                )
            })?;
        let input_schema = tool.get("inputSchema").ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("MCP tool {catalog_key} has no inputSchema"),
            )
        })?;
        projected_tools.insert(
            catalog_key.clone(),
            json!({
                "name": name,
                "description": tool.get("description").cloned().unwrap_or(Value::Null),
                "inputSchema": canonical_json(input_schema),
                "outputSchema": tool.get("outputSchema").map(canonical_json).unwrap_or(Value::Null),
            }),
        );
    }
    let canonical = json!({
        "serverId": server_name,
        "tools": projected_tools,
    });
    serde_json::to_vec(&canonical)
        .map(|bytes| sha256_prefixed(&bytes))
        .map_err(io::Error::other)
}

pub fn canonical_mcp_server_info_digest(server_status: &Value) -> io::Result<String> {
    let server_info = server_status
        .get("serverInfo")
        .and_then(Value::as_object)
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "MCP status entry has no serverInfo object",
            )
        })?;
    let name = server_info
        .get("name")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "MCP serverInfo has no bounded name",
            )
        })?;
    let version = server_info
        .get("version")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "MCP serverInfo has no bounded version",
            )
        })?;
    if name.len() > 256 || version.len() > 256 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "MCP serverInfo identity exceeds the bounded digest input",
        ));
    }
    serde_json::to_vec(&json!({"name": name, "version": version}))
        .map(|bytes| sha256_prefixed(&bytes))
        .map_err(io::Error::other)
}

pub fn evaluate_codex_external_capability_readiness(
    input: CodexExternalCapabilityReadinessInput<'_>,
) -> io::Result<CodexExternalCapabilityReadinessReceiptV1> {
    validate_external_mcp_capability_declarations(input.declarations)?;
    require_sha256_digest(input.config_digest)?;
    require_sha256_digest(input.backend_executable_digest)?;
    if input.pages.len() > MAX_MCP_STATUS_PAGES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "MCP status pagination exceeded the bounded page limit",
        ));
    }
    let selected = input
        .declarations
        .iter()
        .filter(|declaration| {
            declaration.agent_id.eq_ignore_ascii_case(input.agent_id)
                && declaration.provider.eq_ignore_ascii_case(input.provider)
        })
        .cloned()
        .collect::<Vec<_>>();
    let declaration_digest = declaration_digest(&selected)?;
    let mut status_by_name = BTreeMap::<String, Value>::new();
    let mut rows = 0usize;
    for (index, page) in input.pages.iter().enumerate() {
        let data = page.get("data").and_then(Value::as_array).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("MCP status page {index} has no data array"),
            )
        })?;
        rows = rows.saturating_add(data.len());
        if rows > MAX_MCP_SERVERS {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "MCP status pagination exceeded the bounded server limit",
            ));
        }
        for status in data {
            let name = status
                .get("name")
                .and_then(Value::as_str)
                .filter(|value| !value.trim().is_empty())
                .ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidData, "MCP status row has no name")
                })?;
            if status_by_name
                .insert(name.to_string(), status.clone())
                .is_some()
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("MCP status pagination repeated server {name}"),
                ));
            }
        }
        let next_cursor = page.get("nextCursor").unwrap_or(&Value::Null);
        let final_page = index + 1 == input.pages.len();
        if final_page != next_cursor.is_null() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "MCP status pagination cursor chain is incomplete or over-collected",
            ));
        }
    }
    if !selected.is_empty() && input.pages.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "declared MCP capabilities require at least one status page",
        ));
    }

    let mut entries = Vec::new();
    let mut warnings = Vec::new();
    let mut admitted = true;
    for declaration in selected {
        let (status, observed_tool_schema_digest, observed_server_info_digest) =
            if let Some(server) = status_by_name.get(&declaration.server_id) {
                let observed_server_info_digest = server
                    .get("serverInfo")
                    .filter(|value| !value.is_null())
                    .map(|_| canonical_mcp_server_info_digest(server))
                    .transpose()?;
                let auth_status = server
                    .get("authStatus")
                    .and_then(Value::as_str)
                    .ok_or_else(|| {
                        io::Error::new(
                            io::ErrorKind::InvalidData,
                            "MCP status row has no authentication status",
                        )
                    })?;
                match auth_status {
                    "notLoggedIn" => (
                        CodexExternalCapabilityStatusV1::AuthenticationRequired,
                        None,
                        observed_server_info_digest,
                    ),
                    "unsupported" | "bearerToken" | "oAuth" => {
                        let observed = canonical_mcp_tool_schema_digest(server)?;
                        if declaration.expected_server_info_digest.is_some()
                            && declaration.expected_server_info_digest.as_deref()
                                != observed_server_info_digest.as_deref()
                        {
                            (
                                CodexExternalCapabilityStatusV1::ServerInfoMismatch,
                                Some(observed),
                                observed_server_info_digest,
                            )
                        } else if observed == declaration.expected_tool_schema_digest {
                            (
                                CodexExternalCapabilityStatusV1::Ready,
                                Some(observed),
                                observed_server_info_digest,
                            )
                        } else {
                            (
                                CodexExternalCapabilityStatusV1::SchemaMismatch,
                                Some(observed),
                                observed_server_info_digest,
                            )
                        }
                    }
                    _ => {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "MCP status row has an unsupported authentication status",
                        ));
                    }
                }
            } else {
                (CodexExternalCapabilityStatusV1::Missing, None, None)
            };
        if status != CodexExternalCapabilityStatusV1::Ready {
            if declaration.requirement == ExternalCapabilityRequirementV1::Required {
                admitted = false;
            } else {
                warnings.push(format!(
                    "optional external MCP capability {} is not ready ({status:?})",
                    declaration.server_id
                ));
            }
        }
        entries.push(CodexExternalCapabilityReadinessEntryV1 {
            server_id: declaration.server_id,
            requirement: declaration.requirement,
            status,
            expected_tool_schema_digest: declaration.expected_tool_schema_digest,
            observed_tool_schema_digest,
            expected_server_info_digest: declaration.expected_server_info_digest,
            observed_server_info_digest,
        });
    }
    Ok(CodexExternalCapabilityReadinessReceiptV1 {
        schema: CODEX_EXTERNAL_CAPABILITY_READINESS_SCHEMA.to_string(),
        admitted,
        agent_id_digest: identity_digest(input.agent_id),
        provider_digest: identity_digest(input.provider),
        codex_home_digest: identity_digest(&input.codex_home.to_string_lossy()),
        config_digest: input.config_digest.to_string(),
        backend_executable_digest: input.backend_executable_digest.to_string(),
        declaration_digest,
        process_generation: input.process_generation.to_string(),
        thread_id_digest: identity_digest(input.thread_id),
        page_count: input.pages.len(),
        entries,
        warnings,
        observed_at_ms: input.observed_at_ms,
    })
}

fn canonical_json(value: &Value) -> Value {
    match value {
        Value::Array(values) => Value::Array(values.iter().map(canonical_json).collect()),
        Value::Object(values) => {
            let values = values
                .iter()
                .map(|(key, value)| (key.clone(), canonical_json(value)))
                .collect::<BTreeMap<_, _>>();
            serde_json::to_value(values).expect("BTreeMap JSON serialization cannot fail")
        }
        value => value.clone(),
    }
}

fn require_sha256_digest(value: &str) -> io::Result<()> {
    let Some(hex) = value.strip_prefix("sha256:") else {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "capability digest must use the sha256: prefix",
        ));
    };
    if hex.len() != 64
        || !hex
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "capability digest must contain 64 lowercase hexadecimal characters",
        ));
    }
    Ok(())
}

fn identity_digest(value: &str) -> String {
    sha256_prefixed(value.as_bytes())
}

fn sha256_prefixed(value: &[u8]) -> String {
    let digest = digest::digest(&digest::SHA256, value);
    let mut result = String::with_capacity(71);
    result.push_str("sha256:");
    for byte in digest.as_ref() {
        use std::fmt::Write as _;
        write!(&mut result, "{byte:02x}").expect("writing a digest to String cannot fail");
    }
    result
}

fn invalid_data_error(error: impl std::error::Error + Send + Sync + 'static) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, error)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn server(name: &str, description: &str) -> Value {
        json!({
            "name": name,
            "authStatus": "unsupported",
            "serverInfo": {
                "name": name,
                "version": "1.0.0"
            },
            "tools": {
                "echo": {
                    "name": "echo",
                    "description": description,
                    "inputSchema": {
                        "required": ["value"],
                        "properties": {"value": {"type": "string"}},
                        "type": "object"
                    },
                    "outputSchema": {
                        "properties": {"value": {"type": "string"}},
                        "type": "object"
                    }
                }
            }
        })
    }

    fn declaration(
        server_id: &str,
        digest: String,
        requirement: ExternalCapabilityRequirementV1,
    ) -> ExternalMcpCapabilityDeclarationV1 {
        ExternalMcpCapabilityDeclarationV1 {
            server_id: server_id.to_string(),
            transport: ExternalCapabilityTransportV1::Stdio,
            expected_tool_schema_digest: digest,
            expected_server_info_digest: None,
            requirement,
            agent_id: "main".to_string(),
            provider: "openai".to_string(),
        }
    }

    #[test]
    fn canonical_tool_digest_ignores_object_key_order_but_detects_schema_change() {
        let first = server("generic-mock", "fixture");
        let reordered = json!({
            "authStatus": "unsupported",
            "serverInfo": {
                "version": "1.0.0",
                "name": "generic-mock"
            },
            "tools": {
                "echo": {
                    "outputSchema": {"type": "object", "properties": {"value": {"type": "string"}}},
                    "inputSchema": {"type": "object", "properties": {"value": {"type": "string"}}, "required": ["value"]},
                    "description": "fixture",
                    "name": "echo"
                }
            },
            "name": "generic-mock"
        });
        assert_eq!(
            canonical_mcp_tool_schema_digest(&first).unwrap(),
            canonical_mcp_tool_schema_digest(&reordered).unwrap()
        );
        assert_ne!(
            canonical_mcp_tool_schema_digest(&first).unwrap(),
            canonical_mcp_tool_schema_digest(&server("generic-mock", "changed")).unwrap()
        );
    }

    #[test]
    fn required_mismatch_blocks_while_optional_mismatch_only_warns() {
        let observed = server("generic-mock", "fixture");
        let expected = canonical_mcp_tool_schema_digest(&observed).unwrap();
        let changed = canonical_mcp_tool_schema_digest(&server("generic-mock", "changed")).unwrap();
        let pages = vec![json!({"data": [observed], "nextCursor": null})];
        for (requirement, admitted) in [
            (ExternalCapabilityRequirementV1::Required, false),
            (ExternalCapabilityRequirementV1::Optional, true),
        ] {
            let declarations = vec![declaration("generic-mock", changed.clone(), requirement)];
            let receipt = evaluate_codex_external_capability_readiness(
                CodexExternalCapabilityReadinessInput {
                    declarations: &declarations,
                    agent_id: "main",
                    provider: "openai",
                    codex_home: Path::new("fixture-home"),
                    config_digest: &expected,
                    backend_executable_digest: &expected,
                    process_generation: "fixture-process",
                    thread_id: "fixture-thread",
                    pages: &pages,
                    observed_at_ms: 1,
                },
            )
            .unwrap();
            assert_eq!(receipt.admitted, admitted);
            assert_eq!(
                receipt.entries[0].status,
                CodexExternalCapabilityStatusV1::SchemaMismatch
            );
            assert_eq!(receipt.warnings.is_empty(), !admitted);
        }
    }

    #[test]
    fn authenticated_tool_catalog_is_not_ready_while_login_is_missing() {
        let mut observed = server("generic-mock", "fixture");
        let expected = canonical_mcp_tool_schema_digest(&observed).unwrap();
        observed["authStatus"] = json!("notLoggedIn");
        let pages = vec![json!({"data": [observed], "nextCursor": null})];
        for (requirement, admitted) in [
            (ExternalCapabilityRequirementV1::Required, false),
            (ExternalCapabilityRequirementV1::Optional, true),
        ] {
            let declarations = vec![declaration("generic-mock", expected.clone(), requirement)];
            let receipt = evaluate_codex_external_capability_readiness(
                CodexExternalCapabilityReadinessInput {
                    declarations: &declarations,
                    agent_id: "main",
                    provider: "openai",
                    codex_home: Path::new("fixture-home"),
                    config_digest: &expected,
                    backend_executable_digest: &expected,
                    process_generation: "fixture-process",
                    thread_id: "fixture-thread",
                    pages: &pages,
                    observed_at_ms: 1,
                },
            )
            .unwrap();
            assert_eq!(receipt.admitted, admitted);
            assert_eq!(
                receipt.entries[0].status,
                CodexExternalCapabilityStatusV1::AuthenticationRequired
            );
            assert!(receipt.entries[0].observed_tool_schema_digest.is_none());
            assert_eq!(receipt.warnings.is_empty(), !admitted);
        }
    }

    #[test]
    fn optional_server_info_digest_blocks_stale_server_version() {
        let observed = server("generic-mock", "fixture");
        let expected_tools = canonical_mcp_tool_schema_digest(&observed).unwrap();
        let expected_server = canonical_mcp_server_info_digest(&observed).unwrap();
        let mut stale = observed.clone();
        stale["serverInfo"]["version"] = json!("0.9.0");
        let pages = vec![json!({"data": [stale], "nextCursor": null})];
        let mut declaration = declaration(
            "generic-mock",
            expected_tools.clone(),
            ExternalCapabilityRequirementV1::Required,
        );
        declaration.expected_server_info_digest = Some(expected_server.clone());
        let receipt =
            evaluate_codex_external_capability_readiness(CodexExternalCapabilityReadinessInput {
                declarations: &[declaration],
                agent_id: "main",
                provider: "openai",
                codex_home: Path::new("fixture-home"),
                config_digest: &expected_tools,
                backend_executable_digest: &expected_tools,
                process_generation: "fixture-process",
                thread_id: "fixture-thread",
                pages: &pages,
                observed_at_ms: 1,
            })
            .unwrap();
        assert!(!receipt.admitted);
        assert_eq!(
            receipt.entries[0].status,
            CodexExternalCapabilityStatusV1::ServerInfoMismatch
        );
        assert_eq!(
            receipt.entries[0].expected_server_info_digest.as_deref(),
            Some(expected_server.as_str())
        );
        assert_ne!(
            receipt.entries[0].observed_server_info_digest,
            receipt.entries[0].expected_server_info_digest
        );
    }

    #[test]
    fn pagination_and_cross_agent_evidence_fail_closed() {
        let observed = server("generic-mock", "fixture");
        let expected = canonical_mcp_tool_schema_digest(&observed).unwrap();
        let declarations = vec![declaration(
            "generic-mock",
            expected.clone(),
            ExternalCapabilityRequirementV1::Required,
        )];
        let incomplete = vec![json!({"data": [], "nextCursor": "cursor"})];
        assert!(
            evaluate_codex_external_capability_readiness(CodexExternalCapabilityReadinessInput {
                declarations: &declarations,
                agent_id: "main",
                provider: "openai",
                codex_home: Path::new("fixture-home"),
                config_digest: &expected,
                backend_executable_digest: &expected,
                process_generation: "fixture-process",
                thread_id: "fixture-thread",
                pages: &incomplete,
                observed_at_ms: 1,
            })
            .is_err()
        );
        let pages = vec![json!({"data": [observed], "nextCursor": null})];
        let receipt =
            evaluate_codex_external_capability_readiness(CodexExternalCapabilityReadinessInput {
                declarations: &declarations,
                agent_id: "other-agent",
                provider: "openai",
                codex_home: Path::new("fixture-home"),
                config_digest: &expected,
                backend_executable_digest: &expected,
                process_generation: "fixture-process",
                thread_id: "fixture-thread",
                pages: &pages,
                observed_at_ms: 1,
            })
            .unwrap();
        assert!(receipt.entries.is_empty());
        assert!(receipt.admitted);
    }
}
