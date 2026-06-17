use std::collections::BTreeSet;
use std::path::PathBuf;

use serde::Serialize;
use serde_json::{Value, json};

use crate::media::analyze_inbound_media_file;
use crate::memory::{
    OpenClawMemServiceRecallOptions, OpenClawMemServiceStatusOptions,
    OpenClawMemServiceStoreOptions, inspect_openclaw_mem_service, recall_openclaw_mem_service,
    store_openclaw_mem_service_memory,
};
use crate::memory_contracts::{AGENT_HARNESS_CONTEXT_PACK_SCHEMA, ContextPackChunk, ContextPackV1};

const MCP_PROTOCOL_VERSION: &str = "2024-11-05";
const DEFAULT_VISION_ANALYZE_MAX_BYTES: usize = 16 * 1024 * 1024;
const DEFAULT_MEMORY_MCP_LIMIT: usize = 5;
const MAX_MEMORY_MCP_LIMIT: usize = 10;
const DEFAULT_MEMORY_MCP_MAX_BYTES: u64 = 4_000_000;
const MAX_MEMORY_MCP_MAX_BYTES: u64 = 4_000_000;
const DEFAULT_MEMORY_MCP_PACK_MAX_BYTES: usize = 32 * 1024;
const MAX_MEMORY_MCP_PACK_MAX_BYTES: usize = 64 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpRequestOptions {
    pub request: Value,
    pub allowed_tools: BTreeSet<String>,
    pub harness_home: Option<PathBuf>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct McpToolReceipt {
    pub tool: Option<String>,
    pub allowed: bool,
    pub status: String,
    pub reason: String,
}

pub fn handle_mcp_request(options: McpRequestOptions) -> (Value, McpToolReceipt) {
    let id = options.request.get("id").cloned().unwrap_or(Value::Null);
    let Some(method) = options.request.get("method").and_then(Value::as_str) else {
        return error_response(id, -32600, "invalid request: missing method", None);
    };
    match method {
        "initialize" => (
            json!({
                "jsonrpc": "2.0",
                "id": id,
                "result": {
                    "protocolVersion": MCP_PROTOCOL_VERSION,
                    "serverInfo": { "name": "agent-harness-core", "version": env!("CARGO_PKG_VERSION") },
                    "capabilities": { "tools": {} }
                }
            }),
            McpToolReceipt {
                tool: None,
                allowed: true,
                status: "initialized".to_string(),
                reason: "MCP initialize handled in-process".to_string(),
            },
        ),
        "tools/list" => {
            let tools: Vec<_> = supported_tools()
                .into_iter()
                .filter(|name| {
                    options.allowed_tools.is_empty() || options.allowed_tools.contains(*name)
                })
                .map(|name| {
                    json!({
                        "name": name,
                        "description": tool_description(name),
                        "inputSchema": { "type": "object", "additionalProperties": true }
                    })
                })
                .collect();
            (
                json!({"jsonrpc":"2.0","id":id,"result":{"tools":tools}}),
                McpToolReceipt {
                    tool: None,
                    allowed: true,
                    status: "listed".to_string(),
                    reason: "listed allowed harness tools".to_string(),
                },
            )
        }
        "tools/call" => {
            let Some(tool) = options
                .request
                .pointer("/params/name")
                .and_then(Value::as_str)
            else {
                return error_response(id, -32602, "tools/call missing params.name", None);
            };
            if !supported_tools().contains(&tool) {
                return error_response(id, -32601, "unsupported harness tool", Some(tool));
            }
            if !options.allowed_tools.is_empty() && !options.allowed_tools.contains(tool) {
                return error_response(id, -32001, "tool blocked by allow-list", Some(tool));
            }
            call_tool(&options, id, tool)
        }
        _ => error_response(id, -32601, "unsupported method", None),
    }
}

fn supported_tools() -> Vec<&'static str> {
    vec![
        "harness.status",
        "harness.healthz",
        "harness.trace",
        "harness.vision_analyze",
        "mem_status",
        "mem_search",
        "mem_pack",
        "mem_store",
        "mem_trust_inspect",
    ]
}

fn tool_description(name: &str) -> &'static str {
    match name {
        "harness.status" => "Summarize local harness status without exposing secrets.",
        "harness.healthz" => "Return local readiness and liveness health JSON.",
        "harness.trace" => "Reconstruct one trace or queue id from local receipts.",
        "harness.vision_analyze" => {
            "Analyze a harness-contained inbound image artifact by artifactUri or localPath."
        }
        "mem_status" => {
            "Return sanitized OpenClawMem adapter status, capability, and readiness metadata."
        }
        "mem_search" => "Run bounded OpenClawMem local snapshot recall and return sanitized hits.",
        "mem_pack" => {
            "Build a bounded agent-harness.context-pack.v1 from OpenClawMem local snapshot recall."
        }
        "mem_store" => {
            "Record a reviewed OpenClawMem store request; MCP calls return review-required unless separately approved."
        }
        "mem_trust_inspect" => {
            "Return OpenClawMem scope, trust, graph, and mem-engine canary evidence."
        }
        _ => "Unsupported harness tool.",
    }
}

fn call_tool(options: &McpRequestOptions, id: Value, tool: &str) -> (Value, McpToolReceipt) {
    match tool {
        "harness.vision_analyze" => call_vision_analyze(options, id, tool),
        "mem_status" => call_mem_status(options, id, tool),
        "mem_search" => call_mem_search(options, id, tool),
        "mem_pack" => call_mem_pack(options, id, tool),
        "mem_store" => call_mem_store(options, id, tool),
        "mem_trust_inspect" => call_mem_trust_inspect(options, id, tool),
        _ => (
            json!({
                "jsonrpc": "2.0",
                "id": id,
                "result": {
                    "content": [
                        { "type": "text", "text": format!("{} accepted", tool) }
                    ],
                    "isError": false
                }
            }),
            McpToolReceipt {
                tool: Some(tool.to_string()),
                allowed: true,
                status: "called".to_string(),
                reason: "tool call passed allow-list and budget preflight placeholder".to_string(),
            },
        ),
    }
}

fn call_vision_analyze(
    options: &McpRequestOptions,
    id: Value,
    tool: &str,
) -> (Value, McpToolReceipt) {
    let Some(harness_home) = options.harness_home.as_ref() else {
        return error_response(
            id,
            -32602,
            "harness.vision_analyze requires harness_home",
            Some(tool),
        );
    };
    let arguments = options
        .request
        .pointer("/params/arguments")
        .unwrap_or(&Value::Null);
    let artifact_ref = arguments
        .get("artifactUri")
        .or_else(|| arguments.get("artifact_uri"))
        .or_else(|| arguments.get("localPath"))
        .or_else(|| arguments.get("local_path"))
        .and_then(Value::as_str);
    let Some(artifact_ref) = artifact_ref else {
        return error_response(
            id,
            -32602,
            "harness.vision_analyze requires artifactUri or localPath",
            Some(tool),
        );
    };
    let max_read_bytes = arguments
        .get("maxReadBytes")
        .or_else(|| arguments.get("max_read_bytes"))
        .and_then(Value::as_u64)
        .and_then(|value| usize::try_from(value).ok())
        .unwrap_or(DEFAULT_VISION_ANALYZE_MAX_BYTES);
    match analyze_inbound_media_file(harness_home, artifact_ref, max_read_bytes) {
        Ok(analysis) => (
            json!({
                "jsonrpc": "2.0",
                "id": id,
                "result": {
                    "content": [
                        { "type": "text", "text": serde_json::to_string(&analysis).unwrap_or_else(|_| "vision analysis serialization failed".to_string()) }
                    ],
                    "isError": false
                }
            }),
            McpToolReceipt {
                tool: Some(tool.to_string()),
                allowed: true,
                status: "called".to_string(),
                reason: "local harness-contained image analysis completed".to_string(),
            },
        ),
        Err(reason) => (
            json!({
                "jsonrpc": "2.0",
                "id": id,
                "result": {
                    "content": [
                        { "type": "text", "text": reason }
                    ],
                    "isError": true
                }
            }),
            McpToolReceipt {
                tool: Some(tool.to_string()),
                allowed: true,
                status: "failed".to_string(),
                reason: "local harness-contained image analysis failed".to_string(),
            },
        ),
    }
}

fn call_mem_status(options: &McpRequestOptions, id: Value, tool: &str) -> (Value, McpToolReceipt) {
    let Some(harness_home) = options.harness_home.as_ref() else {
        return error_response(id, -32602, "mem_status requires harness_home", Some(tool));
    };
    let arguments = tool_arguments(options);
    match inspect_openclaw_mem_service(OpenClawMemServiceStatusOptions {
        harness_home: harness_home.clone(),
        agent_id: optional_string_argument(arguments, &["agentId", "agent_id"]),
    }) {
        Ok(report) => json_tool_response(
            id,
            tool,
            sanitized_memory_status_value(report),
            false,
            "sanitized OpenClawMem status returned",
        ),
        Err(error) => json_tool_response(
            id,
            tool,
            json!({ "error": error.to_string() }),
            true,
            "OpenClawMem status failed open",
        ),
    }
}

fn call_mem_search(options: &McpRequestOptions, id: Value, tool: &str) -> (Value, McpToolReceipt) {
    let Some(harness_home) = options.harness_home.as_ref() else {
        return error_response(id, -32602, "mem_search requires harness_home", Some(tool));
    };
    let arguments = tool_arguments(options);
    let Some(query) = required_nonempty_string_argument(arguments, &["query"]) else {
        return error_response(id, -32602, "mem_search requires query", Some(tool));
    };
    match recall_openclaw_mem_service(OpenClawMemServiceRecallOptions {
        harness_home: harness_home.clone(),
        agent_id: optional_string_argument(arguments, &["agentId", "agent_id"]),
        query,
        limit: bounded_usize_argument(
            arguments,
            &["maxResults", "max_results", "limit"],
            DEFAULT_MEMORY_MCP_LIMIT,
            1,
            MAX_MEMORY_MCP_LIMIT,
        ),
        max_file_bytes: bounded_u64_argument(
            arguments,
            &["maxBytes", "max_bytes", "maxFileBytes", "max_file_bytes"],
            DEFAULT_MEMORY_MCP_MAX_BYTES,
            1,
            MAX_MEMORY_MCP_MAX_BYTES,
        ),
    }) {
        Ok(report) => json_tool_response(
            id,
            tool,
            serde_json::to_value(report).unwrap_or_else(|error| {
                json!({ "error": format!("failed to serialize mem_search report: {error}") })
            }),
            false,
            "bounded OpenClawMem recall returned",
        ),
        Err(error) => json_tool_response(
            id,
            tool,
            json!({ "error": error.to_string() }),
            true,
            "OpenClawMem recall failed open",
        ),
    }
}

fn call_mem_pack(options: &McpRequestOptions, id: Value, tool: &str) -> (Value, McpToolReceipt) {
    let Some(harness_home) = options.harness_home.as_ref() else {
        return error_response(id, -32602, "mem_pack requires harness_home", Some(tool));
    };
    let arguments = tool_arguments(options);
    let Some(query) = required_nonempty_string_argument(arguments, &["query"]) else {
        return error_response(id, -32602, "mem_pack requires query", Some(tool));
    };
    let agent_id = optional_string_argument(arguments, &["agentId", "agent_id"]);
    let max_pack_bytes = bounded_usize_argument(
        arguments,
        &["maxPackBytes", "max_pack_bytes", "maxBytes", "max_bytes"],
        DEFAULT_MEMORY_MCP_PACK_MAX_BYTES,
        256,
        MAX_MEMORY_MCP_PACK_MAX_BYTES,
    );
    match recall_openclaw_mem_service(OpenClawMemServiceRecallOptions {
        harness_home: harness_home.clone(),
        agent_id: agent_id.clone(),
        query: query.clone(),
        limit: bounded_usize_argument(
            arguments,
            &["maxResults", "max_results", "limit"],
            DEFAULT_MEMORY_MCP_LIMIT,
            1,
            MAX_MEMORY_MCP_LIMIT,
        ),
        max_file_bytes: bounded_u64_argument(
            arguments,
            &["maxFileBytes", "max_file_bytes"],
            DEFAULT_MEMORY_MCP_MAX_BYTES,
            1,
            MAX_MEMORY_MCP_MAX_BYTES,
        ),
    }) {
        Ok(report) => {
            let (pack, dropped_hits, mut warnings) = context_pack_from_recall_report(
                &query,
                agent_id.as_deref(),
                &report,
                max_pack_bytes,
            );
            warnings.extend(report.warnings.clone());
            let packed_chunks = pack.chunks.len();
            json_tool_response(
                id,
                tool,
                json!({
                    "schema": "agent-harness.mcp.mem-pack.v1",
                    "pack": pack,
                    "recallStatus": report.status,
                    "recallBackend": report.backend,
                    "hitCount": report.hit_count,
                    "packedChunks": packed_chunks,
                    "droppedHits": dropped_hits,
                    "maxPackBytes": max_pack_bytes,
                    "warnings": warnings,
                }),
                false,
                "bounded ContextPack returned from OpenClawMem recall",
            )
        }
        Err(error) => json_tool_response(
            id,
            tool,
            json!({ "error": error.to_string() }),
            true,
            "OpenClawMem ContextPack recall failed open",
        ),
    }
}

fn call_mem_store(options: &McpRequestOptions, id: Value, tool: &str) -> (Value, McpToolReceipt) {
    let Some(harness_home) = options.harness_home.as_ref() else {
        return error_response(id, -32602, "mem_store requires harness_home", Some(tool));
    };
    let arguments = tool_arguments(options);
    let Some(text) = required_nonempty_string_argument(
        arguments,
        &["text", "memoryText", "memory_text", "content"],
    ) else {
        return error_response(id, -32602, "mem_store requires text", Some(tool));
    };
    let requested_approval =
        bool_argument(arguments, &["approved", "reviewApproved"]).unwrap_or(false);
    let payload = arguments
        .get("payload")
        .cloned()
        .unwrap_or_else(|| json!({ "source": "mcp.mem_store" }));
    match store_openclaw_mem_service_memory(OpenClawMemServiceStoreOptions {
        harness_home: harness_home.clone(),
        agent_id: optional_string_argument(arguments, &["agentId", "agent_id"]),
        session_key: optional_string_argument(arguments, &["sessionKey", "session_key"]),
        text,
        payload,
        approved: false,
        now_ms: integer_argument(arguments, &["nowMs", "now_ms"]).unwrap_or(0),
    }) {
        Ok(report) => json_tool_response(
            id,
            tool,
            json!({
                "schema": "agent-harness.mcp.mem-store.v1",
                "store": report,
                "requestedApprovalIgnored": requested_approval,
                "reviewRequired": true,
                "reason": "MCP mem_store does not commit memory directly; use reviewed approval path before store.",
            }),
            false,
            "OpenClawMem store request recorded as review-required",
        ),
        Err(error) => json_tool_response(
            id,
            tool,
            json!({ "error": error.to_string() }),
            true,
            "OpenClawMem store failed open",
        ),
    }
}

fn call_mem_trust_inspect(
    options: &McpRequestOptions,
    id: Value,
    tool: &str,
) -> (Value, McpToolReceipt) {
    let Some(harness_home) = options.harness_home.as_ref() else {
        return error_response(
            id,
            -32602,
            "mem_trust_inspect requires harness_home",
            Some(tool),
        );
    };
    let arguments = tool_arguments(options);
    match inspect_openclaw_mem_service(OpenClawMemServiceStatusOptions {
        harness_home: harness_home.clone(),
        agent_id: optional_string_argument(arguments, &["agentId", "agent_id"]),
    }) {
        Ok(report) => json_tool_response(
            id,
            tool,
            json!({
                "schema": "agent-harness.mcp.mem-trust-inspect.v1",
                "status": report.status,
                "reason": report.reason,
                "serviceMode": report.service_mode,
                "activeSlotOwner": report.active_slot_owner,
                "scopePolicy": report.scope_policy,
                "trustPolicy": report.trust_policy,
                "graphReadiness": report.graph_readiness,
                "memEngineCanary": report.mem_engine_canary,
                "warnings": report.warnings,
            }),
            false,
            "OpenClawMem trust evidence returned",
        ),
        Err(error) => json_tool_response(
            id,
            tool,
            json!({ "error": error.to_string() }),
            true,
            "OpenClawMem trust inspection failed open",
        ),
    }
}

fn context_pack_from_recall_report(
    query: &str,
    agent_id: Option<&str>,
    report: &crate::memory::OpenClawMemServiceRecallReport,
    max_pack_bytes: usize,
) -> (ContextPackV1, usize, Vec<String>) {
    let seed = format!(
        "{}:{}:{}:{}",
        agent_id.unwrap_or("global"),
        query,
        report.hit_count,
        report
            .hits
            .iter()
            .map(|hit| hit.id.as_str())
            .collect::<Vec<_>>()
            .join("|")
    );
    let mut pack = ContextPackV1 {
        schema: AGENT_HARNESS_CONTEXT_PACK_SCHEMA.to_string(),
        pack_id: format!(
            "mcp-mem-pack-{}",
            crate::memory_contracts::tool_description_hash(&seed)
        ),
        source: "agent-harness.mcp.mem_pack".to_string(),
        chunks: Vec::new(),
    };
    let mut dropped_hits = 0usize;
    let mut warnings = Vec::new();
    for hit in &report.hits {
        let mut candidate = pack.clone();
        candidate.chunks.push(ContextPackChunk {
            citation_id: hit.id.clone(),
            text: hit.text.clone(),
            source_uri: hit.source.clone(),
        });
        if serialized_json_len(&candidate) <= max_pack_bytes {
            pack = candidate;
        } else {
            dropped_hits += 1;
        }
    }
    if dropped_hits > 0 {
        warnings.push(format!(
            "mem_pack dropped {dropped_hits} hit(s) to stay within maxPackBytes={max_pack_bytes}"
        ));
    }
    (pack, dropped_hits, warnings)
}

fn serialized_json_len<T: Serialize>(value: &T) -> usize {
    serde_json::to_vec(value)
        .map(|bytes| bytes.len())
        .unwrap_or(usize::MAX)
}

fn sanitized_memory_status_value(report: crate::memory::OpenClawMemServiceStatusReport) -> Value {
    let mut value = serde_json::to_value(report).unwrap_or_else(
        |error| json!({ "error": format!("failed to serialize memory status: {error}") }),
    );
    if let Some(object) = value.as_object_mut() {
        if !matches!(object.get("serviceEndpoint"), None | Some(Value::Null)) {
            object.insert(
                "serviceEndpoint".to_string(),
                Value::String("[configured-redacted]".to_string()),
            );
        }
    }
    if let Some(bridge) = value
        .get_mut("credentialBridge")
        .and_then(Value::as_object_mut)
    {
        if !matches!(bridge.get("baseUrl"), None | Some(Value::Null)) {
            bridge.insert(
                "baseUrl".to_string(),
                Value::String("[configured-redacted]".to_string()),
            );
        }
        bridge.insert("subprocessEnvKeys".to_string(), Value::Array(Vec::new()));
        bridge.insert("directCliEnvMappings".to_string(), Value::Array(Vec::new()));
        bridge.insert("windowsUtf8Env".to_string(), json!({}));
    }
    value
}

fn tool_arguments(options: &McpRequestOptions) -> &Value {
    options
        .request
        .pointer("/params/arguments")
        .unwrap_or(&Value::Null)
}

fn json_tool_response(
    id: Value,
    tool: &str,
    value: Value,
    is_error: bool,
    reason: &str,
) -> (Value, McpToolReceipt) {
    let text = serde_json::to_string(&value).unwrap_or_else(|error| {
        format!(r#"{{"error":"failed to serialize tool result: {error}"}}"#)
    });
    (
        json!({
            "jsonrpc": "2.0",
            "id": id,
            "result": {
                "content": [
                    { "type": "text", "text": text }
                ],
                "isError": is_error
            }
        }),
        McpToolReceipt {
            tool: Some(tool.to_string()),
            allowed: true,
            status: if is_error { "failed" } else { "called" }.to_string(),
            reason: reason.to_string(),
        },
    )
}

fn optional_string_argument(arguments: &Value, keys: &[&str]) -> Option<String> {
    keys.iter()
        .find_map(|key| arguments.get(*key).and_then(Value::as_str))
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

fn required_nonempty_string_argument(arguments: &Value, keys: &[&str]) -> Option<String> {
    optional_string_argument(arguments, keys)
}

fn bool_argument(arguments: &Value, keys: &[&str]) -> Option<bool> {
    keys.iter()
        .find_map(|key| arguments.get(*key).and_then(Value::as_bool))
}

fn integer_argument(arguments: &Value, keys: &[&str]) -> Option<i64> {
    keys.iter()
        .find_map(|key| arguments.get(*key).and_then(Value::as_i64))
}

fn bounded_usize_argument(
    arguments: &Value,
    keys: &[&str],
    default: usize,
    min: usize,
    max: usize,
) -> usize {
    keys.iter()
        .find_map(|key| arguments.get(*key).and_then(Value::as_u64))
        .and_then(|value| usize::try_from(value).ok())
        .unwrap_or(default)
        .clamp(min, max)
}

fn bounded_u64_argument(arguments: &Value, keys: &[&str], default: u64, min: u64, max: u64) -> u64 {
    keys.iter()
        .find_map(|key| arguments.get(*key).and_then(Value::as_u64))
        .unwrap_or(default)
        .clamp(min, max)
}

fn error_response(
    id: Value,
    code: i64,
    message: &str,
    tool: Option<&str>,
) -> (Value, McpToolReceipt) {
    (
        json!({"jsonrpc":"2.0","id":id,"error":{"code":code,"message":message}}),
        McpToolReceipt {
            tool: tool.map(ToString::to_string),
            allowed: false,
            status: "rejected".to_string(),
            reason: message.to_string(),
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn mcp_handles_initialize_list_call_and_allow_list_rejection() {
        let (init, init_receipt) = handle_mcp_request(McpRequestOptions {
            request: json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}),
            allowed_tools: BTreeSet::new(),
            harness_home: None,
        });
        assert!(init.get("result").is_some());
        assert!(init_receipt.allowed);

        let allowed = BTreeSet::from(["harness.healthz".to_string()]);
        let (list, _) = handle_mcp_request(McpRequestOptions {
            request: json!({"jsonrpc":"2.0","id":2,"method":"tools/list"}),
            allowed_tools: allowed.clone(),
            harness_home: None,
        });
        assert_eq!(
            list.pointer("/result/tools/0/name").and_then(Value::as_str),
            Some("harness.healthz")
        );

        let (blocked, receipt) = handle_mcp_request(McpRequestOptions {
            request: json!({"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"harness.trace","arguments":{}}}),
            allowed_tools: allowed,
            harness_home: None,
        });
        assert!(blocked.get("error").is_some());
        assert!(!receipt.allowed);
        assert_eq!(receipt.tool.as_deref(), Some("harness.trace"));
    }

    #[test]
    fn mcp_vision_analyze_reads_harness_contained_artifact_uri() {
        let root = temp_root("mcp_vision_analyze");
        let harness_home = root.join(".agent-harness");
        let attachment = crate::inbound_media_attachment_root(&harness_home)
            .join("update-1")
            .join("0.png");
        fs::create_dir_all(attachment.parent().unwrap()).unwrap();
        fs::write(&attachment, png_header_bytes(4, 5)).unwrap();

        let allowed = BTreeSet::from(["harness.vision_analyze".to_string()]);
        let (response, receipt) = handle_mcp_request(McpRequestOptions {
            request: json!({
                "jsonrpc": "2.0",
                "id": 10,
                "method": "tools/call",
                "params": {
                    "name": "harness.vision_analyze",
                    "arguments": {
                        "artifactUri": "agent-harness://inbound-media/telegram/update-1/0.png",
                        "maxReadBytes": 1024
                    }
                }
            }),
            allowed_tools: allowed,
            harness_home: Some(harness_home.clone()),
        });

        assert!(receipt.allowed);
        assert_eq!(receipt.status, "called");
        assert_eq!(
            response.pointer("/result/isError").and_then(Value::as_bool),
            Some(false)
        );
        let text = response
            .pointer("/result/content/0/text")
            .and_then(Value::as_str)
            .unwrap();
        assert!(text.contains(r#""mime":"image/png""#));
        assert!(text.contains(r#""width":4"#));
        assert!(text.contains(r#""height":5"#));
        assert!(!text.contains("file_id"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn memory_mcp_tools_are_registered_and_allow_listed() {
        let allowed = BTreeSet::from(["mem_status".to_string()]);
        let (list, _) = handle_mcp_request(McpRequestOptions {
            request: json!({"jsonrpc":"2.0","id":20,"method":"tools/list"}),
            allowed_tools: allowed.clone(),
            harness_home: None,
        });
        let tools = list
            .pointer("/result/tools")
            .and_then(Value::as_array)
            .unwrap();
        assert_eq!(tools.len(), 1);
        assert_eq!(
            tools[0].get("name").and_then(Value::as_str),
            Some("mem_status")
        );

        let (blocked, receipt) = handle_mcp_request(McpRequestOptions {
            request: json!({"jsonrpc":"2.0","id":21,"method":"tools/call","params":{"name":"mem_search","arguments":{"query":"memory"}}}),
            allowed_tools: allowed,
            harness_home: Some(temp_root("memory_mcp_allow_list")),
        });
        assert!(blocked.get("error").is_some());
        assert!(!receipt.allowed);
        assert_eq!(receipt.tool.as_deref(), Some("mem_search"));
    }

    #[test]
    fn memory_mcp_status_and_trust_are_sanitized() {
        let root = temp_root("memory_mcp_status_and_trust");
        let harness_home = root.join("harness");
        write_memory_fixture(&harness_home);

        let allowed = BTreeSet::from(["mem_status".to_string(), "mem_trust_inspect".to_string()]);
        let (status_response, status_receipt) = handle_mcp_request(McpRequestOptions {
            request: json!({"jsonrpc":"2.0","id":30,"method":"tools/call","params":{"name":"mem_status","arguments":{"agentId":"main"}}}),
            allowed_tools: allowed.clone(),
            harness_home: Some(harness_home.clone()),
        });
        assert!(status_receipt.allowed);
        assert_eq!(status_receipt.status, "called");
        let status = tool_text_json(&status_response);
        assert_eq!(
            status.get("schema").and_then(Value::as_str),
            Some("agent-harness.openclaw-mem-service-status.v1")
        );
        assert_eq!(
            status
                .pointer("/credentialBridge/baseUrl")
                .and_then(Value::as_str),
            Some("[configured-redacted]")
        );
        assert!(!status_response.to_string().contains("OPENAI_API_KEY"));

        let (trust_response, trust_receipt) = handle_mcp_request(McpRequestOptions {
            request: json!({"jsonrpc":"2.0","id":31,"method":"tools/call","params":{"name":"mem_trust_inspect","arguments":{"agentId":"main"}}}),
            allowed_tools: allowed,
            harness_home: Some(harness_home.clone()),
        });
        assert!(trust_receipt.allowed);
        let trust = tool_text_json(&trust_response);
        assert_eq!(
            trust.get("schema").and_then(Value::as_str),
            Some("agent-harness.mcp.mem-trust-inspect.v1")
        );
        assert!(trust.get("scopePolicy").is_some());
        assert!(trust.get("trustPolicy").is_some());
        assert!(trust.get("graphReadiness").is_some());

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn memory_mcp_search_pack_and_store_are_bounded_and_reviewed() {
        let root = temp_root("memory_mcp_search_pack_store");
        let harness_home = root.join("harness");
        write_memory_fixture(&harness_home);

        let allowed = BTreeSet::from([
            "mem_search".to_string(),
            "mem_pack".to_string(),
            "mem_store".to_string(),
        ]);
        let (search_response, search_receipt) = handle_mcp_request(McpRequestOptions {
            request: json!({
                "jsonrpc":"2.0",
                "id":40,
                "method":"tools/call",
                "params":{
                    "name":"mem_search",
                    "arguments":{"query":"Qdrant memory", "maxResults":1, "maxBytes":1024}
                }
            }),
            allowed_tools: allowed.clone(),
            harness_home: Some(harness_home.clone()),
        });
        assert!(search_receipt.allowed);
        let search = tool_text_json(&search_response);
        assert_eq!(search.get("hitCount").and_then(Value::as_u64), Some(1));
        assert_eq!(
            search.pointer("/hits/0/lane").and_then(Value::as_str),
            Some("memory-file")
        );

        let (pack_response, pack_receipt) = handle_mcp_request(McpRequestOptions {
            request: json!({
                "jsonrpc":"2.0",
                "id":41,
                "method":"tools/call",
                "params":{
                    "name":"mem_pack",
                    "arguments":{"query":"Qdrant memory", "maxResults":1, "maxPackBytes":4096}
                }
            }),
            allowed_tools: allowed.clone(),
            harness_home: Some(harness_home.clone()),
        });
        assert!(pack_receipt.allowed);
        let pack = tool_text_json(&pack_response);
        assert_eq!(
            pack.pointer("/pack/schema").and_then(Value::as_str),
            Some(AGENT_HARNESS_CONTEXT_PACK_SCHEMA)
        );
        assert_eq!(pack.get("packedChunks").and_then(Value::as_u64), Some(1));
        assert!(
            pack.pointer("/pack/chunks/0/text")
                .and_then(Value::as_str)
                .unwrap()
                .contains("Qdrant")
        );

        let (store_response, store_receipt) = handle_mcp_request(McpRequestOptions {
            request: json!({
                "jsonrpc":"2.0",
                "id":42,
                "method":"tools/call",
                "params":{
                    "name":"mem_store",
                    "arguments":{
                        "agentId":"main",
                        "sessionKey":"session-1",
                        "text":"Remember that Qdrant snapshot is not active owner.",
                        "approved": true
                    }
                }
            }),
            allowed_tools: allowed,
            harness_home: Some(harness_home.clone()),
        });
        assert!(store_receipt.allowed);
        let store = tool_text_json(&store_response);
        assert_eq!(
            store.pointer("/store/status").and_then(Value::as_str),
            Some("review-required")
        );
        assert_eq!(
            store
                .get("requestedApprovalIgnored")
                .and_then(Value::as_bool),
            Some(true)
        );
        let store_file = PathBuf::from(
            store
                .pointer("/store/storeFile")
                .and_then(Value::as_str)
                .unwrap(),
        );
        assert!(!store_file.exists());

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn memory_mcp_tool_descriptions_are_hash_pinned() {
        let expected = [
            ("mem_status", "e94dccc83ea06341"),
            ("mem_search", "4c88f8754716e313"),
            ("mem_pack", "38f49e75a4660e29"),
            ("mem_store", "809cb40a0d7779eb"),
            ("mem_trust_inspect", "37a5c2ce1ad4279d"),
        ];
        for (tool, hash) in expected {
            assert_eq!(
                crate::memory_contracts::tool_description_hash(tool_description(tool)),
                hash,
                "{tool} description hash drifted"
            );
        }
    }

    fn tool_text_json(response: &Value) -> Value {
        let text = response
            .pointer("/result/content/0/text")
            .and_then(Value::as_str)
            .unwrap();
        serde_json::from_str(text).unwrap()
    }

    fn write_memory_fixture(harness_home: &std::path::Path) {
        let memory = harness_home.join("memory");
        fs::create_dir_all(&memory).unwrap();
        fs::write(
            memory.join("MEMORY.md"),
            "Qdrant memory should remain snapshot-only until promoted.\nOther note.",
        )
        .unwrap();
    }

    fn temp_root(test_name: &str) -> PathBuf {
        let millis = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis();
        std::env::temp_dir().join(format!(
            "agent-harness-core-{test_name}-{}-{millis}",
            std::process::id()
        ))
    }

    fn png_header_bytes(width: u32, height: u32) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(b"\x89PNG\r\n\x1a\n");
        bytes.extend_from_slice(&[0, 0, 0, 13]);
        bytes.extend_from_slice(b"IHDR");
        bytes.extend_from_slice(&width.to_be_bytes());
        bytes.extend_from_slice(&height.to_be_bytes());
        bytes.truncate(24);
        bytes
    }
}
