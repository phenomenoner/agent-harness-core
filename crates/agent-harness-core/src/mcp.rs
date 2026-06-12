use std::collections::BTreeSet;

use serde::Serialize;
use serde_json::{Value, json};

const MCP_PROTOCOL_VERSION: &str = "2024-11-05";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpRequestOptions {
    pub request: Value,
    pub allowed_tools: BTreeSet<String>,
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
            (
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
                    reason: "tool call passed allow-list and budget preflight placeholder"
                        .to_string(),
                },
            )
        }
        _ => error_response(id, -32601, "unsupported method", None),
    }
}

fn supported_tools() -> Vec<&'static str> {
    vec!["harness.status", "harness.healthz", "harness.trace"]
}

fn tool_description(name: &str) -> &'static str {
    match name {
        "harness.status" => "Summarize local harness status without exposing secrets.",
        "harness.healthz" => "Return local readiness and liveness health JSON.",
        "harness.trace" => "Reconstruct one trace or queue id from local receipts.",
        _ => "Unsupported harness tool.",
    }
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

    #[test]
    fn mcp_handles_initialize_list_call_and_allow_list_rejection() {
        let (init, init_receipt) = handle_mcp_request(McpRequestOptions {
            request: json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}),
            allowed_tools: BTreeSet::new(),
        });
        assert!(init.get("result").is_some());
        assert!(init_receipt.allowed);

        let allowed = BTreeSet::from(["harness.healthz".to_string()]);
        let (list, _) = handle_mcp_request(McpRequestOptions {
            request: json!({"jsonrpc":"2.0","id":2,"method":"tools/list"}),
            allowed_tools: allowed.clone(),
        });
        assert_eq!(
            list.pointer("/result/tools/0/name").and_then(Value::as_str),
            Some("harness.healthz")
        );

        let (blocked, receipt) = handle_mcp_request(McpRequestOptions {
            request: json!({"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"harness.trace","arguments":{}}}),
            allowed_tools: allowed,
        });
        assert!(blocked.get("error").is_some());
        assert!(!receipt.allowed);
        assert_eq!(receipt.tool.as_deref(), Some("harness.trace"));
    }
}
