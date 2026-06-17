use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};
use serde_json::Value;

pub const AGENT_HARNESS_CONTEXT_PACK_SCHEMA: &str = "agent-harness.context-pack.v1";
pub const OPENCLAW_MEM_CONTEXT_PACK_SCHEMA: &str = "openclaw-mem.context-pack.v1";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContextPackParseOptions {
    pub raw_json: String,
    pub max_bytes: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ContextPackV1 {
    pub schema: String,
    pub pack_id: String,
    pub source: String,
    pub chunks: Vec<ContextPackChunk>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ContextPackChunk {
    pub citation_id: String,
    pub text: String,
    pub source_uri: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ContextPackParseReport {
    pub accepted: bool,
    pub input_schema: Option<String>,
    pub normalized_schema: Option<String>,
    pub translation_applied: bool,
    pub pack: Option<ContextPackV1>,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MemoryIngestDecision {
    pub ingest_id: String,
    pub accepted: bool,
    pub duplicate: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ToolDescriptionPinReport {
    pub tool: String,
    pub expected_hash: String,
    pub actual_hash: String,
    pub matched: bool,
}

pub fn parse_context_pack(options: ContextPackParseOptions) -> ContextPackParseReport {
    let mut warnings = Vec::new();
    if options.raw_json.len() > options.max_bytes {
        warnings.push("context pack exceeds configured size bound".to_string());
        return ContextPackParseReport {
            accepted: false,
            input_schema: None,
            normalized_schema: None,
            translation_applied: false,
            pack: None,
            warnings,
        };
    }

    let value = match serde_json::from_str::<Value>(&options.raw_json) {
        Ok(value) => value,
        Err(error) => {
            warnings.push(format!("invalid context pack JSON: {error}"));
            return ContextPackParseReport {
                accepted: false,
                input_schema: None,
                normalized_schema: None,
                translation_applied: false,
                pack: None,
                warnings,
            };
        }
    };

    let input_schema = string_field(&value, &["schema"]).map(str::to_string);
    match input_schema.as_deref() {
        Some(AGENT_HARNESS_CONTEXT_PACK_SCHEMA) => {
            let pack = match serde_json::from_value::<ContextPackV1>(value) {
                Ok(pack) => pack,
                Err(error) => {
                    warnings.push(format!("invalid agent-harness context pack: {error}"));
                    return ContextPackParseReport {
                        accepted: false,
                        input_schema,
                        normalized_schema: Some(AGENT_HARNESS_CONTEXT_PACK_SCHEMA.to_string()),
                        translation_applied: false,
                        pack: None,
                        warnings,
                    };
                }
            };
            validate_context_pack(pack, input_schema, false)
        }
        Some(OPENCLAW_MEM_CONTEXT_PACK_SCHEMA) => match translate_openclaw_mem_context_pack(&value)
        {
            Ok(pack) => validate_context_pack(pack, input_schema, true),
            Err(mut translation_warnings) => {
                warnings.append(&mut translation_warnings);
                ContextPackParseReport {
                    accepted: false,
                    input_schema,
                    normalized_schema: Some(AGENT_HARNESS_CONTEXT_PACK_SCHEMA.to_string()),
                    translation_applied: true,
                    pack: None,
                    warnings,
                }
            }
        },
        Some(schema) => {
            warnings.push(format!("unsupported context pack schema `{schema}`"));
            ContextPackParseReport {
                accepted: false,
                input_schema,
                normalized_schema: None,
                translation_applied: false,
                pack: None,
                warnings,
            }
        }
        None => {
            warnings.push("context pack missing schema".to_string());
            ContextPackParseReport {
                accepted: false,
                input_schema: None,
                normalized_schema: None,
                translation_applied: false,
                pack: None,
                warnings,
            }
        }
    }
}

fn validate_context_pack(
    pack: ContextPackV1,
    input_schema: Option<String>,
    translation_applied: bool,
) -> ContextPackParseReport {
    let mut warnings = Vec::new();
    if pack.schema != AGENT_HARNESS_CONTEXT_PACK_SCHEMA {
        warnings.push(format!(
            "unsupported normalized context pack schema `{}`",
            pack.schema
        ));
    }
    if pack.pack_id.trim().is_empty() {
        warnings.push("context pack missing pack id".to_string());
    }
    if pack.source.trim().is_empty() {
        warnings.push("context pack missing source".to_string());
    }
    for (index, chunk) in pack.chunks.iter().enumerate() {
        if chunk.citation_id.trim().is_empty() {
            warnings.push(format!("context pack chunk {index} missing citation id"));
        }
        if chunk.text.trim().is_empty() {
            warnings.push(format!("context pack chunk {index} missing text"));
        }
    }

    let accepted = warnings.is_empty();
    ContextPackParseReport {
        accepted,
        input_schema,
        normalized_schema: Some(AGENT_HARNESS_CONTEXT_PACK_SCHEMA.to_string()),
        translation_applied,
        pack: accepted.then_some(pack),
        warnings,
    }
}

fn translate_openclaw_mem_context_pack(value: &Value) -> Result<ContextPackV1, Vec<String>> {
    let mut warnings = Vec::new();
    let pack_id = required_string_field(
        value,
        &["packId", "pack_id", "id"],
        "context pack missing pack id",
        &mut warnings,
    );
    let source = required_string_field(
        value,
        &["source"],
        "context pack missing source",
        &mut warnings,
    );
    let items = array_field(value, &["chunks", "items", "results"]);
    if items.is_none() {
        warnings.push("context pack missing chunks/items array".to_string());
    }

    let mut chunks = Vec::new();
    for (index, item) in items.into_iter().flatten().enumerate() {
        let citation_id = required_string_field(
            item,
            &[
                "citationId",
                "citation_id",
                "citation",
                "id",
                "itemId",
                "item_id",
            ],
            &format!("context pack chunk {index} missing citation id"),
            &mut warnings,
        );
        let text = required_string_field(
            item,
            &["text", "content", "body"],
            &format!("context pack chunk {index} missing text"),
            &mut warnings,
        );
        if let (Some(citation_id), Some(text)) = (citation_id, text) {
            chunks.push(ContextPackChunk {
                citation_id,
                text,
                source_uri: optional_string_field(
                    item,
                    &["sourceUri", "source_uri", "uri", "url", "source"],
                ),
            });
        }
    }

    match (pack_id, source, warnings.is_empty()) {
        (Some(pack_id), Some(source), true) => Ok(ContextPackV1 {
            schema: AGENT_HARNESS_CONTEXT_PACK_SCHEMA.to_string(),
            pack_id,
            source,
            chunks,
        }),
        _ => Err(warnings),
    }
}

fn string_field<'a>(value: &'a Value, keys: &[&str]) -> Option<&'a str> {
    let object = value.as_object()?;
    keys.iter().find_map(|key| match object.get(*key) {
        Some(Value::String(value)) => Some(value.as_str()),
        _ => None,
    })
}

fn array_field<'a>(value: &'a Value, keys: &[&str]) -> Option<&'a Vec<Value>> {
    let object = value.as_object()?;
    keys.iter().find_map(|key| match object.get(*key) {
        Some(Value::Array(value)) => Some(value),
        _ => None,
    })
}

fn required_string_field(
    value: &Value,
    keys: &[&str],
    warning: &str,
    warnings: &mut Vec<String>,
) -> Option<String> {
    match string_field(value, keys) {
        Some(value) if !value.trim().is_empty() => Some(value.to_string()),
        _ => {
            warnings.push(warning.to_string());
            None
        }
    }
}

fn optional_string_field(value: &Value, keys: &[&str]) -> Option<String> {
    string_field(value, keys)
        .filter(|value| !value.trim().is_empty())
        .map(str::to_string)
}

pub fn decide_memory_ingest(
    ingest_id: String,
    seen_ids: &mut BTreeSet<String>,
) -> MemoryIngestDecision {
    let duplicate = !seen_ids.insert(ingest_id.clone());
    MemoryIngestDecision {
        ingest_id,
        accepted: !duplicate,
        duplicate,
    }
}

pub fn check_tool_description_pin(
    tool: String,
    description: &str,
    expected_hash: String,
) -> ToolDescriptionPinReport {
    let actual_hash = fnv1a_64_hex(description.as_bytes());
    ToolDescriptionPinReport {
        tool,
        matched: actual_hash == expected_hash,
        expected_hash,
        actual_hash,
    }
}

pub fn tool_description_hash(description: &str) -> String {
    fnv1a_64_hex(description.as_bytes())
}

fn fnv1a_64_hex(bytes: &[u8]) -> String {
    let mut hash = 0xcbf29ce484222325_u64;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    format!("{hash:016x}")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_fixture(value: Value) -> ContextPackParseReport {
        parse_context_pack(ContextPackParseOptions {
            raw_json: value.to_string(),
            max_bytes: 4096,
        })
    }

    #[test]
    fn context_pack_accepts_agent_harness_schema_unchanged() {
        let report = parse_fixture(serde_json::json!({
            "schema": "agent-harness.context-pack.v1",
            "packId": "pack-1",
            "source": "openclaw-mem",
            "chunks": [
                {"citationId":"m1","text":"remember this","sourceUri":"memory://m1"}
            ]
        }));
        assert!(report.accepted);
        assert_eq!(
            report.input_schema.as_deref(),
            Some(AGENT_HARNESS_CONTEXT_PACK_SCHEMA)
        );
        assert_eq!(
            report.normalized_schema.as_deref(),
            Some(AGENT_HARNESS_CONTEXT_PACK_SCHEMA)
        );
        assert!(!report.translation_applied);
        let pack = report.pack.expect("accepted pack");
        assert_eq!(pack.schema, AGENT_HARNESS_CONTEXT_PACK_SCHEMA);
        assert_eq!(pack.chunks[0].source_uri.as_deref(), Some("memory://m1"));
    }

    #[test]
    fn context_pack_translates_openclaw_mem_schema() {
        let report = parse_fixture(serde_json::json!({
            "schema": "openclaw-mem.context-pack.v1",
            "pack_id": "pack-openclaw-1",
            "source": "openclaw-mem",
            "items": [
                {
                    "id": "obs:123",
                    "content": "translated memory text",
                    "uri": "memory://obs/123"
                }
            ]
        }));
        assert!(report.accepted, "{:?}", report.warnings);
        assert_eq!(
            report.input_schema.as_deref(),
            Some(OPENCLAW_MEM_CONTEXT_PACK_SCHEMA)
        );
        assert_eq!(
            report.normalized_schema.as_deref(),
            Some(AGENT_HARNESS_CONTEXT_PACK_SCHEMA)
        );
        assert!(report.translation_applied);
        let pack = report.pack.expect("translated pack");
        assert_eq!(pack.schema, AGENT_HARNESS_CONTEXT_PACK_SCHEMA);
        assert_eq!(pack.pack_id, "pack-openclaw-1");
        assert_eq!(pack.chunks[0].citation_id, "obs:123");
        assert_eq!(pack.chunks[0].text, "translated memory text");
        assert_eq!(
            pack.chunks[0].source_uri.as_deref(),
            Some("memory://obs/123")
        );
    }

    #[test]
    fn context_pack_rejects_unknown_schema_with_report_fields() {
        let report = parse_fixture(serde_json::json!({
            "schema": "openclaw-mem.context-pack.v2",
            "packId": "pack-future",
            "source": "openclaw-mem",
            "chunks": []
        }));
        assert!(!report.accepted);
        assert!(report.pack.is_none());
        assert_eq!(
            report.input_schema.as_deref(),
            Some("openclaw-mem.context-pack.v2")
        );
        assert_eq!(report.normalized_schema, None);
        assert!(!report.translation_applied);
        assert!(
            report
                .warnings
                .iter()
                .any(|warning| warning.contains("unsupported context pack schema"))
        );
    }

    #[test]
    fn context_pack_rejects_oversized_pack() {
        let report = parse_context_pack(ContextPackParseOptions {
            raw_json: serde_json::json!({
                "schema": "agent-harness.context-pack.v1",
                "packId": "pack-1",
                "source": "openclaw-mem",
                "chunks": []
            })
            .to_string(),
            max_bytes: 8,
        });
        assert!(!report.accepted);
        assert_eq!(report.input_schema, None);
        assert_eq!(report.normalized_schema, None);
        assert!(!report.translation_applied);
        assert!(
            report
                .warnings
                .iter()
                .any(|warning| warning.contains("size bound"))
        );
    }

    #[test]
    fn context_pack_rejects_missing_citation_id() {
        let report = parse_fixture(serde_json::json!({
            "schema": "agent-harness.context-pack.v1",
            "packId": "pack-1",
            "source": "openclaw-mem",
            "chunks": [
                {"citationId":"","text":"remember this","sourceUri":"memory://m1"}
            ]
        }));
        assert!(!report.accepted);
        assert!(report.pack.is_none());
        assert!(
            report
                .warnings
                .iter()
                .any(|warning| warning.contains("missing citation id"))
        );
    }

    #[test]
    fn context_pack_rejects_missing_text() {
        let report = parse_fixture(serde_json::json!({
            "schema": "openclaw-mem.context-pack.v1",
            "packId": "pack-1",
            "source": "openclaw-mem",
            "items": [
                {"citationId":"obs:123","text":""}
            ]
        }));
        assert!(!report.accepted);
        assert!(report.pack.is_none());
        assert_eq!(
            report.input_schema.as_deref(),
            Some(OPENCLAW_MEM_CONTEXT_PACK_SCHEMA)
        );
        assert_eq!(
            report.normalized_schema.as_deref(),
            Some(AGENT_HARNESS_CONTEXT_PACK_SCHEMA)
        );
        assert!(report.translation_applied);
        assert!(
            report
                .warnings
                .iter()
                .any(|warning| warning.contains("missing text"))
        );
    }

    #[test]
    fn context_pack_ingest_and_pin_contracts_are_deterministic() {
        let pack = parse_fixture(serde_json::json!({
            "schema": "agent-harness.context-pack.v1",
            "packId": "pack-1",
            "source": "openclaw-mem",
            "chunks": [
                {"citationId":"m1","text":"remember this","sourceUri":"memory://m1"}
            ]
        }));
        assert!(pack.accepted);

        let mut seen = BTreeSet::new();
        assert!(decide_memory_ingest("obs-1".to_string(), &mut seen).accepted);
        let dup = decide_memory_ingest("obs-1".to_string(), &mut seen);
        assert!(dup.duplicate);
        assert!(!dup.accepted);

        let hash = tool_description_hash("safe description");
        let pin = check_tool_description_pin(
            "openclaw-mem.recall".to_string(),
            "changed description",
            hash,
        );
        assert!(!pin.matched);
    }
}
