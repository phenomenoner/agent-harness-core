use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

const CONTEXT_PACK_SCHEMA: &str = "agent-harness.context-pack.v1";

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
            pack: None,
            warnings,
        };
    }
    let pack = match serde_json::from_str::<ContextPackV1>(&options.raw_json) {
        Ok(pack) => pack,
        Err(error) => {
            warnings.push(format!("invalid context pack JSON: {error}"));
            return ContextPackParseReport {
                accepted: false,
                pack: None,
                warnings,
            };
        }
    };
    if pack.schema != CONTEXT_PACK_SCHEMA {
        warnings.push(format!("unsupported context pack schema `{}`", pack.schema));
    }
    if pack
        .chunks
        .iter()
        .any(|chunk| chunk.citation_id.is_empty() || chunk.text.is_empty())
    {
        warnings.push("context pack contains chunk with missing citation id or text".to_string());
    }
    ContextPackParseReport {
        accepted: warnings.is_empty(),
        pack: warnings.is_empty().then_some(pack),
        warnings,
    }
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

    #[test]
    fn context_pack_ingest_and_pin_contracts_are_deterministic() {
        let raw = serde_json::json!({
            "schema": "agent-harness.context-pack.v1",
            "packId": "pack-1",
            "source": "openclaw-mem",
            "chunks": [
                {"citationId":"m1","text":"remember this","sourceUri":"memory://m1"}
            ]
        })
        .to_string();
        let pack = parse_context_pack(ContextPackParseOptions {
            raw_json: raw,
            max_bytes: 4096,
        });
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
