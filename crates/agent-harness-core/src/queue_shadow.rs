use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use rusqlite::{Connection, params};
use serde::Serialize;
use serde_json::Value;

const QUEUE_SHADOW_RECORD_SCHEMA: &str = "agent-harness.queue-shadow-record.v1";
const QUEUE_SHADOW_COMPARE_SCHEMA: &str = "agent-harness.queue-shadow-compare.v1";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueueShadowRecordOptions {
    pub harness_home: PathBuf,
    pub queue_id: String,
    pub item: Value,
    pub now_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueueShadowCompareOptions {
    pub harness_home: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct QueueShadowRecordReport {
    pub schema: &'static str,
    pub harness_home: PathBuf,
    pub database_file: PathBuf,
    pub queue_id: String,
    pub item_hash: String,
    pub status: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct QueueShadowCompareReport {
    pub schema: &'static str,
    pub harness_home: PathBuf,
    pub database_file: PathBuf,
    pub pending_file: PathBuf,
    pub receipt_file: PathBuf,
    pub jsonl_items: usize,
    pub sqlite_items: usize,
    pub divergences: Vec<QueueShadowDivergence>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct QueueShadowDivergence {
    pub queue_id: String,
    pub kind: QueueShadowDivergenceKind,
    pub detail: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum QueueShadowDivergenceKind {
    MissingSqlite,
    MissingJsonl,
    HashMismatch,
}

pub fn record_channel_turn_shadow(
    options: QueueShadowRecordOptions,
) -> io::Result<QueueShadowRecordReport> {
    let database_file = queue_shadow_db_file(&options.harness_home);
    let connection = open_queue_shadow_db(&database_file)?;
    let item_json = serde_json::to_string(&options.item).map_err(io::Error::other)?;
    let item_hash = fnv1a_64_hex(item_json.as_bytes());
    connection
        .execute(
            "INSERT INTO channel_turn_shadow (queue_id, item_json, item_hash, created_at_ms, updated_at_ms)
             VALUES (?1, ?2, ?3, ?4, ?4)
             ON CONFLICT(queue_id) DO UPDATE SET
               item_json=excluded.item_json,
               item_hash=excluded.item_hash,
               updated_at_ms=excluded.updated_at_ms",
            params![options.queue_id, item_json, item_hash, options.now_ms],
        )
        .map_err(io::Error::other)?;
    Ok(QueueShadowRecordReport {
        schema: QUEUE_SHADOW_RECORD_SCHEMA,
        harness_home: options.harness_home,
        database_file,
        queue_id: options.queue_id,
        item_hash,
        status: "recorded".to_string(),
    })
}

pub fn compare_channel_turn_shadow(
    options: QueueShadowCompareOptions,
) -> io::Result<QueueShadowCompareReport> {
    let database_file = queue_shadow_db_file(&options.harness_home);
    let pending_file = options
        .harness_home
        .join("state")
        .join("runtime-queue")
        .join("pending.jsonl");
    let receipt_file = options
        .harness_home
        .join("state")
        .join("runtime-queue")
        .join("queue-shadow-divergences.jsonl");
    let connection = open_queue_shadow_db(&database_file)?;
    let jsonl = read_pending_items(&pending_file)?;
    let sqlite = read_shadow_items(&connection)?;
    let mut divergences = Vec::new();
    for (queue_id, hash) in &jsonl {
        match sqlite.get(queue_id) {
            Some(sqlite_hash) if sqlite_hash == hash => {}
            Some(sqlite_hash) => divergences.push(QueueShadowDivergence {
                queue_id: queue_id.clone(),
                kind: QueueShadowDivergenceKind::HashMismatch,
                detail: format!("jsonl hash {hash} != sqlite hash {sqlite_hash}"),
            }),
            None => divergences.push(QueueShadowDivergence {
                queue_id: queue_id.clone(),
                kind: QueueShadowDivergenceKind::MissingSqlite,
                detail: "pending item has no SQLite shadow row".to_string(),
            }),
        }
    }
    for queue_id in sqlite.keys() {
        if !jsonl.contains_key(queue_id) {
            divergences.push(QueueShadowDivergence {
                queue_id: queue_id.clone(),
                kind: QueueShadowDivergenceKind::MissingJsonl,
                detail: "SQLite shadow row has no pending JSONL item".to_string(),
            });
        }
    }
    let report = QueueShadowCompareReport {
        schema: QUEUE_SHADOW_COMPARE_SCHEMA,
        harness_home: options.harness_home,
        database_file,
        pending_file,
        receipt_file: receipt_file.clone(),
        jsonl_items: jsonl.len(),
        sqlite_items: sqlite.len(),
        divergences,
    };
    append_json_line(&receipt_file, &report)?;
    Ok(report)
}

fn queue_shadow_db_file(harness_home: &Path) -> PathBuf {
    harness_home
        .join("state")
        .join("workers")
        .join("worker-store.sqlite")
}

fn open_queue_shadow_db(path: &Path) -> io::Result<Connection> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let connection = Connection::open(path).map_err(io::Error::other)?;
    connection
        .execute_batch(
            "CREATE TABLE IF NOT EXISTS channel_turn_shadow (
                queue_id TEXT PRIMARY KEY,
                item_json TEXT NOT NULL,
                item_hash TEXT NOT NULL,
                created_at_ms INTEGER NOT NULL,
                updated_at_ms INTEGER NOT NULL
             );",
        )
        .map_err(io::Error::other)?;
    Ok(connection)
}

fn read_pending_items(path: &Path) -> io::Result<std::collections::BTreeMap<String, String>> {
    let text = match fs::read_to_string(path) {
        Ok(text) => text,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return Ok(std::collections::BTreeMap::new());
        }
        Err(error) => return Err(error),
    };
    let mut items = std::collections::BTreeMap::new();
    for line in text.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let Ok(value) = serde_json::from_str::<Value>(trimmed) else {
            continue;
        };
        if let Some(queue_id) = value.get("queueId").and_then(Value::as_str) {
            items.insert(queue_id.to_string(), fnv1a_64_hex(trimmed.as_bytes()));
        }
    }
    Ok(items)
}

fn read_shadow_items(
    connection: &Connection,
) -> io::Result<std::collections::BTreeMap<String, String>> {
    let mut statement = connection
        .prepare("SELECT queue_id, item_hash FROM channel_turn_shadow ORDER BY queue_id")
        .map_err(io::Error::other)?;
    let rows = statement
        .query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })
        .map_err(io::Error::other)?;
    let mut items = std::collections::BTreeMap::new();
    for row in rows {
        let (queue_id, item_hash) = row.map_err(io::Error::other)?;
        items.insert(queue_id, item_hash);
    }
    Ok(items)
}

fn append_json_line(path: &Path, value: &impl Serialize) -> io::Result<()> {
    crate::append_jsonl_value(path, value)
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
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn shadow_compare_reports_missing_and_matching_rows() {
        let root = temp_root("shadow_compare_reports_missing_and_matching_rows");
        let harness_home = root.join(".agent-harness");
        let pending = harness_home
            .join("state")
            .join("runtime-queue")
            .join("pending.jsonl");
        fs::create_dir_all(pending.parent().unwrap()).unwrap();
        let item = serde_json::json!({"queueId":"q-1","status":"queued","messageText":"hello"});
        fs::write(&pending, serde_json::to_string(&item).unwrap()).unwrap();

        let missing = compare_channel_turn_shadow(QueueShadowCompareOptions {
            harness_home: harness_home.clone(),
        })
        .unwrap();
        assert_eq!(missing.divergences.len(), 1);
        assert_eq!(
            missing.divergences[0].kind,
            QueueShadowDivergenceKind::MissingSqlite
        );

        record_channel_turn_shadow(QueueShadowRecordOptions {
            harness_home: harness_home.clone(),
            queue_id: "q-1".to_string(),
            item,
            now_ms: 123,
        })
        .unwrap();
        let matched =
            compare_channel_turn_shadow(QueueShadowCompareOptions { harness_home }).unwrap();
        assert!(matched.divergences.is_empty());

        let _ = fs::remove_dir_all(root);
    }

    fn temp_root(test_name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "agent-harness-queue-shadow-{test_name}-{}-{nanos}",
            std::process::id()
        ))
    }
}
