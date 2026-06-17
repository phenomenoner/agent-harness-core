use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use rusqlite::{Connection, OpenFlags, params};
use serde::{Deserialize, Serialize};

pub const MEMORY_EMBEDDING_BACKFILL_SCHEMA: &str = "agent-harness.memory-embedding-backfill.v1";
pub const MEMORY_EMBEDDING_BACKFILL_CURSOR_SCHEMA: &str =
    "agent-harness.memory-embedding-backfill-cursor.v1";
pub const DEFAULT_MEMORY_BACKFILL_BATCH_SIZE: usize = 16;
pub const DEFAULT_MEMORY_BACKFILL_MAX_ITEMS: usize = 64;
pub const DEFAULT_MEMORY_BACKFILL_RATE_LIMIT_PER_MINUTE: usize = 64;
pub const DEFAULT_MEMORY_BACKFILL_RETRY_CAP: usize = 3;
pub const DEFAULT_MEMORY_BACKFILL_VECTOR_DIMENSION: i64 = 1536;
pub const DEFAULT_MEMORY_BACKFILL_COVERAGE_THRESHOLD_BPS: u64 = 9_500;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MemoryEmbeddingBackfillLane {
    Observations,
    EpisodicEvents,
    DocsChunks,
}

impl MemoryEmbeddingBackfillLane {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Observations => "observations",
            Self::EpisodicEvents => "episodic_events",
            Self::DocsChunks => "docs_chunks",
        }
    }
}

impl Default for MemoryEmbeddingBackfillLane {
    fn default() -> Self {
        Self::EpisodicEvents
    }
}

impl std::str::FromStr for MemoryEmbeddingBackfillLane {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "observations" | "observation" => Ok(Self::Observations),
            "episodic_events" | "episodic-events" | "episodes" | "episode" => {
                Ok(Self::EpisodicEvents)
            }
            "docs_chunks" | "docs-chunks" | "docs" | "documents" => Ok(Self::DocsChunks),
            other => Err(format!(
                "unsupported memory embedding backfill lane: {other}"
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemoryEmbeddingBackfillOptions {
    pub harness_home: PathBuf,
    pub lane: MemoryEmbeddingBackfillLane,
    pub model: String,
    pub vector_dimension: i64,
    pub batch_size: usize,
    pub max_items: usize,
    pub rate_limit_per_minute: usize,
    pub retry_cap: usize,
    pub coverage_threshold_bps: u64,
    pub now_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MemoryEmbeddingBackfillCursor {
    pub schema: String,
    pub lane: MemoryEmbeddingBackfillLane,
    pub model: String,
    pub vector_dimension: i64,
    pub next_offset: u64,
    pub processed_items: u64,
    pub attempted_items: u64,
    pub last_item_id: Option<String>,
    pub status: String,
    pub coverage_before_bps: Option<u64>,
    pub coverage_after_bps: Option<u64>,
    pub coverage_threshold_bps: u64,
    pub parity_claim_allowed: bool,
    pub window_started_at_ms: i64,
    pub window_used: u64,
    pub retry_count: u64,
    pub retry_cap: u64,
    pub updated_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MemoryEmbeddingBackfillReport {
    pub schema: &'static str,
    pub harness_home: PathBuf,
    pub sqlite_database: PathBuf,
    pub lane: MemoryEmbeddingBackfillLane,
    pub model: String,
    pub vector_dimension: i64,
    pub status: String,
    pub reason: String,
    pub cursor_file: PathBuf,
    pub receipt_file: PathBuf,
    pub latest_file: PathBuf,
    pub total_items: Option<u64>,
    pub indexed_items_before: Option<u64>,
    pub indexed_items_after: Option<u64>,
    pub missing_items_before: Option<u64>,
    pub selected_item_ids: Vec<String>,
    pub batch_size: usize,
    pub max_items: usize,
    pub rate_limit_per_minute: usize,
    pub rate_limit_remaining: usize,
    pub backpressure: bool,
    pub retry_count: u64,
    pub retry_cap: u64,
    pub coverage_before_bps: Option<u64>,
    pub coverage_after_bps: Option<u64>,
    pub coverage_threshold_bps: u64,
    pub parity_claim_allowed: bool,
    pub warnings: Vec<String>,
}

pub fn memory_embedding_backfill_cursor_file(
    harness_home: impl AsRef<Path>,
    lane: MemoryEmbeddingBackfillLane,
) -> PathBuf {
    harness_home
        .as_ref()
        .join("state")
        .join("memory")
        .join("embedding-backfill")
        .join(format!("{}.json", lane.as_str()))
}

pub fn memory_embedding_backfill_receipts_file(
    harness_home: impl AsRef<Path>,
    lane: MemoryEmbeddingBackfillLane,
) -> PathBuf {
    harness_home
        .as_ref()
        .join("state")
        .join("memory")
        .join("embedding-backfill")
        .join(format!("{}-receipts.jsonl", lane.as_str()))
}

pub fn memory_embedding_backfill_latest_file(
    harness_home: impl AsRef<Path>,
    lane: MemoryEmbeddingBackfillLane,
) -> PathBuf {
    harness_home
        .as_ref()
        .join("state")
        .join("memory")
        .join("embedding-backfill")
        .join(format!("{}-last.json", lane.as_str()))
}

pub fn run_memory_embedding_backfill(
    options: MemoryEmbeddingBackfillOptions,
) -> io::Result<MemoryEmbeddingBackfillReport> {
    let spec = lane_spec(options.lane);
    let cursor_file = memory_embedding_backfill_cursor_file(&options.harness_home, options.lane);
    let receipt_file = memory_embedding_backfill_receipts_file(&options.harness_home, options.lane);
    let latest_file = memory_embedding_backfill_latest_file(&options.harness_home, options.lane);
    let sqlite = options
        .harness_home
        .join("memory")
        .join("openclaw-mem.sqlite");
    let batch_size = options
        .batch_size
        .clamp(1, DEFAULT_MEMORY_BACKFILL_MAX_ITEMS);
    let max_items = options.max_items.max(1);
    let rate_limit_per_minute = options.rate_limit_per_minute;
    let retry_cap = options.retry_cap.max(1);
    let mut warnings = Vec::new();
    let mut cursor = read_cursor(&cursor_file)?.unwrap_or_else(|| {
        default_cursor(
            options.lane,
            &options.model,
            options.vector_dimension,
            options.coverage_threshold_bps,
            retry_cap,
            options.now_ms,
        )
    });
    if cursor.model != options.model || cursor.vector_dimension != options.vector_dimension {
        warnings.push(
            "backfill cursor model namespace or vector dimension changed; cursor offset reset"
                .to_string(),
        );
        cursor = default_cursor(
            options.lane,
            &options.model,
            options.vector_dimension,
            options.coverage_threshold_bps,
            retry_cap,
            options.now_ms,
        );
    }
    if !sqlite.is_file() {
        cursor.status = "blocked".to_string();
        cursor.retry_count = cursor.retry_count.saturating_add(1);
        cursor.updated_at_ms = options.now_ms;
        write_cursor(&cursor_file, &cursor)?;
        let report = report_from_parts(
            options,
            sqlite,
            cursor_file,
            receipt_file.clone(),
            latest_file.clone(),
            cursor,
            "blocked",
            "openclaw-mem SQLite snapshot is missing",
            None,
            None,
            None,
            Vec::new(),
            batch_size,
            max_items,
            rate_limit_per_minute,
            0,
            false,
            warnings,
        );
        write_report(&latest_file, &receipt_file, &report)?;
        return Ok(report);
    }
    let conn = Connection::open_with_flags(&sqlite, OpenFlags::SQLITE_OPEN_READ_ONLY)
        .map_err(io::Error::other)?;
    let total_items = sqlite_count(&conn, spec.source_table)?;
    let indexed_items_before = indexed_count(
        &conn,
        spec.embedding_table,
        spec.embedding_id_column,
        &options.model,
        options.vector_dimension,
    )?;
    let coverage_before_bps = coverage_bps(indexed_items_before, total_items);
    let missing_items_before = total_items.saturating_sub(indexed_items_before);
    let wrong_dims = distinct_wrong_dimensions(
        &conn,
        spec.embedding_table,
        &options.model,
        options.vector_dimension,
    )?;
    if !wrong_dims.is_empty() {
        warnings.push(format!(
            "embedding table {} contains model {} rows with unexpected dimensions {:?}",
            spec.embedding_table, options.model, wrong_dims
        ));
        cursor.status = "dimension-mismatch".to_string();
        cursor.retry_count = cursor.retry_count.saturating_add(1);
        cursor.coverage_before_bps = Some(coverage_before_bps);
        cursor.coverage_after_bps = Some(coverage_before_bps);
        cursor.parity_claim_allowed = false;
        cursor.updated_at_ms = options.now_ms;
        write_cursor(&cursor_file, &cursor)?;
        let report = report_from_parts(
            options,
            sqlite,
            cursor_file,
            receipt_file.clone(),
            latest_file.clone(),
            cursor,
            "dimension-mismatch",
            "backfill blocked by model namespace/vector dimension mismatch",
            Some(total_items),
            Some(indexed_items_before),
            Some(indexed_items_before),
            Vec::new(),
            batch_size,
            max_items,
            rate_limit_per_minute,
            0,
            false,
            warnings,
        );
        write_report(&latest_file, &receipt_file, &report)?;
        return Ok(report);
    }
    if total_items == indexed_items_before {
        cursor.status = "complete".to_string();
        cursor.coverage_before_bps = Some(coverage_before_bps);
        cursor.coverage_after_bps = Some(coverage_before_bps);
        cursor.parity_claim_allowed = coverage_before_bps >= options.coverage_threshold_bps;
        cursor.updated_at_ms = options.now_ms;
        write_cursor(&cursor_file, &cursor)?;
        let report = report_from_parts(
            options,
            sqlite,
            cursor_file,
            receipt_file.clone(),
            latest_file.clone(),
            cursor,
            "complete",
            "all lane rows already have embeddings for the requested model namespace and dimension",
            Some(total_items),
            Some(indexed_items_before),
            Some(indexed_items_before),
            Vec::new(),
            batch_size,
            max_items,
            rate_limit_per_minute,
            rate_limit_per_minute,
            false,
            warnings,
        );
        write_report(&latest_file, &receipt_file, &report)?;
        return Ok(report);
    }
    reset_rate_window_if_needed(&mut cursor, options.now_ms);
    let rate_remaining = rate_limit_per_minute.saturating_sub(cursor.window_used as usize);
    if rate_remaining == 0 {
        cursor.status = "rate-limited".to_string();
        cursor.coverage_before_bps = Some(coverage_before_bps);
        cursor.coverage_after_bps = Some(coverage_before_bps);
        cursor.parity_claim_allowed = false;
        cursor.updated_at_ms = options.now_ms;
        write_cursor(&cursor_file, &cursor)?;
        let report = report_from_parts(
            options,
            sqlite,
            cursor_file,
            receipt_file.clone(),
            latest_file.clone(),
            cursor,
            "rate-limited",
            "embedding backfill batch deferred by lane rate limit",
            Some(total_items),
            Some(indexed_items_before),
            Some(indexed_items_before),
            Vec::new(),
            batch_size,
            max_items,
            rate_limit_per_minute,
            0,
            true,
            warnings,
        );
        write_report(&latest_file, &receipt_file, &report)?;
        return Ok(report);
    }
    let allowed = batch_size.min(max_items).min(rate_remaining);
    let mut selected = missing_item_ids(
        &conn,
        spec,
        &options.model,
        options.vector_dimension,
        allowed,
        cursor.next_offset,
    )?;
    if selected.is_empty() && missing_items_before > 0 && cursor.next_offset > 0 {
        cursor.next_offset = 0;
        selected = missing_item_ids(
            &conn,
            spec,
            &options.model,
            options.vector_dimension,
            allowed,
            0,
        )?;
        warnings.push("backfill cursor offset wrapped to the first missing item".to_string());
    }
    let processed = u64::try_from(selected.len()).unwrap_or(u64::MAX);
    cursor.status = if selected.is_empty() {
        "pending-empty-batch".to_string()
    } else {
        "planned".to_string()
    };
    cursor.attempted_items = cursor.attempted_items.saturating_add(processed);
    cursor.processed_items = cursor.processed_items.saturating_add(processed);
    cursor.window_used = cursor.window_used.saturating_add(processed);
    cursor.next_offset = cursor.next_offset.saturating_add(processed);
    cursor.last_item_id = selected.last().cloned();
    cursor.retry_cap = u64::try_from(retry_cap).unwrap_or(u64::MAX);
    cursor.coverage_before_bps = Some(coverage_before_bps);
    cursor.coverage_after_bps = Some(coverage_before_bps);
    cursor.coverage_threshold_bps = options.coverage_threshold_bps;
    cursor.parity_claim_allowed = false;
    cursor.updated_at_ms = options.now_ms;
    write_cursor(&cursor_file, &cursor)?;
    let report = report_from_parts(
        options,
        sqlite,
        cursor_file,
        receipt_file.clone(),
        latest_file.clone(),
        cursor,
        if selected.is_empty() {
            "pending-empty-batch"
        } else {
            "planned"
        },
        "embedding backfill batch planned; provider execution is external to this offline-safe worker step",
        Some(total_items),
        Some(indexed_items_before),
        Some(indexed_items_before),
        selected,
        batch_size,
        max_items,
        rate_limit_per_minute,
        rate_remaining.saturating_sub(processed as usize),
        false,
        warnings,
    );
    write_report(&latest_file, &receipt_file, &report)?;
    Ok(report)
}

fn report_from_parts(
    options: MemoryEmbeddingBackfillOptions,
    sqlite_database: PathBuf,
    cursor_file: PathBuf,
    receipt_file: PathBuf,
    latest_file: PathBuf,
    cursor: MemoryEmbeddingBackfillCursor,
    status: &str,
    reason: &str,
    total_items: Option<u64>,
    indexed_items_before: Option<u64>,
    indexed_items_after: Option<u64>,
    selected_item_ids: Vec<String>,
    batch_size: usize,
    max_items: usize,
    rate_limit_per_minute: usize,
    rate_limit_remaining: usize,
    backpressure: bool,
    warnings: Vec<String>,
) -> MemoryEmbeddingBackfillReport {
    let missing_items_before = total_items
        .zip(indexed_items_before)
        .map(|(total, indexed)| total.saturating_sub(indexed));
    MemoryEmbeddingBackfillReport {
        schema: MEMORY_EMBEDDING_BACKFILL_SCHEMA,
        harness_home: options.harness_home,
        sqlite_database,
        lane: options.lane,
        model: options.model,
        vector_dimension: options.vector_dimension,
        status: status.to_string(),
        reason: reason.to_string(),
        cursor_file,
        receipt_file,
        latest_file,
        total_items,
        indexed_items_before,
        indexed_items_after,
        missing_items_before,
        selected_item_ids,
        batch_size,
        max_items,
        rate_limit_per_minute,
        rate_limit_remaining,
        backpressure,
        retry_count: cursor.retry_count,
        retry_cap: cursor.retry_cap,
        coverage_before_bps: cursor.coverage_before_bps,
        coverage_after_bps: cursor.coverage_after_bps,
        coverage_threshold_bps: cursor.coverage_threshold_bps,
        parity_claim_allowed: cursor.parity_claim_allowed,
        warnings,
    }
}

fn write_report(
    latest_file: &Path,
    receipt_file: &Path,
    report: &MemoryEmbeddingBackfillReport,
) -> io::Result<()> {
    crate::write_json_atomic(latest_file, report)?;
    crate::append_jsonl_value(receipt_file, report)
}

fn read_cursor(path: &Path) -> io::Result<Option<MemoryEmbeddingBackfillCursor>> {
    let text = match fs::read_to_string(path) {
        Ok(text) => text,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    serde_json::from_str(&text)
        .map(Some)
        .map_err(io::Error::other)
}

fn write_cursor(path: &Path, cursor: &MemoryEmbeddingBackfillCursor) -> io::Result<()> {
    crate::write_json_atomic(path, cursor)
}

fn default_cursor(
    lane: MemoryEmbeddingBackfillLane,
    model: &str,
    vector_dimension: i64,
    coverage_threshold_bps: u64,
    retry_cap: usize,
    now_ms: i64,
) -> MemoryEmbeddingBackfillCursor {
    MemoryEmbeddingBackfillCursor {
        schema: MEMORY_EMBEDDING_BACKFILL_CURSOR_SCHEMA.to_string(),
        lane,
        model: model.to_string(),
        vector_dimension,
        next_offset: 0,
        processed_items: 0,
        attempted_items: 0,
        last_item_id: None,
        status: "initialized".to_string(),
        coverage_before_bps: None,
        coverage_after_bps: None,
        coverage_threshold_bps,
        parity_claim_allowed: false,
        window_started_at_ms: now_ms,
        window_used: 0,
        retry_count: 0,
        retry_cap: u64::try_from(retry_cap).unwrap_or(u64::MAX),
        updated_at_ms: now_ms,
    }
}

fn reset_rate_window_if_needed(cursor: &mut MemoryEmbeddingBackfillCursor, now_ms: i64) {
    if now_ms.saturating_sub(cursor.window_started_at_ms) >= 60_000 {
        cursor.window_started_at_ms = now_ms;
        cursor.window_used = 0;
    }
}

#[derive(Debug, Clone, Copy)]
struct LaneSpec {
    source_table: &'static str,
    source_id_expr: &'static str,
    embedding_table: &'static str,
    embedding_id_column: &'static str,
}

fn lane_spec(lane: MemoryEmbeddingBackfillLane) -> LaneSpec {
    match lane {
        MemoryEmbeddingBackfillLane::Observations => LaneSpec {
            source_table: "observations",
            source_id_expr: "id",
            embedding_table: "observation_embeddings",
            embedding_id_column: "observation_id",
        },
        MemoryEmbeddingBackfillLane::EpisodicEvents => LaneSpec {
            source_table: "episodic_events",
            source_id_expr: "rowid",
            embedding_table: "episodic_event_embeddings",
            embedding_id_column: "event_row_id",
        },
        MemoryEmbeddingBackfillLane::DocsChunks => LaneSpec {
            source_table: "docs_chunks",
            source_id_expr: "id",
            embedding_table: "docs_embeddings",
            embedding_id_column: "chunk_rowid",
        },
    }
}

fn sqlite_count(conn: &Connection, table: &str) -> io::Result<u64> {
    let sql = format!("SELECT count(*) FROM {table}");
    let count = conn
        .query_row(&sql, [], |row| row.get::<_, i64>(0))
        .map_err(io::Error::other)?;
    u64::try_from(count).map_err(io::Error::other)
}

fn indexed_count(
    conn: &Connection,
    embedding_table: &str,
    embedding_id_column: &str,
    model: &str,
    vector_dimension: i64,
) -> io::Result<u64> {
    let sql = format!(
        "SELECT count(DISTINCT {embedding_id_column}) FROM {embedding_table} WHERE model = ?1 AND dim = ?2"
    );
    let count = conn
        .query_row(&sql, params![model, vector_dimension], |row| {
            row.get::<_, i64>(0)
        })
        .map_err(io::Error::other)?;
    u64::try_from(count).map_err(io::Error::other)
}

fn distinct_wrong_dimensions(
    conn: &Connection,
    embedding_table: &str,
    model: &str,
    vector_dimension: i64,
) -> io::Result<Vec<i64>> {
    let sql = format!("SELECT DISTINCT dim FROM {embedding_table} WHERE model = ?1 AND dim != ?2");
    let mut stmt = conn.prepare(&sql).map_err(io::Error::other)?;
    let rows = stmt
        .query_map(params![model, vector_dimension], |row| row.get::<_, i64>(0))
        .map_err(io::Error::other)?;
    let mut dims = Vec::new();
    for row in rows {
        dims.push(row.map_err(io::Error::other)?);
    }
    Ok(dims)
}

fn missing_item_ids(
    conn: &Connection,
    spec: LaneSpec,
    model: &str,
    vector_dimension: i64,
    limit: usize,
    offset: u64,
) -> io::Result<Vec<String>> {
    let sql = format!(
        "SELECT CAST({source_id} AS TEXT) FROM {source_table}
         WHERE {source_id} NOT IN (
           SELECT {embedding_id} FROM {embedding_table} WHERE model = ?1 AND dim = ?2
         )
         ORDER BY {source_id}
         LIMIT ?3 OFFSET ?4",
        source_id = spec.source_id_expr,
        source_table = spec.source_table,
        embedding_id = spec.embedding_id_column,
        embedding_table = spec.embedding_table
    );
    let mut stmt = conn.prepare(&sql).map_err(io::Error::other)?;
    let limit = i64::try_from(limit).map_err(io::Error::other)?;
    let offset = i64::try_from(offset).map_err(io::Error::other)?;
    let rows = stmt
        .query_map(params![model, vector_dimension, limit, offset], |row| {
            row.get::<_, String>(0)
        })
        .map_err(io::Error::other)?;
    let mut ids = Vec::new();
    for row in rows {
        ids.push(row.map_err(io::Error::other)?);
    }
    Ok(ids)
}

fn coverage_bps(indexed: u64, total: u64) -> u64 {
    if total == 0 {
        return 0;
    }
    indexed.saturating_mul(10_000) / total
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn memory_backfill_cursor_tracks_episodic_lane_and_rate_limit() {
        let root = temp_root("cursor_tracks_episodic_lane_and_rate_limit");
        let harness_home = root.join("harness");
        create_sqlite(&harness_home, false);

        let report = run_memory_embedding_backfill(MemoryEmbeddingBackfillOptions {
            harness_home: harness_home.clone(),
            lane: MemoryEmbeddingBackfillLane::EpisodicEvents,
            model: "test-embedding".to_string(),
            vector_dimension: 2,
            batch_size: 2,
            max_items: 2,
            rate_limit_per_minute: 2,
            retry_cap: 3,
            coverage_threshold_bps: 10_000,
            now_ms: 1_000,
        })
        .unwrap();

        assert_eq!(report.status, "planned");
        assert_eq!(report.selected_item_ids, vec!["2", "3"]);
        assert_eq!(report.total_items, Some(3));
        assert_eq!(report.indexed_items_before, Some(1));
        assert_eq!(report.coverage_before_bps, Some(3_333));
        assert!(!report.parity_claim_allowed);
        let cursor_text = fs::read_to_string(memory_embedding_backfill_cursor_file(
            &harness_home,
            report.lane,
        ))
        .unwrap();
        assert!(cursor_text.contains(r#""nextOffset": 2"#));

        let rate_limited = run_memory_embedding_backfill(MemoryEmbeddingBackfillOptions {
            harness_home: harness_home.clone(),
            lane: MemoryEmbeddingBackfillLane::EpisodicEvents,
            model: "test-embedding".to_string(),
            vector_dimension: 2,
            batch_size: 2,
            max_items: 2,
            rate_limit_per_minute: 2,
            retry_cap: 3,
            coverage_threshold_bps: 10_000,
            now_ms: 1_500,
        })
        .unwrap();
        assert_eq!(rate_limited.status, "rate-limited");
        assert!(rate_limited.backpressure);
        assert!(memory_embedding_backfill_receipts_file(&harness_home, report.lane).is_file());

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn memory_backfill_dimension_mismatch_blocks_parity_claim() {
        let root = temp_root("dimension_mismatch_blocks_parity_claim");
        let harness_home = root.join("harness");
        create_sqlite(&harness_home, true);

        let report = run_memory_embedding_backfill(MemoryEmbeddingBackfillOptions {
            harness_home: harness_home.clone(),
            lane: MemoryEmbeddingBackfillLane::EpisodicEvents,
            model: "test-embedding".to_string(),
            vector_dimension: 2,
            batch_size: 2,
            max_items: 2,
            rate_limit_per_minute: 2,
            retry_cap: 3,
            coverage_threshold_bps: 9_500,
            now_ms: 2_000,
        })
        .unwrap();

        assert_eq!(report.status, "dimension-mismatch");
        assert_eq!(report.retry_count, 1);
        assert!(!report.parity_claim_allowed);
        assert!(
            report
                .warnings
                .iter()
                .any(|warning| warning.contains("unexpected dimensions"))
        );

        let _ = fs::remove_dir_all(root);
    }

    fn create_sqlite(harness_home: &Path, wrong_dim: bool) {
        let memory_dir = harness_home.join("memory");
        fs::create_dir_all(&memory_dir).unwrap();
        let sqlite = memory_dir.join("openclaw-mem.sqlite");
        let conn = Connection::open(&sqlite).unwrap();
        conn.execute_batch(
            "
            CREATE TABLE episodic_events (
                ts TEXT,
                summary TEXT
            );
            CREATE TABLE episodic_event_embeddings (
                event_row_id INTEGER,
                model TEXT,
                dim INTEGER,
                vector BLOB,
                norm REAL
            );
            ",
        )
        .unwrap();
        for summary in ["one", "two", "three"] {
            conn.execute(
                "INSERT INTO episodic_events (ts, summary) VALUES ('2026-06-17', ?1)",
                [summary],
            )
            .unwrap();
        }
        conn.execute(
            "INSERT INTO episodic_event_embeddings (event_row_id, model, dim, vector, norm) VALUES (1, 'test-embedding', ?1, x'0000', 1.0)",
            [if wrong_dim { 3 } else { 2 }],
        )
        .unwrap();
    }

    fn temp_root(test_name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "agent-harness-memory-backfill-{test_name}-{}-{nanos}",
            std::process::id()
        ))
    }
}
