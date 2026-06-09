use std::fs;
use std::io;
use std::path::{Path, PathBuf};

const MEMORY_SEARCH_RECEIPT_SCHEMA: &str = "openclaw-harness.memory-search-receipt.v1";
const DEFAULT_MAX_FILE_BYTES: u64 = 1_000_000;
const DEFAULT_SNIPPET_CHARS: usize = 240;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemorySearchOptions {
    pub harness_home: PathBuf,
    pub query: String,
    pub limit: usize,
    pub max_file_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemorySearchReport {
    pub schema: &'static str,
    pub harness_home: PathBuf,
    pub memory_dir: PathBuf,
    pub status: MemorySearchStatus,
    pub reason: String,
    pub query: String,
    pub searched_files: usize,
    pub skipped_files: usize,
    pub hits: Vec<MemorySearchHit>,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemorySearchStatus {
    Ready,
    Failed,
}

impl MemorySearchStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Ready => "ready",
            Self::Failed => "failed",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemorySearchHit {
    pub path: PathBuf,
    pub line: usize,
    pub score: usize,
    pub snippet: String,
}

pub fn memory_search_receipts_file(harness_home: impl AsRef<Path>) -> PathBuf {
    harness_home
        .as_ref()
        .join("state")
        .join("memory")
        .join("search-receipts.jsonl")
}

pub fn memory_search_latest_file(harness_home: impl AsRef<Path>) -> PathBuf {
    harness_home
        .as_ref()
        .join("state")
        .join("memory")
        .join("search-last.json")
}

pub fn search_imported_memory(options: MemorySearchOptions) -> io::Result<MemorySearchReport> {
    let memory_dir = options.harness_home.join("memory");
    let query = options.query.trim().to_string();
    if query.is_empty() {
        return Ok(MemorySearchReport {
            schema: MEMORY_SEARCH_RECEIPT_SCHEMA,
            harness_home: options.harness_home,
            memory_dir,
            status: MemorySearchStatus::Failed,
            reason: "query must not be empty".to_string(),
            query,
            searched_files: 0,
            skipped_files: 0,
            hits: Vec::new(),
            warnings: Vec::new(),
        });
    }
    if !memory_dir.is_dir() {
        return Ok(MemorySearchReport {
            schema: MEMORY_SEARCH_RECEIPT_SCHEMA,
            harness_home: options.harness_home,
            memory_dir: memory_dir.clone(),
            status: MemorySearchStatus::Failed,
            reason: format!(
                "imported memory directory not found at {}",
                memory_dir.display()
            ),
            query,
            searched_files: 0,
            skipped_files: 0,
            hits: Vec::new(),
            warnings: Vec::new(),
        });
    }

    let terms = query_terms(&query);
    let max_file_bytes = if options.max_file_bytes == 0 {
        DEFAULT_MAX_FILE_BYTES
    } else {
        options.max_file_bytes
    };
    let mut warnings = Vec::new();
    let mut searched_files = 0usize;
    let mut skipped_files = 0usize;
    let mut hits = Vec::new();
    let mut stack = vec![memory_dir.clone()];
    while let Some(dir) = stack.pop() {
        let entries = match fs::read_dir(&dir) {
            Ok(entries) => entries,
            Err(error) => {
                warnings.push(format!("could not read {}: {error}", dir.display()));
                continue;
            }
        };
        for entry in entries {
            let entry = match entry {
                Ok(entry) => entry,
                Err(error) => {
                    warnings.push(format!("could not read memory directory entry: {error}"));
                    continue;
                }
            };
            let path = entry.path();
            let file_type = match entry.file_type() {
                Ok(file_type) => file_type,
                Err(error) => {
                    warnings.push(format!("could not inspect {}: {error}", path.display()));
                    continue;
                }
            };
            if file_type.is_dir() {
                if is_binary_memory_backend_dir(&path) {
                    continue;
                }
                stack.push(path);
                continue;
            }
            if !file_type.is_file() || !is_searchable_memory_file(&path) {
                continue;
            }
            let metadata = match entry.metadata() {
                Ok(metadata) => metadata,
                Err(error) => {
                    warnings.push(format!("could not stat {}: {error}", path.display()));
                    skipped_files += 1;
                    continue;
                }
            };
            if metadata.len() > max_file_bytes {
                warnings.push(format!(
                    "skipped {} because size {} exceeds maxFileBytes {}",
                    path.display(),
                    metadata.len(),
                    max_file_bytes
                ));
                skipped_files += 1;
                continue;
            }
            let text = match fs::read_to_string(&path) {
                Ok(text) => text,
                Err(error) => {
                    warnings.push(format!(
                        "could not read {} as UTF-8: {error}",
                        path.display()
                    ));
                    skipped_files += 1;
                    continue;
                }
            };
            searched_files += 1;
            collect_text_hits(&path, &text, &query, &terms, &mut hits);
        }
    }

    hits.sort_by(|left, right| {
        right
            .score
            .cmp(&left.score)
            .then_with(|| left.path.cmp(&right.path))
            .then_with(|| left.line.cmp(&right.line))
    });
    hits.truncate(options.limit.max(1));

    Ok(MemorySearchReport {
        schema: MEMORY_SEARCH_RECEIPT_SCHEMA,
        harness_home: options.harness_home,
        memory_dir,
        status: MemorySearchStatus::Ready,
        reason: format!(
            "read-only imported markdown/text memory search completed; hits={}, searchedFiles={}, skippedFiles={}",
            hits.len(),
            searched_files,
            skipped_files
        ),
        query,
        searched_files,
        skipped_files,
        hits,
        warnings,
    })
}

pub fn write_memory_search_receipt(report: &MemorySearchReport) -> io::Result<()> {
    let last_file = memory_search_latest_file(&report.harness_home);
    let receipts_file = memory_search_receipts_file(&report.harness_home);
    if let Some(parent) = last_file.parent() {
        fs::create_dir_all(parent)?;
    }
    let value = serde_json::json!({
        "schema": report.schema,
        "status": report.status.as_str(),
        "reason": report.reason,
        "memoryDir": report.memory_dir,
        "queryLength": report.query.chars().count(),
        "searchedFiles": report.searched_files,
        "skippedFiles": report.skipped_files,
        "hitCount": report.hits.len(),
        "hits": report.hits.iter().map(|hit| {
            serde_json::json!({
                "path": hit.path,
                "line": hit.line,
                "score": hit.score,
            })
        }).collect::<Vec<_>>(),
        "warnings": report.warnings,
    });
    fs::write(&last_file, serde_json::to_string_pretty(&value)?)?;
    let mut file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(receipts_file)?;
    use std::io::Write;
    writeln!(file, "{value}")?;
    Ok(())
}

fn collect_text_hits(
    path: &Path,
    text: &str,
    query: &str,
    terms: &[String],
    hits: &mut Vec<MemorySearchHit>,
) {
    let query_lower = query.to_lowercase();
    for (index, line) in text.lines().enumerate() {
        let line_lower = line.to_lowercase();
        if !line_lower.contains(&query_lower) && !terms.iter().all(|term| line_lower.contains(term))
        {
            continue;
        }
        let mut score = occurrences(&line_lower, &query_lower);
        for term in terms {
            score += occurrences(&line_lower, term);
        }
        hits.push(MemorySearchHit {
            path: path.to_path_buf(),
            line: index + 1,
            score: score.max(1),
            snippet: snippet(line),
        });
    }
}

fn query_terms(query: &str) -> Vec<String> {
    query
        .split_whitespace()
        .map(str::trim)
        .filter(|term| !term.is_empty())
        .map(str::to_lowercase)
        .collect()
}

fn occurrences(haystack: &str, needle: &str) -> usize {
    if needle.is_empty() {
        return 0;
    }
    haystack.match_indices(needle).count()
}

fn snippet(line: &str) -> String {
    let trimmed = line.trim();
    let mut output = String::new();
    for ch in trimmed.chars().take(DEFAULT_SNIPPET_CHARS) {
        output.push(ch);
    }
    if trimmed.chars().count() > DEFAULT_SNIPPET_CHARS {
        output.push_str("...");
    }
    output
}

fn is_binary_memory_backend_dir(path: &Path) -> bool {
    path.file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| matches!(name, "qdrant-edge" | "lancedb"))
}

fn is_searchable_memory_file(path: &Path) -> bool {
    path.extension()
        .and_then(|ext| ext.to_str())
        .is_some_and(|ext| {
            matches!(
                ext.to_ascii_lowercase().as_str(),
                "md" | "txt" | "json" | "jsonl"
            )
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn search_imported_memory_finds_markdown_hits_and_skips_backends() {
        let root = temp_root("search_imported_memory_finds_markdown_hits_and_skips_backends");
        let harness_home = root.join("harness");
        let memory = harness_home.join("memory");
        fs::create_dir_all(memory.join("qdrant-edge").join("collections")).unwrap();
        fs::write(
            memory.join("2026-06-09.md"),
            "# Memory\n\nOpenClaw should remember the Windows harness handoff.",
        )
        .unwrap();
        fs::write(
            memory
                .join("qdrant-edge")
                .join("collections")
                .join("raw.txt"),
            "OpenClaw",
        )
        .unwrap();

        let report = search_imported_memory(MemorySearchOptions {
            harness_home,
            query: "windows handoff".to_string(),
            limit: 5,
            max_file_bytes: DEFAULT_MAX_FILE_BYTES,
        })
        .unwrap();

        assert_eq!(report.status, MemorySearchStatus::Ready);
        assert_eq!(report.searched_files, 1);
        assert_eq!(report.hits.len(), 1);
        assert!(report.hits[0].snippet.contains("Windows harness handoff"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn write_memory_search_receipt_omits_snippets() {
        let root = temp_root("write_memory_search_receipt_omits_snippets");
        let harness_home = root.join("harness");
        let memory = harness_home.join("memory");
        fs::create_dir_all(&memory).unwrap();
        fs::write(memory.join("MEMORY.md"), "sensitive phrase").unwrap();
        let report = search_imported_memory(MemorySearchOptions {
            harness_home: harness_home.clone(),
            query: "sensitive".to_string(),
            limit: 5,
            max_file_bytes: DEFAULT_MAX_FILE_BYTES,
        })
        .unwrap();

        write_memory_search_receipt(&report).unwrap();

        let receipt = fs::read_to_string(memory_search_latest_file(&harness_home)).unwrap();
        assert!(receipt.contains(r#""hitCount": 1"#));
        assert!(!receipt.contains("sensitive phrase"));

        let _ = fs::remove_dir_all(root);
    }

    fn temp_root(test_name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "openclaw-harness-memory-{test_name}-{}-{nanos}",
            std::process::id()
        ))
    }
}
