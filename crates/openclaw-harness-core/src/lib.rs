use std::fs;
use std::io;
use std::path::{Path, PathBuf};

pub const PROMPT_FILE_NAMES: &[&str] = &[
    "AGENTS.md",
    "SOUL.md",
    "TOOLS.md",
    "USER.md",
    "IDENTITY.md",
    "HEARTBEAT.md",
    "BOOTSTRAP.md",
];

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpenClawSource {
    pub home: PathBuf,
    pub workspace: PathBuf,
}

impl OpenClawSource {
    pub fn new(home: impl Into<PathBuf>) -> Self {
        let home = home.into();
        let workspace = home.join("workspace");
        Self { home, workspace }
    }

    pub fn with_workspace(home: impl Into<PathBuf>, workspace: impl Into<PathBuf>) -> Self {
        Self {
            home: home.into(),
            workspace: workspace.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpenClawInventory {
    pub source: OpenClawSource,
    pub has_config: bool,
    pub prompt_files: Vec<PathBuf>,
    pub session_indexes: Vec<PathBuf>,
    pub transcript_files: usize,
    pub trajectory_files: usize,
    pub codex_binding_files: usize,
    pub memory_files: usize,
    pub plugin_install_record: bool,
    pub plugin_state_db: bool,
}

impl OpenClawInventory {
    pub fn is_empty(&self) -> bool {
        !self.has_config
            && self.prompt_files.is_empty()
            && self.session_indexes.is_empty()
            && self.transcript_files == 0
            && self.trajectory_files == 0
            && self.codex_binding_files == 0
            && self.memory_files == 0
            && !self.plugin_install_record
            && !self.plugin_state_db
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImportPlan {
    pub phases: Vec<ImportPhase>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImportPhase {
    pub name: &'static str,
    pub required: bool,
    pub status: ImportPhaseStatus,
    pub notes: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ImportPhaseStatus {
    Ready,
    Missing,
    Deferred,
}

pub fn inventory(source: OpenClawSource) -> io::Result<OpenClawInventory> {
    let has_config = source.home.join("openclaw.json").is_file();
    let prompt_files = existing_prompt_files(&source.workspace);
    let session_indexes = find_named_files(&source.home.join("agents"), "sessions.json")?;
    let transcript_files = count_transcript_files(&source.home.join("agents"))?;
    let trajectory_files =
        count_files_with_suffix(&source.home.join("agents"), ".trajectory.jsonl")?;
    let codex_binding_files =
        count_files_with_suffix(&source.home.join("agents"), ".jsonl.codex-app-server.json")?;
    let memory_files = count_regular_files(&source.home.join("memory"))?;
    let plugin_install_record = source.home.join("plugins").join("installs.json").is_file();
    let plugin_state_db = source
        .home
        .join("plugin-state")
        .join("state.sqlite")
        .is_file();

    Ok(OpenClawInventory {
        source,
        has_config,
        prompt_files,
        session_indexes,
        transcript_files,
        trajectory_files,
        codex_binding_files,
        memory_files,
        plugin_install_record,
        plugin_state_db,
    })
}

pub fn build_import_plan(inv: &OpenClawInventory) -> ImportPlan {
    let mut phases = Vec::new();

    phases.push(ImportPhase {
        name: "config",
        required: true,
        status: if inv.has_config {
            ImportPhaseStatus::Ready
        } else {
            ImportPhaseStatus::Missing
        },
        notes: vec![if inv.has_config {
            "openclaw.json found; parse and redact secrets before writing new config".to_string()
        } else {
            "openclaw.json not found at source home".to_string()
        }],
    });

    phases.push(ImportPhase {
        name: "workspace",
        required: true,
        status: if inv.prompt_files.is_empty() {
            ImportPhaseStatus::Missing
        } else {
            ImportPhaseStatus::Ready
        },
        notes: vec![format!(
            "{} prompt files found under workspace",
            inv.prompt_files.len()
        )],
    });

    phases.push(ImportPhase {
        name: "sessions",
        required: false,
        status: if inv.session_indexes.is_empty() {
            ImportPhaseStatus::Missing
        } else {
            ImportPhaseStatus::Ready
        },
        notes: vec![format!(
            "{} session indexes, {} transcripts, {} trajectories, {} Codex binding mirrors",
            inv.session_indexes.len(),
            inv.transcript_files,
            inv.trajectory_files,
            inv.codex_binding_files
        )],
    });

    phases.push(ImportPhase {
        name: "memory",
        required: false,
        status: if inv.memory_files == 0 {
            ImportPhaseStatus::Missing
        } else {
            ImportPhaseStatus::Ready
        },
        notes: vec![format!(
            "{} memory files found; SQLite sources require stopped gateway or backup API",
            inv.memory_files
        )],
    });

    phases.push(ImportPhase {
        name: "plugins",
        required: false,
        status: if inv.plugin_install_record || inv.plugin_state_db {
            ImportPhaseStatus::Deferred
        } else {
            ImportPhaseStatus::Missing
        },
        notes: vec![format!(
            "install record: {}, plugin state db: {}; execution should route through sidecar first",
            inv.plugin_install_record, inv.plugin_state_db
        )],
    });

    ImportPlan { phases }
}

fn existing_prompt_files(workspace: &Path) -> Vec<PathBuf> {
    PROMPT_FILE_NAMES
        .iter()
        .map(|name| workspace.join(name))
        .filter(|path| path.is_file())
        .collect()
}

fn find_named_files(root: &Path, name: &str) -> io::Result<Vec<PathBuf>> {
    let mut matches = Vec::new();
    visit_files(root, &mut |path| {
        if path.file_name().and_then(|value| value.to_str()) == Some(name) {
            matches.push(path.to_path_buf());
        }
    })?;
    Ok(matches)
}

fn count_regular_files(root: &Path) -> io::Result<usize> {
    let mut count = 0;
    visit_files(root, &mut |_| count += 1)?;
    Ok(count)
}

fn count_files_with_suffix(root: &Path, suffix: &str) -> io::Result<usize> {
    let mut count = 0;
    visit_files(root, &mut |path| {
        if path.to_string_lossy().ends_with(suffix) {
            count += 1;
        }
    })?;
    Ok(count)
}

fn count_transcript_files(root: &Path) -> io::Result<usize> {
    let mut count = 0;
    visit_files(root, &mut |path| {
        let path = path.to_string_lossy();
        if path.ends_with(".jsonl") && !path.ends_with(".trajectory.jsonl") {
            count += 1;
        }
    })?;
    Ok(count)
}

fn visit_files(root: &Path, on_file: &mut impl FnMut(&Path)) -> io::Result<()> {
    if !root.exists() {
        return Ok(());
    }

    for entry in fs::read_dir(root)? {
        let entry = entry?;
        let path = entry.path();
        let file_type = entry.file_type()?;
        if file_type.is_dir() {
            visit_files(&path, on_file)?;
        } else if file_type.is_file() {
            on_file(&path);
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn inventory_detects_openclaw_layout() {
        let root = temp_root("inventory_detects_openclaw_layout");
        let home = root.join(".openclaw");
        let workspace = home.join("workspace");
        let agent_sessions = home.join("agents").join("main").join("sessions");
        fs::create_dir_all(&workspace).unwrap();
        fs::create_dir_all(&agent_sessions).unwrap();
        fs::create_dir_all(home.join("memory")).unwrap();
        fs::create_dir_all(home.join("plugins")).unwrap();
        fs::create_dir_all(home.join("plugin-state")).unwrap();

        fs::write(home.join("openclaw.json"), "{}").unwrap();
        fs::write(workspace.join("AGENTS.md"), "# Agent").unwrap();
        fs::write(agent_sessions.join("sessions.json"), "{}").unwrap();
        fs::write(agent_sessions.join("abc.jsonl"), "{}\n").unwrap();
        fs::write(agent_sessions.join("abc.trajectory.jsonl"), "{}\n").unwrap();
        fs::write(agent_sessions.join("abc.jsonl.codex-app-server.json"), "{}").unwrap();
        fs::write(home.join("memory").join("2026-06-08.md"), "# Memory").unwrap();
        fs::write(home.join("plugins").join("installs.json"), "{}").unwrap();
        fs::write(home.join("plugin-state").join("state.sqlite"), "").unwrap();

        let inv = inventory(OpenClawSource::new(&home)).unwrap();

        assert!(inv.has_config);
        assert_eq!(inv.prompt_files.len(), 1);
        assert_eq!(inv.session_indexes.len(), 1);
        assert_eq!(inv.transcript_files, 1);
        assert_eq!(inv.trajectory_files, 1);
        assert_eq!(inv.codex_binding_files, 1);
        assert_eq!(inv.memory_files, 1);
        assert!(inv.plugin_install_record);
        assert!(inv.plugin_state_db);

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn import_plan_marks_ready_and_deferred_phases() {
        let inv = OpenClawInventory {
            source: OpenClawSource::new("unused"),
            has_config: true,
            prompt_files: vec![PathBuf::from("AGENTS.md")],
            session_indexes: vec![PathBuf::from("sessions.json")],
            transcript_files: 2,
            trajectory_files: 1,
            codex_binding_files: 1,
            memory_files: 3,
            plugin_install_record: true,
            plugin_state_db: true,
        };

        let plan = build_import_plan(&inv);

        assert_eq!(plan.phases[0].status, ImportPhaseStatus::Ready);
        assert_eq!(plan.phases[1].status, ImportPhaseStatus::Ready);
        assert_eq!(plan.phases[2].status, ImportPhaseStatus::Ready);
        assert_eq!(plan.phases[3].status, ImportPhaseStatus::Ready);
        assert_eq!(plan.phases[4].status, ImportPhaseStatus::Deferred);
    }

    fn temp_root(test_name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "openclaw-harness-core-{test_name}-{}-{nanos}",
            std::process::id()
        ))
    }
}
