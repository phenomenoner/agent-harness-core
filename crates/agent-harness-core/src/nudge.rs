use std::collections::BTreeMap;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::harness_config_candidates;

const DEFAULT_MEMORY_NUDGE_TURN_INTERVAL: usize = 6;
const DEFAULT_SKILL_NUDGE_TURN_INTERVAL: usize = 8;
const SKILL_SYNTHESIS_SECTION_THRESHOLD: usize = 3;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NudgeConfig {
    pub enabled: bool,
    pub turn_interval: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NudgeDueInfo {
    pub skill: bool,
    pub memory: bool,
    pub skill_turns: usize,
    pub memory_turns: usize,
    pub prompt_sections: usize,
}

impl NudgeConfig {
    fn is_active(&self) -> bool {
        self.enabled && self.turn_interval > 0
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct LearningNudgeState {
    #[serde(default)]
    sessions: BTreeMap<String, PerSessionNudgeState>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PerSessionNudgeState {
    #[serde(default)]
    pub skill_turns: usize,
    #[serde(default)]
    pub memory_turns: usize,
    #[serde(default)]
    pub prompt_sections: usize,
}

pub fn load_skill_nudge_config(harness_home: impl AsRef<Path>) -> io::Result<NudgeConfig> {
    load_learning_nudge_config(
        harness_home,
        "skillNudge",
        DEFAULT_SKILL_NUDGE_TURN_INTERVAL,
    )
}

pub fn load_memory_nudge_config(harness_home: impl AsRef<Path>) -> io::Result<NudgeConfig> {
    load_learning_nudge_config(
        harness_home,
        "memoryNudge",
        DEFAULT_MEMORY_NUDGE_TURN_INTERVAL,
    )
}

pub fn advance_learning_nudge_counters(
    harness_home: impl AsRef<Path>,
    session_key: &str,
    prompt_sections: usize,
    now_ms: i64,
) -> io::Result<(PerSessionNudgeState, NudgeDueInfo)> {
    let _ = now_ms;
    let harness_home = harness_home.as_ref();
    let mut state = load_learning_nudge_state(harness_home)?;
    let entry = state
        .sessions
        .entry(session_key.to_string())
        .or_insert_with(PerSessionNudgeState::default);
    entry.skill_turns += 1;
    entry.memory_turns += 1;
    entry.prompt_sections = prompt_sections;

    let skill_config = load_skill_nudge_config(harness_home)?;
    let memory_config = load_memory_nudge_config(harness_home)?;
    let due = NudgeDueInfo {
        skill: should_trigger_nudge(
            entry.skill_turns,
            if skill_config.is_active() {
                skill_config.turn_interval
            } else {
                0
            },
            entry.prompt_sections,
            SKILL_SYNTHESIS_SECTION_THRESHOLD,
        ),
        memory: should_trigger_nudge(
            entry.memory_turns,
            if memory_config.is_active() {
                memory_config.turn_interval
            } else {
                0
            },
            entry.prompt_sections,
            SKILL_SYNTHESIS_SECTION_THRESHOLD,
        ),
        skill_turns: entry.skill_turns,
        memory_turns: entry.memory_turns,
        prompt_sections: entry.prompt_sections,
    };
    let entry_snapshot = entry.clone();
    save_learning_nudge_state(harness_home, &state)?;
    Ok((entry_snapshot, due))
}

pub fn reset_session_nudge_state(
    harness_home: impl AsRef<Path>,
    session_key: &str,
) -> io::Result<bool> {
    let harness_home = harness_home.as_ref();
    let mut state = load_learning_nudge_state(harness_home)?;
    let removed = state.sessions.remove(session_key).is_some();
    if removed {
        save_learning_nudge_state(harness_home, &state)?;
    }
    Ok(removed)
}

fn load_learning_nudge_state(harness_home: &Path) -> io::Result<LearningNudgeState> {
    let state_file = learning_nudge_state_file(harness_home);
    match fs::read_to_string(&state_file) {
        Ok(text) => serde_json::from_str(&text).map_err(io::Error::other),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(LearningNudgeState::default()),
        Err(error) => Err(error),
    }
}

fn save_learning_nudge_state(harness_home: &Path, state: &LearningNudgeState) -> io::Result<()> {
    if let Some(parent) = learning_nudge_state_file(harness_home).parent() {
        fs::create_dir_all(parent)?;
    }
    let text = serde_json::to_string_pretty(state).map_err(io::Error::other)?;
    fs::write(learning_nudge_state_file(harness_home), text)
}

pub fn learning_nudge_state_file(harness_home: impl AsRef<Path>) -> PathBuf {
    let harness_home = harness_home.as_ref();
    harness_home
        .join("state")
        .join("learning")
        .join("nudge-state.json")
}

pub fn should_trigger_nudge(
    turn_count: usize,
    turn_interval: usize,
    prompt_sections: usize,
    prompt_section_threshold: usize,
) -> bool {
    if turn_interval == 0 {
        return false;
    }
    if turn_count > 0 && turn_count.is_multiple_of(turn_interval) {
        return true;
    }
    prompt_section_threshold > 0 && prompt_sections >= prompt_section_threshold
}

fn load_learning_nudge_config(
    harness_home: impl AsRef<Path>,
    key: &str,
    default_turn_interval: usize,
) -> io::Result<NudgeConfig> {
    let mut config = NudgeConfig {
        enabled: true,
        turn_interval: default_turn_interval,
    };

    let harness_home = harness_home.as_ref();
    let Some(config_file) = harness_config_candidates(harness_home)
        .into_iter()
        .find(|path| path.is_file())
    else {
        return Ok(config);
    };
    let text = fs::read_to_string(config_file)?;
    let value: Value = match serde_json::from_str(&text) {
        Ok(value) => value,
        Err(error) => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("invalid learning nudge config JSON: {error}"),
            ));
        }
    };
    let Some(learning) = value.get("learning").and_then(Value::as_object) else {
        return Ok(config);
    };
    let Some(section) = learning.get(key).and_then(Value::as_object) else {
        return Ok(config);
    };
    if let Some(enabled) = section.get("enabled").and_then(Value::as_bool) {
        config.enabled = enabled;
    }
    if let Some(value) = section.get("turnInterval").and_then(Value::as_u64) {
        config.turn_interval = usize::try_from(value).unwrap_or(default_turn_interval);
    }
    Ok(config)
}

#[cfg(test)]
mod tests {
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::*;

    #[test]
    fn nudge_should_trigger_on_turn_boundary_or_prompt_section_threshold() {
        assert!(should_trigger_nudge(8, 4, 0, 3));
        assert!(should_trigger_nudge(3, 4, 3, 3));
        assert!(!should_trigger_nudge(3, 4, 1, 3));
    }

    #[test]
    fn reset_session_nudge_state_removes_session_counters() {
        let root = temp_root("nudge_state_reset_removes_session");
        let home = root.join(".agent-harness");
        let mut state = LearningNudgeState::default();
        state.sessions.insert(
            "session-1".to_string(),
            PerSessionNudgeState {
                skill_turns: 2,
                memory_turns: 2,
                prompt_sections: 4,
            },
        );
        save_learning_nudge_state(&home, &state).unwrap();
        assert!(reset_session_nudge_state(&home, "session-1").unwrap());
        let restored = load_learning_nudge_state(&home).unwrap();
        assert!(restored.sessions.is_empty());

        let _ = fs::remove_dir_all(root);
    }

    fn temp_root(test_name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "agent-harness-nudge-{test_name}-{}-{nanos}",
            std::process::id()
        ))
    }
}
