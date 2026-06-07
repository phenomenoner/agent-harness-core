use std::collections::BTreeMap;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use serde::Serialize;
use serde_json::Value;

use crate::OpenClawSource;

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AgentRegistry {
    pub source_home: PathBuf,
    pub source_workspace: PathBuf,
    pub config_found: bool,
    pub config_parsed: bool,
    pub config_parse_error: Option<String>,
    pub defaults: AgentDefaults,
    pub agents: Vec<AgentProfile>,
    pub providers: Vec<ProviderProfile>,
    pub plugins: Vec<PluginProfile>,
    pub channels: ChannelRegistry,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AgentDefaults {
    pub workspace: Option<String>,
    pub provider: Option<String>,
    pub model: Option<String>,
    pub timezone: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AgentProfile {
    pub id: String,
    pub enabled: Option<bool>,
    pub workspace: Option<String>,
    pub provider: Option<String>,
    pub model: Option<String>,
    pub source: AgentProfileSource,
    pub directory: PathBuf,
    pub directory_exists: bool,
    pub sessions_index_exists: bool,
    pub local_models_file: bool,
    pub auth_profiles_file: bool,
    pub auth_state_file: bool,
    pub auth_file: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum AgentProfileSource {
    Config,
    Directory,
    ConfigAndDirectory,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ProviderProfile {
    pub id: String,
    pub source: String,
    pub has_base_url: bool,
    pub has_api_key_reference: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PluginProfile {
    pub id: String,
    pub enabled: Option<bool>,
    pub source: String,
    pub memory_related: bool,
    pub channel_related: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ChannelRegistry {
    pub telegram: bool,
    pub discord: bool,
}

pub fn load_agent_registry(source: &OpenClawSource) -> io::Result<AgentRegistry> {
    let config_path = source.home.join("openclaw.json");
    let mut registry = AgentRegistry {
        source_home: source.home.clone(),
        source_workspace: source.workspace.clone(),
        config_found: config_path.is_file(),
        ..AgentRegistry::default()
    };

    let config = if registry.config_found {
        match fs::read_to_string(&config_path) {
            Ok(text) => match serde_json::from_str::<Value>(&text) {
                Ok(value) => {
                    registry.config_parsed = true;
                    Some(value)
                }
                Err(error) => {
                    registry.config_parse_error = Some(error.to_string());
                    None
                }
            },
            Err(error) => return Err(error),
        }
    } else {
        None
    };

    let mut agent_configs = BTreeMap::new();
    if let Some(config) = &config {
        registry.defaults = collect_agent_defaults(config);
        collect_agent_configs(config, &mut agent_configs);
        registry.providers = collect_providers(config);
        registry.plugins = collect_plugins(config);
        registry.channels = collect_channels(config, &registry.plugins);
    }

    let agent_dirs = child_directory_names(&source.home.join("agents"))?;
    for id in &agent_dirs {
        agent_configs.entry(id.clone()).or_insert(Value::Null);
    }

    registry.agents = agent_configs
        .iter()
        .map(|(id, value)| build_agent_profile(source, id, value, &registry.defaults, &agent_dirs))
        .collect();

    if registry.config_parsed && registry.agents.is_empty() {
        registry
            .warnings
            .push("openclaw.json parsed but no agents were found".to_string());
    }

    Ok(registry)
}

fn collect_agent_defaults(config: &Value) -> AgentDefaults {
    let defaults = config
        .pointer("/agents/defaults")
        .or_else(|| config.pointer("/agent/defaults"))
        .or_else(|| config.get("defaults"));

    let Some(defaults) = defaults else {
        return AgentDefaults::default();
    };

    AgentDefaults {
        workspace: string_field(defaults, &["workspace", "workspacePath", "workspace_path"])
            .map(ToString::to_string),
        provider: string_field(defaults, &["provider", "providerId", "provider_id"])
            .map(ToString::to_string),
        model: model_id(defaults).map(ToString::to_string),
        timezone: string_field(defaults, &["timezone", "timeZone", "tz"]).map(ToString::to_string),
    }
}

fn collect_agent_configs(config: &Value, agents: &mut BTreeMap<String, Value>) {
    for path in ["/agents/list", "/agents/items", "/agent/list"] {
        if let Some(value) = config.pointer(path) {
            collect_agent_collection(value, agents);
        }
    }

    if let Some(value) = config.get("agents")
        && value.get("list").is_none()
        && value.get("items").is_none()
    {
        collect_agent_keyed_object(value, agents);
    }
}

fn collect_agent_collection(value: &Value, agents: &mut BTreeMap<String, Value>) {
    if let Some(array) = value.as_array() {
        for agent in array {
            if let Some(id) = agent_id(agent) {
                agents.insert(id.to_string(), agent.clone());
            }
        }
    } else if value.is_object() {
        collect_agent_keyed_object(value, agents);
    }
}

fn collect_agent_keyed_object(value: &Value, agents: &mut BTreeMap<String, Value>) {
    let Some(object) = value.as_object() else {
        return;
    };

    for (key, value) in object {
        if matches!(key.as_str(), "defaults" | "list" | "items") {
            continue;
        }
        let id = agent_id(value).unwrap_or(key);
        agents.insert(id.to_string(), value.clone());
    }
}

fn build_agent_profile(
    source: &OpenClawSource,
    id: &str,
    value: &Value,
    defaults: &AgentDefaults,
    agent_dirs: &[String],
) -> AgentProfile {
    let directory = source.home.join("agents").join(id);
    let directory_exists = agent_dirs.iter().any(|value| value == id);
    let configured = !value.is_null();
    let source = match (configured, directory_exists) {
        (true, true) => AgentProfileSource::ConfigAndDirectory,
        (true, false) => AgentProfileSource::Config,
        (false, true) => AgentProfileSource::Directory,
        (false, false) => AgentProfileSource::Config,
    };
    let agent_home = directory.join("agent");

    AgentProfile {
        id: id.to_string(),
        enabled: value.get("enabled").and_then(Value::as_bool),
        workspace: string_field(value, &["workspace", "workspacePath", "workspace_path"])
            .map(ToString::to_string)
            .or_else(|| defaults.workspace.clone()),
        provider: string_field(value, &["provider", "providerId", "provider_id"])
            .map(ToString::to_string)
            .or_else(|| defaults.provider.clone()),
        model: model_id(value)
            .map(ToString::to_string)
            .or_else(|| defaults.model.clone()),
        source,
        directory,
        directory_exists,
        sessions_index_exists: agent_home
            .parent()
            .is_some_and(|agent_dir| agent_dir.join("sessions").join("sessions.json").is_file()),
        local_models_file: agent_home.join("models.json").is_file(),
        auth_profiles_file: agent_home.join("auth-profiles.json").is_file(),
        auth_state_file: agent_home.join("auth-state.json").is_file(),
        auth_file: agent_home.join("auth.json").is_file(),
    }
}

fn collect_providers(config: &Value) -> Vec<ProviderProfile> {
    let mut providers = BTreeMap::new();
    for (path, source) in [
        ("/models/providers", "models.providers"),
        ("/models/customProviders", "models.customProviders"),
        ("/models/custom_providers", "models.custom_providers"),
        ("/providers", "providers"),
        ("/modelProviders", "modelProviders"),
    ] {
        let Some(value) = config.pointer(path) else {
            continue;
        };
        collect_provider_collection(value, source, &mut providers);
    }
    providers.into_values().collect()
}

fn collect_provider_collection(
    value: &Value,
    source: &str,
    providers: &mut BTreeMap<String, ProviderProfile>,
) {
    if let Some(object) = value.as_object() {
        for (key, value) in object {
            let id = string_field(value, &["id", "name"]).unwrap_or(key);
            providers.insert(id.to_string(), provider_profile(id, source, value));
        }
    } else if let Some(array) = value.as_array() {
        for value in array {
            if let Some(id) = string_field(value, &["id", "name"]) {
                providers.insert(id.to_string(), provider_profile(id, source, value));
            }
        }
    }
}

fn provider_profile(id: &str, source: &str, value: &Value) -> ProviderProfile {
    ProviderProfile {
        id: id.to_string(),
        source: source.to_string(),
        has_base_url: any_key_present(value, &["baseURL", "baseUrl", "base_url", "apiBase"]),
        has_api_key_reference: any_key_present(
            value,
            &["apiKey", "api_key", "key", "env", "envVar", "env_var"],
        ),
    }
}

fn collect_plugins(config: &Value) -> Vec<PluginProfile> {
    let mut plugins = BTreeMap::new();
    for (path, source) in [
        ("/plugins", "plugins"),
        ("/extensions", "extensions"),
        ("/pluginSlots", "pluginSlots"),
        ("/plugin_slots", "plugin_slots"),
    ] {
        let Some(value) = config.pointer(path) else {
            continue;
        };
        collect_plugin_collection(value, source, &mut plugins);
    }
    plugins.into_values().collect()
}

fn collect_plugin_collection(
    value: &Value,
    source: &str,
    plugins: &mut BTreeMap<String, PluginProfile>,
) {
    if let Some(object) = value.as_object() {
        for (key, value) in object {
            let id = string_field(value, &["id", "name", "package", "plugin"]).unwrap_or(key);
            plugins.insert(id.to_string(), plugin_profile(id, source, value));
        }
    } else if let Some(array) = value.as_array() {
        for value in array {
            if let Some(id) = string_field(value, &["id", "name", "package", "plugin"]) {
                plugins.insert(id.to_string(), plugin_profile(id, source, value));
            }
        }
    }
}

fn plugin_profile(id: &str, source: &str, value: &Value) -> PluginProfile {
    let lower = id.to_ascii_lowercase();
    PluginProfile {
        id: id.to_string(),
        enabled: value.get("enabled").and_then(Value::as_bool),
        source: source.to_string(),
        memory_related: lower.contains("openclaw-mem") || lower.contains("mem-engine"),
        channel_related: lower.contains("telegram") || lower.contains("discord"),
    }
}

fn collect_channels(config: &Value, plugins: &[PluginProfile]) -> ChannelRegistry {
    ChannelRegistry {
        telegram: contains_key_recursive(config, "telegram")
            || plugins
                .iter()
                .any(|plugin| plugin.id.eq_ignore_ascii_case("telegram")),
        discord: contains_key_recursive(config, "discord")
            || plugins
                .iter()
                .any(|plugin| plugin.id.eq_ignore_ascii_case("discord")),
    }
}

fn agent_id(value: &Value) -> Option<&str> {
    value
        .as_str()
        .or_else(|| string_field(value, &["id", "agentId", "agent_id", "name"]))
}

fn model_id(value: &Value) -> Option<&str> {
    string_field(value, &["model", "modelId", "model_id"]).or_else(|| {
        value
            .get("model")
            .and_then(|model| string_field(model, &["id", "name"]))
    })
}

fn string_field<'a>(value: &'a Value, keys: &[&str]) -> Option<&'a str> {
    for key in keys {
        if let Some(text) = value.get(*key).and_then(Value::as_str) {
            return Some(text);
        }
    }
    None
}

fn any_key_present(value: &Value, keys: &[&str]) -> bool {
    keys.iter().any(|key| value.get(*key).is_some())
}

fn contains_key_recursive(value: &Value, needle: &str) -> bool {
    match value {
        Value::Object(object) => object.iter().any(|(key, value)| {
            key.eq_ignore_ascii_case(needle) || contains_key_recursive(value, needle)
        }),
        Value::Array(array) => array
            .iter()
            .any(|value| contains_key_recursive(value, needle)),
        _ => false,
    }
}

fn child_directory_names(root: &Path) -> io::Result<Vec<String>> {
    if !root.exists() {
        return Ok(Vec::new());
    }

    let mut names = Vec::new();
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        if entry.file_type()?.is_dir()
            && let Some(name) = entry.file_name().to_str()
        {
            names.push(name.to_string());
        }
    }
    names.sort();
    Ok(names)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn registry_merges_config_agents_with_agent_directories() {
        let root = temp_root("registry_merges_config_agents_with_agent_directories");
        let home = root.join(".openclaw");
        let workspace = home.join("workspace");
        let main_agent = home.join("agents").join("main");
        let cron_agent = home.join("agents").join("cron-lite");
        let orphan_agent = home.join("agents").join("orphan");
        fs::create_dir_all(&workspace).unwrap();
        fs::create_dir_all(main_agent.join("agent")).unwrap();
        fs::create_dir_all(main_agent.join("sessions")).unwrap();
        fs::create_dir_all(cron_agent.join("agent")).unwrap();
        fs::create_dir_all(orphan_agent.join("sessions")).unwrap();

        fs::write(
            home.join("openclaw.json"),
            r#"{
              "agents": {
                "defaults": {
                  "workspace": "/workspace",
                  "provider": "openai",
                  "model": "codex"
                },
                "list": [
                  { "id": "main", "model": "gpt-5", "enabled": true },
                  { "id": "cron-lite", "provider": "openrouter", "model": { "id": "claude-sonnet-4" } }
                ]
              },
              "models": {
                "providers": {
                  "openai": { "apiKey": "${OPENAI_API_KEY}" },
                  "openrouter": { "baseURL": "https://openrouter.ai/api/v1", "apiKey": "${OPENROUTER_API_KEY}" }
                }
              },
              "plugins": [
                { "id": "telegram", "enabled": true },
                { "id": "discord" },
                { "id": "openclaw-mem-engine" }
              ]
            }"#,
        )
        .unwrap();
        fs::write(main_agent.join("agent").join("models.json"), "{}").unwrap();
        fs::write(main_agent.join("agent").join("auth-profiles.json"), "{}").unwrap();
        fs::write(main_agent.join("agent").join("auth-state.json"), "{}").unwrap();
        fs::write(main_agent.join("sessions").join("sessions.json"), "{}").unwrap();
        fs::write(cron_agent.join("agent").join("auth.json"), "{}").unwrap();
        fs::write(orphan_agent.join("sessions").join("sessions.json"), "{}").unwrap();

        let registry = load_agent_registry(&OpenClawSource::new(&home)).unwrap();

        assert!(registry.config_found);
        assert!(registry.config_parsed);
        assert_eq!(registry.defaults.provider.as_deref(), Some("openai"));
        assert_eq!(registry.agents.len(), 3);
        assert!(registry.channels.telegram);
        assert!(registry.channels.discord);
        assert_eq!(registry.providers.len(), 2);
        assert_eq!(registry.plugins.len(), 3);

        let main = agent(&registry, "main");
        assert_eq!(main.source, AgentProfileSource::ConfigAndDirectory);
        assert_eq!(main.provider.as_deref(), Some("openai"));
        assert_eq!(main.model.as_deref(), Some("gpt-5"));
        assert!(main.sessions_index_exists);
        assert!(main.local_models_file);
        assert!(main.auth_profiles_file);
        assert!(main.auth_state_file);

        let cron = agent(&registry, "cron-lite");
        assert_eq!(cron.provider.as_deref(), Some("openrouter"));
        assert_eq!(cron.model.as_deref(), Some("claude-sonnet-4"));
        assert!(cron.auth_file);

        let orphan = agent(&registry, "orphan");
        assert_eq!(orphan.source, AgentProfileSource::Directory);
        assert!(orphan.sessions_index_exists);
        assert_eq!(orphan.provider.as_deref(), Some("openai"));

        let openrouter = registry
            .providers
            .iter()
            .find(|provider| provider.id == "openrouter")
            .unwrap();
        assert!(openrouter.has_base_url);
        assert!(openrouter.has_api_key_reference);

        let memory_plugin = registry
            .plugins
            .iter()
            .find(|plugin| plugin.id == "openclaw-mem-engine")
            .unwrap();
        assert!(memory_plugin.memory_related);

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn registry_surfaces_config_parse_errors_without_losing_directories() {
        let root = temp_root("registry_surfaces_config_parse_errors_without_losing_directories");
        let home = root.join(".openclaw");
        fs::create_dir_all(home.join("agents").join("main")).unwrap();
        fs::write(home.join("openclaw.json"), "{bad json").unwrap();

        let registry = load_agent_registry(&OpenClawSource::new(&home)).unwrap();

        assert!(registry.config_found);
        assert!(!registry.config_parsed);
        assert!(registry.config_parse_error.is_some());
        assert_eq!(registry.agents.len(), 1);
        assert_eq!(registry.agents[0].id, "main");

        let _ = fs::remove_dir_all(root);
    }

    fn agent<'a>(registry: &'a AgentRegistry, id: &str) -> &'a AgentProfile {
        registry
            .agents
            .iter()
            .find(|agent| agent.id == id)
            .unwrap_or_else(|| panic!("missing agent {id}"))
    }

    fn temp_root(test_name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "openclaw-harness-registry-{test_name}-{}-{nanos}",
            std::process::id()
        ))
    }
}
