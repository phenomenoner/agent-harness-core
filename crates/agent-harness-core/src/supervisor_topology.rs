use std::collections::{BTreeMap, btree_map::Entry};
use std::io;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::supervisor_inventory::SupervisorInventoryServiceConfig;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum SupervisorTopologySource {
    ExplicitService,
    BuiltInLoop,
    ConfiguredTelegramLoop,
    OverrideTelegramAccount,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SupervisorTopologyEntry {
    pub service_id: String,
    pub service_kind: String,
    pub source: SupervisorTopologySource,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub telegram_account: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_id: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SupervisorTopologyOverrides {
    pub manage_all: Option<bool>,
    pub include_runtime: Option<bool>,
    pub include_worker: Option<bool>,
    pub include_cron_scheduler: Option<bool>,
    pub include_progress: Option<bool>,
    pub include_ledger_maintenance: Option<bool>,
    pub include_telegram: Option<bool>,
    pub include_discord: Option<bool>,
    pub telegram_accounts: Vec<String>,
}

pub fn derive_supervisor_topology(
    config: &Value,
    overrides: &SupervisorTopologyOverrides,
) -> io::Result<BTreeMap<String, SupervisorTopologyEntry>> {
    let supervisor = config.get("supervisor").unwrap_or(&Value::Null);
    let manage_all = overrides.manage_all.unwrap_or_else(|| {
        supervisor
            .get("manageAllLoops")
            .and_then(Value::as_bool)
            .unwrap_or(false)
    });
    let mut services = BTreeMap::new();

    if let Some(raw_services) = supervisor.get("services") {
        let configured =
            serde_json::from_value::<Vec<SupervisorInventoryServiceConfig>>(raw_services.clone())
                .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
        for service in configured.into_iter().filter(|service| service.enabled) {
            validate_supervisor_service_id(&service.service_id)?;
            let service_id = service.service_id.clone();
            match services.entry(service_id.clone()) {
                Entry::Vacant(entry) => {
                    entry.insert(SupervisorTopologyEntry {
                        service_id: service.service_id,
                        service_kind: service.service_kind,
                        source: SupervisorTopologySource::ExplicitService,
                        telegram_account: None,
                        agent_id: None,
                    });
                }
                Entry::Occupied(_) => {
                    return Err(duplicate_service_id(&service_id, "explicit services"));
                }
            }
        }
    }

    if configured_loop_enabled(
        overrides.include_runtime,
        supervisor,
        "runtimeLoop",
        manage_all,
    ) {
        insert_builtin(&mut services, "runtime-loop")?;
    }
    if configured_loop_enabled(
        overrides.include_worker,
        supervisor,
        "workerLoop",
        manage_all,
    ) {
        insert_builtin(&mut services, "worker-loop")?;
    }
    let cron_enabled = overrides.include_cron_scheduler.unwrap_or_else(|| {
        supervisor
            .get("cronSchedulerLoop")
            .and_then(|value| value.get("enabled"))
            .and_then(Value::as_bool)
            .or_else(|| {
                config
                    .get("cronScheduler")
                    .and_then(|value| value.get("enabled"))
                    .and_then(Value::as_bool)
            })
            .unwrap_or(manage_all)
    });
    if cron_enabled {
        insert_builtin(&mut services, "cron-scheduler-loop")?;
    }
    if configured_loop_enabled(
        overrides.include_progress,
        supervisor,
        "progressDeliveryLoop",
        manage_all,
    ) {
        insert_builtin(&mut services, "progress-delivery-loop")?;
    }
    if configured_loop_enabled(
        overrides.include_ledger_maintenance,
        supervisor,
        "ledgerMaintenanceLoop",
        manage_all,
    ) {
        insert_builtin(&mut services, "ledger-maintenance-loop")?;
    }

    let telegram_enabled = configured_loop_enabled(
        overrides.include_telegram,
        supervisor,
        "telegramLoop",
        manage_all,
    );
    if telegram_enabled {
        if overrides.telegram_accounts.is_empty() {
            insert_builtin(&mut services, "telegram-loop")?;
        } else {
            for account in &overrides.telegram_accounts {
                let service_id = telegram_service_id(account);
                validate_supervisor_service_id(&service_id)?;
                insert_telegram(
                    &mut services,
                    SupervisorTopologyEntry {
                        service_id,
                        service_kind: "telegram-ingress".to_string(),
                        source: SupervisorTopologySource::OverrideTelegramAccount,
                        telegram_account: Some(account.clone()),
                        agent_id: None,
                    },
                )?;
            }
        }
    }

    if let Some(telegram_loops) = supervisor.get("telegramLoops").and_then(Value::as_array) {
        for entry in telegram_loops {
            if !entry
                .get("enabled")
                .and_then(Value::as_bool)
                .unwrap_or(true)
            {
                continue;
            }
            let account = entry
                .get("account")
                .or_else(|| entry.get("telegramAccount"))
                .and_then(Value::as_str)
                .map(ToString::to_string);
            let service_id = entry
                .get("serviceId")
                .and_then(Value::as_str)
                .map(ToString::to_string)
                .or_else(|| account.as_deref().map(telegram_service_id))
                .unwrap_or_else(|| "telegram-loop".to_string());
            validate_supervisor_service_id(&service_id)?;
            insert_telegram(
                &mut services,
                SupervisorTopologyEntry {
                    service_id,
                    service_kind: "telegram-ingress".to_string(),
                    source: SupervisorTopologySource::ConfiguredTelegramLoop,
                    telegram_account: account,
                    agent_id: entry
                        .get("agent")
                        .or_else(|| entry.get("agentId"))
                        .and_then(Value::as_str)
                        .map(ToString::to_string),
                },
            )?;
        }
    }

    for (key, service_id) in [
        ("discordOutboxLoop", "discord-outbox-loop"),
        ("discordGatewayLoop", "discord-gateway-loop"),
    ] {
        if configured_loop_enabled(overrides.include_discord, supervisor, key, manage_all) {
            insert_builtin(&mut services, service_id)?;
        }
    }

    Ok(services)
}

pub fn supervisor_loop_service_kind(service_id: &str) -> &'static str {
    match service_id {
        "runtime-loop" => "runtime",
        "worker-loop" => "worker",
        "cron-scheduler-loop" => "cron",
        "progress-delivery-loop" => "progress-delivery",
        "ledger-maintenance-loop" => "ledger-maintenance",
        "telegram-loop" => "telegram-ingress",
        "discord-outbox-loop" => "final-outbox",
        "discord-gateway-loop" => "discord-gateway",
        service_id if service_id.starts_with("telegram-loop-") => "telegram-ingress",
        _ => "loop",
    }
}

pub fn supervisor_service_id_suffix(value: &str) -> String {
    let mut suffix = value
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || matches!(character, '-' | '_') {
                character.to_ascii_lowercase()
            } else {
                '-'
            }
        })
        .collect::<String>();
    while suffix.contains("--") {
        suffix = suffix.replace("--", "-");
    }
    suffix.trim_matches('-').to_string()
}

pub fn validate_supervisor_service_id(service_id: &str) -> io::Result<()> {
    let safe = !service_id.is_empty()
        && service_id.chars().all(|character| {
            character.is_ascii_alphanumeric() || matches!(character, '-' | '_' | '.')
        });
    if safe {
        Ok(())
    } else {
        Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unsafe reconcile-managed supervisor serviceId `{service_id}`"),
        ))
    }
}

fn configured_loop_enabled(
    override_value: Option<bool>,
    supervisor: &Value,
    key: &str,
    manage_all: bool,
) -> bool {
    override_value.unwrap_or_else(|| {
        supervisor
            .get(key)
            .and_then(|value| value.get("enabled"))
            .and_then(Value::as_bool)
            .unwrap_or(manage_all)
    })
}

fn insert_builtin(
    services: &mut BTreeMap<String, SupervisorTopologyEntry>,
    service_id: &str,
) -> io::Result<()> {
    validate_supervisor_service_id(service_id)?;
    services
        .entry(service_id.to_string())
        .or_insert_with(|| SupervisorTopologyEntry {
            service_id: service_id.to_string(),
            service_kind: supervisor_loop_service_kind(service_id).to_string(),
            source: SupervisorTopologySource::BuiltInLoop,
            telegram_account: None,
            agent_id: None,
        });
    Ok(())
}

fn insert_telegram(
    services: &mut BTreeMap<String, SupervisorTopologyEntry>,
    candidate: SupervisorTopologyEntry,
) -> io::Result<()> {
    let service_id = candidate.service_id.clone();
    match services.entry(service_id.clone()) {
        Entry::Vacant(entry) => {
            entry.insert(candidate);
        }
        Entry::Occupied(mut entry) => match entry.get().source {
            SupervisorTopologySource::ExplicitService => {}
            SupervisorTopologySource::BuiltInLoop => {
                entry.insert(candidate);
            }
            SupervisorTopologySource::ConfiguredTelegramLoop
            | SupervisorTopologySource::OverrideTelegramAccount => {
                if entry.get() != &candidate {
                    return Err(duplicate_service_id(
                        &service_id,
                        "Telegram loop derivation",
                    ));
                }
            }
        },
    }
    Ok(())
}

fn duplicate_service_id(service_id: &str, source: &str) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!("duplicate reconcile-managed supervisor serviceId `{service_id}` in {source}"),
    )
}

fn telegram_service_id(account: &str) -> String {
    let suffix = supervisor_service_id_suffix(account);
    if suffix.is_empty() {
        "telegram-loop".to_string()
    } else {
        format!("telegram-loop-{suffix}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn manage_all_derives_every_builtin_loop_from_one_topology() {
        let topology = derive_supervisor_topology(
            &serde_json::json!({"supervisor": {"manageAllLoops": true}}),
            &SupervisorTopologyOverrides::default(),
        )
        .unwrap();

        assert_eq!(
            topology.keys().map(String::as_str).collect::<Vec<_>>(),
            vec![
                "cron-scheduler-loop",
                "discord-gateway-loop",
                "discord-outbox-loop",
                "ledger-maintenance-loop",
                "progress-delivery-loop",
                "runtime-loop",
                "telegram-loop",
                "worker-loop",
            ]
        );
    }

    #[test]
    fn config_precedence_and_custom_telegram_identity_are_canonical() {
        let config = serde_json::json!({
            "cronScheduler": {"enabled": false},
            "supervisor": {
                "manageAllLoops": true,
                "telegramLoop": {"enabled": false},
                "discordGatewayLoop": {"enabled": false},
                "services": [
                    {
                        "enabled": true,
                        "serviceId": "runtime-loop",
                        "serviceKind": "runtime",
                        "args": [],
                        "priority": "critical",
                        "restartDelayMs": 1000,
                        "heartbeatTimeoutMs": 120000
                    },
                    {
                        "enabled": false,
                        "serviceId": "ledger-maintenance-loop",
                        "serviceKind": "ledger-maintenance",
                        "args": [],
                        "priority": "low",
                        "restartDelayMs": 1000,
                        "heartbeatTimeoutMs": 120000
                    }
                ],
                "ledgerMaintenanceLoop": {"enabled": false},
                "telegramLoops": [
                    {"enabled": true, "account": "North Desk", "agentId": "main"},
                    {"enabled": true, "serviceId": "telegram-loop-blue"},
                    {"enabled": false, "serviceId": "telegram-loop-disabled"}
                ]
            }
        });
        let topology = derive_supervisor_topology(&config, &Default::default()).unwrap();

        assert_eq!(
            topology.keys().map(String::as_str).collect::<Vec<_>>(),
            vec![
                "discord-outbox-loop",
                "progress-delivery-loop",
                "runtime-loop",
                "telegram-loop-blue",
                "telegram-loop-north-desk",
                "worker-loop",
            ]
        );
        assert_eq!(
            topology["runtime-loop"].source,
            SupervisorTopologySource::ExplicitService
        );
        assert_eq!(
            topology["telegram-loop-north-desk"]
                .telegram_account
                .as_deref(),
            Some("North Desk")
        );
    }

    #[test]
    fn cli_overrides_use_the_same_topology_contract() {
        let config = serde_json::json!({"supervisor": {"manageAllLoops": false}});
        let topology = derive_supervisor_topology(
            &config,
            &SupervisorTopologyOverrides {
                manage_all: Some(true),
                include_runtime: Some(false),
                include_discord: Some(false),
                telegram_accounts: vec!["Desk A".to_string(), "Desk B".to_string()],
                ..SupervisorTopologyOverrides::default()
            },
        )
        .unwrap();

        assert!(!topology.contains_key("runtime-loop"));
        assert!(!topology.contains_key("discord-gateway-loop"));
        assert!(topology.contains_key("telegram-loop-desk-a"));
        assert!(topology.contains_key("telegram-loop-desk-b"));
    }

    #[test]
    fn unsafe_service_identity_fails_closed() {
        let error = derive_supervisor_topology(
            &serde_json::json!({
                "supervisor": {
                    "services": [{
                        "enabled": true,
                        "serviceId": "../outside",
                        "serviceKind": "loop",
                        "args": [],
                        "priority": "low",
                        "restartDelayMs": 1000,
                        "heartbeatTimeoutMs": 120000
                    }]
                }
            }),
            &Default::default(),
        )
        .unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(error.to_string().contains("unsafe"));
    }

    #[test]
    fn explicit_service_wins_over_builtin_derivation() {
        let topology = derive_supervisor_topology(
            &serde_json::json!({
                "supervisor": {
                    "manageAllLoops": true,
                    "services": [{
                        "enabled": true,
                        "serviceId": "runtime-loop",
                        "serviceKind": "custom-runtime",
                        "args": ["runtime-loop", "--idle-ms", "17"],
                        "priority": "critical",
                        "restartDelayMs": 777,
                        "heartbeatTimeoutMs": 999
                    }]
                }
            }),
            &Default::default(),
        )
        .unwrap();

        assert_eq!(
            topology["runtime-loop"].source,
            SupervisorTopologySource::ExplicitService
        );
        assert_eq!(topology["runtime-loop"].service_kind, "custom-runtime");
    }

    #[test]
    fn normalized_telegram_account_collision_fails_closed() {
        let error = derive_supervisor_topology(
            &serde_json::json!({"supervisor": {"manageAllLoops": false}}),
            &SupervisorTopologyOverrides {
                include_telegram: Some(true),
                telegram_accounts: vec!["Desk A".to_string(), "Desk-A".to_string()],
                ..Default::default()
            },
        )
        .unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(error.to_string().contains("duplicate"));
    }

    #[test]
    fn configured_telegram_service_id_collision_fails_closed() {
        let error = derive_supervisor_topology(
            &serde_json::json!({
                "supervisor": {
                    "telegramLoops": [
                        {"serviceId": "telegram-loop-desk", "account": "desk-a"},
                        {"serviceId": "telegram-loop-desk", "account": "desk-b"}
                    ]
                }
            }),
            &Default::default(),
        )
        .unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(error.to_string().contains("duplicate"));
    }

    #[test]
    fn duplicate_explicit_service_id_fails_closed() {
        let service = serde_json::json!({
            "enabled": true,
            "serviceId": "runtime-loop",
            "serviceKind": "runtime",
            "args": [],
            "priority": "critical",
            "restartDelayMs": 1000,
            "heartbeatTimeoutMs": 120000
        });
        let error = derive_supervisor_topology(
            &serde_json::json!({"supervisor": {"services": [service.clone(), service]}}),
            &Default::default(),
        )
        .unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(error.to_string().contains("duplicate"));
    }
}
