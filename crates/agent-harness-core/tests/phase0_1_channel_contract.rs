use std::fs;
use std::path::PathBuf;
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use agent_harness_core::latency::{
    LatencyStage, latency_receipts_file, read_latest_queue_receipt, record_latency_stage,
};
use agent_harness_core::{
    AgentProgressContext, AgentProgressDeliveryPlanOptions, AgentProgressKind,
    AgentProgressSessionSupersedeOptions, AgentProgressStatus, ChannelDeliveryRecordOptions,
    ChannelDeliveryStatus, ChannelOutboundMessage, ChannelOutboundMessageKind,
    append_agent_progress_event, append_channel_outbox_message, plan_agent_progress_delivery,
    record_channel_delivery_for_source_queue, supersede_agent_progress_session_surfaces,
};
use rusqlite::Connection;

#[test]
fn t3_session_supersede_and_delivery_latency_converge_under_contention() {
    let root = temp_root("t3_session_supersede_and_delivery_latency_converge_under_contention");
    let harness_home = root.join(".agent-harness");
    let old_discord = AgentProgressContext {
        queue_id: "queue-discord-old".to_string(),
        agent_id: Some("main".to_string()),
        account_id: Some("default".to_string()),
        thread_id: None,
        session_key: "discord:default:channel-old:user-old:main:session-old".to_string(),
        platform: "discord".to_string(),
        channel_id: "channel-old".to_string(),
        user_id: "user-old".to_string(),
    };
    append_agent_progress_event(
        &harness_home,
        &agent_harness_core::AgentProgressEvent::new(
            &old_discord,
            AgentProgressKind::Todo,
            "todo",
            "old session work",
            AgentProgressStatus::Started,
            1_000,
        ),
    )
    .unwrap();
    assert_eq!(
        plan_agent_progress_delivery(AgentProgressDeliveryPlanOptions {
            harness_home: harness_home.clone(),
            platform: Some("discord".to_string()),
            now_ms: 2_000,
            min_update_interval_ms: 0,
            ..AgentProgressDeliveryPlanOptions::default()
        })
        .unwrap()
        .pending
        .len(),
        2
    );

    let telegram_queue = "queue-telegram-final";
    let latency_file = latency_receipts_file(&harness_home);
    record_latency_stage(
        &latency_file,
        telegram_queue,
        "telegram",
        LatencyStage::InboundReceived,
        Some(1_100),
    )
    .unwrap();
    let mut outbound = ChannelOutboundMessage {
        platform: "telegram".to_string(),
        account_id: Some("default".to_string()),
        channel_id: "chat-final".to_string(),
        user_id: "user-final".to_string(),
        session_key: "telegram:default:chat-final:user-final:main:session-final".to_string(),
        delivery_id: None,
        kind: ChannelOutboundMessageKind::AgentReply,
        source_queue_id: Some(telegram_queue.to_string()),
        source_completion_file: None,
        text: "final".to_string(),
        presentation: None,
        delivery_intent: None,
        attachments: Vec::new(),
    };
    let append = append_channel_outbox_message(&harness_home, &mut outbound).unwrap();
    let latency_index = latency_file.with_file_name("latency-receipts-index.sqlite");
    let latency_writer = Connection::open(latency_index).unwrap();
    latency_writer.execute_batch("BEGIN IMMEDIATE").unwrap();

    let barrier = Arc::new(Barrier::new(3));
    let delivery_barrier = Arc::clone(&barrier);
    let delivery_home = harness_home.clone();
    let delivery = std::thread::spawn(move || {
        delivery_barrier.wait();
        record_channel_delivery_for_source_queue(
            ChannelDeliveryRecordOptions {
                harness_home: delivery_home,
                delivery_id: append.delivery_id,
                status: ChannelDeliveryStatus::Delivered,
                platform: outbound.platform,
                account_id: outbound.account_id,
                channel_id: outbound.channel_id,
                user_id: outbound.user_id,
                session_key: outbound.session_key,
                provider_message_id: Some("provider-final".to_string()),
                error: None,
                now_ms: 2_100,
                rendered_units: Vec::new(),
                presentation: None,
            },
            Some(telegram_queue),
        )
    });
    let supersede_barrier = Arc::clone(&barrier);
    let supersede_home = harness_home.clone();
    let supersede_context = old_discord.clone();
    let supersede = std::thread::spawn(move || {
        supersede_barrier.wait();
        supersede_agent_progress_session_surfaces(AgentProgressSessionSupersedeOptions {
            harness_home: supersede_home,
            platform: supersede_context.platform,
            account_id: supersede_context.account_id,
            channel_id: supersede_context.channel_id,
            user_id: supersede_context.user_id,
            agent_id: supersede_context.agent_id,
            session_key: supersede_context.session_key,
            now_ms: 2_050,
        })
    });

    let external_effect_started = Instant::now();
    barrier.wait();
    supersede.join().unwrap().unwrap();
    delivery.join().unwrap().unwrap();
    assert!(
        external_effect_started.elapsed() < Duration::from_secs(2),
        "durable provider receipt must not wait indefinitely on diagnostic SQLite contention"
    );
    latency_writer.execute_batch("COMMIT").unwrap();

    let old_plan = plan_agent_progress_delivery(AgentProgressDeliveryPlanOptions {
        harness_home: harness_home.clone(),
        platform: Some("discord".to_string()),
        now_ms: 3_000,
        min_update_interval_ms: 0,
        ..AgentProgressDeliveryPlanOptions::default()
    })
    .unwrap();
    assert!(old_plan.pending.is_empty());

    let latency = read_latest_queue_receipt(&latency_file, telegram_queue)
        .unwrap()
        .unwrap();
    assert_eq!(latency.stages[&LatencyStage::DeliveryDone], 2_100);
    assert_eq!(
        latency.derived_stage_sources[&LatencyStage::DeliveryDone],
        "channel-delivery-receipt-index"
    );

    let mut new_discord = old_discord;
    new_discord.queue_id = "queue-discord-new".to_string();
    new_discord.session_key = "discord:default:channel-old:user-old:main:session-new".to_string();
    append_agent_progress_event(
        &harness_home,
        &agent_harness_core::AgentProgressEvent::new(
            &new_discord,
            AgentProgressKind::Todo,
            "todo",
            "new session work",
            AgentProgressStatus::Started,
            3_100,
        ),
    )
    .unwrap();
    assert_eq!(
        plan_agent_progress_delivery(AgentProgressDeliveryPlanOptions {
            harness_home: harness_home.clone(),
            platform: Some("discord".to_string()),
            now_ms: 3_200,
            min_update_interval_ms: 0,
            ..AgentProgressDeliveryPlanOptions::default()
        })
        .unwrap()
        .pending
        .len(),
        2
    );

    let _ = fs::remove_dir_all(root);
}

fn temp_root(test_name: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!(
        "agent-harness-phase0-1-{test_name}-{}-{nanos}",
        std::process::id()
    ))
}
