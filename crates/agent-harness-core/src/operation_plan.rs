use std::collections::{HashMap, HashSet};
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

const OPERATION_PLAN_SCHEMA: &str = "agent-harness.operation-plan.v1";
const OPERATION_PLAN_ITEM_SCHEMA: &str = "agent-harness.operation-plan-item.v1";
const OPERATION_PLAN_EVENT_SCHEMA: &str = "agent-harness.operation-plan-event.v1";
const OPERATION_PLAN_COMMENT_SCHEMA: &str = "agent-harness.operation-plan-comment.v1";
const OPERATION_PLAN_RECEIPT_SCHEMA: &str = "agent-harness.operation-plan-receipt.v1";

const PLAN_FILE: &str = "plan.json";
const ITEMS_FILE: &str = "items.jsonl";
const EVENTS_FILE: &str = "events.jsonl";
const COMMENTS_FILE: &str = "comments.jsonl";
const RECEIPTS_FILE: &str = "receipts.jsonl";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateOperationPlanOptions {
    pub harness_home: PathBuf,
    pub plan_id: String,
    pub origin_queue_id: Option<String>,
    pub session_key: String,
    pub agent_id: String,
    pub goal: String,
    pub acceptance_criteria: Option<String>,
    pub constraints: Option<String>,
    pub max_open_items: Option<usize>,
    pub max_fanout: Option<usize>,
    pub now_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperationPlanAddItemOptions {
    pub harness_home: PathBuf,
    pub plan_id: String,
    pub item_id: String,
    pub title: String,
    pub body: String,
    pub depends_on: Vec<String>,
    pub acceptance_criteria: Option<String>,
    pub risk: Option<String>,
    pub now_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperationPlanUpdateItemOptions {
    pub harness_home: PathBuf,
    pub plan_id: String,
    pub item_id: String,
    pub expected_item_version: Option<u64>,
    pub status: Option<OperationPlanItemStatus>,
    pub title: Option<String>,
    pub body: Option<String>,
    pub depends_on: Option<Vec<String>>,
    pub assignee: Option<String>,
    pub worker_job_id: Option<String>,
    pub queue_id: Option<String>,
    pub risk: Option<String>,
    pub evidence: Option<Vec<String>>,
    pub replace_evidence: bool,
    pub add_evidence: Vec<String>,
    pub now_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperationPlanDelegateItemOptions {
    pub harness_home: PathBuf,
    pub plan_id: String,
    pub item_id: String,
    pub expected_item_version: Option<u64>,
    pub idempotency_key: String,
    pub assignee: String,
    pub worker_job_id: Option<String>,
    pub queue_id: Option<String>,
    pub now_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperationPlanCommentOptions {
    pub harness_home: PathBuf,
    pub plan_id: String,
    pub author: Option<String>,
    pub body: String,
    pub now_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperationPlanCompleteOptions {
    pub harness_home: PathBuf,
    pub plan_id: String,
    pub reason: Option<String>,
    pub now_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperationPlanBlockOptions {
    pub harness_home: PathBuf,
    pub plan_id: String,
    pub reason: Option<String>,
    pub now_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperationPlanShowOptions {
    pub harness_home: PathBuf,
    pub plan_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperationPlanPromoteDependenciesOptions {
    pub harness_home: PathBuf,
    pub plan_id: String,
    pub now_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OperationPlanShowReport {
    pub schema: &'static str,
    pub harness_home: PathBuf,
    pub plan: OperationPlan,
    pub items: Vec<OperationPlanItem>,
    pub events_file: PathBuf,
    pub comments_file: PathBuf,
    pub receipts_file: PathBuf,
    pub receipt: OperationPlanReceipt,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OperationPlanCreateReport {
    pub schema: &'static str,
    pub harness_home: PathBuf,
    pub plan_file: PathBuf,
    pub plan: OperationPlan,
    pub created: bool,
    pub reason: String,
    pub receipt: OperationPlanReceipt,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OperationPlanAddItemReport {
    pub schema: &'static str,
    pub harness_home: PathBuf,
    pub plan_file: PathBuf,
    pub item: OperationPlanItem,
    pub receipt: OperationPlanReceipt,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OperationPlanUpdateItemReport {
    pub schema: &'static str,
    pub harness_home: PathBuf,
    pub plan_file: PathBuf,
    pub item: OperationPlanItem,
    pub receipt: OperationPlanReceipt,
    pub duplicated: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OperationPlanCommentReport {
    pub schema: &'static str,
    pub harness_home: PathBuf,
    pub plan_file: PathBuf,
    pub comments_file: PathBuf,
    pub comment: OperationPlanComment,
    pub receipt: OperationPlanReceipt,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OperationPlanCompleteReport {
    pub schema: &'static str,
    pub harness_home: PathBuf,
    pub plan_file: PathBuf,
    pub plan: OperationPlan,
    pub receipt: OperationPlanReceipt,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OperationPlanBlockReport {
    pub schema: &'static str,
    pub harness_home: PathBuf,
    pub plan_file: PathBuf,
    pub plan: OperationPlan,
    pub receipt: OperationPlanReceipt,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OperationPlanPromoteDependenciesReport {
    pub schema: &'static str,
    pub harness_home: PathBuf,
    pub plan_file: PathBuf,
    pub promoted_item_ids: Vec<String>,
    pub receipt: OperationPlanReceipt,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OperationPlanSummary {
    pub plan_id: String,
    pub status: OperationPlanStatus,
    pub goal: String,
    pub updated_at_ms: i64,
    pub version: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OperationPlan {
    pub schema: String,
    pub plan_id: String,
    pub origin_queue_id: Option<String>,
    pub session_key: String,
    pub agent_id: String,
    pub goal: String,
    pub status: OperationPlanStatus,
    pub acceptance_criteria: Option<String>,
    pub constraints: Option<String>,
    pub max_open_items: Option<usize>,
    pub max_fanout: Option<usize>,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
    pub version: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum OperationPlanStatus {
    Open,
    Blocked,
    Completed,
    Canceled,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OperationPlanItem {
    pub schema: String,
    pub plan_id: String,
    pub item_id: String,
    pub title: String,
    pub body: String,
    pub status: OperationPlanItemStatus,
    pub depends_on: Vec<String>,
    pub assignee: Option<String>,
    pub worker_job_id: Option<String>,
    pub queue_id: Option<String>,
    pub acceptance_criteria: Option<String>,
    pub evidence: Vec<String>,
    pub artifacts: Vec<String>,
    pub risk: Option<String>,
    pub delegation_idempotency_key: Option<String>,
    pub version: u64,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum OperationPlanItemStatus {
    Todo,
    Ready,
    Running,
    Review,
    Done,
    Blocked,
    Canceled,
}

impl OperationPlanItemStatus {
    pub fn can_transition_to(self, next: Self) -> bool {
        match (self, next) {
            (_, Self::Canceled) => true,
            (Self::Todo, Self::Ready) => true,
            (Self::Ready, Self::Running) => true,
            (Self::Running, Self::Review) => true,
            (Self::Review, Self::Done) => true,
            (Self::Todo, Self::Blocked)
            | (Self::Ready, Self::Blocked)
            | (Self::Running, Self::Blocked)
            | (Self::Review, Self::Blocked) => true,
            (_, _) => false,
        }
    }

    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Done | Self::Blocked | Self::Canceled)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OperationPlanEvent {
    pub schema: &'static str,
    pub at_ms: i64,
    pub plan_id: String,
    pub item_id: Option<String>,
    pub kind: OperationPlanEventKind,
    pub detail: String,
    pub plan_version: u64,
    pub item_version: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum OperationPlanEventKind {
    Created,
    ItemAdded,
    ItemUpdated,
    ItemDelegated,
    ItemPromotedToReady,
    CommentAdded,
    Blocked,
    Completed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OperationPlanReceipt {
    pub schema: &'static str,
    pub at_ms: i64,
    pub plan_id: String,
    pub action: OperationPlanReceiptAction,
    pub item_id: Option<String>,
    pub plan_version: u64,
    pub item_version: Option<u64>,
    pub success: bool,
    pub reason: String,
    pub duplicated: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum OperationPlanReceiptAction {
    CreatePlan,
    AddItem,
    UpdateItem,
    DelegateItem,
    PromoteDependencies,
    Comment,
    Block,
    Complete,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OperationPlanComment {
    pub schema: &'static str,
    pub at_ms: i64,
    pub plan_id: String,
    pub author: Option<String>,
    pub body: String,
}

pub fn create_operation_plan(
    options: CreateOperationPlanOptions,
) -> io::Result<OperationPlanCreateReport> {
    let plan_dir = operation_plan_dir(&options.harness_home, &options.plan_id);
    let plan_file = plan_dir.join(PLAN_FILE);
    let receipts_file = plan_dir.join(RECEIPTS_FILE);
    fs::create_dir_all(&plan_dir)?;

    if let Ok(plan) = read_json_plan(&plan_file) {
        let receipt = OperationPlanReceipt {
            schema: OPERATION_PLAN_RECEIPT_SCHEMA,
            at_ms: options.now_ms,
            plan_id: options.plan_id,
            action: OperationPlanReceiptAction::CreatePlan,
            item_id: None,
            plan_version: plan.version,
            item_version: None,
            success: true,
            reason: "plan already exists".to_string(),
            duplicated: true,
        };
        return Ok(OperationPlanCreateReport {
            schema: OPERATION_PLAN_SCHEMA,
            harness_home: options.harness_home,
            plan_file,
            plan,
            created: false,
            reason: "plan already existed".to_string(),
            receipt,
        });
    }

    let plan = OperationPlan {
        schema: OPERATION_PLAN_SCHEMA.to_string(),
        plan_id: options.plan_id.clone(),
        origin_queue_id: options.origin_queue_id,
        session_key: options.session_key,
        agent_id: options.agent_id,
        goal: options.goal,
        status: OperationPlanStatus::Open,
        acceptance_criteria: options.acceptance_criteria,
        constraints: options.constraints,
        max_open_items: options.max_open_items,
        max_fanout: options.max_fanout,
        created_at_ms: options.now_ms,
        updated_at_ms: options.now_ms,
        version: 1,
    };

    write_json_plan(&plan_file, &plan)?;
    append_plan_event(
        &plan_dir.join(EVENTS_FILE),
        &OperationPlanEvent {
            schema: OPERATION_PLAN_EVENT_SCHEMA,
            at_ms: options.now_ms,
            plan_id: options.plan_id.clone(),
            item_id: None,
            kind: OperationPlanEventKind::Created,
            detail: "created operation plan".to_string(),
            plan_version: plan.version,
            item_version: None,
        },
    )?;
    let receipt = OperationPlanReceipt {
        schema: OPERATION_PLAN_RECEIPT_SCHEMA,
        at_ms: options.now_ms,
        plan_id: options.plan_id,
        action: OperationPlanReceiptAction::CreatePlan,
        item_id: None,
        plan_version: plan.version,
        item_version: None,
        success: true,
        reason: "created plan".to_string(),
        duplicated: false,
    };
    append_plan_receipt(&receipts_file, &receipt)?;

    Ok(OperationPlanCreateReport {
        schema: OPERATION_PLAN_SCHEMA,
        harness_home: options.harness_home,
        plan_file,
        plan,
        created: true,
        reason: "created new plan".to_string(),
        receipt,
    })
}

pub fn show_operation_plan(
    options: OperationPlanShowOptions,
) -> io::Result<OperationPlanShowReport> {
    let plan_dir = operation_plan_dir(&options.harness_home, &options.plan_id);
    let plan = read_json_plan(&plan_dir.join(PLAN_FILE))?;
    let items = read_items(&plan_dir.join(ITEMS_FILE))?;
    let events_file = plan_dir.join(EVENTS_FILE);
    let comments_file = plan_dir.join(COMMENTS_FILE);
    let receipt_file = plan_dir.join(RECEIPTS_FILE);

    let receipt = OperationPlanReceipt {
        schema: OPERATION_PLAN_RECEIPT_SCHEMA,
        at_ms: 0,
        plan_id: options.plan_id,
        action: OperationPlanReceiptAction::CreatePlan,
        item_id: None,
        plan_version: plan.version,
        item_version: None,
        success: true,
        reason: "read snapshot".to_string(),
        duplicated: false,
    };

    Ok(OperationPlanShowReport {
        schema: OPERATION_PLAN_SCHEMA,
        harness_home: options.harness_home,
        plan,
        items,
        events_file,
        comments_file,
        receipts_file: receipt_file,
        receipt,
    })
}

pub fn list_operation_plans(harness_home: PathBuf) -> io::Result<Vec<OperationPlanSummary>> {
    let base_dir = harness_home.join("state").join("operation-plans");
    if !base_dir.exists() {
        return Ok(Vec::new());
    }

    let mut plans = Vec::new();
    for entry in fs::read_dir(&base_dir)? {
        let entry = entry?;
        if !entry.file_type()?.is_dir() {
            continue;
        }
        let plan_file = entry.path().join(PLAN_FILE);
        if !plan_file.is_file() {
            continue;
        }
        let plan = read_json_plan(&plan_file)?;
        plans.push(OperationPlanSummary {
            plan_id: plan.plan_id,
            status: plan.status,
            goal: plan.goal,
            updated_at_ms: plan.updated_at_ms,
            version: plan.version,
        });
    }
    plans.sort_by(|left, right| {
        right
            .updated_at_ms
            .cmp(&left.updated_at_ms)
            .then_with(|| left.plan_id.cmp(&right.plan_id))
    });
    Ok(plans)
}

pub fn add_operation_plan_item(
    options: OperationPlanAddItemOptions,
) -> io::Result<OperationPlanAddItemReport> {
    let plan_dir = operation_plan_dir(&options.harness_home, &options.plan_id);
    let plan_file = plan_dir.join(PLAN_FILE);
    let items_file = plan_dir.join(ITEMS_FILE);
    let receipts_file = plan_dir.join(RECEIPTS_FILE);

    let mut plan = read_json_plan(&plan_file)?;
    let mut items = read_item_map(&items_file)?;

    if items.contains_key(&options.item_id) {
        return Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            format!("item {} already exists", options.item_id),
        ));
    }
    validate_dependency_ids(&options.depends_on, &items)?;
    let item = OperationPlanItem {
        schema: OPERATION_PLAN_ITEM_SCHEMA.to_string(),
        plan_id: options.plan_id.clone(),
        item_id: options.item_id.clone(),
        title: options.title,
        body: options.body,
        status: OperationPlanItemStatus::Todo,
        depends_on: options.depends_on,
        assignee: None,
        worker_job_id: None,
        queue_id: None,
        acceptance_criteria: options.acceptance_criteria,
        evidence: Vec::new(),
        artifacts: Vec::new(),
        risk: options.risk,
        delegation_idempotency_key: None,
        version: 1,
        created_at_ms: options.now_ms,
        updated_at_ms: options.now_ms,
    };

    append_item_line(&items_file, &item)?;
    items.insert(options.item_id.clone(), item.clone());
    plan.version = plan.version.saturating_add(1);
    plan.updated_at_ms = options.now_ms;
    write_json_plan(&plan_file, &plan)?;

    append_plan_event(
        &plan_dir.join(EVENTS_FILE),
        &OperationPlanEvent {
            schema: OPERATION_PLAN_EVENT_SCHEMA,
            at_ms: options.now_ms,
            plan_id: options.plan_id.clone(),
            item_id: Some(item.item_id.clone()),
            kind: OperationPlanEventKind::ItemAdded,
            detail: format!("added item {}", options.item_id),
            plan_version: plan.version,
            item_version: Some(item.version),
        },
    )?;

    let receipt = OperationPlanReceipt {
        schema: OPERATION_PLAN_RECEIPT_SCHEMA,
        at_ms: options.now_ms,
        plan_id: item.plan_id.clone(),
        action: OperationPlanReceiptAction::AddItem,
        item_id: Some(item.item_id.clone()),
        plan_version: plan.version,
        item_version: Some(item.version),
        success: true,
        reason: "added item".to_string(),
        duplicated: false,
    };
    append_plan_receipt(&receipts_file, &receipt)?;

    Ok(OperationPlanAddItemReport {
        schema: OPERATION_PLAN_SCHEMA,
        harness_home: options.harness_home,
        plan_file,
        item,
        receipt,
    })
}

pub fn update_operation_plan_item(
    options: OperationPlanUpdateItemOptions,
) -> io::Result<OperationPlanUpdateItemReport> {
    let plan_dir = operation_plan_dir(&options.harness_home, &options.plan_id);
    let plan_file = plan_dir.join(PLAN_FILE);
    let items_file = plan_dir.join(ITEMS_FILE);
    let receipts_file = plan_dir.join(RECEIPTS_FILE);
    let mut plan = read_json_plan(&plan_file)?;

    let mut items = read_item_map(&items_file)?;
    let mut current = items
        .remove(&options.item_id)
        .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "item not found"))?;

    let Some(expected) = options.expected_item_version else {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "--expected-version is required when updating an existing operation-plan item",
        ));
    };
    if current.version != expected {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "version mismatch: expected {}, found {}",
                expected, current.version
            ),
        ));
    }

    if let Some(status) = options.status {
        if !current.status.can_transition_to(status) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("invalid transition {:?} -> {:?}", current.status, status),
            ));
        }
        current.status = status;
    }
    if let Some(title) = options.title {
        current.title = title;
    }
    if let Some(body) = options.body {
        current.body = body;
    }
    if let Some(depends_on) = options.depends_on {
        validate_dependency_ids(&depends_on, &items)?;
        current.depends_on = depends_on;
    }
    if let Some(assignee) = options.assignee {
        current.assignee = Some(assignee);
    }
    if let Some(worker_job_id) = options.worker_job_id {
        current.worker_job_id = Some(worker_job_id);
    }
    if let Some(queue_id) = options.queue_id {
        current.queue_id = Some(queue_id);
    }
    if let Some(risk) = options.risk {
        current.risk = Some(risk);
    }
    let mut evidence_to_add = options.add_evidence;
    if let Some(evidence) = options.evidence {
        if options.replace_evidence {
            current.evidence = evidence;
        } else {
            evidence_to_add.extend(evidence);
        }
    }
    if !evidence_to_add.is_empty() {
        current.evidence.extend(evidence_to_add);
        current.evidence.sort();
        current.evidence.dedup();
    }

    current.version = current.version.saturating_add(1);
    current.updated_at_ms = options.now_ms;

    append_item_line(&items_file, &current)?;
    plan.version = plan.version.saturating_add(1);
    plan.updated_at_ms = options.now_ms;
    write_json_plan(&plan_file, &plan)?;

    append_plan_event(
        &plan_dir.join(EVENTS_FILE),
        &OperationPlanEvent {
            schema: OPERATION_PLAN_EVENT_SCHEMA,
            at_ms: options.now_ms,
            plan_id: options.plan_id.clone(),
            item_id: Some(current.item_id.clone()),
            kind: OperationPlanEventKind::ItemUpdated,
            detail: format!("updated item {}", current.item_id),
            plan_version: plan.version,
            item_version: Some(current.version),
        },
    )?;

    let receipt = OperationPlanReceipt {
        schema: OPERATION_PLAN_RECEIPT_SCHEMA,
        at_ms: options.now_ms,
        plan_id: options.plan_id,
        action: OperationPlanReceiptAction::UpdateItem,
        item_id: Some(current.item_id.clone()),
        plan_version: plan.version,
        item_version: Some(current.version),
        success: true,
        reason: "updated item".to_string(),
        duplicated: false,
    };
    append_plan_receipt(&receipts_file, &receipt)?;

    Ok(OperationPlanUpdateItemReport {
        schema: OPERATION_PLAN_SCHEMA,
        harness_home: options.harness_home,
        plan_file,
        item: current,
        receipt,
        duplicated: false,
    })
}

pub fn delegate_operation_plan_item(
    options: OperationPlanDelegateItemOptions,
) -> io::Result<OperationPlanUpdateItemReport> {
    let plan_dir = operation_plan_dir(&options.harness_home, &options.plan_id);
    let plan_file = plan_dir.join(PLAN_FILE);
    let items_file = plan_dir.join(ITEMS_FILE);
    let receipts_file = plan_dir.join(RECEIPTS_FILE);

    let mut plan = read_json_plan(&plan_file)?;
    let mut items = read_item_map(&items_file)?;
    let current = items
        .remove(&options.item_id)
        .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "item not found"))?;

    if current.delegation_idempotency_key.as_deref() == Some(&options.idempotency_key) {
        let reason = format!("idempotency key already used for item {}", current.item_id);
        let receipt = OperationPlanReceipt {
            schema: OPERATION_PLAN_RECEIPT_SCHEMA,
            at_ms: options.now_ms,
            plan_id: options.plan_id,
            action: OperationPlanReceiptAction::DelegateItem,
            item_id: Some(options.item_id),
            plan_version: plan.version,
            item_version: Some(current.version),
            success: false,
            reason: reason.clone(),
            duplicated: true,
        };
        append_plan_receipt(&receipts_file, &receipt)?;
        return Err(io::Error::new(io::ErrorKind::AlreadyExists, reason));
    }

    if let Some(existing_item_id) =
        find_item_id_by_idempotency_key(&options.idempotency_key, &items)
    {
        let reason = format!("idempotency key reused for item {existing_item_id}");
        let receipt = OperationPlanReceipt {
            schema: OPERATION_PLAN_RECEIPT_SCHEMA,
            at_ms: options.now_ms,
            plan_id: options.plan_id,
            action: OperationPlanReceiptAction::DelegateItem,
            item_id: Some(options.item_id),
            plan_version: plan.version,
            item_version: Some(current.version),
            success: false,
            reason: reason.clone(),
            duplicated: true,
        };
        append_plan_receipt(&receipts_file, &receipt)?;
        return Err(io::Error::new(io::ErrorKind::AlreadyExists, reason));
    }

    let Some(expected) = options.expected_item_version else {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "--expected-version is required when delegating an existing operation-plan item",
        ));
    };
    if current.version != expected {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "version mismatch: expected {}, found {}",
                expected, current.version
            ),
        ));
    }

    let mut delegated = current;
    delegated.version = delegated.version.saturating_add(1);
    delegated.updated_at_ms = options.now_ms;
    delegated.assignee = Some(options.assignee);
    delegated.worker_job_id = options.worker_job_id;
    delegated.queue_id = options.queue_id;
    delegated.delegation_idempotency_key = Some(options.idempotency_key.clone());

    append_item_line(&items_file, &delegated)?;
    append_plan_event(
        &plan_dir.join(EVENTS_FILE),
        &OperationPlanEvent {
            schema: OPERATION_PLAN_EVENT_SCHEMA,
            at_ms: options.now_ms,
            plan_id: delegated.plan_id.clone(),
            item_id: Some(delegated.item_id.clone()),
            kind: OperationPlanEventKind::ItemDelegated,
            detail: format!(
                "delegated item {} with key {}",
                delegated.item_id, options.idempotency_key
            ),
            plan_version: plan.version,
            item_version: Some(delegated.version),
        },
    )?;
    plan.version = plan.version.saturating_add(1);
    plan.updated_at_ms = options.now_ms;
    write_json_plan(&plan_file, &plan)?;

    let receipt = OperationPlanReceipt {
        schema: OPERATION_PLAN_RECEIPT_SCHEMA,
        at_ms: options.now_ms,
        plan_id: delegated.plan_id.clone(),
        action: OperationPlanReceiptAction::DelegateItem,
        item_id: Some(delegated.item_id.clone()),
        plan_version: plan.version,
        item_version: Some(delegated.version),
        success: true,
        reason: "delegated item".to_string(),
        duplicated: false,
    };
    append_plan_receipt(&receipts_file, &receipt)?;

    Ok(OperationPlanUpdateItemReport {
        schema: OPERATION_PLAN_SCHEMA,
        harness_home: options.harness_home,
        plan_file,
        item: delegated,
        receipt,
        duplicated: false,
    })
}

pub fn promote_operation_plan_items_from_dependencies(
    options: OperationPlanPromoteDependenciesOptions,
) -> io::Result<OperationPlanPromoteDependenciesReport> {
    let plan_dir = operation_plan_dir(&options.harness_home, &options.plan_id);
    let plan_file = plan_dir.join(PLAN_FILE);
    let items_file = plan_dir.join(ITEMS_FILE);
    let receipts_file = plan_dir.join(RECEIPTS_FILE);

    let mut plan = read_json_plan(&plan_file)?;
    let items = read_items(&items_file)?;
    let mut latest_map = HashMap::new();
    for item in items {
        latest_map.insert(item.item_id.clone(), item);
    }

    let done_items: HashSet<_> = latest_map
        .values()
        .filter(|item| item.status == OperationPlanItemStatus::Done)
        .map(|item| item.item_id.clone())
        .collect();

    let mut promoted = Vec::new();
    for item in latest_map.values() {
        if item.status != OperationPlanItemStatus::Todo {
            continue;
        }
        if item
            .depends_on
            .iter()
            .all(|dep| latest_map.contains_key(dep) && done_items.contains(dep))
        {
            let mut next = item.clone();
            next.status = OperationPlanItemStatus::Ready;
            next.version = next.version.saturating_add(1);
            next.updated_at_ms = options.now_ms;
            append_item_line(&items_file, &next)?;
            promoted.push(next.item_id.clone());
            append_plan_event(
                &plan_dir.join(EVENTS_FILE),
                &OperationPlanEvent {
                    schema: OPERATION_PLAN_EVENT_SCHEMA,
                    at_ms: options.now_ms,
                    plan_id: options.plan_id.clone(),
                    item_id: Some(next.item_id.clone()),
                    kind: OperationPlanEventKind::ItemPromotedToReady,
                    detail: "dependency-ready promotion".to_string(),
                    plan_version: plan.version,
                    item_version: Some(next.version),
                },
            )?;
        }
    }

    if !promoted.is_empty() {
        plan.version = plan.version.saturating_add(1);
        plan.updated_at_ms = options.now_ms;
        write_json_plan(&plan_file, &plan)?;
    }

    let receipt = OperationPlanReceipt {
        schema: OPERATION_PLAN_RECEIPT_SCHEMA,
        at_ms: options.now_ms,
        plan_id: options.plan_id,
        action: OperationPlanReceiptAction::PromoteDependencies,
        item_id: None,
        plan_version: plan.version,
        item_version: None,
        success: true,
        reason: format!("promoted {} items", promoted.len()),
        duplicated: false,
    };
    append_plan_receipt(&receipts_file, &receipt)?;

    Ok(OperationPlanPromoteDependenciesReport {
        schema: OPERATION_PLAN_SCHEMA,
        harness_home: options.harness_home,
        plan_file,
        promoted_item_ids: promoted,
        receipt,
    })
}

pub fn comment_on_operation_plan(
    options: OperationPlanCommentOptions,
) -> io::Result<OperationPlanCommentReport> {
    let plan_dir = operation_plan_dir(&options.harness_home, &options.plan_id);
    let plan_file = plan_dir.join(PLAN_FILE);
    let comments_file = plan_dir.join(COMMENTS_FILE);
    let receipts_file = plan_dir.join(RECEIPTS_FILE);
    let mut plan = read_json_plan(&plan_file)?;
    let comment = OperationPlanComment {
        schema: OPERATION_PLAN_COMMENT_SCHEMA,
        at_ms: options.now_ms,
        plan_id: options.plan_id.clone(),
        author: options.author,
        body: options.body,
    };
    append_jsonl(&comments_file, &comment)?;
    append_plan_event(
        &plan_dir.join(EVENTS_FILE),
        &OperationPlanEvent {
            schema: OPERATION_PLAN_EVENT_SCHEMA,
            at_ms: options.now_ms,
            plan_id: options.plan_id.clone(),
            item_id: None,
            kind: OperationPlanEventKind::CommentAdded,
            detail: format!("comment added to plan {}", comment.plan_id),
            plan_version: plan.version,
            item_version: None,
        },
    )?;

    plan.version = plan.version.saturating_add(1);
    plan.updated_at_ms = options.now_ms;
    write_json_plan(&plan_file, &plan)?;

    let receipt = OperationPlanReceipt {
        schema: OPERATION_PLAN_RECEIPT_SCHEMA,
        at_ms: options.now_ms,
        plan_id: comment.plan_id.clone(),
        action: OperationPlanReceiptAction::Comment,
        item_id: None,
        plan_version: plan.version,
        item_version: None,
        success: true,
        reason: "comment appended".to_string(),
        duplicated: false,
    };
    append_plan_receipt(&receipts_file, &receipt)?;

    Ok(OperationPlanCommentReport {
        schema: OPERATION_PLAN_SCHEMA,
        harness_home: options.harness_home,
        plan_file,
        comments_file,
        comment,
        receipt,
    })
}

pub fn block_operation_plan(
    options: OperationPlanBlockOptions,
) -> io::Result<OperationPlanBlockReport> {
    let plan_dir = operation_plan_dir(&options.harness_home, &options.plan_id);
    let plan_file = plan_dir.join(PLAN_FILE);
    let receipts_file = plan_dir.join(RECEIPTS_FILE);
    let mut plan = read_json_plan(&plan_file)?;

    if plan.status == OperationPlanStatus::Completed {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "cannot block completed plan",
        ));
    }

    plan.status = OperationPlanStatus::Blocked;
    plan.version = plan.version.saturating_add(1);
    plan.updated_at_ms = options.now_ms;
    write_json_plan(&plan_file, &plan)?;

    append_plan_event(
        &plan_dir.join(EVENTS_FILE),
        &OperationPlanEvent {
            schema: OPERATION_PLAN_EVENT_SCHEMA,
            at_ms: options.now_ms,
            plan_id: plan.plan_id.clone(),
            item_id: None,
            kind: OperationPlanEventKind::Blocked,
            detail: options.reason.unwrap_or_else(|| "plan blocked".to_string()),
            plan_version: plan.version,
            item_version: None,
        },
    )?;

    let receipt = OperationPlanReceipt {
        schema: OPERATION_PLAN_RECEIPT_SCHEMA,
        at_ms: options.now_ms,
        plan_id: plan.plan_id.clone(),
        action: OperationPlanReceiptAction::Block,
        item_id: None,
        plan_version: plan.version,
        item_version: None,
        success: true,
        reason: "plan blocked".to_string(),
        duplicated: false,
    };
    append_plan_receipt(&receipts_file, &receipt)?;

    Ok(OperationPlanBlockReport {
        schema: OPERATION_PLAN_SCHEMA,
        harness_home: options.harness_home,
        plan_file,
        plan,
        receipt,
    })
}

pub fn complete_operation_plan(
    options: OperationPlanCompleteOptions,
) -> io::Result<OperationPlanCompleteReport> {
    let plan_dir = operation_plan_dir(&options.harness_home, &options.plan_id);
    let plan_file = plan_dir.join(PLAN_FILE);
    let receipts_file = plan_dir.join(RECEIPTS_FILE);
    let mut plan = read_json_plan(&plan_file)?;
    let items = read_items(&plan_dir.join(ITEMS_FILE))?;

    if plan.status == OperationPlanStatus::Completed {
        let receipt = OperationPlanReceipt {
            schema: OPERATION_PLAN_RECEIPT_SCHEMA,
            at_ms: options.now_ms,
            plan_id: options.plan_id,
            action: OperationPlanReceiptAction::Complete,
            item_id: None,
            plan_version: plan.version,
            item_version: None,
            success: true,
            reason: "already completed".to_string(),
            duplicated: true,
        };
        append_plan_receipt(&receipts_file, &receipt)?;
        return Ok(OperationPlanCompleteReport {
            schema: OPERATION_PLAN_SCHEMA,
            harness_home: options.harness_home,
            plan_file,
            plan,
            receipt,
        });
    }
    if plan.status == OperationPlanStatus::Canceled {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "cannot complete canceled plan",
        ));
    }

    for item in &items {
        if matches!(
            item.status,
            OperationPlanItemStatus::Todo
                | OperationPlanItemStatus::Ready
                | OperationPlanItemStatus::Running
                | OperationPlanItemStatus::Review
        ) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "cannot complete plan because item {} is {}",
                    item.item_id,
                    item.status_as_str()
                ),
            ));
        }
        if item.status == OperationPlanItemStatus::Done && item.evidence.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "cannot complete plan because done item {} has no evidence",
                    item.item_id
                ),
            ));
        }
    }

    plan.status = OperationPlanStatus::Completed;
    plan.version = plan.version.saturating_add(1);
    plan.updated_at_ms = options.now_ms;
    write_json_plan(&plan_file, &plan)?;
    append_plan_event(
        &plan_dir.join(EVENTS_FILE),
        &OperationPlanEvent {
            schema: OPERATION_PLAN_EVENT_SCHEMA,
            at_ms: options.now_ms,
            plan_id: options.plan_id.clone(),
            item_id: None,
            kind: OperationPlanEventKind::Completed,
            detail: options
                .reason
                .unwrap_or_else(|| "plan completed".to_string()),
            plan_version: plan.version,
            item_version: None,
        },
    )?;

    let receipt = OperationPlanReceipt {
        schema: OPERATION_PLAN_RECEIPT_SCHEMA,
        at_ms: options.now_ms,
        plan_id: options.plan_id,
        action: OperationPlanReceiptAction::Complete,
        item_id: None,
        plan_version: plan.version,
        item_version: None,
        success: true,
        reason: "plan completed".to_string(),
        duplicated: false,
    };
    append_plan_receipt(&receipts_file, &receipt)?;

    Ok(OperationPlanCompleteReport {
        schema: OPERATION_PLAN_SCHEMA,
        harness_home: options.harness_home,
        plan_file,
        plan,
        receipt,
    })
}

fn validate_dependency_ids(
    depends_on: &[String],
    existing_items: &HashMap<String, OperationPlanItem>,
) -> io::Result<()> {
    for dep in depends_on {
        if dep.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "dependency id cannot be empty",
            ));
        }
        if !existing_items.contains_key(dep) {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("dependency {dep} does not exist"),
            ));
        }
        if depends_on.iter().filter(|id| *id == dep).count() > 1 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("duplicate dependency {dep}"),
            ));
        }
    }
    Ok(())
}

fn operation_plan_dir(harness_home: &Path, plan_id: &str) -> PathBuf {
    harness_home
        .join("state")
        .join("operation-plans")
        .join(safe_name(plan_id))
}

fn read_json_plan(path: &Path) -> io::Result<OperationPlan> {
    let text = fs::read_to_string(path)?;
    serde_json::from_str(&text).map_err(io::Error::other)
}

fn write_json_plan(path: &Path, plan: &OperationPlan) -> io::Result<()> {
    crate::write_json_atomic(path, plan)
}

fn append_jsonl(path: &Path, value: &impl Serialize) -> io::Result<()> {
    crate::append_jsonl_value(path, value)
}

fn append_item_line(path: &Path, item: &OperationPlanItem) -> io::Result<()> {
    append_jsonl(path, item)
}

fn append_plan_event(path: &Path, event: &OperationPlanEvent) -> io::Result<()> {
    append_jsonl(path, event)
}

fn append_plan_receipt(path: &Path, receipt: &OperationPlanReceipt) -> io::Result<()> {
    append_jsonl(path, receipt)
}

fn read_items(path: &Path) -> io::Result<Vec<OperationPlanItem>> {
    let text = match fs::read_to_string(path) {
        Ok(text) => text,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(error),
    };
    let mut items: HashMap<String, OperationPlanItem> = HashMap::new();
    for line in text.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        match serde_json::from_str::<OperationPlanItem>(trimmed) {
            Ok(item) => {
                items.insert(item.item_id.clone(), item);
            }
            Err(_) => continue,
        }
    }
    let mut items = items.into_values().collect::<Vec<_>>();
    items.sort_by(|left, right| {
        left.created_at_ms
            .cmp(&right.created_at_ms)
            .then_with(|| left.item_id.cmp(&right.item_id))
    });
    Ok(items)
}

fn read_item_map(path: &Path) -> io::Result<HashMap<String, OperationPlanItem>> {
    let items = read_items(path)?;
    let mut map = HashMap::new();
    for item in items {
        map.insert(item.item_id.clone(), item);
    }
    Ok(map)
}

fn find_item_id_by_idempotency_key(
    key: &str,
    items: &HashMap<String, OperationPlanItem>,
) -> Option<String> {
    for (item_id, item) in items {
        if item.delegation_idempotency_key.as_deref() == Some(key) {
            return Some(item_id.clone());
        }
    }
    None
}

impl OperationPlanItem {
    pub fn status_as_str(&self) -> &'static str {
        self.status.as_str()
    }
}

impl OperationPlanItemStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Todo => "todo",
            Self::Ready => "ready",
            Self::Running => "running",
            Self::Review => "review",
            Self::Done => "done",
            Self::Blocked => "blocked",
            Self::Canceled => "canceled",
        }
    }
}

fn safe_name(value: &str) -> String {
    let normalized = value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.') {
                ch
            } else {
                '_'
            }
        })
        .collect::<String>();
    if normalized.is_empty() {
        "plan".to_string()
    } else {
        normalized
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn operation_plan_valid_and_invalid_item_transitions() {
        let root = temp_root("operation_plan_valid_and_invalid_item_transitions");
        let harness_home = root.join(".agent-harness");

        let plan = create_operation_plan(CreateOperationPlanOptions {
            harness_home: harness_home.clone(),
            plan_id: "plan-1".to_string(),
            origin_queue_id: None,
            session_key: "session-a".to_string(),
            agent_id: "agent-main".to_string(),
            goal: "ship core".to_string(),
            acceptance_criteria: None,
            constraints: None,
            max_open_items: None,
            max_fanout: None,
            now_ms: 1000,
        })
        .unwrap();
        assert!(plan.created);

        let added = add_operation_plan_item(OperationPlanAddItemOptions {
            harness_home: harness_home.clone(),
            plan_id: "plan-1".to_string(),
            item_id: "item-1".to_string(),
            title: "start".to_string(),
            body: "first task".to_string(),
            depends_on: Vec::new(),
            acceptance_criteria: None,
            risk: None,
            now_ms: 1001,
        })
        .unwrap();
        assert_eq!(added.item.status, OperationPlanItemStatus::Todo);

        let ready = update_operation_plan_item(OperationPlanUpdateItemOptions {
            harness_home: harness_home.clone(),
            plan_id: "plan-1".to_string(),
            item_id: "item-1".to_string(),
            expected_item_version: Some(added.item.version),
            status: Some(OperationPlanItemStatus::Ready),
            title: None,
            body: None,
            depends_on: None,
            assignee: None,
            worker_job_id: None,
            queue_id: None,
            risk: None,
            evidence: None,
            replace_evidence: false,
            add_evidence: Vec::new(),
            now_ms: 1002,
        })
        .unwrap();
        assert_eq!(ready.item.status, OperationPlanItemStatus::Ready);

        let invalid = update_operation_plan_item(OperationPlanUpdateItemOptions {
            harness_home: harness_home.clone(),
            plan_id: "plan-1".to_string(),
            item_id: "item-1".to_string(),
            expected_item_version: Some(ready.item.version),
            status: Some(OperationPlanItemStatus::Todo),
            title: None,
            body: None,
            depends_on: None,
            assignee: None,
            worker_job_id: None,
            queue_id: None,
            risk: None,
            evidence: None,
            replace_evidence: false,
            add_evidence: Vec::new(),
            now_ms: 1003,
        });
        assert!(invalid.is_err());

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn operation_plan_delegation_is_idempotent_by_idempotency_key() {
        let root = temp_root("operation_plan_delegation_is_idempotent_by_idempotency_key");
        let harness_home = root.join(".agent-harness");

        let _ = create_operation_plan(CreateOperationPlanOptions {
            harness_home: harness_home.clone(),
            plan_id: "plan-2".to_string(),
            origin_queue_id: None,
            session_key: "session-b".to_string(),
            agent_id: "agent-main".to_string(),
            goal: "delegate test".to_string(),
            acceptance_criteria: None,
            constraints: None,
            max_open_items: None,
            max_fanout: None,
            now_ms: 2000,
        })
        .unwrap();
        let item = add_operation_plan_item(OperationPlanAddItemOptions {
            harness_home: harness_home.clone(),
            plan_id: "plan-2".to_string(),
            item_id: "item-1".to_string(),
            title: "delegated".to_string(),
            body: "task".to_string(),
            depends_on: Vec::new(),
            acceptance_criteria: None,
            risk: None,
            now_ms: 2001,
        })
        .unwrap();

        let first = delegate_operation_plan_item(OperationPlanDelegateItemOptions {
            harness_home: harness_home.clone(),
            plan_id: "plan-2".to_string(),
            item_id: "item-1".to_string(),
            expected_item_version: Some(item.item.version),
            idempotency_key: "dup-key-1".to_string(),
            assignee: "worker-1".to_string(),
            worker_job_id: Some("job-1".to_string()),
            queue_id: Some("queue-1".to_string()),
            now_ms: 2002,
        })
        .unwrap();
        assert_eq!(
            first.item.delegation_idempotency_key.as_deref(),
            Some("dup-key-1")
        );

        let duplicate = delegate_operation_plan_item(OperationPlanDelegateItemOptions {
            harness_home: harness_home.clone(),
            plan_id: "plan-2".to_string(),
            item_id: "item-1".to_string(),
            expected_item_version: Some(first.item.version),
            idempotency_key: "dup-key-1".to_string(),
            assignee: "worker-2".to_string(),
            worker_job_id: Some("job-2".to_string()),
            queue_id: Some("queue-2".to_string()),
            now_ms: 2003,
        });
        assert!(duplicate.is_err());

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn operation_plan_dependency_promotion_moves_todo_items_to_ready() {
        let root = temp_root("operation_plan_dependency_promotion_moves_todo_items_to_ready");
        let harness_home = root.join(".agent-harness");
        let _ = create_operation_plan(CreateOperationPlanOptions {
            harness_home: harness_home.clone(),
            plan_id: "plan-3".to_string(),
            origin_queue_id: None,
            session_key: "session-c".to_string(),
            agent_id: "agent-main".to_string(),
            goal: "promote deps".to_string(),
            acceptance_criteria: None,
            constraints: None,
            max_open_items: None,
            max_fanout: None,
            now_ms: 3000,
        })
        .unwrap();

        let dep = add_operation_plan_item(OperationPlanAddItemOptions {
            harness_home: harness_home.clone(),
            plan_id: "plan-3".to_string(),
            item_id: "dep".to_string(),
            title: "dependency".to_string(),
            body: "base".to_string(),
            depends_on: Vec::new(),
            acceptance_criteria: None,
            risk: None,
            now_ms: 3001,
        })
        .unwrap();
        let _ = add_operation_plan_item(OperationPlanAddItemOptions {
            harness_home: harness_home.clone(),
            plan_id: "plan-3".to_string(),
            item_id: "follow".to_string(),
            title: "follow up".to_string(),
            body: "use dep".to_string(),
            depends_on: vec!["dep".to_string()],
            acceptance_criteria: None,
            risk: None,
            now_ms: 3002,
        })
        .unwrap();

        let dep_running = update_operation_plan_item(OperationPlanUpdateItemOptions {
            harness_home: harness_home.clone(),
            plan_id: "plan-3".to_string(),
            item_id: "dep".to_string(),
            expected_item_version: Some(dep.item.version),
            status: Some(OperationPlanItemStatus::Ready),
            title: None,
            body: None,
            depends_on: None,
            assignee: None,
            worker_job_id: None,
            queue_id: None,
            risk: None,
            evidence: None,
            replace_evidence: false,
            add_evidence: Vec::new(),
            now_ms: 3003,
        })
        .unwrap();
        let dep_running = update_operation_plan_item(OperationPlanUpdateItemOptions {
            harness_home: harness_home.clone(),
            plan_id: "plan-3".to_string(),
            item_id: "dep".to_string(),
            expected_item_version: Some(dep_running.item.version),
            status: Some(OperationPlanItemStatus::Running),
            title: None,
            body: None,
            depends_on: None,
            assignee: None,
            worker_job_id: None,
            queue_id: None,
            risk: None,
            evidence: None,
            replace_evidence: false,
            add_evidence: Vec::new(),
            now_ms: 3004,
        })
        .unwrap();
        let dep_done = update_operation_plan_item(OperationPlanUpdateItemOptions {
            harness_home: harness_home.clone(),
            plan_id: "plan-3".to_string(),
            item_id: "dep".to_string(),
            expected_item_version: Some(dep_running.item.version),
            status: Some(OperationPlanItemStatus::Review),
            title: None,
            body: None,
            depends_on: None,
            assignee: None,
            worker_job_id: None,
            queue_id: None,
            risk: None,
            evidence: None,
            replace_evidence: false,
            add_evidence: Vec::new(),
            now_ms: 3005,
        })
        .unwrap();
        let _ = update_operation_plan_item(OperationPlanUpdateItemOptions {
            harness_home: harness_home.clone(),
            plan_id: "plan-3".to_string(),
            item_id: "dep".to_string(),
            expected_item_version: Some(dep_done.item.version),
            status: Some(OperationPlanItemStatus::Done),
            title: None,
            body: None,
            depends_on: None,
            assignee: None,
            worker_job_id: None,
            queue_id: None,
            risk: None,
            evidence: Some(vec!["dep evidence".to_string()]),
            replace_evidence: false,
            add_evidence: Vec::new(),
            now_ms: 3006,
        })
        .unwrap();

        let report = promote_operation_plan_items_from_dependencies(
            OperationPlanPromoteDependenciesOptions {
                harness_home: harness_home.clone(),
                plan_id: "plan-3".to_string(),
                now_ms: 3007,
            },
        )
        .unwrap();
        assert_eq!(report.promoted_item_ids, vec!["follow".to_string()]);

        let show = show_operation_plan(OperationPlanShowOptions {
            harness_home,
            plan_id: "plan-3".to_string(),
        })
        .unwrap();
        let follow = show
            .items
            .into_iter()
            .find(|item| item.item_id == "follow")
            .unwrap();
        assert_eq!(follow.status, OperationPlanItemStatus::Ready);
        assert_eq!(follow.version, 2);
    }

    #[test]
    fn operation_plan_item_update_fails_with_version_mismatch() {
        let root = temp_root("operation_plan_item_update_fails_with_version_mismatch");
        let harness_home = root.join(".agent-harness");
        let _ = create_operation_plan(CreateOperationPlanOptions {
            harness_home: harness_home.clone(),
            plan_id: "plan-4".to_string(),
            origin_queue_id: None,
            session_key: "session-d".to_string(),
            agent_id: "agent-main".to_string(),
            goal: "version test".to_string(),
            acceptance_criteria: None,
            constraints: None,
            max_open_items: None,
            max_fanout: None,
            now_ms: 4000,
        })
        .unwrap();

        let item = add_operation_plan_item(OperationPlanAddItemOptions {
            harness_home: harness_home.clone(),
            plan_id: "plan-4".to_string(),
            item_id: "item-1".to_string(),
            title: "version".to_string(),
            body: "check stale write".to_string(),
            depends_on: Vec::new(),
            acceptance_criteria: None,
            risk: None,
            now_ms: 4001,
        })
        .unwrap();

        let stale = update_operation_plan_item(OperationPlanUpdateItemOptions {
            harness_home: harness_home.clone(),
            plan_id: "plan-4".to_string(),
            item_id: "item-1".to_string(),
            expected_item_version: Some(item.item.version + 1),
            status: Some(OperationPlanItemStatus::Ready),
            title: None,
            body: None,
            depends_on: None,
            assignee: None,
            worker_job_id: None,
            queue_id: None,
            risk: None,
            evidence: None,
            replace_evidence: false,
            add_evidence: Vec::new(),
            now_ms: 4002,
        });
        assert!(stale.is_err());

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn operation_plan_completion_requires_evidence_for_done_items() {
        let root = temp_root("operation_plan_completion_requires_evidence_for_done_items");
        let harness_home = root.join(".agent-harness");
        let _ = create_operation_plan(CreateOperationPlanOptions {
            harness_home: harness_home.clone(),
            plan_id: "plan-5".to_string(),
            origin_queue_id: None,
            session_key: "session-e".to_string(),
            agent_id: "agent-main".to_string(),
            goal: "completion evidence".to_string(),
            acceptance_criteria: None,
            constraints: None,
            max_open_items: None,
            max_fanout: None,
            now_ms: 5000,
        })
        .unwrap();
        let item = add_operation_plan_item(OperationPlanAddItemOptions {
            harness_home: harness_home.clone(),
            plan_id: "plan-5".to_string(),
            item_id: "item-1".to_string(),
            title: "close".to_string(),
            body: "finish".to_string(),
            depends_on: Vec::new(),
            acceptance_criteria: None,
            risk: None,
            now_ms: 5001,
        })
        .unwrap();
        let ready = update_operation_plan_item(OperationPlanUpdateItemOptions {
            harness_home: harness_home.clone(),
            plan_id: "plan-5".to_string(),
            item_id: "item-1".to_string(),
            expected_item_version: Some(item.item.version),
            status: Some(OperationPlanItemStatus::Ready),
            title: None,
            body: None,
            depends_on: None,
            assignee: None,
            worker_job_id: None,
            queue_id: None,
            risk: None,
            evidence: None,
            replace_evidence: false,
            add_evidence: Vec::new(),
            now_ms: 5002,
        })
        .unwrap();
        let running = update_operation_plan_item(OperationPlanUpdateItemOptions {
            harness_home: harness_home.clone(),
            plan_id: "plan-5".to_string(),
            item_id: "item-1".to_string(),
            expected_item_version: Some(ready.item.version),
            status: Some(OperationPlanItemStatus::Running),
            title: None,
            body: None,
            depends_on: None,
            assignee: None,
            worker_job_id: None,
            queue_id: None,
            risk: None,
            evidence: None,
            replace_evidence: false,
            add_evidence: Vec::new(),
            now_ms: 5003,
        })
        .unwrap();
        let review = update_operation_plan_item(OperationPlanUpdateItemOptions {
            harness_home: harness_home.clone(),
            plan_id: "plan-5".to_string(),
            item_id: "item-1".to_string(),
            expected_item_version: Some(running.item.version),
            status: Some(OperationPlanItemStatus::Review),
            title: None,
            body: None,
            depends_on: None,
            assignee: None,
            worker_job_id: None,
            queue_id: None,
            risk: None,
            evidence: None,
            replace_evidence: false,
            add_evidence: Vec::new(),
            now_ms: 5004,
        })
        .unwrap();
        let done = update_operation_plan_item(OperationPlanUpdateItemOptions {
            harness_home: harness_home.clone(),
            plan_id: "plan-5".to_string(),
            item_id: "item-1".to_string(),
            expected_item_version: Some(review.item.version),
            status: Some(OperationPlanItemStatus::Done),
            title: None,
            body: None,
            depends_on: None,
            assignee: None,
            worker_job_id: None,
            queue_id: None,
            risk: None,
            evidence: None,
            replace_evidence: false,
            add_evidence: Vec::new(),
            now_ms: 5005,
        })
        .unwrap();

        let missing = complete_operation_plan(OperationPlanCompleteOptions {
            harness_home: harness_home.clone(),
            plan_id: "plan-5".to_string(),
            reason: None,
            now_ms: 5006,
        });
        assert!(missing.is_err());

        let _ = update_operation_plan_item(OperationPlanUpdateItemOptions {
            harness_home: harness_home.clone(),
            plan_id: "plan-5".to_string(),
            item_id: "item-1".to_string(),
            expected_item_version: Some(done.item.version),
            status: None,
            title: None,
            body: None,
            depends_on: None,
            assignee: None,
            worker_job_id: None,
            queue_id: None,
            risk: None,
            evidence: Some(vec!["artifact".to_string()]),
            replace_evidence: false,
            add_evidence: Vec::new(),
            now_ms: 5007,
        })
        .unwrap();

        let completed = complete_operation_plan(OperationPlanCompleteOptions {
            harness_home: harness_home.clone(),
            plan_id: "plan-5".to_string(),
            reason: Some("done".to_string()),
            now_ms: 5008,
        })
        .unwrap();
        assert_eq!(completed.plan.status, OperationPlanStatus::Completed);

        let _ = fs::remove_dir_all(root);
    }

    fn temp_root(test_name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "agent-harness-operation-plan-{test_name}-{}-{nanos}",
            std::process::id()
        ))
    }
}
