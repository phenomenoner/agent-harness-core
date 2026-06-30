use serde::{Deserialize, Serialize};

pub const RICH_MESSAGE_PRESENTATION_SCHEMA: &str = "agent-harness.rich-message-presentation.v1";

const MAX_FALLBACK_TEXT_CHARS: usize = 4_096;
const MAX_BLOCKS: usize = 16;
const MAX_ACTIONS: usize = 8;
const MAX_MEDIA_REFS: usize = 8;
const MAX_PARAGRAPH_CHARS: usize = 4_096;
const MAX_CODE_CHARS: usize = 8_000;
const MAX_FIELD_LABEL_CHARS: usize = 80;
const MAX_FIELD_VALUE_CHARS: usize = 1_000;
const MAX_ACTION_LABEL_CHARS: usize = 80;
const DISCORD_CONTENT_LIMIT: usize = 2_000;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RichMessagePresentation {
    #[serde(default = "default_rich_message_presentation_schema")]
    pub schema: String,
    pub fallback_text: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub blocks: Vec<RichPresentationBlock>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub actions: Vec<RichPresentationAction>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub media: Vec<RichPresentationMediaRef>,
    #[serde(
        default,
        skip_serializing_if = "RichPresentationLinkPreview::is_default"
    )]
    pub link_preview: RichPresentationLinkPreview,
    #[serde(
        default,
        skip_serializing_if = "RichPresentationDeliveryPolicy::is_default"
    )]
    pub delivery_policy: RichPresentationDeliveryPolicy,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", tag = "kind")]
pub enum RichPresentationBlock {
    Paragraph {
        text: String,
    },
    FieldList {
        fields: Vec<RichPresentationField>,
    },
    Code {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        language: Option<String>,
        text: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RichPresentationField {
    pub label: String,
    pub value: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub style: Option<RichPresentationTextStyle>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum RichPresentationTextStyle {
    Plain,
    Code,
    Bold,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RichPresentationAction {
    pub id: String,
    pub label: String,
    pub kind: RichPresentationActionKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum RichPresentationActionKind {
    Url,
    Callback,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RichPresentationMediaRef {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attachment_index: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub artifact_ref: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub caption: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub role: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RichPresentationLinkPreview {
    #[serde(default = "default_link_preview_mode")]
    pub mode: RichPresentationLinkPreviewMode,
}

impl Default for RichPresentationLinkPreview {
    fn default() -> Self {
        Self {
            mode: RichPresentationLinkPreviewMode::Off,
        }
    }
}

impl RichPresentationLinkPreview {
    fn is_default(&self) -> bool {
        *self == Self::default()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum RichPresentationLinkPreviewMode {
    Off,
    On,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RichPresentationDeliveryPolicy {
    #[serde(default = "default_delivery_atomicity")]
    pub atomicity: RichPresentationAtomicity,
    #[serde(default = "default_allow_fallback_text")]
    pub allow_fallback_text: bool,
}

impl Default for RichPresentationDeliveryPolicy {
    fn default() -> Self {
        Self {
            atomicity: RichPresentationAtomicity::AllOrTerminal,
            allow_fallback_text: true,
        }
    }
}

impl RichPresentationDeliveryPolicy {
    fn is_default(&self) -> bool {
        *self == Self::default()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum RichPresentationAtomicity {
    AllOrTerminal,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RichPresentationValidationOptions {
    pub attachment_count: usize,
    pub allow_url_actions: bool,
    pub allow_callback_actions: bool,
}

impl Default for RichPresentationValidationOptions {
    fn default() -> Self {
        Self {
            attachment_count: 0,
            allow_url_actions: true,
            allow_callback_actions: false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RichPresentationValidationError {
    pub field: &'static str,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RenderedTelegramPresentation {
    pub text: String,
    pub parse_mode: &'static str,
    pub link_preview_disabled: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RenderedDiscordPresentation {
    pub chunks: Vec<String>,
    pub allowed_mentions_parse: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RenderedRichBatch {
    pub atomicity: RichPresentationAtomicity,
    pub units: Vec<RenderedRichUnit>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RenderedRichUnit {
    pub unit_id: String,
    pub kind: RenderedRichUnitKind,
    pub text: Option<String>,
    pub attachment_index: Option<usize>,
    pub artifact_ref: Option<String>,
    pub action_id: Option<String>,
    pub provider_action_kind: Option<String>,
    pub requires_reentry_gate: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RenderedRichUnitKind {
    Text,
    Media,
    ComponentAction,
}

pub fn validate_rich_message_presentation(
    presentation: &RichMessagePresentation,
    options: &RichPresentationValidationOptions,
) -> Result<(), RichPresentationValidationError> {
    require(
        "schema",
        presentation.schema == RICH_MESSAGE_PRESENTATION_SCHEMA,
        format!("schema must be exactly {RICH_MESSAGE_PRESENTATION_SCHEMA}"),
    )?;
    require(
        "fallbackText",
        !presentation.fallback_text.trim().is_empty(),
        "fallbackText is required".to_string(),
    )?;
    require(
        "fallbackText",
        char_count(&presentation.fallback_text) <= MAX_FALLBACK_TEXT_CHARS,
        format!("fallbackText must be <= {MAX_FALLBACK_TEXT_CHARS} characters"),
    )?;
    require(
        "blocks",
        presentation.blocks.len() <= MAX_BLOCKS,
        format!("blocks must contain <= {MAX_BLOCKS} items"),
    )?;
    require(
        "actions",
        presentation.actions.len() <= MAX_ACTIONS,
        format!("actions must contain <= {MAX_ACTIONS} items"),
    )?;
    require(
        "media",
        presentation.media.len() <= MAX_MEDIA_REFS,
        format!("media must contain <= {MAX_MEDIA_REFS} items"),
    )?;
    for block in &presentation.blocks {
        validate_block(block)?;
    }
    for action in &presentation.actions {
        validate_action(action, options)?;
    }
    for media in &presentation.media {
        validate_media_ref(media, options)?;
    }
    Ok(())
}

pub fn rich_presentation_from_plain_final(text: &str) -> Option<RichMessagePresentation> {
    rich_presentation_from_plain_final_with_attachment_count(text, 0)
}

pub fn rich_presentation_from_plain_final_with_attachment_count(
    text: &str,
    attachment_count: usize,
) -> Option<RichMessagePresentation> {
    let text = text.trim();
    if text.is_empty() {
        return None;
    }
    let fallback_text = truncate_chars(text, MAX_FALLBACK_TEXT_CHARS);
    let blocks = rich_blocks_from_plain_final(text);
    let media = (0..attachment_count)
        .map(|index| RichPresentationMediaRef {
            attachment_index: Some(index),
            artifact_ref: None,
            caption: None,
            role: None,
        })
        .collect::<Vec<_>>();
    let presentation = RichMessagePresentation {
        schema: RICH_MESSAGE_PRESENTATION_SCHEMA.to_string(),
        fallback_text,
        blocks,
        actions: Vec::new(),
        media,
        link_preview: RichPresentationLinkPreview::default(),
        delivery_policy: RichPresentationDeliveryPolicy::default(),
    };
    validate_rich_message_presentation(
        &presentation,
        &RichPresentationValidationOptions {
            attachment_count,
            ..RichPresentationValidationOptions::default()
        },
    )
    .ok()
    .map(|_| presentation)
}

pub fn render_rich_presentation_for_telegram(
    presentation: &RichMessagePresentation,
    options: &RichPresentationValidationOptions,
) -> Result<RenderedTelegramPresentation, RichPresentationValidationError> {
    validate_rich_message_presentation(presentation, options)?;
    let mut lines = Vec::new();
    for block in &presentation.blocks {
        match block {
            RichPresentationBlock::Paragraph { text } => lines.push(html_escape_text(text)),
            RichPresentationBlock::FieldList { fields } => {
                for field in fields {
                    lines.push(render_telegram_field(field));
                }
            }
            RichPresentationBlock::Code { language, text } => {
                let body = html_escape_text(text);
                let rendered = language
                    .as_deref()
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .map(|language| {
                        format!(
                            "<pre><code class=\"language-{}\">{body}</code></pre>",
                            html_escape_attr(language)
                        )
                    })
                    .unwrap_or_else(|| format!("<pre>{body}</pre>"));
                lines.push(rendered);
            }
        }
    }
    for action in &presentation.actions {
        if action.kind == RichPresentationActionKind::Url
            && let Some(url) = action.url.as_deref()
        {
            lines.push(format!(
                "<a href=\"{}\">{}</a>",
                html_escape_attr(url),
                html_escape_text(&action.label)
            ));
        }
    }
    let text = if lines.is_empty() {
        html_escape_text(&presentation.fallback_text)
    } else {
        lines.join("\n")
    };
    Ok(RenderedTelegramPresentation {
        text,
        parse_mode: "HTML",
        link_preview_disabled: presentation.link_preview.mode
            == RichPresentationLinkPreviewMode::Off,
    })
}

pub fn render_rich_presentation_for_discord(
    presentation: &RichMessagePresentation,
    options: &RichPresentationValidationOptions,
) -> Result<RenderedDiscordPresentation, RichPresentationValidationError> {
    validate_rich_message_presentation(presentation, options)?;
    let mut lines = Vec::new();
    for block in &presentation.blocks {
        match block {
            RichPresentationBlock::Paragraph { text } => {
                lines.push(discord_escape_text(text));
            }
            RichPresentationBlock::FieldList { fields } => {
                for field in fields {
                    lines.push(render_discord_field(field));
                }
            }
            RichPresentationBlock::Code { language, text } => {
                let language = language
                    .as_deref()
                    .map(sanitize_code_language)
                    .unwrap_or_default();
                lines.push(format!(
                    "```{}\n{}\n```",
                    language,
                    discord_escape_code_block(text)
                ));
            }
        }
    }
    for action in &presentation.actions {
        if action.kind == RichPresentationActionKind::Url
            && let Some(url) = action.url.as_deref()
        {
            lines.push(format!(
                "[{}]({})",
                discord_escape_link_label(&action.label),
                discord_escape_url(url)
            ));
        }
    }
    let text = if lines.is_empty() {
        discord_escape_text(&presentation.fallback_text)
    } else {
        lines.join("\n")
    };
    Ok(RenderedDiscordPresentation {
        chunks: split_discord_chunks(&text, DISCORD_CONTENT_LIMIT),
        allowed_mentions_parse: Vec::new(),
    })
}

pub fn render_rich_presentation_batch_for_telegram(
    presentation: &RichMessagePresentation,
    options: &RichPresentationValidationOptions,
) -> Result<RenderedRichBatch, RichPresentationValidationError> {
    let rendered = render_rich_presentation_for_telegram(presentation, options)?;
    let mut units = Vec::new();
    if !rendered.text.trim().is_empty() {
        units.push(RenderedRichUnit {
            unit_id: "text:0".to_string(),
            kind: RenderedRichUnitKind::Text,
            text: Some(rendered.text),
            attachment_index: None,
            artifact_ref: None,
            action_id: None,
            provider_action_kind: None,
            requires_reentry_gate: false,
        });
    }
    append_media_units(&mut units, presentation);
    append_action_units(&mut units, presentation, "telegram");
    Ok(RenderedRichBatch {
        atomicity: presentation.delivery_policy.atomicity,
        units,
    })
}

pub fn render_rich_presentation_batch_for_discord(
    presentation: &RichMessagePresentation,
    options: &RichPresentationValidationOptions,
) -> Result<RenderedRichBatch, RichPresentationValidationError> {
    let rendered = render_rich_presentation_for_discord(presentation, options)?;
    let mut units = Vec::new();
    for (index, chunk) in rendered.chunks.into_iter().enumerate() {
        if !chunk.trim().is_empty() {
            units.push(RenderedRichUnit {
                unit_id: format!("text:{index}"),
                kind: RenderedRichUnitKind::Text,
                text: Some(chunk),
                attachment_index: None,
                artifact_ref: None,
                action_id: None,
                provider_action_kind: None,
                requires_reentry_gate: false,
            });
        }
    }
    append_media_units(&mut units, presentation);
    append_action_units(&mut units, presentation, "discord");
    Ok(RenderedRichBatch {
        atomicity: presentation.delivery_policy.atomicity,
        units,
    })
}

fn validate_block(block: &RichPresentationBlock) -> Result<(), RichPresentationValidationError> {
    match block {
        RichPresentationBlock::Paragraph { text } => require(
            "blocks.text",
            char_count(text) <= MAX_PARAGRAPH_CHARS,
            format!("paragraph text must be <= {MAX_PARAGRAPH_CHARS} characters"),
        ),
        RichPresentationBlock::FieldList { fields } => {
            require(
                "blocks.fields",
                !fields.is_empty(),
                "fieldList requires at least one field".to_string(),
            )?;
            for field in fields {
                require(
                    "blocks.fields.label",
                    !field.label.trim().is_empty(),
                    "field label is required".to_string(),
                )?;
                require(
                    "blocks.fields.label",
                    char_count(&field.label) <= MAX_FIELD_LABEL_CHARS,
                    format!("field label must be <= {MAX_FIELD_LABEL_CHARS} characters"),
                )?;
                require(
                    "blocks.fields.value",
                    char_count(&field.value) <= MAX_FIELD_VALUE_CHARS,
                    format!("field value must be <= {MAX_FIELD_VALUE_CHARS} characters"),
                )?;
            }
            Ok(())
        }
        RichPresentationBlock::Code { text, .. } => require(
            "blocks.code.text",
            char_count(text) <= MAX_CODE_CHARS,
            format!("code text must be <= {MAX_CODE_CHARS} characters"),
        ),
    }
}

fn validate_action(
    action: &RichPresentationAction,
    options: &RichPresentationValidationOptions,
) -> Result<(), RichPresentationValidationError> {
    require(
        "actions.id",
        is_safe_identifier(&action.id),
        "action id must be a bounded safe identifier".to_string(),
    )?;
    require(
        "actions.label",
        !action.label.trim().is_empty(),
        "action label is required".to_string(),
    )?;
    require(
        "actions.label",
        char_count(&action.label) <= MAX_ACTION_LABEL_CHARS,
        format!("action label must be <= {MAX_ACTION_LABEL_CHARS} characters"),
    )?;
    match action.kind {
        RichPresentationActionKind::Url => {
            require(
                "actions.kind",
                options.allow_url_actions,
                "URL actions are disabled by channel capability".to_string(),
            )?;
            let url = action.url.as_deref().unwrap_or_default();
            require(
                "actions.url",
                is_safe_http_url(url),
                "unsafe URL".to_string(),
            )
        }
        RichPresentationActionKind::Callback => require(
            "actions.kind",
            options.allow_callback_actions,
            "callback actions are disabled by channel capability".to_string(),
        ),
    }
}

fn validate_media_ref(
    media: &RichPresentationMediaRef,
    options: &RichPresentationValidationOptions,
) -> Result<(), RichPresentationValidationError> {
    require(
        "media",
        media.attachment_index.is_some() || media.artifact_ref.is_some(),
        "media requires attachmentIndex or artifactRef".to_string(),
    )?;
    if let Some(index) = media.attachment_index {
        require(
            "media.attachmentIndex",
            index < options.attachment_count,
            "attachmentIndex does not reference an existing attachment".to_string(),
        )?;
    }
    if let Some(artifact_ref) = media.artifact_ref.as_deref() {
        require(
            "media.artifactRef",
            is_safe_artifact_ref(artifact_ref),
            "artifactRef must be a harness artifact reference, not raw/provider data".to_string(),
        )?;
    }
    if let Some(caption) = media.caption.as_deref() {
        require(
            "media.caption",
            char_count(caption) <= MAX_FIELD_VALUE_CHARS,
            format!("media caption must be <= {MAX_FIELD_VALUE_CHARS} characters"),
        )?;
    }
    Ok(())
}

fn rich_blocks_from_plain_final(text: &str) -> Vec<RichPresentationBlock> {
    let mut blocks = Vec::new();
    let mut paragraph = Vec::new();
    let mut code = Vec::new();
    let mut code_language: Option<String> = None;
    let mut in_code = false;

    for line in text.lines() {
        let trimmed = line.trim();
        if let Some(fence) = trimmed.strip_prefix("```") {
            if in_code {
                push_code_block(&mut blocks, code_language.take(), &mut code);
                in_code = false;
            } else {
                push_paragraph_blocks(&mut blocks, &mut paragraph);
                code_language = fence
                    .split_whitespace()
                    .next()
                    .filter(|value| !value.is_empty())
                    .map(|value| value.to_string());
                in_code = true;
            }
            if blocks.len() >= MAX_BLOCKS {
                break;
            }
            continue;
        }

        if in_code {
            code.push(line.to_string());
            continue;
        }

        if trimmed.is_empty() {
            push_paragraph_blocks(&mut blocks, &mut paragraph);
            if blocks.len() >= MAX_BLOCKS {
                break;
            }
            continue;
        }
        paragraph.push(line.to_string());
    }

    if blocks.len() < MAX_BLOCKS {
        if in_code {
            push_code_block(&mut blocks, code_language, &mut code);
        } else {
            push_paragraph_blocks(&mut blocks, &mut paragraph);
        }
    }

    if blocks.is_empty() {
        blocks.push(RichPresentationBlock::Paragraph {
            text: truncate_chars(text, MAX_PARAGRAPH_CHARS),
        });
    }
    blocks.truncate(MAX_BLOCKS);
    blocks
}

fn push_paragraph_blocks(blocks: &mut Vec<RichPresentationBlock>, paragraph: &mut Vec<String>) {
    if paragraph.is_empty() || blocks.len() >= MAX_BLOCKS {
        paragraph.clear();
        return;
    }
    let text = paragraph.join("\n").trim().to_string();
    paragraph.clear();
    for chunk in chunk_chars(&text, MAX_PARAGRAPH_CHARS) {
        if blocks.len() >= MAX_BLOCKS {
            break;
        }
        if !chunk.trim().is_empty() {
            blocks.push(RichPresentationBlock::Paragraph { text: chunk });
        }
    }
}

fn push_code_block(
    blocks: &mut Vec<RichPresentationBlock>,
    language: Option<String>,
    code: &mut Vec<String>,
) {
    if code.is_empty() || blocks.len() >= MAX_BLOCKS {
        code.clear();
        return;
    }
    let text = code.join("\n");
    code.clear();
    for chunk in chunk_chars(&text, MAX_CODE_CHARS) {
        if blocks.len() >= MAX_BLOCKS {
            break;
        }
        blocks.push(RichPresentationBlock::Code {
            language: language.clone(),
            text: chunk,
        });
    }
}

fn append_media_units(units: &mut Vec<RenderedRichUnit>, presentation: &RichMessagePresentation) {
    for (index, media) in presentation.media.iter().enumerate() {
        units.push(RenderedRichUnit {
            unit_id: format!("media:{index}"),
            kind: RenderedRichUnitKind::Media,
            text: media.caption.clone(),
            attachment_index: media.attachment_index,
            artifact_ref: media.artifact_ref.clone(),
            action_id: None,
            provider_action_kind: None,
            requires_reentry_gate: false,
        });
    }
}

fn append_action_units(
    units: &mut Vec<RenderedRichUnit>,
    presentation: &RichMessagePresentation,
    provider: &str,
) {
    for action in &presentation.actions {
        let provider_action_kind = match (provider, action.kind) {
            ("telegram", RichPresentationActionKind::Url) => "telegram-url",
            ("telegram", RichPresentationActionKind::Callback) => "telegram-callback",
            ("discord", RichPresentationActionKind::Url) => "discord-url",
            ("discord", RichPresentationActionKind::Callback) => "discord-callback",
            (_, RichPresentationActionKind::Url) => "url",
            (_, RichPresentationActionKind::Callback) => "callback",
        };
        units.push(RenderedRichUnit {
            unit_id: format!("component-action:{}", action.id),
            kind: RenderedRichUnitKind::ComponentAction,
            text: Some(action.label.clone()),
            attachment_index: None,
            artifact_ref: None,
            action_id: Some(action.id.clone()),
            provider_action_kind: Some(provider_action_kind.to_string()),
            requires_reentry_gate: action.kind == RichPresentationActionKind::Callback,
        });
    }
}

fn render_telegram_field(field: &RichPresentationField) -> String {
    let label = html_escape_text(&field.label);
    let value = html_escape_text(&field.value);
    match field.style.unwrap_or(RichPresentationTextStyle::Plain) {
        RichPresentationTextStyle::Plain => format!("<b>{label}</b>: {value}"),
        RichPresentationTextStyle::Code => format!("<b>{label}</b>: <code>{value}</code>"),
        RichPresentationTextStyle::Bold => format!("<b>{label}</b>: <b>{value}</b>"),
    }
}

fn render_discord_field(field: &RichPresentationField) -> String {
    let label = discord_escape_text(&field.label);
    let value = discord_escape_text(&field.value);
    match field.style.unwrap_or(RichPresentationTextStyle::Plain) {
        RichPresentationTextStyle::Plain => format!("**{label}**: {value}"),
        RichPresentationTextStyle::Code => {
            format!("**{label}**: `{}`", value.replace('`', "'"))
        }
        RichPresentationTextStyle::Bold => format!("**{label}**: **{value}**"),
    }
}

fn split_discord_chunks(text: &str, max_chars: usize) -> Vec<String> {
    if text.is_empty() {
        return vec![String::new()];
    }
    let mut chunks = Vec::new();
    let mut current = String::new();
    let mut current_chars = 0usize;
    for segment in text.split_inclusive('\n') {
        let segment_chars = char_count(segment);
        if segment_chars > max_chars {
            if !current.is_empty() {
                chunks.push(std::mem::take(&mut current));
                current_chars = 0;
            }
            for ch in segment.chars() {
                current.push(ch);
                current_chars += 1;
                if current_chars == max_chars {
                    chunks.push(std::mem::take(&mut current));
                    current_chars = 0;
                }
            }
        } else if current_chars + segment_chars <= max_chars {
            current.push_str(segment);
            current_chars += segment_chars;
        } else {
            chunks.push(std::mem::take(&mut current));
            current.push_str(segment);
            current_chars = segment_chars;
        }
    }
    if !current.is_empty() {
        chunks.push(current);
    }
    chunks
}

fn truncate_chars(value: &str, max_chars: usize) -> String {
    value.chars().take(max_chars).collect()
}

fn chunk_chars(value: &str, max_chars: usize) -> Vec<String> {
    if value.is_empty() {
        return Vec::new();
    }
    let mut chunks = Vec::new();
    let mut current = String::new();
    let mut current_chars = 0usize;
    for ch in value.chars() {
        current.push(ch);
        current_chars += 1;
        if current_chars == max_chars {
            chunks.push(std::mem::take(&mut current));
            current_chars = 0;
        }
    }
    if !current.is_empty() {
        chunks.push(current);
    }
    chunks
}

fn require(
    field: &'static str,
    condition: bool,
    message: String,
) -> Result<(), RichPresentationValidationError> {
    if condition {
        Ok(())
    } else {
        Err(RichPresentationValidationError { field, message })
    }
}

fn default_rich_message_presentation_schema() -> String {
    RICH_MESSAGE_PRESENTATION_SCHEMA.to_string()
}

fn default_link_preview_mode() -> RichPresentationLinkPreviewMode {
    RichPresentationLinkPreviewMode::Off
}

fn default_delivery_atomicity() -> RichPresentationAtomicity {
    RichPresentationAtomicity::AllOrTerminal
}

fn default_allow_fallback_text() -> bool {
    true
}

fn html_escape_text(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for ch in value.chars() {
        match ch {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&#39;"),
            _ => out.push(ch),
        }
    }
    out
}

fn html_escape_attr(value: &str) -> String {
    html_escape_text(value)
}

fn discord_escape_text(value: &str) -> String {
    value
        .replace('@', "@ ")
        .replace("<@", "< @")
        .replace("<#", "< #")
        .replace("<@&", "< @&")
}

fn discord_escape_code_block(value: &str) -> String {
    value.replace("```", "'''").replace('@', "@ ")
}

fn discord_escape_link_label(value: &str) -> String {
    discord_escape_text(value)
        .replace('[', "(")
        .replace(']', ")")
}

fn discord_escape_url(value: &str) -> String {
    value.replace(')', "%29")
}

fn sanitize_code_language(value: &str) -> String {
    value
        .chars()
        .filter(|ch| ch.is_ascii_alphanumeric() || *ch == '-' || *ch == '_')
        .take(32)
        .collect()
}

fn is_safe_identifier(value: &str) -> bool {
    let len = char_count(value);
    (1..=64).contains(&len)
        && value
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '_' || ch == '-')
}

fn is_safe_http_url(value: &str) -> bool {
    let trimmed = value.trim();
    (trimmed.starts_with("https://") || trimmed.starts_with("http://"))
        && !trimmed.chars().any(char::is_whitespace)
        && !trimmed.contains('<')
        && !trimmed.contains('>')
}

fn is_safe_artifact_ref(value: &str) -> bool {
    let trimmed = value.trim();
    trimmed.starts_with("agent-harness://")
        && !trimmed.contains("..")
        && !trimmed.contains('\\')
        && !trimmed.chars().any(char::is_whitespace)
}

fn char_count(value: &str) -> usize {
    value.chars().count()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ChannelOutboundMessage;

    #[test]
    fn rich_presentation_round_trips_as_optional_channel_payload_shape() {
        let json = r#"{
          "fallbackText": "Plain fallback",
          "blocks": [
            {"kind": "paragraph", "text": "Done <ok> & safe"},
            {"kind": "fieldList", "fields": [{"label": "Status", "value": "PASS", "style": "code"}]},
            {"kind": "code", "language": "text", "text": "cargo test"}
          ],
          "actions": [
            {"id": "open_report", "label": "Open report", "kind": "url", "url": "https://example.invalid/report"}
          ],
          "linkPreview": {"mode": "off"},
          "deliveryPolicy": {"atomicity": "all-or-terminal", "allowFallbackText": true}
        }"#;
        let presentation: RichMessagePresentation = serde_json::from_str(json).unwrap();
        assert_eq!(presentation.schema, RICH_MESSAGE_PRESENTATION_SCHEMA);
        assert_eq!(presentation.blocks.len(), 3);
        assert_eq!(
            serde_json::to_value(&presentation).unwrap()["schema"],
            RICH_MESSAGE_PRESENTATION_SCHEMA
        );
    }

    #[test]
    fn legacy_channel_outbound_message_without_presentation_stays_plain_text() {
        let json = r#"{
          "platform": "discord",
          "channelId": "channel-1",
          "userId": "user-1",
          "sessionKey": "discord:channel-1:user-1:main",
          "kind": "agent-reply",
          "text": "plain legacy reply",
          "attachments": []
        }"#;
        let message: ChannelOutboundMessage = serde_json::from_str(json).unwrap();
        assert_eq!(message.text, "plain legacy reply");
        assert!(message.presentation.is_none());
    }

    #[test]
    fn plain_final_bridge_builds_safe_paragraph_and_code_blocks() {
        let presentation = rich_presentation_from_plain_final(
            "Done <ok> & safe\n\n```powershell\ncargo test\n```\n\nNext line.",
        )
        .unwrap();

        assert_eq!(
            presentation.fallback_text,
            "Done <ok> & safe\n\n```powershell\ncargo test\n```\n\nNext line."
        );
        assert_eq!(presentation.blocks.len(), 3);
        assert!(matches!(
            &presentation.blocks[1],
            RichPresentationBlock::Code {
                language,
                text
            } if language.as_deref() == Some("powershell") && text == "cargo test"
        ));
        let telegram = render_rich_presentation_for_telegram(
            &presentation,
            &RichPresentationValidationOptions::default(),
        )
        .unwrap();
        assert!(telegram.text.contains("Done &lt;ok&gt; &amp; safe"));
        assert!(
            telegram
                .text
                .contains("<pre><code class=\"language-powershell\">cargo test</code></pre>")
        );
    }

    #[test]
    fn plain_final_bridge_maps_attachments_to_rendered_media_units() {
        let presentation =
            rich_presentation_from_plain_final_with_attachment_count("Done with files.", 2)
                .unwrap();

        assert_eq!(presentation.fallback_text, "Done with files.");
        assert_eq!(presentation.media.len(), 2);
        assert_eq!(presentation.media[0].attachment_index, Some(0));
        assert_eq!(presentation.media[1].attachment_index, Some(1));
        validate_rich_message_presentation(
            &presentation,
            &RichPresentationValidationOptions {
                attachment_count: 2,
                ..RichPresentationValidationOptions::default()
            },
        )
        .unwrap();

        let telegram = render_rich_presentation_batch_for_telegram(
            &presentation,
            &RichPresentationValidationOptions {
                attachment_count: 2,
                ..RichPresentationValidationOptions::default()
            },
        )
        .unwrap();
        let media_units = telegram
            .units
            .iter()
            .filter(|unit| unit.kind == RenderedRichUnitKind::Media)
            .collect::<Vec<_>>();
        assert_eq!(media_units.len(), 2);
        assert_eq!(media_units[0].unit_id, "media:0");
        assert_eq!(media_units[0].attachment_index, Some(0));
        assert_eq!(media_units[1].unit_id, "media:1");
        assert_eq!(media_units[1].attachment_index, Some(1));

        let discord = render_rich_presentation_batch_for_discord(
            &presentation,
            &RichPresentationValidationOptions {
                attachment_count: 2,
                ..RichPresentationValidationOptions::default()
            },
        )
        .unwrap();
        assert_eq!(
            discord
                .units
                .iter()
                .filter(|unit| unit.kind == RenderedRichUnitKind::Media)
                .count(),
            2
        );
    }

    #[test]
    fn rich_presentation_validation_fails_closed_for_unsafe_shapes() {
        let mut presentation = fixture_presentation();
        presentation.fallback_text.clear();
        let error = validate_rich_message_presentation(
            &presentation,
            &RichPresentationValidationOptions::default(),
        )
        .unwrap_err();
        assert_eq!(error.field, "fallbackText");

        let mut presentation = fixture_presentation();
        presentation.actions[0].url = Some("javascript:alert(1)".to_string());
        let error = validate_rich_message_presentation(
            &presentation,
            &RichPresentationValidationOptions::default(),
        )
        .unwrap_err();
        assert_eq!(error.field, "actions.url");

        let mut presentation = fixture_presentation();
        presentation.media.push(RichPresentationMediaRef {
            attachment_index: Some(99),
            artifact_ref: None,
            caption: None,
            role: None,
        });
        let error = validate_rich_message_presentation(
            &presentation,
            &RichPresentationValidationOptions {
                attachment_count: 1,
                ..Default::default()
            },
        )
        .unwrap_err();
        assert_eq!(error.field, "media.attachmentIndex");
    }

    #[test]
    fn telegram_render_fixture_escapes_html_and_disables_preview() {
        let rendered = render_rich_presentation_for_telegram(
            &fixture_presentation(),
            &RichPresentationValidationOptions::default(),
        )
        .unwrap();
        assert_eq!(rendered.parse_mode, "HTML");
        assert!(rendered.link_preview_disabled);
        assert!(rendered.text.contains("Done &lt;ok&gt; &amp; safe"));
        assert!(rendered.text.contains("<b>Status</b>: <code>PASS</code>"));
        assert!(rendered.text.contains("<pre>cargo test</pre>"));
        assert!(
            rendered
                .text
                .contains("<a href=\"https://example.invalid/report\">Open report</a>")
        );
    }

    #[test]
    fn discord_render_fixture_splits_and_suppresses_mentions() {
        let mut presentation = fixture_presentation();
        presentation.blocks.push(RichPresentationBlock::Paragraph {
            text: format!("@everyone {}", "x".repeat(2_200)),
        });
        let rendered = render_rich_presentation_for_discord(
            &presentation,
            &RichPresentationValidationOptions::default(),
        )
        .unwrap();
        assert!(rendered.allowed_mentions_parse.is_empty());
        assert!(rendered.chunks.len() > 1);
        assert!(
            rendered
                .chunks
                .iter()
                .all(|chunk| chunk.chars().count() <= 2_000)
        );
        assert!(!rendered.chunks.join("\n").contains("@everyone"));
        assert!(rendered.chunks.join("\n").contains("@ everyone"));
    }

    #[test]
    fn telegram_rendered_batch_gates_callback_actions_and_units() {
        let mut presentation = fixture_presentation();
        presentation.actions.push(RichPresentationAction {
            id: "ack".to_string(),
            label: "Ack".to_string(),
            kind: RichPresentationActionKind::Callback,
            url: None,
        });
        presentation.media.push(RichPresentationMediaRef {
            attachment_index: Some(0),
            artifact_ref: None,
            caption: Some("Result".to_string()),
            role: Some("primary".to_string()),
        });

        let disabled = render_rich_presentation_batch_for_telegram(
            &presentation,
            &RichPresentationValidationOptions {
                attachment_count: 1,
                allow_url_actions: true,
                allow_callback_actions: false,
            },
        )
        .unwrap_err();
        assert_eq!(disabled.field, "actions.kind");

        let batch = render_rich_presentation_batch_for_telegram(
            &presentation,
            &RichPresentationValidationOptions {
                attachment_count: 1,
                allow_url_actions: true,
                allow_callback_actions: true,
            },
        )
        .unwrap();
        assert_eq!(batch.atomicity, RichPresentationAtomicity::AllOrTerminal);
        assert!(
            batch
                .units
                .iter()
                .any(|unit| unit.unit_id == "text:0" && unit.kind == RenderedRichUnitKind::Text)
        );
        assert!(batch.units.iter().any(|unit| {
            unit.unit_id == "media:0"
                && unit.kind == RenderedRichUnitKind::Media
                && unit.attachment_index == Some(0)
        }));
        let callback = batch
            .units
            .iter()
            .find(|unit| unit.unit_id == "component-action:ack")
            .unwrap();
        assert_eq!(callback.kind, RenderedRichUnitKind::ComponentAction);
        assert_eq!(
            callback.provider_action_kind.as_deref(),
            Some("telegram-callback")
        );
        assert!(callback.requires_reentry_gate);
    }

    #[test]
    fn discord_rendered_batch_accounts_chunks_and_action_units() {
        let mut presentation = fixture_presentation();
        presentation.blocks.push(RichPresentationBlock::Paragraph {
            text: "x".repeat(2_200),
        });
        let batch = render_rich_presentation_batch_for_discord(
            &presentation,
            &RichPresentationValidationOptions::default(),
        )
        .unwrap();
        assert!(batch.units.iter().any(|unit| unit.unit_id == "text:0"));
        assert!(batch.units.iter().any(|unit| unit.unit_id == "text:1"));
        let url = batch
            .units
            .iter()
            .find(|unit| unit.unit_id == "component-action:open_report")
            .unwrap();
        assert_eq!(url.provider_action_kind.as_deref(), Some("discord-url"));
        assert!(!url.requires_reentry_gate);
    }

    fn fixture_presentation() -> RichMessagePresentation {
        RichMessagePresentation {
            schema: RICH_MESSAGE_PRESENTATION_SCHEMA.to_string(),
            fallback_text: "Plain fallback".to_string(),
            blocks: vec![
                RichPresentationBlock::Paragraph {
                    text: "Done <ok> & safe".to_string(),
                },
                RichPresentationBlock::FieldList {
                    fields: vec![RichPresentationField {
                        label: "Status".to_string(),
                        value: "PASS".to_string(),
                        style: Some(RichPresentationTextStyle::Code),
                    }],
                },
                RichPresentationBlock::Code {
                    language: None,
                    text: "cargo test".to_string(),
                },
            ],
            actions: vec![RichPresentationAction {
                id: "open_report".to_string(),
                label: "Open report".to_string(),
                kind: RichPresentationActionKind::Url,
                url: Some("https://example.invalid/report".to_string()),
            }],
            media: Vec::new(),
            link_preview: RichPresentationLinkPreview::default(),
            delivery_policy: RichPresentationDeliveryPolicy::default(),
        }
    }
}
