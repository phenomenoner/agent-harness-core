use std::error::Error;
use std::fmt;
use std::str;

use ring::digest;
use serde::Serialize;

pub const SKILL_INVOCATION_ENVELOPE_SCHEMA: &str = "agent-harness.skill-invocation-envelope.v1";
const START: &str = "<<<agent-harness.skill-invocation-envelope.v1>>>\n";
const END: &str = "\n<<<agent-harness.skill-invocation-envelope.v1/end>>>";
const MAX_SKILL_ID_BYTES: usize = 256;
const MAX_USER_INSTRUCTION_BYTES: usize = 16 * 1024;
const MAX_BODY_CHECKSUM_BYTES: usize = 96;
const MAX_BODY_BYTES: usize = 256 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SkillInvocationEnvelope {
    pub schema: &'static str,
    pub skill_id: String,
    pub user_instruction: String,
    pub body_checksum: String,
    pub body: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SkillEnvelopeError {
    message: String,
}

impl SkillEnvelopeError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }

    pub fn message(&self) -> &str {
        &self.message
    }
}

impl fmt::Display for SkillEnvelopeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl Error for SkillEnvelopeError {}

pub fn skill_body_checksum(body: &str) -> String {
    let digest = digest::digest(&digest::SHA256, body.as_bytes());
    format!("sha256:{}", hex(digest.as_ref()))
}

pub fn render_skill_invocation_envelope(
    skill_id: &str,
    user_instruction: &str,
    body: &str,
) -> String {
    let body_checksum = skill_body_checksum(body);
    let skill_id = skill_id.trim();
    let user_instruction = user_instruction.trim();
    format!(
        "{START}skillIdBytes: {}\nuserInstructionBytes: {}\nbodyChecksumBytes: {}\nbodyBytes: {}\n\n{}{}{}{}{END}",
        skill_id.len(),
        user_instruction.len(),
        body_checksum.len(),
        body.len(),
        skill_id,
        user_instruction,
        body_checksum,
        body
    )
}

pub fn parse_skill_invocation_envelopes(
    text: &str,
) -> Result<Vec<SkillInvocationEnvelope>, SkillEnvelopeError> {
    let mut envelopes = Vec::new();
    let mut cursor = 0usize;
    while let Some(relative_start) = text[cursor..].find(START) {
        let start = cursor + relative_start;
        let header_start = start + START.len();
        let Some(header_end_relative) = text[header_start..].find("\n\n") else {
            return Err(SkillEnvelopeError::new(
                "skill envelope header terminator missing",
            ));
        };
        let header_end = header_start + header_end_relative;
        let header = &text[header_start..header_end];
        let skill_id_len = header_len(header, "skillIdBytes", MAX_SKILL_ID_BYTES)?;
        let user_instruction_len =
            header_len(header, "userInstructionBytes", MAX_USER_INSTRUCTION_BYTES)?;
        let body_checksum_len = header_len(header, "bodyChecksumBytes", MAX_BODY_CHECKSUM_BYTES)?;
        let body_len = header_len(header, "bodyBytes", MAX_BODY_BYTES)?;
        let payload_start = header_end + 2;
        let payload_len = skill_id_len
            .checked_add(user_instruction_len)
            .and_then(|len| len.checked_add(body_checksum_len))
            .and_then(|len| len.checked_add(body_len))
            .ok_or_else(|| SkillEnvelopeError::new("skill envelope payload length overflow"))?;
        let payload_end = payload_start
            .checked_add(payload_len)
            .ok_or_else(|| SkillEnvelopeError::new("skill envelope payload end overflow"))?;
        if payload_end > text.len() {
            return Err(SkillEnvelopeError::new("skill envelope payload truncated"));
        }
        let bytes = text.as_bytes();
        let skill_id = utf8_slice(bytes, payload_start, skill_id_len, "skill id")?;
        let instruction_start = payload_start + skill_id_len;
        let user_instruction = utf8_slice(
            bytes,
            instruction_start,
            user_instruction_len,
            "user instruction",
        )?;
        let checksum_start = instruction_start + user_instruction_len;
        let body_checksum = utf8_slice(bytes, checksum_start, body_checksum_len, "body checksum")?;
        let body_start = checksum_start + body_checksum_len;
        let body = utf8_slice(bytes, body_start, body_len, "body")?;
        let actual_checksum = skill_body_checksum(body);
        if body_checksum != actual_checksum {
            return Err(SkillEnvelopeError::new(format!(
                "skill envelope checksum mismatch: declared {body_checksum}, actual {actual_checksum}"
            )));
        }
        envelopes.push(SkillInvocationEnvelope {
            schema: SKILL_INVOCATION_ENVELOPE_SCHEMA,
            skill_id: skill_id.to_string(),
            user_instruction: user_instruction.to_string(),
            body_checksum: body_checksum.to_string(),
            body: body.to_string(),
        });
        cursor = payload_end;
        if text[cursor..].starts_with(END) {
            cursor += END.len();
        }
    }
    Ok(envelopes)
}

pub fn extract_user_instruction_from_skill_envelope(
    text: &str,
) -> Result<Option<String>, SkillEnvelopeError> {
    Ok(parse_skill_invocation_envelopes(text)?
        .into_iter()
        .map(|envelope| envelope.user_instruction)
        .find(|instruction| !instruction.trim().is_empty()))
}

pub fn strip_skill_envelopes_for_memory(text: &str) -> Result<String, SkillEnvelopeError> {
    let mut output = String::new();
    let mut cursor = 0usize;
    while let Some(relative_start) = text[cursor..].find(START) {
        let start = cursor + relative_start;
        output.push_str(&text[cursor..start]);
        let parsed = parse_first_envelope_at(text, start)?;
        if !parsed.envelope.user_instruction.trim().is_empty() {
            output.push_str(parsed.envelope.user_instruction.trim());
        }
        cursor = parsed.end;
    }
    output.push_str(&text[cursor..]);
    Ok(output)
}

struct ParsedEnvelope {
    envelope: SkillInvocationEnvelope,
    end: usize,
}

fn parse_first_envelope_at(text: &str, start: usize) -> Result<ParsedEnvelope, SkillEnvelopeError> {
    let header_start = start + START.len();
    let Some(header_end_relative) = text[header_start..].find("\n\n") else {
        return Err(SkillEnvelopeError::new(
            "skill envelope header terminator missing",
        ));
    };
    let header_end = header_start + header_end_relative;
    let header = &text[header_start..header_end];
    let skill_id_len = header_len(header, "skillIdBytes", MAX_SKILL_ID_BYTES)?;
    let user_instruction_len =
        header_len(header, "userInstructionBytes", MAX_USER_INSTRUCTION_BYTES)?;
    let body_checksum_len = header_len(header, "bodyChecksumBytes", MAX_BODY_CHECKSUM_BYTES)?;
    let body_len = header_len(header, "bodyBytes", MAX_BODY_BYTES)?;
    let payload_start = header_end + 2;
    let payload_len = skill_id_len
        .checked_add(user_instruction_len)
        .and_then(|len| len.checked_add(body_checksum_len))
        .and_then(|len| len.checked_add(body_len))
        .ok_or_else(|| SkillEnvelopeError::new("skill envelope payload length overflow"))?;
    let payload_end = payload_start
        .checked_add(payload_len)
        .ok_or_else(|| SkillEnvelopeError::new("skill envelope payload end overflow"))?;
    if payload_end > text.len() {
        return Err(SkillEnvelopeError::new("skill envelope payload truncated"));
    }
    let bytes = text.as_bytes();
    let skill_id = utf8_slice(bytes, payload_start, skill_id_len, "skill id")?;
    let instruction_start = payload_start + skill_id_len;
    let user_instruction = utf8_slice(
        bytes,
        instruction_start,
        user_instruction_len,
        "user instruction",
    )?;
    let checksum_start = instruction_start + user_instruction_len;
    let body_checksum = utf8_slice(bytes, checksum_start, body_checksum_len, "body checksum")?;
    let body_start = checksum_start + body_checksum_len;
    let body = utf8_slice(bytes, body_start, body_len, "body")?;
    let actual_checksum = skill_body_checksum(body);
    if body_checksum != actual_checksum {
        return Err(SkillEnvelopeError::new(format!(
            "skill envelope checksum mismatch: declared {body_checksum}, actual {actual_checksum}"
        )));
    }
    let mut end = payload_end;
    if text[end..].starts_with(END) {
        end += END.len();
    }
    Ok(ParsedEnvelope {
        envelope: SkillInvocationEnvelope {
            schema: SKILL_INVOCATION_ENVELOPE_SCHEMA,
            skill_id: skill_id.to_string(),
            user_instruction: user_instruction.to_string(),
            body_checksum: body_checksum.to_string(),
            body: body.to_string(),
        },
        end,
    })
}

fn header_len(header: &str, key: &str, max: usize) -> Result<usize, SkillEnvelopeError> {
    let Some(value) = header.lines().find_map(|line| {
        let (line_key, line_value) = line.split_once(':')?;
        (line_key.trim() == key).then_some(line_value.trim())
    }) else {
        return Err(SkillEnvelopeError::new(format!(
            "skill envelope header missing {key}"
        )));
    };
    let len = value.parse::<usize>().map_err(|_| {
        SkillEnvelopeError::new(format!("skill envelope header {key} is not a usize"))
    })?;
    if len > max {
        return Err(SkillEnvelopeError::new(format!(
            "skill envelope {key} exceeds cap {max}"
        )));
    }
    Ok(len)
}

fn utf8_slice<'a>(
    bytes: &'a [u8],
    start: usize,
    len: usize,
    label: &str,
) -> Result<&'a str, SkillEnvelopeError> {
    let end = start
        .checked_add(len)
        .ok_or_else(|| SkillEnvelopeError::new("skill envelope slice length overflow"))?;
    let slice = bytes
        .get(start..end)
        .ok_or_else(|| SkillEnvelopeError::new(format!("skill envelope {label} truncated")))?;
    str::from_utf8(slice)
        .map_err(|_| SkillEnvelopeError::new(format!("skill envelope {label} is not UTF-8")))
}

fn hex(bytes: &[u8]) -> String {
    const TABLE: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(TABLE[(byte >> 4) as usize] as char);
        output.push(TABLE[(byte & 0x0f) as usize] as char);
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn skill_envelope_round_trips_with_nested_sentinel_text() {
        let body =
            "# Skill\n\nBody contains <<<agent-harness.skill-invocation-envelope.v1/end>>> safely.";
        let envelope = render_skill_invocation_envelope("memory-cron", "repair cron", body);
        let parsed = parse_skill_invocation_envelopes(&envelope).unwrap();

        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].skill_id, "memory-cron");
        assert_eq!(parsed[0].user_instruction, "repair cron");
        assert_eq!(parsed[0].body, body);
        assert_eq!(
            extract_user_instruction_from_skill_envelope(&envelope).unwrap(),
            Some("repair cron".to_string())
        );
    }

    #[test]
    fn skill_envelope_strip_preserves_only_user_instruction_for_memory() {
        let envelope = render_skill_invocation_envelope(
            "danger",
            "remember the user instruction",
            "do not store this skill body",
        );

        let stripped =
            strip_skill_envelopes_for_memory(&format!("before {envelope} after")).unwrap();

        assert!(stripped.contains("before remember the user instruction after"));
        assert!(!stripped.contains("do not store this skill body"));
    }

    #[test]
    fn skill_envelope_rejects_tampered_body_checksum() {
        let envelope = render_skill_invocation_envelope("memory-cron", "repair cron", "original");
        let tampered = envelope.replace("original", "tampered");

        let error = parse_skill_invocation_envelopes(&tampered).unwrap_err();

        assert!(error.message().contains("checksum mismatch"));
    }
}
