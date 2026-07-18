use std::error::Error;
use std::fmt;

use crate::lane::FullLaneKeyV1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionKeyNormalization {
    AlreadyCanonical,
    AccountBindingInserted,
    LegacyDuplicateAccountCollapsed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SessionKeyError {
    EmptyRoot,
    InvalidContinuation,
    MissingExpectedAccount,
    MismatchedAccount,
    AmbiguousDuplicateAccount,
}

impl fmt::Display for SessionKeyError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::EmptyRoot => "channel session key has an empty root",
            Self::InvalidContinuation => "channel session key has an invalid continuation suffix",
            Self::MissingExpectedAccount => {
                "channel session key is missing its exact-lane account binding"
            }
            Self::MismatchedAccount => {
                "channel session key contains a different exact-lane account binding"
            }
            Self::AmbiguousDuplicateAccount => {
                "channel session key contains ambiguous duplicate account bindings"
            }
        })
    }
}

impl Error for SessionKeyError {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanonicalChannelSessionKey {
    root: String,
    account_binding: String,
    continuation_index: u64,
    normalization: SessionKeyNormalization,
}

impl CanonicalChannelSessionKey {
    #[allow(clippy::too_many_arguments)]
    pub fn parse_for_lane(
        raw_session_key: &str,
        platform: &str,
        account_id: &str,
        channel_id: &str,
        user_id: &str,
        agent_id: &str,
    ) -> Result<Self, SessionKeyError> {
        let expected =
            expected_account_binding(platform, account_id, channel_id, user_id, agent_id);
        parse_bound(raw_session_key, &expected)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn bind_for_lane(
        raw_session_key: &str,
        platform: &str,
        account_id: &str,
        channel_id: &str,
        user_id: &str,
        agent_id: &str,
    ) -> Result<Self, SessionKeyError> {
        let expected =
            expected_account_binding(platform, account_id, channel_id, user_id, agent_id);
        match parse_bound(raw_session_key, &expected) {
            Ok(parsed) => Ok(parsed),
            Err(SessionKeyError::MissingExpectedAccount) => {
                let (root, continuation_index) = parse_structural(raw_session_key)?;
                if contains_account_binding(&root) {
                    return Err(SessionKeyError::MismatchedAccount);
                }
                Ok(Self {
                    root,
                    account_binding: expected,
                    continuation_index,
                    normalization: SessionKeyNormalization::AccountBindingInserted,
                })
            }
            Err(error) => Err(error),
        }
    }

    pub fn root_key(&self) -> &str {
        &self.root
    }

    pub fn continuation_index(&self) -> u64 {
        self.continuation_index
    }

    pub fn normalization(&self) -> SessionKeyNormalization {
        self.normalization
    }

    pub fn canonical_string(&self) -> String {
        let bound_root = format!("{}:{}", self.root, self.account_binding);
        if self.continuation_index == 0 {
            bound_root
        } else {
            format!("{bound_root}:cont-{}", self.continuation_index)
        }
    }

    pub fn continuation(&self, continuation_index: u64) -> Result<Self, SessionKeyError> {
        if continuation_index == 0 {
            return Err(SessionKeyError::InvalidContinuation);
        }
        Ok(Self {
            root: self.root.clone(),
            account_binding: self.account_binding.clone(),
            continuation_index,
            normalization: SessionKeyNormalization::AlreadyCanonical,
        })
    }
}

#[allow(clippy::too_many_arguments)]
pub fn expected_account_binding(
    platform: &str,
    account_id: &str,
    channel_id: &str,
    user_id: &str,
    agent_id: &str,
) -> String {
    let fallback = format!("acct-{}", normalize_key_part(account_id));
    let Ok(lane) = FullLaneKeyV1::new(
        platform,
        account_id,
        channel_id,
        user_id,
        agent_id,
        "interactive",
        "channel-session",
        "channel-session",
    ) else {
        return fallback;
    };
    lane.identity_hash()
        .map(|identity_hash| format!("acct-{identity_hash}"))
        .unwrap_or(fallback)
}

pub fn structural_root_and_continuation(
    raw_session_key: &str,
) -> Result<(String, u64), SessionKeyError> {
    let raw = validate_raw(raw_session_key)?;

    if let Some((without_tail, tail_account)) = raw.rsplit_once(":acct-") {
        let tail_binding = format!("acct-{tail_account}");
        if let Ok((prefix, continuation_index)) = parse_terminal_continuation(without_tail)
            && continuation_index > 0
            && prefix.ends_with(&format!(":{tail_binding}"))
        {
            let root = prefix
                .strip_suffix(&format!(":{tail_binding}"))
                .ok_or(SessionKeyError::AmbiguousDuplicateAccount)?;
            if root.is_empty() || contains_account_binding(root) {
                return Err(SessionKeyError::AmbiguousDuplicateAccount);
            }
            return Ok((format!("{root}:{tail_binding}"), continuation_index));
        }
    }

    parse_terminal_continuation(raw)
}

fn parse_bound(
    raw_session_key: &str,
    expected: &str,
) -> Result<CanonicalChannelSessionKey, SessionKeyError> {
    let raw = validate_raw(raw_session_key)?;
    let expected_segment = format!(":{expected}");

    if let Some(without_duplicate) = raw.strip_suffix(&expected_segment)
        && let Ok((bound_root, continuation_index)) = parse_terminal_continuation(without_duplicate)
        && continuation_index > 0
        && bound_root.ends_with(&expected_segment)
    {
        let root = bound_root
            .strip_suffix(&expected_segment)
            .ok_or(SessionKeyError::AmbiguousDuplicateAccount)?;
        validate_root(root)?;
        if contains_account_binding(root) {
            return Err(SessionKeyError::AmbiguousDuplicateAccount);
        }
        return Ok(CanonicalChannelSessionKey {
            root: root.to_string(),
            account_binding: expected.to_string(),
            continuation_index,
            normalization: SessionKeyNormalization::LegacyDuplicateAccountCollapsed,
        });
    }

    if let Some((prefix, tail_account)) = raw.rsplit_once(":acct-")
        && prefix.contains(":cont-")
        && format!("acct-{tail_account}") != expected
    {
        return Err(SessionKeyError::MismatchedAccount);
    }

    let (bound_root, continuation_index) = parse_terminal_continuation(raw)?;
    if !bound_root.ends_with(&expected_segment) {
        return if contains_account_binding(&bound_root) {
            Err(SessionKeyError::MismatchedAccount)
        } else {
            Err(SessionKeyError::MissingExpectedAccount)
        };
    }
    let root = bound_root
        .strip_suffix(&expected_segment)
        .ok_or(SessionKeyError::MissingExpectedAccount)?;
    validate_root(root)?;
    if contains_account_binding(root) {
        return Err(SessionKeyError::AmbiguousDuplicateAccount);
    }
    Ok(CanonicalChannelSessionKey {
        root: root.to_string(),
        account_binding: expected.to_string(),
        continuation_index,
        normalization: SessionKeyNormalization::AlreadyCanonical,
    })
}

fn parse_structural(raw_session_key: &str) -> Result<(String, u64), SessionKeyError> {
    let raw = validate_raw(raw_session_key)?;
    parse_terminal_continuation(raw)
}

fn parse_terminal_continuation(value: &str) -> Result<(String, u64), SessionKeyError> {
    let Some((root, suffix)) = value.rsplit_once(":cont-") else {
        return Ok((value.to_string(), 0));
    };
    if root.is_empty() || suffix.is_empty() || !suffix.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(SessionKeyError::InvalidContinuation);
    }
    let continuation_index = suffix
        .parse::<u64>()
        .map_err(|_| SessionKeyError::InvalidContinuation)?;
    if continuation_index == 0 {
        return Err(SessionKeyError::InvalidContinuation);
    }
    Ok((root.to_string(), continuation_index))
}

fn validate_raw(value: &str) -> Result<&str, SessionKeyError> {
    if value.is_empty() || value.trim() != value || value.chars().any(char::is_control) {
        return Err(SessionKeyError::EmptyRoot);
    }
    Ok(value)
}

fn validate_root(root: &str) -> Result<(), SessionKeyError> {
    if root.is_empty() {
        Err(SessionKeyError::EmptyRoot)
    } else {
        Ok(())
    }
}

fn contains_account_binding(value: &str) -> bool {
    value.split(':').any(|part| part.starts_with("acct-"))
}

fn normalize_key_part(value: &str) -> String {
    let mut normalized = String::new();
    for ch in value.chars() {
        if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.') {
            normalized.push(ch.to_ascii_lowercase());
        } else {
            normalized.push('_');
        }
    }
    if normalized.is_empty() {
        "unknown".to_string()
    } else {
        normalized
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bind(value: &str) -> Result<CanonicalChannelSessionKey, SessionKeyError> {
        CanonicalChannelSessionKey::bind_for_lane(
            value,
            "telegram",
            "account-a",
            "channel-a",
            "user-a",
            "main",
        )
    }

    #[test]
    fn legacy_duplicate_account_after_continuation_collapses_only_for_exact_lane() {
        let canonical = bind("synthetic-root").unwrap();
        let continuation = canonical.continuation(1).unwrap().canonical_string();
        let duplicate = format!("{continuation}:{}", canonical.account_binding);
        let parsed = bind(&duplicate).unwrap();

        assert_eq!(parsed.canonical_string(), continuation);
        assert_eq!(
            parsed.normalization(),
            SessionKeyNormalization::LegacyDuplicateAccountCollapsed
        );
    }

    #[test]
    fn mismatched_or_ambiguous_account_binding_fails_closed() {
        assert_eq!(
            bind("synthetic-root:acct-other:cont-1"),
            Err(SessionKeyError::MismatchedAccount)
        );
        assert_eq!(
            bind("synthetic-root:acct-other:cont-1:acct-other"),
            Err(SessionKeyError::MismatchedAccount)
        );
    }

    #[test]
    fn canonical_algebra_is_stable() {
        let root = bind("synthetic-root").unwrap();
        let continuation = root.continuation(7).unwrap();
        let reparsed = bind(&continuation.canonical_string()).unwrap();
        assert_eq!(reparsed.canonical_string(), continuation.canonical_string());
        assert_eq!(reparsed.root_key(), "synthetic-root");
        assert_eq!(reparsed.continuation_index(), 7);
    }

    #[test]
    fn exact_identity_isolation_matrix_covers_telegram_discord_and_all_axes() {
        let cases = [
            (
                "telegram",
                "account-a",
                "channel-a",
                "user-a",
                "main",
                "root-a",
                1,
            ),
            (
                "discord",
                "account-a",
                "channel-a",
                "user-a",
                "main",
                "root-a",
                1,
            ),
            (
                "telegram",
                "account-b",
                "channel-a",
                "user-a",
                "main",
                "root-a",
                1,
            ),
            (
                "telegram",
                "account-a",
                "channel-b",
                "user-a",
                "main",
                "root-a",
                1,
            ),
            (
                "telegram",
                "account-a",
                "channel-a",
                "user-b",
                "main",
                "root-a",
                1,
            ),
            (
                "telegram",
                "account-a",
                "channel-a",
                "user-a",
                "other",
                "root-a",
                1,
            ),
            (
                "telegram",
                "account-a",
                "channel-a",
                "user-a",
                "main",
                "root-b",
                1,
            ),
            (
                "telegram",
                "account-a",
                "channel-a",
                "user-a",
                "main",
                "root-a",
                2,
            ),
        ];
        let canonical = cases
            .into_iter()
            .map(
                |(platform, account, channel, user, agent, root, continuation)| {
                    CanonicalChannelSessionKey::bind_for_lane(
                        root, platform, account, channel, user, agent,
                    )
                    .unwrap()
                    .continuation(continuation)
                    .unwrap()
                    .canonical_string()
                },
            )
            .collect::<std::collections::BTreeSet<_>>();

        assert_eq!(canonical.len(), cases.len());
    }
}
