use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};

const MAX_HANDLE_LEN: usize = 32;
const MAX_DISPLAY_NAME_LEN: usize = 64;
const MAX_BIO_LEN: usize = 200;
const MAX_FUTURE_SKEW_SECS: u64 = 600;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FrayHandleRecord {
    pub handle: String,
    pub display_name: Option<String>,
    pub bio: Option<String>,
    pub claimed_at: u64,
    pub previous_handle: Option<String>,
}

pub fn validate_handle_record(record: &FrayHandleRecord) -> Result<(), String> {
    validate_handle(&record.handle)?;
    validate_optional_len(
        &record.display_name,
        1,
        MAX_DISPLAY_NAME_LEN,
        "display_name",
    )?;
    validate_optional_len(&record.bio, 1, MAX_BIO_LEN, "bio")?;
    let now = now_secs();
    if record.claimed_at > now.saturating_add(MAX_FUTURE_SKEW_SECS) {
        return Err("claimed_at is too far in the future".to_string());
    }
    if let Some(previous_handle) = &record.previous_handle {
        validate_handle(previous_handle)?;
    }
    Ok(())
}

pub fn validate_handle(handle: &str) -> Result<(), String> {
    if handle.is_empty() || handle.len() > MAX_HANDLE_LEN {
        return Err("handle must be between 1 and 32 characters".to_string());
    }
    if !handle
        .chars()
        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-' || c == '_')
    {
        return Err(
            "handle may only contain lowercase letters, digits, hyphens, and underscores"
                .to_string(),
        );
    }
    Ok(())
}

fn validate_optional_len(
    value: &Option<String>,
    min: usize,
    max: usize,
    field: &str,
) -> Result<(), String> {
    let Some(value) = value.as_ref() else {
        return Ok(());
    };
    let trimmed = value.trim();
    if trimmed.len() < min || trimmed.len() > max {
        return Err(format!(
            "{field} must be between {min} and {max} characters"
        ));
    }
    Ok(())
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid_record() -> FrayHandleRecord {
        FrayHandleRecord {
            handle: "alice".to_string(),
            display_name: Some("Alice".to_string()),
            bio: Some("hello".to_string()),
            claimed_at: now_secs(),
            previous_handle: None,
        }
    }

    #[test]
    fn rejects_invalid_handle_characters() {
        let mut record = valid_record();
        record.handle = "Fordz0!".to_string();
        assert!(validate_handle_record(&record).is_err());
    }

    #[test]
    fn rejects_long_handle() {
        let mut record = valid_record();
        record.handle = "a".repeat(33);
        assert!(validate_handle_record(&record).is_err());
    }

    #[test]
    fn rejects_future_claimed_at() {
        let mut record = valid_record();
        record.claimed_at = now_secs() + 601;
        assert!(validate_handle_record(&record).is_err());
    }
}
