use semver::Version;
use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppRegistryRecord {
    pub app_id: String,
    pub version: String,
    pub description: String,
    pub linux_x86_64_url: Option<String>,
    pub linux_x86_64_sha256: Option<String>,
    pub linux_aarch64_url: Option<String>,
    pub linux_aarch64_sha256: Option<String>,
    pub macos_aarch64_url: Option<String>,
    pub macos_aarch64_sha256: Option<String>,
    pub macos_x86_64_url: Option<String>,
    pub macos_x86_64_sha256: Option<String>,
    pub published_at: u64,
}

pub fn validate_app_registry_record(record: &AppRegistryRecord) -> Result<(), String> {
    validate_app_id(&record.app_id)?;
    Version::parse(&record.version).map_err(|err| format!("invalid semver version: {err}"))?;

    validate_https_url(record.linux_x86_64_url.as_deref())?;
    validate_https_url(record.linux_aarch64_url.as_deref())?;
    validate_https_url(record.macos_aarch64_url.as_deref())?;
    validate_https_url(record.macos_x86_64_url.as_deref())?;

    validate_sha256(record.linux_x86_64_sha256.as_deref())?;
    validate_sha256(record.linux_aarch64_sha256.as_deref())?;
    validate_sha256(record.macos_aarch64_sha256.as_deref())?;
    validate_sha256(record.macos_x86_64_sha256.as_deref())?;

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0);
    if record.published_at > now.saturating_add(600) {
        return Err("published_at must not be more than 10 minutes in the future".to_string());
    }

    Ok(())
}

fn validate_app_id(app_id: &str) -> Result<(), String> {
    if app_id.is_empty() || app_id.len() > 32 {
        return Err("app_id must be between 1 and 32 characters".to_string());
    }
    if !app_id
        .chars()
        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
    {
        return Err("app_id may only contain lowercase letters, digits, and hyphens".to_string());
    }
    if app_id.starts_with('-') || app_id.ends_with('-') {
        return Err("app_id cannot start or end with a hyphen".to_string());
    }
    Ok(())
}

fn validate_https_url(url: Option<&str>) -> Result<(), String> {
    if let Some(url) = url {
        if !url.starts_with("https://") {
            return Err("app registry URLs must start with https://".to_string());
        }
    }
    Ok(())
}

fn validate_sha256(hash: Option<&str>) -> Result<(), String> {
    if let Some(hash) = hash {
        if hash.len() != 64 || !hash.chars().all(|c| c.is_ascii_hexdigit()) {
            return Err("SHA256 hashes must be exactly 64 hex characters".to_string());
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{validate_app_registry_record, AppRegistryRecord};

    fn valid_record() -> AppRegistryRecord {
        AppRegistryRecord {
            app_id: "fray".to_string(),
            version: "1.2.3".to_string(),
            description: "Fray app".to_string(),
            linux_x86_64_url: Some("https://example.com/fray-linux-x86_64".to_string()),
            linux_x86_64_sha256: Some("a".repeat(64)),
            linux_aarch64_url: None,
            linux_aarch64_sha256: None,
            macos_aarch64_url: None,
            macos_aarch64_sha256: None,
            macos_x86_64_url: None,
            macos_x86_64_sha256: None,
            published_at: 0,
        }
    }

    #[test]
    fn rejects_non_https_urls() {
        let mut record = valid_record();
        record.linux_x86_64_url = Some("http://example.com/fray".to_string());
        assert!(validate_app_registry_record(&record).is_err());
    }

    #[test]
    fn rejects_invalid_semver() {
        let mut record = valid_record();
        record.version = "nope".to_string();
        assert!(validate_app_registry_record(&record).is_err());
    }

    #[test]
    fn rejects_bad_sha256_length() {
        let mut record = valid_record();
        record.linux_x86_64_sha256 = Some("abcd".to_string());
        assert!(validate_app_registry_record(&record).is_err());
    }

    #[test]
    fn rejects_future_published_at() {
        let mut record = valid_record();
        record.published_at = u64::MAX;
        assert!(validate_app_registry_record(&record).is_err());
    }

    #[test]
    fn accepts_valid_record() {
        let record = valid_record();
        assert!(validate_app_registry_record(&record).is_ok());
    }
}
