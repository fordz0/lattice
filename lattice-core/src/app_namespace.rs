pub const APP_PREFIX: &str = "app:";
pub const APP_REGISTRY_PREFIX: &str = "app:lattice:registry:";
pub const FRAY_FEED_PREFIX: &str = "app:fray:feed:";
pub const FRAY_TRUST_PREFIX: &str = "app:fray:trust:";
pub const FRAY_IDENTITY_PREFIX: &str = "app:fray:identity:";
pub const FRAY_DIRECTORY_KEY: &str = "app:fray:directory";
const MAX_APP_ID_LEN: usize = 32;
const MAX_RECORD_TYPE_LEN: usize = 32;
const MAX_RECORD_ID_LEN: usize = 128;

pub fn validate_app_key(key: &str) -> Result<(), String> {
    let Some(rest) = key.strip_prefix(APP_PREFIX) else {
        return Err("app key must start with app:".to_string());
    };
    let parts: Vec<&str> = rest.split(':').collect();
    if parts.len() != 3 {
        return Err("app key must have format app:{app_id}:{record_type}:{record_id}".to_string());
    }

    let app_id = parts[0];
    let record_type = parts[1];
    let record_id = parts[2];

    validate_app_id(app_id)?;
    validate_record_type(record_type)?;
    validate_record_id(record_id)?;
    Ok(())
}

pub fn validate_fray_dht_key(key: &str) -> Result<(), String> {
    if key == FRAY_DIRECTORY_KEY {
        return Ok(());
    }
    validate_app_key(key)?;
    if key.starts_with(FRAY_FEED_PREFIX)
        || key.starts_with(FRAY_TRUST_PREFIX)
        || key.starts_with(FRAY_IDENTITY_PREFIX)
    {
        return Ok(());
    }
    Err("fray only permits app:fray:feed:{fray_name}, app:fray:trust:{fray_name}, app:fray:identity:{handle}, or app:fray:directory DHT keys".to_string())
}

fn validate_app_id(app_id: &str) -> Result<(), String> {
    if app_id.is_empty() || app_id.len() > MAX_APP_ID_LEN {
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

fn validate_record_type(record_type: &str) -> Result<(), String> {
    if record_type.is_empty() || record_type.len() > MAX_RECORD_TYPE_LEN {
        return Err("record_type must be between 1 and 32 characters".to_string());
    }
    if !record_type
        .chars()
        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit())
    {
        return Err("record_type may only contain lowercase letters and digits".to_string());
    }
    Ok(())
}

fn validate_record_id(record_id: &str) -> Result<(), String> {
    if record_id.is_empty() || record_id.len() > MAX_RECORD_ID_LEN {
        return Err("record_id must be between 1 and 128 characters".to_string());
    }
    if record_id.chars().any(char::is_whitespace) {
        return Err("record_id cannot contain whitespace".to_string());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_valid_app_keys() {
        assert!(validate_app_key("app:fray:feed:lattice").is_ok());
        assert!(validate_app_key("app:my-app:type:record-01").is_ok());
        assert!(validate_fray_dht_key("app:fray:feed:lattice").is_ok());
        assert!(validate_fray_dht_key("app:fray:trust:lattice").is_ok());
        assert!(validate_fray_dht_key("app:fray:identity:fordz0").is_ok());
        assert!(validate_fray_dht_key("app:fray:directory").is_ok());
    }

    #[test]
    fn rejects_invalid_app_keys() {
        assert!(validate_app_key("app:fray").is_err());
        assert!(validate_app_key("app:Fray:feed:lattice").is_err());
        assert!(validate_app_key("app:fray:feed:bad id").is_err());
        assert!(validate_app_key("name:fray").is_err());
        assert!(validate_fray_dht_key("app:fray:post:abc").is_err());
    }
}
