use anyhow::{bail, Context, Result};
use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::path::Path;

pub const DEFAULT_CHUNK_SIZE_BYTES: usize = 256 * 1024;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileEntry {
    pub path: String,
    pub hash: String,
    pub size: u64,
    #[serde(default)]
    pub chunks: Vec<String>,
    #[serde(default)]
    pub chunk_size: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct AppManifest {
    pub proxy_port: u16,
    pub proxy_paths: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SiteManifest {
    pub name: String,
    pub version: u64,
    pub publisher_key: String,
    pub rating: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub app: Option<AppManifest>,
    pub files: Vec<FileEntry>,
    pub signature: String,
}

pub fn hash_file(path: &Path) -> Result<String> {
    let bytes = std::fs::read(path)
        .with_context(|| format!("failed to read file for hashing: {}", path.display()))?;
    Ok(hash_bytes(&bytes))
}

pub fn hash_bytes(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex::encode(hasher.finalize())
}

pub fn sign_manifest(manifest: &mut SiteManifest, keypair: &SigningKey) -> Result<()> {
    manifest.signature.clear();
    let canonical = canonical_manifest_bytes(manifest)?;
    let signature = keypair.sign(&canonical);
    manifest.signature = hex::encode(signature.to_bytes());
    Ok(())
}

pub fn verify_manifest(manifest: &SiteManifest) -> Result<()> {
    validate_manifest(manifest).context("manifest validation failed")?;

    let pubkey_bytes =
        hex::decode(&manifest.publisher_key).context("publisher_key is not valid hex")?;
    if pubkey_bytes.len() != 32 {
        bail!(
            "invalid publisher key length: expected 32 bytes, got {}",
            pubkey_bytes.len()
        );
    }

    let mut pubkey = [0_u8; 32];
    pubkey.copy_from_slice(&pubkey_bytes);
    let verifying_key = VerifyingKey::from_bytes(&pubkey).context("invalid publisher_key bytes")?;

    let signature_bytes = hex::decode(&manifest.signature).context("signature is not valid hex")?;
    let signature = Signature::from_slice(&signature_bytes).context("invalid signature bytes")?;

    let mut unsigned = manifest.clone();
    unsigned.signature.clear();
    let canonical = canonical_manifest_bytes(&unsigned)?;

    verifying_key
        .verify(&canonical, &signature)
        .context("manifest signature verification failed")?;

    Ok(())
}

pub fn validate_manifest(manifest: &SiteManifest) -> Result<()> {
    if let Some(app) = manifest.app.as_ref() {
        validate_app_manifest(app).map_err(|err| anyhow::anyhow!(err))?;
    }
    Ok(())
}

pub fn validate_app_manifest(app: &AppManifest) -> std::result::Result<(), String> {
    if app.proxy_port < 1024 {
        return Err("app proxy_port must be between 1024 and 65535".to_string());
    }
    if app.proxy_paths.is_empty() {
        return Err("app proxy_paths must not be empty".to_string());
    }
    if app.proxy_paths.len() > 8 {
        return Err("app proxy_paths must contain at most 8 prefixes".to_string());
    }
    for prefix in &app.proxy_paths {
        validate_proxy_path_prefix(prefix)?;
    }
    Ok(())
}

pub fn validate_proxy_path_prefix(prefix: &str) -> std::result::Result<(), String> {
    if prefix.is_empty() || prefix.len() > 64 {
        return Err("app proxy path prefix must be between 1 and 64 characters".to_string());
    }
    if !prefix.starts_with('/') {
        return Err("app proxy path prefix must start with '/'".to_string());
    }
    if prefix.contains("..") {
        return Err("app proxy path prefix must not contain '..'".to_string());
    }
    if prefix.as_bytes().contains(&0) {
        return Err("app proxy path prefix must not contain null bytes".to_string());
    }
    if !prefix
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '/' || c == '-')
    {
        return Err(
            "app proxy path prefix may only contain ASCII letters, digits, '/' and '-'".to_string(),
        );
    }
    Ok(())
}

fn canonical_manifest_bytes(manifest: &SiteManifest) -> Result<Vec<u8>> {
    let value = serde_json::to_value(manifest).context("failed to convert manifest to JSON")?;
    let canonical = sort_json(value);
    serde_json::to_vec(&canonical).context("failed to serialize canonical manifest JSON")
}

fn sort_json(value: Value) -> Value {
    match value {
        Value::Object(map) => {
            let mut sorted = BTreeMap::new();
            for (k, v) in map {
                sorted.insert(k, sort_json(v));
            }

            let mut out = Map::new();
            for (k, v) in sorted {
                out.insert(k, v);
            }
            Value::Object(out)
        }
        Value::Array(values) => Value::Array(values.into_iter().map(sort_json).collect()),
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use super::{validate_app_manifest, validate_proxy_path_prefix, AppManifest};

    #[test]
    fn rejects_low_app_proxy_port() {
        let err = validate_app_manifest(&AppManifest {
            proxy_port: 1023,
            proxy_paths: vec!["/api".to_string()],
        })
        .expect_err("low port should fail");
        assert!(err.contains("proxy_port"));
    }

    #[test]
    fn rejects_bad_proxy_paths() {
        let err = validate_proxy_path_prefix("/api/../bad").expect_err("bad path should fail");
        assert!(err.contains(".."));

        let err = validate_proxy_path_prefix("api").expect_err("missing slash should fail");
        assert!(err.contains("start"));
    }
}
