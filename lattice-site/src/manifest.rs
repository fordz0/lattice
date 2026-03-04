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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SiteManifest {
    pub name: String,
    pub version: u64,
    pub publisher_key: String,
    pub rating: String,
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
