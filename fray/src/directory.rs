use crate::routes::FrayName;
use crate::trust::{decode_public_key_b64, unix_ts};
use anyhow::Result;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use base64::Engine as _;
use ed25519_dalek::{Signer, SigningKey, Verifier};
use lattice_core::identity::canonical_json_bytes;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

const MAX_DIRECTORY_ENTRIES: usize = 10_000;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum FrayStatus {
    Listed,
    Unlisted,
    Banned { reason: Option<String> },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FrayDirectoryEntry {
    pub fray_name: String,
    pub owner_key_b64: String,
    pub status: FrayStatus,
    pub listed_at: u64,
    pub updated_at: u64,
    pub description: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FrayDirectory {
    // Field order is canonical — do not reorder.
    pub version: u64,
    pub operator_key_b64: String,
    pub entries: Vec<FrayDirectoryEntry>,
    pub generated_at: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignedFrayDirectory {
    pub directory: FrayDirectory,
    pub signature_b64: String,
}

pub fn validate_directory(dir: &FrayDirectory) -> Result<(), String> {
    if dir.version == 0 {
        return Err("directory version must be greater than zero".to_string());
    }
    decode_public_key_b64(&dir.operator_key_b64)?;
    if dir.entries.len() > MAX_DIRECTORY_ENTRIES {
        return Err("directory has too many entries".to_string());
    }
    let mut seen = HashSet::new();
    for entry in &dir.entries {
        FrayName::parse(&entry.fray_name).map_err(|err| err.to_string())?;
        decode_public_key_b64(&entry.owner_key_b64)?;
        if !seen.insert(entry.fray_name.clone()) {
            return Err("directory contains duplicate fray names".to_string());
        }
    }
    if dir.generated_at > unix_ts().saturating_add(600) {
        return Err("directory generated_at is too far in the future".to_string());
    }
    Ok(())
}

pub fn sign_directory(directory: &FrayDirectory, signing_key: &SigningKey) -> Result<SignedFrayDirectory> {
    validate_directory(directory).map_err(anyhow::Error::msg)?;
    let payload = canonical_json_bytes(directory)?;
    let signature = signing_key.sign(&payload);
    Ok(SignedFrayDirectory {
        directory: directory.clone(),
        signature_b64: BASE64_STANDARD.encode(signature.to_bytes()),
    })
}

pub fn verify_signed_directory(directory: &SignedFrayDirectory) -> Result<()> {
    validate_directory(&directory.directory).map_err(anyhow::Error::msg)?;
    let payload = canonical_json_bytes(&directory.directory)?;
    let verifying_key = decode_public_key_b64(&directory.directory.operator_key_b64)
        .map_err(anyhow::Error::msg)?;
    let signature_bytes = BASE64_STANDARD
        .decode(&directory.signature_b64)
        .map_err(|err| anyhow::anyhow!("invalid directory signature base64: {err}"))?;
    let signature = ed25519_dalek::Signature::from_slice(&signature_bytes)
        .map_err(|err| anyhow::anyhow!("invalid directory signature bytes: {err}"))?;
    verifying_key
        .verify(&payload, &signature)
        .map_err(|err| anyhow::anyhow!("directory signature verification failed: {err}"))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::trust::unix_ts;

    fn sample_key(seed: u8) -> String {
        BASE64_STANDARD.encode(SigningKey::from_bytes(&[seed; 32]).verifying_key().as_bytes())
    }

    #[test]
    fn rejects_duplicate_fray_names() {
        let dir = FrayDirectory {
            version: 1,
            operator_key_b64: sample_key(1),
            entries: vec![
                FrayDirectoryEntry {
                    fray_name: "lattice".into(),
                    owner_key_b64: sample_key(2),
                    status: FrayStatus::Listed,
                    listed_at: unix_ts(),
                    updated_at: unix_ts(),
                    description: None,
                },
                FrayDirectoryEntry {
                    fray_name: "lattice".into(),
                    owner_key_b64: sample_key(3),
                    status: FrayStatus::Unlisted,
                    listed_at: unix_ts(),
                    updated_at: unix_ts(),
                    description: None,
                },
            ],
            generated_at: unix_ts(),
        };
        assert!(validate_directory(&dir).is_err());
    }
}
