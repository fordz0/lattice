use crate::routes::FrayName;
use anyhow::Result;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use base64::Engine as _;
use ed25519_dalek::{Signer, SigningKey, Verifier, VerifyingKey};
use lattice_core::identity::canonical_json_bytes;
use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};

const MAX_MODERATOR_KEYS: usize = 32;
const MAX_TRUST_ENTRIES: usize = 1_000;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum KeyStanding {
    Trusted,
    Normal,
    Restricted { reason: Option<String> },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KeyRecord {
    pub key_b64: String,
    pub standing: KeyStanding,
    pub label: Option<String>,
    pub updated_at: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FrayTrustRecord {
    // Field order is canonical — do not reorder.
    pub version: u64,
    pub fray: String,
    pub owner_key_b64: String,
    pub moderator_keys: Vec<String>,
    pub entries: Vec<KeyRecord>,
    pub generated_at: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignedTrustRecord {
    pub record: FrayTrustRecord,
    pub signature_b64: String,
}

pub fn validate_trust_record(record: &FrayTrustRecord) -> Result<(), String> {
    if record.version == 0 {
        return Err("trust record version must be greater than zero".to_string());
    }
    FrayName::parse(&record.fray).map_err(|err| err.to_string())?;
    decode_public_key_b64(&record.owner_key_b64)?;
    if record.moderator_keys.len() > MAX_MODERATOR_KEYS {
        return Err("trust record has too many moderator keys".to_string());
    }
    for key in &record.moderator_keys {
        decode_public_key_b64(key)?;
    }
    if record.entries.len() > MAX_TRUST_ENTRIES {
        return Err("trust record has too many entries".to_string());
    }
    for entry in &record.entries {
        decode_public_key_b64(&entry.key_b64)?;
    }
    let now = unix_ts();
    if record.generated_at > now.saturating_add(600) {
        return Err("trust record generated_at is too far in the future".to_string());
    }
    Ok(())
}

pub fn sign_trust_record(
    record: &FrayTrustRecord,
    signing_key: &SigningKey,
) -> Result<SignedTrustRecord> {
    validate_trust_record(record).map_err(anyhow::Error::msg)?;
    let payload = canonical_json_bytes(record)?;
    let signature = signing_key.sign(&payload);
    Ok(SignedTrustRecord {
        record: record.clone(),
        signature_b64: BASE64_STANDARD.encode(signature.to_bytes()),
    })
}

pub fn verify_signed_trust_record(record: &SignedTrustRecord) -> Result<()> {
    validate_trust_record(&record.record).map_err(anyhow::Error::msg)?;
    let payload = canonical_json_bytes(&record.record)?;
    let verifying_key =
        decode_public_key_b64(&record.record.owner_key_b64).map_err(anyhow::Error::msg)?;
    let signature_bytes = BASE64_STANDARD
        .decode(&record.signature_b64)
        .map_err(|err| anyhow::anyhow!("invalid trust record signature base64: {err}"))?;
    let signature = ed25519_dalek::Signature::from_slice(&signature_bytes)
        .map_err(|err| anyhow::anyhow!("invalid trust record signature bytes: {err}"))?;
    verifying_key
        .verify(&payload, &signature)
        .map_err(|err| anyhow::anyhow!("trust record signature verification failed: {err}"))?;
    Ok(())
}

pub fn decode_public_key_b64(value: &str) -> Result<VerifyingKey, String> {
    let bytes = BASE64_STANDARD
        .decode(value)
        .map_err(|err| format!("invalid base64 public key: {err}"))?;
    let bytes: [u8; 32] = bytes
        .try_into()
        .map_err(|_| "public key must be exactly 32 bytes".to_string())?;
    VerifyingKey::from_bytes(&bytes).map_err(|err| format!("invalid ed25519 public key: {err}"))
}

pub fn unix_ts() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_key(seed: u8) -> String {
        BASE64_STANDARD.encode(
            SigningKey::from_bytes(&[seed; 32])
                .verifying_key()
                .as_bytes(),
        )
    }

    #[test]
    fn rejects_future_generated_at() {
        let record = FrayTrustRecord {
            version: 1,
            fray: "lattice".to_string(),
            owner_key_b64: sample_key(7),
            moderator_keys: Vec::new(),
            entries: Vec::new(),
            generated_at: unix_ts() + 601,
        };
        assert!(validate_trust_record(&record).is_err());
    }

    #[test]
    fn rejects_invalid_owner_key() {
        let record = FrayTrustRecord {
            version: 1,
            fray: "lattice".to_string(),
            owner_key_b64: "bad".to_string(),
            moderator_keys: Vec::new(),
            entries: Vec::new(),
            generated_at: unix_ts(),
        };
        assert!(validate_trust_record(&record).is_err());
    }
}
