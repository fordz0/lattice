use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NameRecord {
    pub key: String,
    pub claimed_at: u64,
    pub heartbeat_at: u64,
    pub signature: String,
}

impl NameRecord {
    pub fn new_signed(pubkey_hex: String, name: &str, signing_key: &SigningKey) -> Self {
        let now = now_secs();
        let mut record = Self {
            key: pubkey_hex,
            claimed_at: now,
            heartbeat_at: now,
            signature: String::new(),
        };
        record.signature = record.sign(name, signing_key);
        record
    }

    pub fn is_expired(&self) -> bool {
        now_secs().saturating_sub(self.heartbeat_at) > 30 * 24 * 60 * 60
    }

    pub fn sign(&self, name: &str, signing_key: &SigningKey) -> String {
        let payload = self.signing_payload(name);
        let sig: Signature = signing_key.sign(payload.as_bytes());
        hex::encode(sig.to_bytes())
    }

    pub fn verify(&self, name: &str) -> bool {
        let Ok(key_bytes) = hex::decode(&self.key) else {
            return false;
        };
        let Ok(key_array) = <[u8; 32]>::try_from(key_bytes.as_slice()) else {
            return false;
        };
        let Ok(verifying_key) = VerifyingKey::from_bytes(&key_array) else {
            return false;
        };
        let Ok(sig_bytes) = hex::decode(&self.signature) else {
            return false;
        };
        let Ok(sig_array) = <[u8; 64]>::try_from(sig_bytes.as_slice()) else {
            return false;
        };
        let sig = Signature::from_bytes(&sig_array);
        let payload = self.signing_payload(name);
        verifying_key.verify(payload.as_bytes(), &sig).is_ok()
    }

    fn signing_payload(&self, name: &str) -> String {
        format!(
            r#"{{"claimed_at":{},"heartbeat_at":{},"key":"{}","name":"{}"}}"#,
            self.claimed_at, self.heartbeat_at, self.key, name
        )
    }

    pub fn refresh_signed(&mut self, name: &str, signing_key: &SigningKey) {
        self.heartbeat_at = now_secs();
        self.signature = self.sign(name, signing_key);
    }
}

fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}
