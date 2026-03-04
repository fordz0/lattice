use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NameRecord {
    pub key: String,
    pub claimed_at: u64,
    pub heartbeat_at: u64,
}

impl NameRecord {
    pub fn new(pubkey_hex: String) -> Self {
        let now = now_secs();
        Self {
            key: pubkey_hex,
            claimed_at: now,
            heartbeat_at: now,
        }
    }

    pub fn is_expired(&self) -> bool {
        now_secs().saturating_sub(self.heartbeat_at) > 30 * 24 * 60 * 60
    }

    pub fn refresh(&mut self) {
        self.heartbeat_at = now_secs();
    }
}

fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}
