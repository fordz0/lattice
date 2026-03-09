use lru::LruCache;
use serde::{Deserialize, Serialize};
use std::num::NonZeroUsize;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum CachePolicy {
    // Ephemeral blocks are session-only; only Pinned blocks are persisted to disk.
    Ephemeral,
    Pinned,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockCacheMeta {
    pub site_name: String,
    pub cache_policy: CachePolicy,
    pub created_at: u64,
    pub cached_at: u64,
    pub last_accessed_at: u64,
}

pub struct SessionBlockCache {
    cache: LruCache<String, Vec<u8>>,
    current_bytes: usize,
    max_bytes: usize,
}

impl SessionBlockCache {
    pub fn new(max_bytes: usize) -> Self {
        let capacity = NonZeroUsize::new(max_bytes.max(1)).expect("non-zero capacity");
        Self {
            cache: LruCache::new(capacity),
            current_bytes: 0,
            max_bytes,
        }
    }

    pub fn insert(&mut self, hash: String, data: Vec<u8>) {
        if let Some(existing) = self.cache.pop(&hash) {
            self.current_bytes = self.current_bytes.saturating_sub(existing.len());
        }

        let data_len = data.len();
        if data_len > self.max_bytes {
            return;
        }

        while self.current_bytes.saturating_add(data_len) > self.max_bytes {
            let Some((_old_hash, old_data)) = self.cache.pop_lru() else {
                break;
            };
            self.current_bytes = self.current_bytes.saturating_sub(old_data.len());
        }

        self.current_bytes = self.current_bytes.saturating_add(data_len);
        self.cache.put(hash, data);
    }

    pub fn get(&mut self, hash: &str) -> Option<&Vec<u8>> {
        self.cache.get(hash)
    }

    pub fn len(&self) -> usize {
        self.cache.len()
    }

    pub fn bytes(&self) -> usize {
        self.current_bytes
    }
}
