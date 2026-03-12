use aes_gcm::aead::{Aead, KeyInit};
use aes_gcm::{Aes256Gcm, Nonce};
use anyhow::{bail, Context, Result};
use lattice_core::moderation::ModerationRule;
use crate::cache::{BlockCacheMeta, CachePolicy};
use crate::rpc::{
    KnownPublisher, KnownPublisherStatus, QuarantineEntryResponse, TrustedPublisher,
};
use rand::rngs::OsRng;
use rand::RngCore;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::Path;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecordMeta {
    pub created_at: u64,
    pub updated_at: u64,
}

#[derive(Debug, Default)]
pub struct LocalRecordGcStats {
    pub removed_records: usize,
    pub removed_bytes: usize,
}

#[derive(Debug, Default)]
pub struct BlockCacheGcStats {
    pub removed_blocks: usize,
    pub removed_bytes: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuarantineEntry {
    pub id: String,
    pub created_at: u64,
    pub matched_rule_id: String,
    pub matched_kind: String,
    pub matched_value: String,
    pub record_key: Option<String>,
    pub publisher: Option<String>,
    pub content_hash: Option<String>,
    pub site_name: Option<String>,
}

pub struct LocalRecordStore {
    db: sled::Db,
    records: sled::Tree,
    meta: sled::Tree,
    blocks: sled::Tree,
    block_meta: sled::Tree,
    mod_rules: sled::Tree,
    mod_quarantine: sled::Tree,
    trusted_publishers: sled::Tree,
    known_publishers: sled::Tree,
    owned_app_records: sled::Tree,
    claim_rate_limits: sled::Tree,
    block_cipher: Aes256Gcm,
}

pub fn unix_ts() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|dur| dur.as_secs())
        .unwrap_or(0)
}

pub fn key_should_be_pinned(key: &str) -> bool {
    key.starts_with("name:") || key.starts_with("site:")
}

impl LocalRecordStore {
    pub fn open(path: &Path, block_cache_key: [u8; 32]) -> Result<Self> {
        fs::create_dir_all(path)
            .with_context(|| format!("failed to create local records db dir {}", path.display()))?;
        let db = sled::open(path)
            .with_context(|| format!("failed to open local records db at {}", path.display()))?;
        let records = db
            .open_tree("records")
            .context("failed to open records tree")?;
        let meta = db.open_tree("meta").context("failed to open meta tree")?;
        let blocks = db
            .open_tree("blocks")
            .context("failed to open blocks tree")?;
        let block_meta = db
            .open_tree("block_meta")
            .context("failed to open block_meta tree")?;
        let mod_rules = db
            .open_tree("mod_rules")
            .context("failed to open mod_rules tree")?;
        let mod_quarantine = db
            .open_tree("mod_quarantine")
            .context("failed to open mod_quarantine tree")?;
        let trusted_publishers = db
            .open_tree("trusted_publishers")
            .context("failed to open trusted_publishers tree")?;
        let known_publishers = db
            .open_tree("known_publishers")
            .context("failed to open known_publishers tree")?;
        let owned_app_records = db
            .open_tree("owned_app_records")
            .context("failed to open owned_app_records tree")?;
        let claim_rate_limits = db
            .open_tree("claim_rate_limits")
            .context("failed to open claim_rate_limits tree")?;
        Ok(Self {
            db,
            records,
            meta,
            blocks,
            block_meta,
            mod_rules,
            mod_quarantine,
            trusted_publishers,
            known_publishers,
            owned_app_records,
            claim_rate_limits,
            block_cipher: Aes256Gcm::new_from_slice(&block_cache_key)
                .context("failed to initialize block cache cipher")?,
        })
    }

    pub fn load_records(&self) -> Result<HashMap<String, Vec<u8>>> {
        let mut out = HashMap::new();
        for item in self.records.iter() {
            let (key, value) = item.context("failed to iterate local records")?;
            let key = match std::str::from_utf8(&key) {
                Ok(key) => key.to_string(),
                Err(_) => {
                    tracing::warn!("local records db contained non-utf8 key; skipping");
                    continue;
                }
            };
            out.insert(key, value.to_vec());
        }
        Ok(out)
    }

    pub fn put_record(&self, key: &str, value: &[u8], _pinned: bool) -> Result<()> {
        self.records
            .insert(key.as_bytes(), value)
            .context("failed to persist local record value")?;
        let now = unix_ts();
        let created_at = self
            .meta
            .get(key.as_bytes())
            .context("failed to read local record metadata")?
            .and_then(|raw| serde_json::from_slice::<RecordMeta>(&raw).ok())
            .map(|meta| meta.created_at)
            .unwrap_or(now);

        let meta = RecordMeta {
            created_at,
            updated_at: now,
        };
        let meta_bytes = serde_json::to_vec(&meta).context("failed to encode local record meta")?;
        self.meta
            .insert(key.as_bytes(), meta_bytes)
            .context("failed to persist local record metadata")?;
        self.db.flush().context("failed to flush local records db")?;
        Ok(())
    }

    pub fn put_block(
        &self,
        hash: &str,
        value: &[u8],
        site_name: &str,
        cache_policy: CachePolicy,
    ) -> Result<()> {
        if cache_policy != CachePolicy::Pinned {
            bail!("ephemeral blocks are session-only and must not be persisted to sled");
        }
        let encrypted = self.encrypt_block(value)?;
        self.blocks
            .insert(hash.as_bytes(), encrypted)
            .context("failed to persist block bytes")?;
        let now = unix_ts();
        let existing = self
            .block_meta
            .get(hash.as_bytes())
            .context("failed to read block metadata")?
            .and_then(|raw| serde_json::from_slice::<BlockCacheMeta>(&raw).ok());
        let meta = BlockCacheMeta {
            site_name: site_name.to_string(),
            cache_policy,
            created_at: existing.as_ref().map(|m| m.created_at).unwrap_or(now),
            cached_at: existing.as_ref().map(|m| m.cached_at).unwrap_or(now),
            last_accessed_at: now,
        };
        let meta_bytes = serde_json::to_vec(&meta).context("failed to encode block metadata")?;
        self.block_meta
            .insert(hash.as_bytes(), meta_bytes)
            .context("failed to persist block metadata")?;
        self.db.flush().context("failed to flush block cache write")?;
        Ok(())
    }

    pub fn get_block(&self, hash: &str) -> Result<Option<Vec<u8>>> {
        self.blocks
            .get(hash.as_bytes())
            .context("failed to read block bytes")?
            .map(|value| self.decrypt_block(&value))
            .transpose()
    }

    pub fn raw_block_bytes(&self, hash: &str) -> Result<Option<Vec<u8>>> {
        self.blocks
            .get(hash.as_bytes())
            .context("failed to read raw block bytes")?
            .map(|value| Ok(value.to_vec()))
            .transpose()
    }

    pub fn touch_block(&self, hash: &str) -> Result<()> {
        let Some(raw) = self
            .block_meta
            .get(hash.as_bytes())
            .context("failed to read block metadata")?
        else {
            return Ok(());
        };
        let mut meta: BlockCacheMeta =
            serde_json::from_slice(&raw).context("failed to decode block metadata")?;
        meta.last_accessed_at = unix_ts();
        let encoded = serde_json::to_vec(&meta).context("failed to encode block metadata")?;
        self.block_meta
            .insert(hash.as_bytes(), encoded)
            .context("failed to persist block touch")?;
        self.db.flush().context("failed to flush block touch")?;
        Ok(())
    }

    pub fn list_site_block_hashes(&self, site_name: &str) -> Result<Vec<String>> {
        let mut hashes = Vec::new();
        for item in self.block_meta.iter() {
            let (key, value) = item.context("failed to iterate block metadata")?;
            let meta: BlockCacheMeta =
                serde_json::from_slice(&value).context("failed to decode block metadata")?;
            if meta.site_name == site_name {
                let hash = std::str::from_utf8(&key)
                    .context("block metadata key was not utf-8")?
                    .to_string();
                hashes.push(hash);
            }
        }
        Ok(hashes)
    }

    pub fn set_site_cache_policy(&self, site_name: &str, policy: CachePolicy) -> Result<usize> {
        if policy == CachePolicy::Ephemeral {
            let hashes = self.list_site_block_hashes(site_name)?;
            for hash in &hashes {
                self.blocks
                    .remove(hash.as_bytes())
                    .context("failed to remove block bytes while unpinning site")?;
                self.block_meta
                    .remove(hash.as_bytes())
                    .context("failed to remove block metadata while unpinning site")?;
            }
            if !hashes.is_empty() {
                self.db
                    .flush()
                    .context("failed to flush site unpin block removals")?;
            }
            return Ok(hashes.len());
        }

        let mut updated = 0usize;
        for hash in self.list_site_block_hashes(site_name)? {
            let Some(raw) = self
                .block_meta
                .get(hash.as_bytes())
                .context("failed to read block metadata")?
            else {
                continue;
            };
            let mut meta: BlockCacheMeta =
                serde_json::from_slice(&raw).context("failed to decode block metadata")?;
            meta.cache_policy = policy.clone();
            meta.last_accessed_at = unix_ts();
            let encoded = serde_json::to_vec(&meta).context("failed to encode block metadata")?;
            self.block_meta
                .insert(hash.as_bytes(), encoded)
                .context("failed to persist updated block metadata")?;
            updated = updated.saturating_add(1);
        }
        if updated > 0 {
            self.db.flush().context("failed to flush block cache policy update")?;
        }
        Ok(updated)
    }

    pub fn list_pinned_sites(&self) -> Result<Vec<String>> {
        let mut sites = HashSet::new();
        for item in self.block_meta.iter() {
            let (_key, value) = item.context("failed to iterate block metadata")?;
            let meta: BlockCacheMeta =
                serde_json::from_slice(&value).context("failed to decode block metadata")?;
            if meta.cache_policy == CachePolicy::Pinned {
                sites.insert(meta.site_name);
            }
        }
        let mut out = sites.into_iter().collect::<Vec<_>>();
        out.sort();
        Ok(out)
    }

    pub fn is_site_pinned(&self, site_name: &str) -> Result<bool> {
        for item in self.block_meta.iter() {
            let (_key, value) = item.context("failed to iterate block metadata")?;
            let meta: BlockCacheMeta =
                serde_json::from_slice(&value).context("failed to decode block metadata")?;
            if meta.site_name == site_name && meta.cache_policy == CachePolicy::Pinned {
                return Ok(true);
            }
        }
        Ok(false)
    }

    pub fn remove_record(&self, key: &str) -> Result<()> {
        self.records
            .remove(key.as_bytes())
            .context("failed to remove record value")?;
        self.meta
            .remove(key.as_bytes())
            .context("failed to remove record metadata")?;
        self.db.flush().context("failed to flush record removal")?;
        Ok(())
    }

    pub fn remove_block(&self, hash: &str) -> Result<usize> {
        let size = self
            .blocks
            .remove(hash.as_bytes())
            .context("failed to remove block bytes")?
            .map(|value| value.len())
            .unwrap_or(0);
        self.block_meta
            .remove(hash.as_bytes())
            .context("failed to remove block metadata")?;
        self.db.flush().context("failed to flush block removal")?;
        Ok(size)
    }

    pub fn load_moderation_rules(&self) -> Result<Vec<ModerationRule>> {
        let mut rules = Vec::new();
        for item in self.mod_rules.iter() {
            let (_key, value) = item.context("failed to iterate moderation rules")?;
            let rule: ModerationRule =
                serde_json::from_slice(&value).context("failed to decode moderation rule")?;
            rules.push(rule);
        }
        rules.sort_by_key(|rule| rule.created_at);
        Ok(rules)
    }

    pub fn insert_moderation_rule(&self, rule: &ModerationRule) -> Result<()> {
        let encoded = serde_json::to_vec(rule).context("failed to encode moderation rule")?;
        self.mod_rules
            .insert(rule.id.as_bytes(), encoded)
            .context("failed to persist moderation rule")?;
        self.db.flush().context("failed to flush moderation rule write")?;
        Ok(())
    }

    pub fn remove_moderation_rule(&self, id: &str) -> Result<bool> {
        let removed = self
            .mod_rules
            .remove(id.as_bytes())
            .context("failed to remove moderation rule")?
            .is_some();
        if removed {
            self.db.flush().context("failed to flush moderation rule removal")?;
        }
        Ok(removed)
    }

    pub fn insert_quarantine_entry(&self, entry: &QuarantineEntry) -> Result<()> {
        let encoded = serde_json::to_vec(entry).context("failed to encode quarantine entry")?;
        self.mod_quarantine
            .insert(entry.id.as_bytes(), encoded)
            .context("failed to persist quarantine entry")?;
        self.db.flush().context("failed to flush quarantine write")?;
        Ok(())
    }

    pub fn list_quarantine_entries(&self) -> Result<Vec<QuarantineEntryResponse>> {
        let mut entries = Vec::new();
        for item in self.mod_quarantine.iter() {
            let (_key, value) = item.context("failed to iterate quarantine entries")?;
            let entry: QuarantineEntry =
                serde_json::from_slice(&value).context("failed to decode quarantine entry")?;
            entries.push(QuarantineEntryResponse {
                id: entry.id,
                created_at: entry.created_at,
                matched_rule_id: entry.matched_rule_id,
                matched_kind: entry.matched_kind,
                matched_value: entry.matched_value,
                record_key: entry.record_key,
                publisher: entry.publisher,
                content_hash: entry.content_hash,
                site_name: entry.site_name,
            });
        }
        entries.sort_by_key(|entry| entry.created_at);
        Ok(entries)
    }

    pub fn add_trusted_publisher(
        &self,
        publisher_b64: String,
        label: String,
        note: Option<String>,
    ) -> Result<()> {
        let trusted = TrustedPublisher {
            publisher_b64: publisher_b64.clone(),
            label,
            added_at: unix_ts(),
            note,
        };
        let encoded = serde_json::to_vec(&trusted).context("failed to encode trusted publisher")?;
        self.trusted_publishers
            .insert(publisher_b64.as_bytes(), encoded)
            .context("failed to persist trusted publisher")?;
        self.db
            .flush()
            .context("failed to flush trusted publisher write")?;
        Ok(())
    }

    pub fn remove_trusted_publisher(&self, publisher_b64: &str) -> Result<bool> {
        let removed = self
            .trusted_publishers
            .remove(publisher_b64.as_bytes())
            .context("failed to remove trusted publisher")?
            .is_some();
        if removed {
            self.db
                .flush()
                .context("failed to flush trusted publisher removal")?;
        }
        Ok(removed)
    }

    pub fn list_trusted_publishers(&self) -> Result<Vec<TrustedPublisher>> {
        let mut trusted = Vec::new();
        for item in self.trusted_publishers.iter() {
            let (_key, value) = item.context("failed to iterate trusted publishers")?;
            let publisher: TrustedPublisher =
                serde_json::from_slice(&value).context("failed to decode trusted publisher")?;
            trusted.push(publisher);
        }
        trusted.sort_by_key(|publisher| publisher.added_at);
        Ok(trusted)
    }

    pub fn get_trusted_publisher(&self, publisher_b64: &str) -> Result<Option<TrustedPublisher>> {
        self.trusted_publishers
            .get(publisher_b64.as_bytes())
            .context("failed to read trusted publisher")?
            .map(|value| {
                serde_json::from_slice(&value).context("failed to decode trusted publisher")
            })
            .transpose()
    }

    pub fn is_trusted_publisher(&self, publisher_b64: &str) -> Result<bool> {
        Ok(self.get_trusted_publisher(publisher_b64)?.is_some())
    }

    pub fn record_known_publisher(
        &self,
        site_name: &str,
        publisher_b64: &str,
    ) -> Result<KnownPublisherStatus> {
        if let Some(raw) = self
            .known_publishers
            .get(site_name.as_bytes())
            .context("failed to read known publisher")?
        {
            let mut known: KnownPublisher =
                serde_json::from_slice(&raw).context("failed to decode known publisher")?;
            if known.publisher_b64 == publisher_b64 {
                return Ok(KnownPublisherStatus::Matches);
            }
            let status = KnownPublisherStatus::KeyChanged {
                previous_key: known.publisher_b64.clone(),
                first_seen_at: known.first_seen_at,
            };
            known.publisher_b64 = publisher_b64.to_string();
            let encoded = serde_json::to_vec(&known).context("failed to encode known publisher")?;
            self.known_publishers
                .insert(site_name.as_bytes(), encoded)
                .context("failed to persist known publisher update")?;
            self.db
                .flush()
                .context("failed to flush known publisher update")?;
            return Ok(status);
        }

        let known = KnownPublisher {
            site_name: site_name.to_string(),
            publisher_b64: publisher_b64.to_string(),
            first_seen_at: unix_ts(),
            explicitly_trusted: false,
            explicitly_trusted_at: None,
        };
        let encoded = serde_json::to_vec(&known).context("failed to encode known publisher")?;
        self.known_publishers
            .insert(site_name.as_bytes(), encoded)
            .context("failed to persist known publisher")?;
        self.db
            .flush()
            .context("failed to flush known publisher write")?;
        Ok(KnownPublisherStatus::FirstSeen)
    }

    pub fn set_explicitly_trusted(&self, site_name: &str, trusted: bool) -> Result<()> {
        let Some(raw) = self
            .known_publishers
            .get(site_name.as_bytes())
            .context("failed to read known publisher")?
        else {
            bail!("known publisher not found for site");
        };
        let mut known: KnownPublisher =
            serde_json::from_slice(&raw).context("failed to decode known publisher")?;
        known.explicitly_trusted = trusted;
        known.explicitly_trusted_at = if trusted { Some(unix_ts()) } else { None };
        let encoded =
            serde_json::to_vec(&known).context("failed to encode known publisher trust state")?;
        self.known_publishers
            .insert(site_name.as_bytes(), encoded)
            .context("failed to persist known publisher trust state")?;
        self.db
            .flush()
            .context("failed to flush known publisher trust state")?;
        Ok(())
    }

    pub fn get_known_publisher(&self, site_name: &str) -> Result<Option<KnownPublisher>> {
        self.known_publishers
            .get(site_name.as_bytes())
            .context("failed to read known publisher")?
            .map(|value| {
                serde_json::from_slice(&value).context("failed to decode known publisher")
            })
            .transpose()
    }

    pub fn get_app_record_owner(&self, record_key: &str) -> Result<Option<String>> {
        self.owned_app_records
            .get(record_key.as_bytes())
            .context("failed to read owned app record")?
            .map(|value| {
                std::str::from_utf8(&value)
                    .context("invalid owned app record encoding")
                    .map(|value| value.to_string())
            })
            .transpose()
    }

    pub fn set_app_record_owner(&self, record_key: &str, owner_key_b64: &str) -> Result<()> {
        if self
            .owned_app_records
            .contains_key(record_key.as_bytes())
            .context("failed to check existing owned app record")?
        {
            return Ok(());
        }
        self.owned_app_records
            .insert(record_key.as_bytes(), owner_key_b64.as_bytes())
            .context("failed to persist owned app record")?;
        self.db
            .flush()
            .context("failed to flush owned app record write")?;
        Ok(())
    }

    pub fn get_last_claim_ts(&self, key_b64: &str) -> Result<Option<u64>> {
        self.claim_rate_limits
            .get(key_b64.as_bytes())
            .context("failed to read claim rate limit")?
            .map(|value| {
                let bytes: [u8; 8] = value
                    .as_ref()
                    .try_into()
                    .map_err(|_| anyhow::anyhow!("invalid claim rate limit encoding"))?;
                Ok(u64::from_be_bytes(bytes))
            })
            .transpose()
    }

    pub fn set_last_claim_ts(&self, key_b64: &str, ts: u64) -> Result<()> {
        self.claim_rate_limits
            .insert(key_b64.as_bytes(), ts.to_be_bytes().to_vec())
            .context("failed to persist claim rate limit")?;
        self.db.flush().context("failed to flush claim rate limit write")?;
        Ok(())
    }

    pub fn check_and_update_claim_rate_limit(&self, key_b64: &str, now: u64) -> Result<(), String> {
        if let Some(last_claim_ts) = self
            .get_last_claim_ts(key_b64)
            .map_err(|err| err.to_string())?
        {
            if now.saturating_sub(last_claim_ts) < 86_400 {
                return Err(
                    "claim rate limit: one new claim per key per 24 hours".to_string(),
                );
            }
        }
        self.set_last_claim_ts(key_b64, now)
            .map_err(|err| err.to_string())?;
        Ok(())
    }

    pub fn gc_ephemeral_blocks(&self, max_age_secs: u64) -> Result<BlockCacheGcStats> {
        let _ = max_age_secs;
        // Ephemeral blocks are session-only now; disk-backed block GC is superseded by the
        // in-memory session cache and pinned blocks are managed explicitly.
        Ok(BlockCacheGcStats::default())
    }

    pub fn gc_unpinned(&self, max_age_secs: u64, max_total_bytes: usize) -> Result<LocalRecordGcStats> {
        let now = unix_ts();
        let mut stats = LocalRecordGcStats::default();

        let mut candidates: Vec<(String, usize, u64)> = Vec::new();
        let mut total_bytes: usize = 0;

        for item in self.records.iter() {
            let (key, value) = item.context("failed to iterate local records for gc")?;
            let key_str = match std::str::from_utf8(&key) {
                Ok(key) => key.to_string(),
                Err(_) => continue,
            };
            let size = value.len();
            total_bytes = total_bytes.saturating_add(size);

            let meta = self
                .meta
                .get(&key)
                .context("failed to read metadata for gc")?
                .and_then(|raw| serde_json::from_slice::<RecordMeta>(&raw).ok())
                .unwrap_or(RecordMeta {
                    created_at: 0,
                    updated_at: 0,
                });
            if !key_should_be_pinned(&key_str) {
                candidates.push((key_str, size, meta.updated_at));
            }
        }

        let mut to_remove: Vec<(String, usize)> = Vec::new();
        for (key, size, updated_at) in &candidates {
            if now.saturating_sub(*updated_at) > max_age_secs {
                to_remove.push((key.clone(), *size));
            }
        }

        for (key, size) in &to_remove {
            self.records
                .remove(key.as_bytes())
                .context("failed to remove stale local record")?;
            self.meta
                .remove(key.as_bytes())
                .context("failed to remove stale local record meta")?;
            stats.removed_records = stats.removed_records.saturating_add(1);
            stats.removed_bytes = stats.removed_bytes.saturating_add(*size);
            total_bytes = total_bytes.saturating_sub(*size);
        }

        if total_bytes > max_total_bytes {
            let mut remaining = candidates
                .into_iter()
                .filter(|(key, _, _)| !to_remove.iter().any(|(removed, _)| removed == key))
                .collect::<Vec<_>>();
            remaining.sort_by_key(|(_, _, updated_at)| *updated_at);

            for (key, size, _) in remaining {
                if total_bytes <= max_total_bytes {
                    break;
                }
                self.records
                    .remove(key.as_bytes())
                    .context("failed to remove oversized local record")?;
                self.meta
                    .remove(key.as_bytes())
                    .context("failed to remove oversized local record meta")?;
                stats.removed_records = stats.removed_records.saturating_add(1);
                stats.removed_bytes = stats.removed_bytes.saturating_add(size);
                total_bytes = total_bytes.saturating_sub(size);
            }
        }

        if stats.removed_records > 0 {
            self.db.flush().context("failed to flush gc changes")?;
        }
        Ok(stats)
    }

    fn encrypt_block(&self, value: &[u8]) -> Result<Vec<u8>> {
        let mut nonce_bytes = [0_u8; 12];
        OsRng.fill_bytes(&mut nonce_bytes);
        let nonce = Nonce::from_slice(&nonce_bytes);
        let ciphertext = self
            .block_cipher
            .encrypt(nonce, value)
            .map_err(|_| anyhow::anyhow!("failed to encrypt cached block"))?;
        let mut out = Vec::with_capacity(12 + ciphertext.len());
        out.extend_from_slice(&nonce_bytes);
        out.extend_from_slice(&ciphertext);
        Ok(out)
    }

    fn decrypt_block(&self, stored: &[u8]) -> Result<Vec<u8>> {
        if stored.len() < 12 + 16 {
            bail!("cached block ciphertext is too short");
        }
        let (nonce_bytes, ciphertext) = stored.split_at(12);
        let nonce = Nonce::from_slice(nonce_bytes);
        self.block_cipher
            .decrypt(nonce, ciphertext)
            .map_err(|_| anyhow::anyhow!("failed to decrypt cached block"))
    }
}
