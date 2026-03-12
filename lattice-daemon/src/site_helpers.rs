use anyhow::{Context, Result};
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use base64::Engine as _;
use lattice_core::moderation::ModerationEngine;
use libp2p::kad;
use libp2p::kad::store::RecordStore as _;
use libp2p::multiaddr::Protocol;
use libp2p::{Multiaddr, PeerId};
use std::collections::{HashMap, HashSet};
use tracing::warn;

use crate::cache::{CachePolicy, SessionBlockCache};
use crate::dht;
use crate::moderation_helpers::{
    action_name, record_publisher_b64, republish_rule, site_manifest_publisher_b64,
};
use crate::names::NameRecord;
use crate::rpc::{KnownPublisher, KnownPublisherStatus, TrustState};
use crate::store::{key_should_be_pinned, LocalRecordStore};

pub fn site_name_from_site_key(site_key: &str) -> Option<&str> {
    site_key.strip_prefix("site:")
}

pub fn site_manifest_key(site_name: &str) -> String {
    format!("site:{site_name}")
}

pub fn publisher_hex_to_b64(value: &str) -> Option<String> {
    let bytes = hex::decode(value).ok()?;
    Some(BASE64_STANDARD.encode(bytes))
}

pub fn trust_state_from_status(known: &KnownPublisher, status: KnownPublisherStatus) -> TrustState {
    match status {
        KnownPublisherStatus::FirstSeen => TrustState {
            status: "first_seen".to_string(),
            explicitly_trusted: known.explicitly_trusted,
            first_seen_at: Some(known.first_seen_at),
            previous_key: None,
        },
        KnownPublisherStatus::Matches => TrustState {
            status: "matches".to_string(),
            explicitly_trusted: known.explicitly_trusted,
            first_seen_at: Some(known.first_seen_at),
            previous_key: None,
        },
        KnownPublisherStatus::KeyChanged {
            previous_key,
            first_seen_at,
        } => TrustState {
            status: "key_changed".to_string(),
            explicitly_trusted: known.explicitly_trusted,
            first_seen_at: Some(first_seen_at),
            previous_key: Some(previous_key),
        },
    }
}

pub fn site_manifest_trust_state(
    local_record_store: &LocalRecordStore,
    site_name: &str,
    manifest_json: &str,
) -> Result<TrustState> {
    let manifest: lattice_site::manifest::SiteManifest =
        serde_json::from_str(manifest_json).context("failed to decode site manifest")?;
    let Some(publisher_b64) = publisher_hex_to_b64(&manifest.publisher_key) else {
        return Ok(TrustState {
            status: "first_seen".to_string(),
            explicitly_trusted: false,
            first_seen_at: None,
            previous_key: None,
        });
    };
    let status = local_record_store.record_known_publisher(site_name, &publisher_b64)?;
    let known = local_record_store
        .get_known_publisher(site_name)?
        .context("known publisher missing after record")?;
    Ok(trust_state_from_status(&known, status))
}

pub fn local_record_value(
    kademlia: &mut kad::Behaviour<kad::store::MemoryStore>,
    key: &str,
) -> Option<Vec<u8>> {
    let target = kad::RecordKey::new(&key);
    for record in kademlia.store_mut().records() {
        let record = record.as_ref();
        if record.key == target {
            return Some(record.value.clone());
        }
    }
    None
}

pub fn cached_manifest_json(
    kademlia: &mut kad::Behaviour<kad::store::MemoryStore>,
    site_name: &str,
) -> Option<String> {
    local_record_value(kademlia, &site_manifest_key(site_name))
        .and_then(|bytes| String::from_utf8(bytes).ok())
}

pub fn pin_cached_site_blocks(
    local_record_store: &LocalRecordStore,
    session_block_cache: &mut SessionBlockCache,
    site_name: &str,
    manifest_json: &str,
) -> Result<usize> {
    let mut pinned_count =
        local_record_store.set_site_cache_policy(site_name, CachePolicy::Pinned)?;
    let manifest: lattice_site::manifest::SiteManifest = serde_json::from_str(manifest_json)
        .context("failed to decode site manifest for pinning")?;
    let mut seen = HashSet::new();
    for file in manifest.files {
        for hash in file_block_hashes(&file) {
            if !seen.insert(hash.clone()) {
                continue;
            }
            if local_record_store.get_block(&hash)?.is_some() {
                continue;
            }
            if let Some(bytes) = session_block_cache.get(&hash).cloned() {
                local_record_store.put_block(&hash, &bytes, site_name, CachePolicy::Pinned)?;
                pinned_count = pinned_count.saturating_add(1);
            }
        }
    }
    Ok(pinned_count)
}

pub fn file_block_hashes(file: &lattice_site::manifest::FileEntry) -> Vec<String> {
    if !file.chunks.is_empty() {
        return file.chunks.clone();
    }
    vec![file.hash.clone()]
}

pub fn maybe_put_record(
    kademlia: &mut kad::Behaviour<kad::store::MemoryStore>,
    moderation_engine: &ModerationEngine,
    key: String,
    value: Vec<u8>,
) -> Result<Option<kad::QueryId>> {
    let publisher_b64 = record_publisher_b64(&key, &value);
    let site_name = key.strip_prefix("site:");
    if let Some(rule) = republish_rule(moderation_engine, &key, publisher_b64.as_deref(), site_name)
    {
        warn!(
            rule_id = %rule.id,
            rule_kind = ?rule.kind,
            matched_value = %rule.value,
            action = %action_name(&rule.action),
            key = %key,
            "refused to republish record due to moderation rule"
        );
        return Ok(None);
    }
    let query_id = dht::put_record_bytes(kademlia, key, value)
        .map_err(|err| anyhow::anyhow!(err.to_string()))?;
    Ok(Some(query_id))
}

pub fn start_providing_site(
    kademlia: &mut kad::Behaviour<kad::store::MemoryStore>,
    moderation_engine: &ModerationEngine,
    site_name: &str,
) -> Result<()> {
    let site_key = site_manifest_key(site_name);
    let publisher_b64 = cached_manifest_json(kademlia, site_name)
        .and_then(|manifest_json| site_manifest_publisher_b64(&manifest_json));
    if let Some(rule) = republish_rule(
        moderation_engine,
        &site_key,
        publisher_b64.as_deref(),
        Some(site_name),
    ) {
        warn!(
            rule_id = %rule.id,
            rule_kind = ?rule.kind,
            matched_value = %rule.value,
            action = %action_name(&rule.action),
            site = %site_name,
            matched_publisher = ?publisher_b64,
            "refused to announce site provider due to moderation rule"
        );
        return Ok(());
    }
    dht::start_providing(kademlia, site_key)?;
    Ok(())
}

pub fn reannounce_pinned_sites(
    local_record_store: &LocalRecordStore,
    moderation_engine: &ModerationEngine,
    kademlia: &mut kad::Behaviour<kad::store::MemoryStore>,
) {
    match local_record_store.list_pinned_sites() {
        Ok(sites) => {
            for site in sites {
                if let Err(err) = start_providing_site(kademlia, moderation_engine, &site) {
                    warn!(site = %site, error = %err, "failed to reannounce pinned site provider");
                }
            }
        }
        Err(err) => warn!(error = %err, "failed to list pinned sites on startup"),
    }
}

pub fn restore_local_records_to_store(
    kademlia: &mut kad::Behaviour<kad::store::MemoryStore>,
    local_records: &HashMap<String, Vec<u8>>,
) {
    for (key, value) in local_records {
        let record = kad::Record::new(kad::RecordKey::new(key), value.clone());
        if let Err(err) = kademlia.store_mut().put(record) {
            warn!(
                key = %key,
                error = %err,
                "failed to restore persisted record to local store"
            );
        }
    }
}

pub fn remember_local_record(
    local_record_store: &LocalRecordStore,
    local_records: &mut HashMap<String, Vec<u8>>,
    key: String,
    value: Vec<u8>,
) {
    let should_write = match local_records.get(&key) {
        Some(existing) => existing != &value,
        None => true,
    };
    if !should_write {
        return;
    }

    let pinned = key_should_be_pinned(&key);
    local_records.insert(key.clone(), value.clone());
    if let Err(err) = local_record_store.put_record(&key, &value, pinned) {
        warn!(key = %key, error = %err, "failed to persist local record");
    }
}

pub fn owned_names_from_local_records(
    local_records: &HashMap<String, Vec<u8>>,
    local_pubkey_hex: &str,
) -> HashSet<String> {
    let mut names = HashSet::new();
    for (key, value) in local_records {
        let Some(name) = key.strip_prefix("name:") else {
            continue;
        };

        let value_str = match std::str::from_utf8(value) {
            Ok(value_str) => value_str.to_string(),
            Err(_) => continue,
        };

        if let Some(record) = parse_verified_name_record(name, &value_str) {
            if record.key == local_pubkey_hex {
                names.insert(name.to_string());
            }
        }
    }
    names
}

pub fn parse_verified_name_record(name: &str, value: &str) -> Option<NameRecord> {
    let record: NameRecord = serde_json::from_str(value).ok()?;
    if !record.verify(name) {
        return None;
    }
    if record.heartbeat_at < record.claimed_at {
        return None;
    }
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .ok()?
        .as_secs();
    if record.heartbeat_at > now.saturating_add(300) {
        return None;
    }
    Some(record)
}

pub fn normalize_get_record_value(key: &str, value: String) -> Option<String> {
    if !key.starts_with("name:") {
        return Some(value);
    }

    let name = key.strip_prefix("name:")?;
    if let Some(record) = parse_verified_name_record(name, &value) {
        if record.is_expired() {
            return None;
        }
        return Some(record.key);
    }

    None
}

pub fn validate_name(name: &str) -> Result<(), String> {
    if name.is_empty() {
        return Err("name cannot be empty".to_string());
    }
    if name.len() > 63 {
        return Err("name must be 63 characters or fewer".to_string());
    }
    if !name
        .chars()
        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
    {
        return Err("name may only contain lowercase letters, digits, and hyphens".to_string());
    }
    if name.starts_with('-') || name.ends_with('-') {
        return Err("name cannot start or end with a hyphen".to_string());
    }
    Ok(())
}

pub fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        out.push(HEX[(b >> 4) as usize] as char);
        out.push(HEX[(b & 0x0f) as usize] as char);
    }
    out
}

pub fn addr_is_loopback_or_private(addr: &Multiaddr) -> bool {
    for proto in addr.iter() {
        match proto {
            Protocol::Ip4(ip) => {
                if ip.is_loopback() || ip.is_private() {
                    return true;
                }
            }
            Protocol::Ip6(ip) => {
                if ip.is_loopback() {
                    return true;
                }
            }
            _ => {}
        }
    }
    false
}

pub fn build_bootstrap_peer_ids(bootstrap_peers: &[String]) -> HashSet<PeerId> {
    bootstrap_peers
        .iter()
        .filter_map(|entry| {
            let mut ma: Multiaddr = entry.parse().ok()?;
            match ma.pop() {
                Some(Protocol::P2p(peer_id)) => Some(peer_id),
                _ => None,
            }
        })
        .collect()
}

pub fn build_relay_reservation_addr(peer_addr: &Multiaddr, peer_id: PeerId) -> Multiaddr {
    let mut relay_addr = peer_addr.clone();
    if let Some(Protocol::P2p(_)) = relay_addr.iter().last() {
        relay_addr.pop();
    }
    relay_addr.push(Protocol::P2p(peer_id));
    relay_addr.push(Protocol::P2pCircuit);
    relay_addr
}
