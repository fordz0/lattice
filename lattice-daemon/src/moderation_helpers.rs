use anyhow::Result;
use lattice_core::app_namespace::{validate_app_key, validate_fray_dht_key};
use lattice_core::identity::SignedRecord;
use lattice_core::moderation::{ModerationEngine, ModerationRule, RuleAction, RuleKind};
use libp2p::kad;
use libp2p::kad::store::RecordStore as _;
use std::collections::HashMap;
use tracing::{info, warn};
use uuid::Uuid;

use crate::site_helpers::publisher_hex_to_b64;
use crate::store::{unix_ts, LocalRecordStore, QuarantineEntry};

pub fn parse_rule_kind(kind: &str) -> Result<RuleKind, String> {
    match kind {
        "PublisherKey" | "publisher_key" => Ok(RuleKind::PublisherKey),
        "RecordKey" | "record_key" => Ok(RuleKind::RecordKey),
        "ContentHash" | "content_hash" => Ok(RuleKind::ContentHash),
        "SiteName" | "site_name" => Ok(RuleKind::SiteName),
        "PostId" | "post_id" => Ok(RuleKind::PostId),
        "CommentId" | "comment_id" => Ok(RuleKind::CommentId),
        _ => Err(format!("unknown moderation rule kind: {kind}")),
    }
}

pub fn parse_rule_action(action: &str) -> Result<RuleAction, String> {
    match action {
        "Hide" | "hide" => Ok(RuleAction::Hide),
        "RejectIngest" | "reject_ingest" => Ok(RuleAction::RejectIngest),
        "PurgeLocal" | "purge_local" => Ok(RuleAction::PurgeLocal),
        "RefuseRepublish" | "refuse_republish" => Ok(RuleAction::RefuseRepublish),
        "Quarantine" | "quarantine" => Ok(RuleAction::Quarantine),
        _ => Err(format!("unknown moderation action: {action}")),
    }
}

pub fn action_name(action: &RuleAction) -> String {
    format!("{action:?}")
}

pub fn signed_record_publisher_b64(value: &[u8]) -> Option<String> {
    let raw = std::str::from_utf8(value).ok()?;
    let signed: SignedRecord = serde_json::from_str(raw).ok()?;
    Some(signed.publisher_b64())
}

pub fn record_publisher_b64(key: &str, value: &[u8]) -> Option<String> {
    if key.starts_with("app:") {
        return signed_record_publisher_b64(value);
    }
    None
}

pub fn match_rule<'a>(
    engine: &'a ModerationEngine,
    checks: &[(RuleKind, &str)],
    actions: &[RuleAction],
) -> Option<&'a ModerationRule> {
    for (kind, value) in checks {
        if let Some(rule) = engine.match_rule(kind.clone(), value) {
            if actions.contains(&rule.action) {
                return Some(rule);
            }
        }
    }
    None
}

pub fn ingest_rule<'a>(
    engine: &'a ModerationEngine,
    key: &str,
    publisher_b64: Option<&str>,
) -> Option<&'a ModerationRule> {
    let mut checks = vec![(RuleKind::RecordKey, key)];
    if let Some(publisher_b64) = publisher_b64 {
        checks.insert(0, (RuleKind::PublisherKey, publisher_b64));
    }
    match_rule(
        engine,
        &checks,
        &[RuleAction::RejectIngest, RuleAction::Quarantine],
    )
}

pub fn republish_rule<'a>(
    engine: &'a ModerationEngine,
    key: &str,
    publisher_b64: Option<&str>,
    site_name: Option<&str>,
) -> Option<&'a ModerationRule> {
    let mut checks = vec![(RuleKind::RecordKey, key)];
    if let Some(site_name) = site_name {
        checks.push((RuleKind::SiteName, site_name));
    }
    if let Some(publisher_b64) = publisher_b64 {
        checks.insert(0, (RuleKind::PublisherKey, publisher_b64));
    }
    match_rule(engine, &checks, &[RuleAction::RefuseRepublish])
}

pub fn hide_record_rule<'a>(
    engine: &'a ModerationEngine,
    key: &str,
    publisher_b64: Option<&str>,
) -> Option<&'a ModerationRule> {
    let mut checks = vec![(RuleKind::RecordKey, key)];
    if let Some(publisher_b64) = publisher_b64 {
        checks.insert(0, (RuleKind::PublisherKey, publisher_b64));
    }
    match_rule(engine, &checks, &[RuleAction::Hide])
}

pub fn hide_block_rule<'a>(
    engine: &'a ModerationEngine,
    site_name: &str,
    hash: &str,
) -> Option<&'a ModerationRule> {
    match_rule(
        engine,
        &[(RuleKind::ContentHash, hash), (RuleKind::SiteName, site_name)],
        &[RuleAction::Hide],
    )
}

pub fn block_ingest_rule<'a>(
    engine: &'a ModerationEngine,
    site_name: &str,
    hash: &str,
) -> Option<&'a ModerationRule> {
    match_rule(
        engine,
        &[(RuleKind::ContentHash, hash), (RuleKind::SiteName, site_name)],
        &[RuleAction::RejectIngest, RuleAction::Quarantine],
    )
}

pub fn site_manifest_publisher_b64(manifest_json: &str) -> Option<String> {
    let manifest: lattice_site::manifest::SiteManifest = serde_json::from_str(manifest_json).ok()?;
    publisher_hex_to_b64(&manifest.publisher_key)
}

pub fn site_manifest_suppression_rule<'a>(
    engine: &'a ModerationEngine,
    key: &str,
    publisher_b64: Option<&str>,
) -> Option<&'a ModerationRule> {
    let mut checks = vec![(RuleKind::RecordKey, key)];
    if let Some(publisher_b64) = publisher_b64 {
        checks.insert(0, (RuleKind::PublisherKey, publisher_b64));
    }
    match_rule(
        engine,
        &checks,
        &[RuleAction::Hide, RuleAction::RejectIngest],
    )
}

pub fn quarantine_record(
    local_record_store: &LocalRecordStore,
    rule: &ModerationRule,
    record_key: Option<String>,
    publisher: Option<String>,
    content_hash: Option<String>,
    site_name: Option<String>,
) {
    let entry = QuarantineEntry {
        id: Uuid::new_v4().to_string(),
        created_at: unix_ts(),
        matched_rule_id: rule.id.clone(),
        matched_kind: format!("{:?}", rule.kind),
        matched_value: rule.value.clone(),
        record_key,
        publisher,
        content_hash,
        site_name,
    };
    if let Err(err) = local_record_store.insert_quarantine_entry(&entry) {
        warn!(rule_id = %rule.id, error = %err, "failed to persist quarantine entry");
    }
}

pub fn purge_local_matches(
    local_record_store: &LocalRecordStore,
    local_records: &mut HashMap<String, Vec<u8>>,
    kademlia: &mut kad::Behaviour<kad::store::MemoryStore>,
    kind: &RuleKind,
    value: &str,
) -> Result<(), String> {
    match kind {
        RuleKind::RecordKey => {
            local_records.remove(value);
            let record_key = kad::RecordKey::new(&value);
            kademlia.store_mut().remove(&record_key);
            local_record_store
                .remove_record(value)
                .map_err(|err| err.to_string())?;
            info!(key = %value, "purged local record");
        }
        RuleKind::ContentHash => {
            let size = local_record_store
                .remove_block(value)
                .map_err(|err| err.to_string())?;
            info!(key = %value, size, "purged cached block");
        }
        RuleKind::SiteName => {
            let manifest_key = format!("site:{value}");
            if local_records.remove(&manifest_key).is_some() {
                let record_key = kad::RecordKey::new(&manifest_key);
                kademlia.store_mut().remove(&record_key);
                local_record_store
                    .remove_record(&manifest_key)
                    .map_err(|err| err.to_string())?;
                info!(key = %manifest_key, "purged cached site manifest");
            }
            for hash in local_record_store
                .list_site_block_hashes(value)
                .map_err(|err| err.to_string())?
            {
                let size = local_record_store
                    .remove_block(&hash)
                    .map_err(|err| err.to_string())?;
                info!(key = %hash, size, "purged cached site block");
            }
        }
        RuleKind::PublisherKey => {
            let keys = local_records
                .iter()
                .filter_map(|(key, record_value)| {
                    let publisher = record_publisher_b64(key, record_value)?;
                    if publisher == value {
                        Some(key.clone())
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>();
            for key in keys {
                local_records.remove(&key);
                let record_key = kad::RecordKey::new(&key);
                kademlia.store_mut().remove(&record_key);
                local_record_store
                    .remove_record(&key)
                    .map_err(|err| err.to_string())?;
                info!(key = %key, "purged publisher-matched record");
            }
        }
        RuleKind::PostId | RuleKind::CommentId => {}
    }
    Ok(())
}

pub fn validate_put_record_request(key: &str, value: &[u8]) -> Result<(), String> {
    if !key.starts_with("app:") {
        return Ok(());
    }
    validate_app_key(key)?;
    if key.starts_with("app:fray:") {
        validate_fray_dht_key(key)?;
    }
    let value_str = std::str::from_utf8(value)
        .map_err(|_| "app record value must be valid utf-8 json".to_string())?;
    let signed_record: SignedRecord = serde_json::from_str(value_str)
        .map_err(|err| format!("app records must be SignedRecord JSON: {err}"))?;
    if !signed_record.verify() {
        return Err("app record signature verification failed".to_string());
    }
    Ok(())
}
