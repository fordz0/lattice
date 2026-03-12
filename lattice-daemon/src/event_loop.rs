use anyhow::Result;
use ed25519_dalek::SigningKey;
use lattice_core::moderation::{ModerationEngine, ModerationRule, RuleAction};
use lattice_daemon::app_registry::{AppRegistry, LocalAppRegistration};
use libp2p::{autonat, identify, kad, mdns, relay, Swarm};
use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tokio::sync::{mpsc, oneshot};
use tokio::time::Duration;
use tracing::{error, info, warn};
use uuid::Uuid;

use lattice_daemon::app_ownership::enforce_app_record_ownership;
use lattice_daemon::cache::{CachePolicy, SessionBlockCache};
use lattice_daemon::dht;
use lattice_daemon::moderation_helpers::{
    action_name, hide_record_rule, ingest_rule, parse_rule_action, parse_rule_kind,
    purge_local_matches, quarantine_record, record_publisher_b64, site_manifest_publisher_b64,
    site_manifest_suppression_rule, validate_put_record_request,
};
use lattice_daemon::names::NameRecord;
use lattice_daemon::publish::{
    prepare_publish, start_publish_task, validate_site_dir, PublishQuery, PublishTask,
};
use lattice_daemon::rpc::{
    GetSiteManifestResponse, NodeInfoResponse, PublishSiteOk, RpcCommand, TrustState,
};
use lattice_daemon::site_helpers::{
    addr_is_loopback_or_private, build_relay_reservation_addr, cached_manifest_json,
    maybe_put_record, normalize_get_record_value, parse_verified_name_record,
    pin_cached_site_blocks, publisher_hex_to_b64, remember_local_record, site_manifest_trust_state,
    site_name_from_site_key, start_providing_site, validate_name,
};
use lattice_daemon::store::{unix_ts, LocalRecordStore};

use crate::fetch::{
    self, handle_block_fetch_event, handle_get_providers_result, BlockConsumer, GetSiteQuery,
    GetSiteTask, PendingBlockRequest, PendingProviderQuery,
};
use crate::{
    LatticeBehaviour, GET_RECORD_MAX_ATTEMPTS, MAX_CONCURRENT_GET_SITE, MAX_CONCURRENT_PUBLISH,
    PUBLISH_OWNERSHIP_PROBES, PUBLISH_OWNERSHIP_PROBE_DELAY_SECS, RELAY_RESERVATION_RETRY_SECS,
};

pub struct PendingTextQuery {
    pub key: String,
    pub attempts: u8,
    pub respond_to: oneshot::Sender<Option<String>>,
}

pub struct PendingManifestQuery {
    pub key: String,
    pub attempts: u8,
    pub respond_to: oneshot::Sender<Option<GetSiteManifestResponse>>,
}

pub struct PendingPut {
    pub key: String,
    pub value: Vec<u8>,
    pub respond_to: oneshot::Sender<Result<(), String>>,
}

pub struct PendingClaimGet {
    pub name: String,
    pub pubkey_hex: String,
    pub respond_to: oneshot::Sender<Result<(), String>>,
}

pub struct PendingClaimPut {
    pub name: String,
    pub respond_to: oneshot::Sender<Result<(), String>>,
}

pub struct PendingNameProbe {
    pub name: String,
    pub pubkey_hex: String,
    pub respond_to: oneshot::Sender<Result<(), String>>,
    pub probe_count: u32,
    pub found_owner: bool,
}

pub struct PendingPublishOwnershipCheck {
    pub name: String,
    pub site_dir: String,
    pub probe_count: u32,
    pub respond_to: oneshot::Sender<Result<PublishSiteOk, String>>,
}

pub struct PendingPublishVersionCheck {
    pub name: String,
    pub site_dir: String,
    pub respond_to: oneshot::Sender<Result<PublishSiteOk, String>>,
    pub claimed: bool,
}

pub struct PendingPublishClaimPut {
    pub name: String,
    pub site_dir: String,
    pub respond_to: oneshot::Sender<Result<PublishSiteOk, String>>,
}

enum PublishNameOwnership {
    OwnedByLocal,
    Unclaimed,
    OwnedByOther,
}

fn publish_name_ownership(
    name: &str,
    result: Result<kad::GetRecordOk, kad::GetRecordError>,
    local_pubkey_hex: &str,
) -> Result<PublishNameOwnership, String> {
    match result {
        Ok(kad::GetRecordOk::FoundRecord(record)) => {
            let value = String::from_utf8(record.record.value)
                .map_err(|_| "invalid ownership record for name".to_string())?;

            if let Some(existing) = parse_verified_name_record(name, &value) {
                if existing.is_expired() {
                    return Ok(PublishNameOwnership::Unclaimed);
                }
                if existing.key == local_pubkey_hex {
                    return Ok(PublishNameOwnership::OwnedByLocal);
                }
                return Ok(PublishNameOwnership::OwnedByOther);
            }

            Err("invalid ownership record for name".to_string())
        }
        Ok(_) => Ok(PublishNameOwnership::Unclaimed),
        Err(kad::GetRecordError::NotFound { .. }) => Ok(PublishNameOwnership::Unclaimed),
        Err(err) => Err(format!("failed to resolve name ownership: {err}")),
    }
}

fn current_dht_site_version(
    result: Result<kad::GetRecordOk, kad::GetRecordError>,
) -> Result<Option<u64>, String> {
    match result {
        Ok(kad::GetRecordOk::FoundRecord(record)) => {
            let value = String::from_utf8(record.record.value)
                .map_err(|_| "invalid site manifest record".to_string())?;
            let manifest: lattice_site::manifest::SiteManifest = serde_json::from_str(&value)
                .map_err(|_| "invalid site manifest record".to_string())?;
            Ok(Some(manifest.version))
        }
        Ok(_) => Ok(None),
        Err(kad::GetRecordError::NotFound { .. }) => Ok(None),
        Err(err) => Err(format!("failed to read current site version: {err}")),
    }
}

fn should_rate_limit_existing_name_claim(
    existing_owner_pubkey_hex: Option<&str>,
    claimant_pubkey_hex: &str,
) -> bool {
    !matches!(existing_owner_pubkey_hex, Some(owner) if owner == claimant_pubkey_hex)
}

fn local_name_record_owned_by_key(
    local_records: &HashMap<String, Vec<u8>>,
    name: &str,
    claimant_pubkey_hex: &str,
) -> bool {
    let key = format!("name:{name}");
    local_records
        .get(&key)
        .and_then(|value| std::str::from_utf8(value).ok())
        .and_then(|value| parse_verified_name_record(name, value))
        .is_some_and(|record| !record.is_expired() && record.key == claimant_pubkey_hex)
}

#[allow(clippy::too_many_arguments)]
fn handle_claim_name_lookup_result(
    claim: PendingClaimGet,
    result: Result<kad::GetRecordOk, kad::GetRecordError>,
    swarm: &mut Swarm<LatticeBehaviour>,
    pending_claim_put: &mut HashMap<kad::QueryId, PendingClaimPut>,
    site_signing_key: &SigningKey,
    moderation_engine: &ModerationEngine,
    local_record_store: &LocalRecordStore,
    local_records: &mut HashMap<String, Vec<u8>>,
) {
    let PendingClaimGet {
        name,
        pubkey_hex,
        respond_to,
    } = claim;

    let mut record_to_store = NameRecord::new_signed(pubkey_hex.clone(), &name, site_signing_key);
    let mut existing_owner_pubkey_hex = None;

    if let Ok(kad::GetRecordOk::FoundRecord(record)) = result {
        let existing_value = match String::from_utf8(record.record.value) {
            Ok(value) => value,
            Err(_) => {
                let _ = respond_to.send(Err("invalid name record".to_string()));
                return;
            }
        };

        if let Some(mut existing_record) = parse_verified_name_record(&name, &existing_value) {
            if !existing_record.is_expired() && existing_record.key != pubkey_hex {
                let _ = respond_to.send(Err("name already claimed".to_string()));
                return;
            }

            if existing_record.key == pubkey_hex {
                existing_owner_pubkey_hex = Some(existing_record.key.clone());
                existing_record.refresh_signed(&name, site_signing_key);
                record_to_store = existing_record;
            }
        }
    }

    let payload = match serde_json::to_string(&record_to_store) {
        Ok(payload) => payload,
        Err(err) => {
            let _ = respond_to.send(Err(format!("failed to encode name record: {err}")));
            return;
        }
    };

    let key = format!("name:{name}");
    let payload_bytes = payload.into_bytes();
    let Some(pubkey_b64) = publisher_hex_to_b64(&pubkey_hex) else {
        let _ = respond_to.send(Err("invalid claim key".to_string()));
        return;
    };
    if should_rate_limit_existing_name_claim(existing_owner_pubkey_hex.as_deref(), &pubkey_hex) {
        if let Err(err) =
            local_record_store.check_and_update_claim_rate_limit(&pubkey_b64, unix_ts())
        {
            let _ = respond_to.send(Err(err));
            return;
        }
    }
    remember_local_record(
        local_record_store,
        local_records,
        key.clone(),
        payload_bytes.clone(),
    );

    match maybe_put_record(
        &mut swarm.behaviour_mut().kademlia,
        moderation_engine,
        key,
        payload_bytes,
    ) {
        Ok(Some(query_id)) => {
            pending_claim_put.insert(query_id, PendingClaimPut { name, respond_to });
        }
        Ok(None) => {
            let _ = respond_to.send(Err("claim blocked by moderation rule".to_string()));
        }
        Err(err) => {
            let _ = respond_to.send(Err(err.to_string()));
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn handle_get_site_query_result(
    query: GetSiteQuery,
    result: Result<kad::GetRecordOk, kad::GetRecordError>,
    swarm: &mut Swarm<LatticeBehaviour>,
    get_site_tasks: &mut HashMap<u64, GetSiteTask>,
    get_site_queries: &mut HashMap<kad::QueryId, GetSiteQuery>,
    pending_provider_queries: &mut HashMap<kad::QueryId, PendingProviderQuery>,
    pending_block_requests: &mut HashMap<
        lattice_daemon::block_fetch::OutboundRequestId,
        PendingBlockRequest,
    >,
    moderation_engine: &ModerationEngine,
    local_record_store: &LocalRecordStore,
    session_block_cache: &mut SessionBlockCache,
) {
    match query {
        GetSiteQuery::Manifest { task_id } => {
            let mut task = if let Some(task) = get_site_tasks.remove(&task_id) {
                task
            } else {
                return;
            };

            let manifest = match result {
                Ok(kad::GetRecordOk::FoundRecord(record)) => {
                    let manifest_key = format!("site:{}", task.requested_name);
                    if let Some(rule) = hide_record_rule(moderation_engine, &manifest_key, None) {
                        warn!(
                            rule_id = %rule.id,
                            rule_kind = ?rule.kind,
                            matched_value = %rule.value,
                            action = %action_name(&rule.action),
                            key = %manifest_key,
                            "site manifest hidden by moderation rule"
                        );
                        let _ = task.respond_to.send(Err("site not found".to_string()));
                        return;
                    }
                    let value = match String::from_utf8(record.record.value) {
                        Ok(value) => value,
                        Err(_) => {
                            let _ = task
                                .respond_to
                                .send(Err("invalid site manifest".to_string()));
                            return;
                        }
                    };
                    let manifest = match serde_json::from_str::<lattice_site::manifest::SiteManifest>(
                        &value,
                    ) {
                        Ok(manifest) => manifest,
                        Err(_) => {
                            let _ = task
                                .respond_to
                                .send(Err("invalid site manifest".to_string()));
                            return;
                        }
                    };
                    if let Err(err) = lattice_site::manifest::verify_manifest(&manifest) {
                        let _ = task
                            .respond_to
                            .send(Err(format!("manifest signature invalid: {err}")));
                        return;
                    }
                    manifest
                }
                _ => {
                    let _ = task.respond_to.send(Err("site not found".to_string()));
                    return;
                }
            };

            if manifest.name != task.requested_name {
                let _ = task
                    .respond_to
                    .send(Err("manifest name mismatch".to_string()));
                return;
            }
            if manifest.files.len() > crate::MAX_GET_SITE_FILES {
                let _ = task
                    .respond_to
                    .send(Err("site exceeds maximum file count".to_string()));
                return;
            }
            let declared_bytes = manifest
                .files
                .iter()
                .fold(0_u64, |acc, file| acc.saturating_add(file.size));
            if declared_bytes > crate::MAX_GET_SITE_TOTAL_BYTES {
                let _ = task
                    .respond_to
                    .send(Err("site exceeds maximum total size".to_string()));
                return;
            }

            task.manifest = Some(manifest);
            let query_id = dht::get_record(
                &mut swarm.behaviour_mut().kademlia,
                format!("name:{}", task.requested_name),
            );
            get_site_queries.insert(
                query_id,
                GetSiteQuery::NameOwner {
                    task_id,
                    name: task.requested_name.clone(),
                },
            );
            get_site_tasks.insert(task_id, task);
        }
        GetSiteQuery::NameOwner { task_id, name } => {
            let task = if let Some(task) = get_site_tasks.remove(&task_id) {
                task
            } else {
                return;
            };

            let manifest_publisher_key = match task.manifest.as_ref() {
                Some(manifest) => manifest.publisher_key.clone(),
                None => {
                    let _ = task
                        .respond_to
                        .send(Err("site task missing manifest".to_string()));
                    return;
                }
            };

            match result {
                Ok(kad::GetRecordOk::FoundRecord(record)) => {
                    let value = match String::from_utf8(record.record.value) {
                        Ok(value) => value,
                        Err(_) => {
                            let _ = task
                                .respond_to
                                .send(Err("invalid name ownership record".to_string()));
                            return;
                        }
                    };

                    let owner_key = if let Some(owner) = parse_verified_name_record(&name, &value) {
                        owner.key
                    } else {
                        let _ = task
                            .respond_to
                            .send(Err("invalid name ownership record".to_string()));
                        return;
                    };

                    if owner_key != manifest_publisher_key {
                        let _ = task.respond_to.send(Err(
                            "manifest publisher does not match name owner".to_string(),
                        ));
                        return;
                    }
                }
                Ok(_) | Err(kad::GetRecordError::NotFound { .. }) => {
                    let _ = task
                        .respond_to
                        .send(Err("name owner record missing".to_string()));
                    return;
                }
                Err(err) => {
                    let _ = task
                        .respond_to
                        .send(Err(format!("failed to resolve name ownership: {err}")));
                    return;
                }
            }

            let (manifest_name, manifest_version, has_files) = match task.manifest.as_ref() {
                Some(manifest) => (
                    manifest.name.clone(),
                    manifest.version,
                    !manifest.files.is_empty(),
                ),
                None => {
                    let _ = task
                        .respond_to
                        .send(Err("site task missing manifest".to_string()));
                    return;
                }
            };

            if !has_files {
                let response = lattice_daemon::rpc::GetSiteResponse {
                    name: manifest_name,
                    version: manifest_version,
                    files: Vec::new(),
                };
                let _ = task.respond_to.send(Ok(response));
                return;
            }

            fetch::drive_get_site_task(
                task_id,
                task,
                swarm,
                get_site_tasks,
                pending_provider_queries,
                pending_block_requests,
                moderation_engine,
                local_record_store,
                session_block_cache,
            );
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub fn handle_rpc_command(
    cmd: RpcCommand,
    swarm: &mut Swarm<LatticeBehaviour>,
    peer_id: libp2p::PeerId,
    local_pubkey_hex: &str,
    moderation_engine: &mut ModerationEngine,
    local_record_store: &LocalRecordStore,
    local_records: &mut HashMap<String, Vec<u8>>,
    session_block_cache: &mut SessionBlockCache,
    pending_put: &mut HashMap<kad::QueryId, PendingPut>,
    pending_get_text: &mut HashMap<kad::QueryId, PendingTextQuery>,
    pending_get_manifest: &mut HashMap<kad::QueryId, PendingManifestQuery>,
    pending_name_probes: &mut HashMap<kad::QueryId, PendingNameProbe>,
    pending_publish_checks: &mut HashMap<kad::QueryId, PendingPublishOwnershipCheck>,
    pending_publish_claim_put: &mut HashMap<kad::QueryId, PendingPublishClaimPut>,
    pending_publish_version_checks: &mut HashMap<kad::QueryId, PendingPublishVersionCheck>,
    publish_tasks: &mut HashMap<u64, PublishTask>,
    get_site_tasks: &mut HashMap<u64, GetSiteTask>,
    next_get_site_task_id: &mut u64,
    get_site_queries: &mut HashMap<kad::QueryId, GetSiteQuery>,
    pending_provider_queries: &mut HashMap<kad::QueryId, PendingProviderQuery>,
    pending_block_requests: &mut HashMap<
        lattice_daemon::block_fetch::OutboundRequestId,
        PendingBlockRequest,
    >,
    owned_names: &Arc<Mutex<HashSet<String>>>,
    app_registry: &AppRegistry,
) {
    match cmd {
        RpcCommand::NodeInfo { respond_to } => {
            let connected_peer_ids = swarm.connected_peers().map(ToString::to_string).collect();
            let info = NodeInfoResponse {
                peer_id: peer_id.to_string(),
                connected_peers: swarm.connected_peers().count() as u32,
                connected_peer_ids,
                listen_addrs: swarm.listeners().map(ToString::to_string).collect(),
            };
            let _ = respond_to.send(info);
        }
        RpcCommand::PutRecord {
            key,
            value,
            respond_to,
        } => {
            let value_bytes = value.into_bytes();
            if let Err(err) = validate_put_record_request(&key, &value_bytes) {
                let _ = respond_to.send(Err(err));
                return;
            }
            let publisher_b64 = record_publisher_b64(&key, &value_bytes);
            if let Some(rule) = ingest_rule(moderation_engine, &key, publisher_b64.as_deref()) {
                warn!(
                    rule_id = %rule.id,
                    rule_kind = ?rule.kind,
                    matched_value = %rule.value,
                    action = %action_name(&rule.action),
                    key = %key,
                    "moderation rule matched on record ingest"
                );
                if rule.action == RuleAction::Quarantine {
                    quarantine_record(
                        local_record_store,
                        rule,
                        Some(key.clone()),
                        publisher_b64.clone(),
                        None,
                        None,
                    );
                }
                let _ = respond_to.send(Ok(()));
                return;
            }
            if let Err(err) =
                enforce_app_record_ownership(local_record_store, &key, &value_bytes, unix_ts())
            {
                let _ = respond_to.send(Err(err));
                return;
            }
            remember_local_record(
                local_record_store,
                local_records,
                key.clone(),
                value_bytes.clone(),
            );
            match maybe_put_record(
                &mut swarm.behaviour_mut().kademlia,
                moderation_engine,
                key.clone(),
                value_bytes.clone(),
            ) {
                Ok(Some(query_id)) => {
                    pending_put.insert(
                        query_id,
                        PendingPut {
                            key,
                            value: value_bytes,
                            respond_to,
                        },
                    );
                }
                Ok(None) => {
                    let _ = respond_to.send(Ok(()));
                }
                Err(err) => {
                    let _ = respond_to.send(Err(err.to_string()));
                }
            }
        }
        RpcCommand::GetRecord { key, respond_to } => {
            if let Some(bytes) = lattice_daemon::site_helpers::local_record_value(
                &mut swarm.behaviour_mut().kademlia,
                &key,
            ) {
                let publisher_b64 = record_publisher_b64(&key, &bytes);
                if let Some(rule) =
                    hide_record_rule(moderation_engine, &key, publisher_b64.as_deref())
                {
                    warn!(
                        rule_id = %rule.id,
                        rule_kind = ?rule.kind,
                        matched_value = %rule.value,
                        action = %action_name(&rule.action),
                        key = %key,
                        "record hidden by moderation rule"
                    );
                    let _ = respond_to.send(None);
                    return;
                }
                if let Some(value) = String::from_utf8(bytes)
                    .ok()
                    .and_then(|value| normalize_get_record_value(&key, value))
                {
                    let _ = respond_to.send(Some(value));
                    return;
                }
            }
            if let Some(rule) = hide_record_rule(moderation_engine, &key, None) {
                warn!(
                    rule_id = %rule.id,
                    rule_kind = ?rule.kind,
                    matched_value = %rule.value,
                    action = %action_name(&rule.action),
                    key = %key,
                    "record hidden by moderation rule"
                );
                let _ = respond_to.send(None);
                return;
            }
            let query_id = dht::get_record(&mut swarm.behaviour_mut().kademlia, key.clone());
            pending_get_text.insert(
                query_id,
                PendingTextQuery {
                    key,
                    attempts: 1,
                    respond_to,
                },
            );
        }
        RpcCommand::PublishSite {
            name,
            site_dir,
            respond_to,
        } => {
            if pending_publish_checks.len()
                + pending_publish_claim_put.len()
                + pending_publish_version_checks.len()
                + publish_tasks.len()
                >= MAX_CONCURRENT_PUBLISH
            {
                let _ = respond_to.send(Err("too many concurrent publish tasks".to_string()));
                return;
            }
            if let Err(err) = validate_name(&name) {
                let _ = respond_to.send(Err(err));
                return;
            }
            let canonical_site_dir = match validate_site_dir(&site_dir) {
                Ok(path) => path,
                Err(err) => {
                    let _ = respond_to.send(Err(err));
                    return;
                }
            };
            let query_id =
                dht::get_record(&mut swarm.behaviour_mut().kademlia, format!("name:{name}"));
            pending_publish_checks.insert(
                query_id,
                PendingPublishOwnershipCheck {
                    name,
                    site_dir: canonical_site_dir.to_string_lossy().into_owned(),
                    probe_count: 0,
                    respond_to,
                },
            );
        }
        RpcCommand::GetSiteManifest { name, respond_to } => {
            let key = format!("site:{name}");
            if let Some(rule) = hide_record_rule(moderation_engine, &key, None) {
                warn!(
                    rule_id = %rule.id,
                    rule_kind = ?rule.kind,
                    matched_value = %rule.value,
                    action = %action_name(&rule.action),
                    key = %key,
                    "site manifest hidden by moderation rule"
                );
                let _ = respond_to.send(None);
                return;
            }
            if let Some(value) = lattice_daemon::site_helpers::local_record_value(
                &mut swarm.behaviour_mut().kademlia,
                &key,
            )
            .and_then(|bytes| String::from_utf8(bytes).ok())
            {
                let publisher_b64 = site_manifest_publisher_b64(&value);
                if let Some(rule) = site_manifest_suppression_rule(
                    moderation_engine,
                    &key,
                    publisher_b64.as_deref(),
                ) {
                    warn!(
                        rule_id = %rule.id,
                        matched_publisher = ?publisher_b64,
                        site = %name,
                        action = %action_name(&rule.action),
                        "site manifest suppressed by publisher moderation rule"
                    );
                    let _ = respond_to.send(None);
                    return;
                }
                let trust = site_manifest_trust_state(local_record_store, &name, &value).unwrap_or(
                    TrustState {
                        status: "first_seen".to_string(),
                        explicitly_trusted: false,
                        first_seen_at: None,
                        previous_key: None,
                    },
                );
                let pinned = local_record_store.is_site_pinned(&name).unwrap_or(false);
                let _ = respond_to.send(Some(GetSiteManifestResponse {
                    manifest_json: value,
                    trust,
                    pinned,
                }));
                return;
            }
            let query_id = dht::get_record(&mut swarm.behaviour_mut().kademlia, key.clone());
            pending_get_manifest.insert(
                query_id,
                PendingManifestQuery {
                    key,
                    attempts: 1,
                    respond_to,
                },
            );
        }
        RpcCommand::GetBlock {
            hash,
            site_key,
            respond_to,
        } => {
            let consumer = BlockConsumer::Rpc { respond_to };
            if let Some(site_key) = site_key {
                if let Some(site_name) = site_name_from_site_key(&site_key) {
                    if let Some(rule) = lattice_daemon::moderation_helpers::hide_block_rule(
                        moderation_engine,
                        site_name,
                        &hash,
                    ) {
                        warn!(
                            rule_id = %rule.id,
                            rule_kind = ?rule.kind,
                            matched_value = %rule.value,
                            action = %action_name(&rule.action),
                            site = %site_name,
                            hash = %hash,
                            "block hidden by moderation rule"
                        );
                        if let BlockConsumer::Rpc { respond_to } = consumer {
                            let _ = respond_to.send(None);
                        }
                        return;
                    }
                }
                fetch::start_block_lookup(
                    swarm,
                    moderation_engine,
                    local_record_store,
                    session_block_cache,
                    pending_provider_queries,
                    pending_block_requests,
                    get_site_tasks,
                    hash,
                    site_key,
                    consumer,
                );
            } else if let Some(value) = session_block_cache
                .get(&hash)
                .cloned()
                .or_else(|| local_record_store.get_block(&hash).ok().flatten())
            {
                if let Some(rule) = moderation_engine
                    .match_rule(lattice_core::moderation::RuleKind::ContentHash, &hash)
                {
                    if rule.action == RuleAction::Hide {
                        if let BlockConsumer::Rpc { respond_to } = consumer {
                            let _ = respond_to.send(None);
                        }
                        return;
                    }
                }
                if local_record_store.get_block(&hash).ok().flatten().is_some() {
                    let _ = local_record_store.touch_block(&hash);
                }
                if let BlockConsumer::Rpc { respond_to } = consumer {
                    let _ = respond_to.send(Some(lattice_daemon::site_helpers::hex_encode(&value)));
                }
            } else if let BlockConsumer::Rpc { respond_to } = consumer {
                let _ = respond_to.send(None);
            }
        }
        RpcCommand::GetSite { name, respond_to } => {
            if get_site_tasks.len() >= MAX_CONCURRENT_GET_SITE {
                let _ = respond_to.send(Err("too many concurrent requests".to_string()));
                return;
            }
            let task_id = *next_get_site_task_id;
            *next_get_site_task_id = next_get_site_task_id.saturating_add(1);
            let task = GetSiteTask {
                respond_to,
                requested_name: name.clone(),
                manifest: None,
                next_file_index: 0,
                active_file: None,
                total_bytes: 0,
                files: Vec::new(),
            };
            let query_id =
                dht::get_record(&mut swarm.behaviour_mut().kademlia, format!("site:{name}"));
            get_site_queries.insert(query_id, GetSiteQuery::Manifest { task_id });
            get_site_tasks.insert(task_id, task);
        }
        RpcCommand::ClaimName {
            name,
            pubkey_hex,
            respond_to,
        } => {
            if let Err(err) = validate_name(&name) {
                let _ = respond_to.send(Err(err));
                return;
            }
            let effective_pubkey = if pubkey_hex.is_empty() {
                local_pubkey_hex.to_string()
            } else if pubkey_hex == local_pubkey_hex {
                pubkey_hex
            } else {
                let _ = respond_to.send(Err("name already claimed by another key".to_string()));
                return;
            };
            let query_id =
                dht::get_record(&mut swarm.behaviour_mut().kademlia, format!("name:{name}"));
            pending_name_probes.insert(
                query_id,
                PendingNameProbe {
                    name,
                    pubkey_hex: effective_pubkey,
                    respond_to,
                    probe_count: 0,
                    found_owner: false,
                },
            );
        }
        RpcCommand::ListNames { respond_to } => {
            let guard = match owned_names.lock() {
                Ok(guard) => guard,
                Err(poisoned) => {
                    error!("owned_names mutex poisoned — recovering");
                    poisoned.into_inner()
                }
            };
            let mut names = guard.iter().cloned().collect::<Vec<_>>();
            names.sort_unstable();
            let _ = respond_to.send(names);
        }
        RpcCommand::PinSite { name, respond_to } => {
            if let Err(err) = validate_name(&name) {
                let _ = respond_to.send(Err(err));
                return;
            }
            let result = cached_manifest_json(&mut swarm.behaviour_mut().kademlia, &name)
                .ok_or_else(|| anyhow::anyhow!("no cached site manifest found for site"))
                .and_then(|manifest_json| {
                    let count = pin_cached_site_blocks(
                        local_record_store,
                        session_block_cache,
                        &name,
                        &manifest_json,
                    )?;
                    if count == 0 {
                        anyhow::bail!("no cached blocks found for site");
                    }
                    start_providing_site(
                        &mut swarm.behaviour_mut().kademlia,
                        moderation_engine,
                        &name,
                    )?;
                    Ok(())
                })
                .map_err(|err| err.to_string());
            let _ = respond_to.send(result);
        }
        RpcCommand::UnpinSite { name, respond_to } => {
            if let Err(err) = validate_name(&name) {
                let _ = respond_to.send(Err(err));
                return;
            }
            let result = local_record_store
                .set_site_cache_policy(&name, CachePolicy::Ephemeral)
                .map(|count| {
                    if count == 0 {
                        Err("no cached blocks found for site".to_string())
                    } else {
                        Ok(())
                    }
                })
                .unwrap_or_else(|err| Err(err.to_string()));
            let _ = respond_to.send(result);
        }
        RpcCommand::ListPinned { respond_to } => {
            let sites = local_record_store.list_pinned_sites().unwrap_or_default();
            let _ = respond_to.send(sites);
        }
        RpcCommand::TrustSite {
            name,
            pin,
            respond_to,
        } => {
            if let Err(err) = validate_name(&name) {
                let _ = respond_to.send(Err(err));
                return;
            }
            let result = (|| -> Result<(), String> {
                if pin {
                    let manifest_json =
                        cached_manifest_json(&mut swarm.behaviour_mut().kademlia, &name)
                            .ok_or_else(|| "no cached site manifest found for site".to_string())?;
                    let count = pin_cached_site_blocks(
                        local_record_store,
                        session_block_cache,
                        &name,
                        &manifest_json,
                    )
                    .map_err(|err| err.to_string())?;
                    if count == 0 {
                        return Err("no cached blocks found for site".to_string());
                    }
                    start_providing_site(
                        &mut swarm.behaviour_mut().kademlia,
                        moderation_engine,
                        &name,
                    )
                    .map_err(|err| err.to_string())?;
                }
                local_record_store
                    .set_explicitly_trusted(&name, true)
                    .map_err(|err| err.to_string())?;
                Ok(())
            })();
            let _ = respond_to.send(result);
        }
        RpcCommand::UntrustSite {
            name,
            unpin,
            respond_to,
        } => {
            if let Err(err) = validate_name(&name) {
                let _ = respond_to.send(Err(err));
                return;
            }
            let result = (|| -> Result<(), String> {
                local_record_store
                    .set_explicitly_trusted(&name, false)
                    .map_err(|err| err.to_string())?;
                if unpin {
                    let count = local_record_store
                        .set_site_cache_policy(&name, CachePolicy::Ephemeral)
                        .map_err(|err| err.to_string())?;
                    if count == 0 {
                        return Err("no cached blocks found for site".to_string());
                    }
                }
                Ok(())
            })();
            let _ = respond_to.send(result);
        }
        RpcCommand::KnownPublisherStatus { name, respond_to } => {
            if let Err(_err) = validate_name(&name) {
                let _ = respond_to.send(None);
                return;
            }
            let known = local_record_store
                .get_known_publisher(&name)
                .unwrap_or(None);
            let _ = respond_to.send(known);
        }
        RpcCommand::AppRegister {
            site_name,
            proxy_port,
            proxy_paths,
            pid,
            respond_to,
        } => {
            let result = app_registry.register(LocalAppRegistration {
                site_name,
                proxy_port,
                proxy_paths,
                registered_at: unix_ts(),
                pid,
            });
            let _ = respond_to.send(result);
        }
        RpcCommand::AppUnregister {
            site_name,
            pid,
            respond_to,
        } => {
            let result = app_registry.unregister(&site_name, pid);
            let _ = respond_to.send(result);
        }
        RpcCommand::AppList { respond_to } => {
            let _ = respond_to.send(app_registry.list());
        }
        RpcCommand::ModAddRule {
            kind,
            value,
            action,
            note,
            respond_to,
        } => {
            let result = (|| -> Result<String, String> {
                let kind = parse_rule_kind(&kind)?;
                let action = parse_rule_action(&action)?;
                let rule = ModerationRule {
                    id: Uuid::new_v4().to_string(),
                    kind,
                    value,
                    action,
                    created_at: unix_ts(),
                    note,
                };
                local_record_store
                    .insert_moderation_rule(&rule)
                    .map_err(|err| err.to_string())?;
                *moderation_engine = ModerationEngine::load(
                    local_record_store
                        .load_moderation_rules()
                        .map_err(|err| err.to_string())?,
                );
                Ok(rule.id)
            })();
            let _ = respond_to.send(result);
        }
        RpcCommand::ModRemoveRule { id, respond_to } => {
            let result = local_record_store
                .remove_moderation_rule(&id)
                .map_err(|err| err.to_string())
                .and_then(|removed| {
                    if removed {
                        *moderation_engine = ModerationEngine::load(
                            local_record_store
                                .load_moderation_rules()
                                .map_err(|err| err.to_string())?,
                        );
                        Ok(())
                    } else {
                        Err("moderation rule not found".to_string())
                    }
                });
            let _ = respond_to.send(result);
        }
        RpcCommand::ModListRules { respond_to } => {
            let rules = local_record_store
                .load_moderation_rules()
                .unwrap_or_default();
            let _ = respond_to.send(rules);
        }
        RpcCommand::ModPurgeLocal {
            kind,
            value,
            respond_to,
        } => {
            let result = parse_rule_kind(&kind).and_then(|kind| {
                purge_local_matches(
                    local_record_store,
                    local_records,
                    &mut swarm.behaviour_mut().kademlia,
                    &kind,
                    &value,
                )
            });
            let _ = respond_to.send(result);
        }
        RpcCommand::ModQuarantineList { respond_to } => {
            let entries = local_record_store
                .list_quarantine_entries()
                .unwrap_or_default();
            let _ = respond_to.send(entries);
        }
        RpcCommand::ModCheck {
            kind,
            value,
            respond_to,
        } => {
            let action = parse_rule_kind(&kind)
                .ok()
                .and_then(|kind| moderation_engine.match_rule(kind, &value))
                .map(|rule| action_name(&rule.action));
            let _ = respond_to.send(action);
        }
        RpcCommand::ModCheckMany { checks, respond_to } => {
            let action = checks.iter().find_map(|check| {
                let kind = parse_rule_kind(&check.kind).ok()?;
                moderation_engine
                    .match_rule(kind, &check.value)
                    .map(|rule| action_name(&rule.action))
            });
            let _ = respond_to.send(action);
        }
        RpcCommand::TrustAdd {
            publisher_b64,
            label,
            note,
            respond_to,
        } => {
            let result = local_record_store
                .add_trusted_publisher(publisher_b64, label, note)
                .map_err(|err| err.to_string());
            let _ = respond_to.send(result);
        }
        RpcCommand::TrustRemove {
            publisher_b64,
            respond_to,
        } => {
            let result = local_record_store
                .remove_trusted_publisher(&publisher_b64)
                .map_err(|err| err.to_string())
                .and_then(|removed| {
                    if removed {
                        Ok(())
                    } else {
                        Err("trusted publisher not found".to_string())
                    }
                });
            let _ = respond_to.send(result);
        }
        RpcCommand::TrustList { respond_to } => {
            let trusted = local_record_store
                .list_trusted_publishers()
                .unwrap_or_default();
            let _ = respond_to.send(trusted);
        }
        RpcCommand::TrustCheck {
            publisher_b64,
            respond_to,
        } => {
            let trusted = local_record_store
                .get_trusted_publisher(&publisher_b64)
                .unwrap_or(None);
            let _ = respond_to.send(trusted);
        }
        RpcCommand::RetryNameProbe {
            name,
            pubkey_hex,
            probe_count,
            respond_to,
        } => {
            if let Err(err) = validate_name(&name) {
                let _ = respond_to.send(Err(err));
                return;
            }
            let effective_pubkey = if pubkey_hex.is_empty() {
                local_pubkey_hex.to_string()
            } else if pubkey_hex == local_pubkey_hex {
                pubkey_hex
            } else {
                let _ = respond_to.send(Err("name already claimed by another key".to_string()));
                return;
            };
            let query_id =
                dht::get_record(&mut swarm.behaviour_mut().kademlia, format!("name:{name}"));
            pending_name_probes.insert(
                query_id,
                PendingNameProbe {
                    name,
                    pubkey_hex: effective_pubkey,
                    respond_to,
                    probe_count,
                    found_owner: false,
                },
            );
        }
        RpcCommand::RetryPublishOwnershipCheck {
            name,
            site_dir,
            probe_count,
            respond_to,
        } => {
            if let Err(err) = validate_name(&name) {
                let _ = respond_to.send(Err(err));
                return;
            }
            let canonical_site_dir = match validate_site_dir(&site_dir) {
                Ok(path) => path,
                Err(err) => {
                    let _ = respond_to.send(Err(err));
                    return;
                }
            };
            let query_id =
                dht::get_record(&mut swarm.behaviour_mut().kademlia, format!("name:{name}"));
            pending_publish_checks.insert(
                query_id,
                PendingPublishOwnershipCheck {
                    name,
                    site_dir: canonical_site_dir.to_string_lossy().into_owned(),
                    probe_count,
                    respond_to,
                },
            );
        }
        RpcCommand::RepublishLocalRecords => {
            if !local_records.is_empty() {
                info!(
                    count = local_records.len(),
                    "republishing local DHT records"
                );
                for (key, value) in local_records.iter() {
                    if let Err(err) = maybe_put_record(
                        &mut swarm.behaviour_mut().kademlia,
                        moderation_engine,
                        key.clone(),
                        value.clone(),
                    )
                    .map(|_| ())
                    {
                        warn!(key = %key, error = %err, "failed to start republish put_record");
                    }
                }
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub fn handle_swarm_event(
    event: libp2p::swarm::SwarmEvent<crate::LatticeBehaviourEvent>,
    swarm: &mut Swarm<LatticeBehaviour>,
    pending_put: &mut HashMap<kad::QueryId, PendingPut>,
    pending_get_text: &mut HashMap<kad::QueryId, PendingTextQuery>,
    pending_get_manifest: &mut HashMap<kad::QueryId, PendingManifestQuery>,
    pending_provider_queries: &mut HashMap<kad::QueryId, PendingProviderQuery>,
    pending_block_requests: &mut HashMap<
        lattice_daemon::block_fetch::OutboundRequestId,
        PendingBlockRequest,
    >,
    session_block_cache: &mut SessionBlockCache,
    pending_claim_put: &mut HashMap<kad::QueryId, PendingClaimPut>,
    pending_name_probes: &mut HashMap<kad::QueryId, PendingNameProbe>,
    pending_publish_checks: &mut HashMap<kad::QueryId, PendingPublishOwnershipCheck>,
    pending_publish_claim_put: &mut HashMap<kad::QueryId, PendingPublishClaimPut>,
    pending_publish_version_checks: &mut HashMap<kad::QueryId, PendingPublishVersionCheck>,
    publish_tasks: &mut HashMap<u64, PublishTask>,
    publish_query_to_task: &mut HashMap<kad::QueryId, PublishQuery>,
    next_publish_task_id: &mut u64,
    get_site_tasks: &mut HashMap<u64, GetSiteTask>,
    get_site_queries: &mut HashMap<kad::QueryId, GetSiteQuery>,
    bootstrap_peer_ids: &HashSet<libp2p::PeerId>,
    relay_reservations: &mut HashSet<libp2p::PeerId>,
    relay_reservation_requests: &mut HashMap<libp2p::PeerId, Instant>,
    relay_connection_counts: &mut HashMap<libp2p::PeerId, usize>,
    owned_names: &Arc<Mutex<HashSet<String>>>,
    rpc_tx: &mpsc::Sender<RpcCommand>,
    site_signing_key: &SigningKey,
    local_pubkey_hex: &str,
    moderation_engine: &ModerationEngine,
    mime_policy_strict: bool,
    local_record_store: &LocalRecordStore,
    local_records: &mut HashMap<String, Vec<u8>>,
) {
    match event {
        libp2p::swarm::SwarmEvent::ConnectionEstablished {
            peer_id, endpoint, ..
        } => {
            info!(peer = %peer_id, address = ?endpoint.get_remote_address(), "new peer connected");
            if bootstrap_peer_ids.contains(&peer_id) {
                let count = relay_connection_counts.entry(peer_id).or_insert(0);
                *count = count.saturating_add(1);

                if !relay_reservations.contains(&peer_id) {
                    let should_request = match relay_reservation_requests.get(&peer_id) {
                        Some(started_at) => {
                            started_at.elapsed()
                                >= Duration::from_secs(RELAY_RESERVATION_RETRY_SECS)
                        }
                        None => true,
                    };

                    if should_request {
                        let relay_addr =
                            build_relay_reservation_addr(endpoint.get_remote_address(), peer_id);
                        match swarm.listen_on(relay_addr.clone()) {
                            Ok(_) => {
                                relay_reservation_requests.insert(peer_id, Instant::now());
                                info!(relay = %peer_id, relay_addr = %relay_addr, "requested relay reservation");
                            }
                            Err(err) => {
                                warn!(relay = %peer_id, error = %err, "failed to request relay reservation");
                            }
                        }
                    }
                }
            }
        }
        libp2p::swarm::SwarmEvent::ConnectionClosed { peer_id, .. } => {
            info!(peer = %peer_id, "peer disconnected");
            if bootstrap_peer_ids.contains(&peer_id) {
                if let Some(count) = relay_connection_counts.get_mut(&peer_id) {
                    if *count > 1 {
                        *count -= 1;
                    } else {
                        relay_connection_counts.remove(&peer_id);
                        relay_reservations.remove(&peer_id);
                        relay_reservation_requests.remove(&peer_id);
                    }
                }
            }
        }
        libp2p::swarm::SwarmEvent::NewListenAddr { address, .. } => {
            info!(address = %address, "node listening");
        }
        libp2p::swarm::SwarmEvent::Behaviour(crate::LatticeBehaviourEvent::Mdns(
            mdns::Event::Discovered(list),
        )) => {
            for (peer_id, addr) in list {
                info!(peer = %peer_id, address = %addr, "mDNS peer discovered");
                swarm.behaviour_mut().kademlia.add_address(&peer_id, addr);
            }
        }
        libp2p::swarm::SwarmEvent::Behaviour(crate::LatticeBehaviourEvent::Mdns(
            mdns::Event::Expired(list),
        )) => {
            for (peer_id, addr) in list {
                swarm
                    .behaviour_mut()
                    .kademlia
                    .remove_address(&peer_id, &addr);
            }
        }
        libp2p::swarm::SwarmEvent::Behaviour(crate::LatticeBehaviourEvent::Kademlia(
            kad::Event::OutboundQueryProgressed { id, result, .. },
        )) => match result {
            kad::QueryResult::PutRecord(result) => {
                if let Some(pending) = pending_put.remove(&id) {
                    match result {
                        Ok(ok) => {
                            info!(key = ?ok.key, "kademlia put_record succeeded");
                            remember_local_record(
                                local_record_store,
                                local_records,
                                pending.key,
                                pending.value,
                            );
                            let _ = pending.respond_to.send(Ok(()));
                        }
                        Err(err) => {
                            warn!(error = %err, "kademlia put_record failed");
                            let _ = pending.respond_to.send(Err(err.to_string()));
                        }
                    }
                } else if let Some(publish_query) = publish_query_to_task.remove(&id) {
                    let task_id = publish_query.task_id;
                    let Some(mut task) = publish_tasks.remove(&task_id) else {
                        error!(task_id, "publish task missing from map — internal error");
                        return;
                    };

                    task.remaining = task.remaining.saturating_sub(1);
                    match result {
                        Ok(ok) => {
                            info!(key = ?ok.key, "kademlia publish put_record succeeded");
                        }
                        Err(kad::PutRecordError::QuorumFailed {
                            key,
                            success,
                            quorum,
                        }) => {
                            warn!(
                                task_id,
                                key = ?key,
                                success_count = success.len(),
                                quorum_required = quorum.get(),
                                peers_at_start = task.connected_peers_at_start,
                                "put_record quorum failed; record stored locally and will replicate once peers are ready"
                            );
                            task.had_quorum_failed = true;
                        }
                        Err(err) => {
                            warn!(task_id, error = %err, "kademlia publish put_record failed");
                            if task.failed.is_none() {
                                task.failed = Some(err.to_string());
                            }
                        }
                    }

                    if task.remaining > 0 {
                        publish_tasks.insert(task_id, task);
                        return;
                    }

                    let succeeded = task.failed.is_none();
                    let response = match task.failed.clone() {
                        Some(err) => Err(err),
                        None => Ok(PublishSiteOk {
                            version: task.version,
                            file_count: task.file_count,
                            claimed: task.claimed,
                        }),
                    };
                    let _ = task.respond_to.send(response);

                    if succeeded || task.had_quorum_failed {
                        let rpc_tx_retry = rpc_tx.clone();
                        tokio::spawn(async move {
                            tokio::time::sleep(Duration::from_secs(30)).await;
                            let _ = rpc_tx_retry.send(RpcCommand::RepublishLocalRecords).await;
                        });
                    }
                } else if let Some(pending) = pending_publish_claim_put.remove(&id) {
                    match result {
                        Ok(ok) => {
                            info!(key = ?ok.key, name = %pending.name, "kademlia auto-claim put_record succeeded");
                            let mut guard = match owned_names.lock() {
                                Ok(guard) => guard,
                                Err(poisoned) => {
                                    error!("owned_names mutex poisoned — recovering");
                                    poisoned.into_inner()
                                }
                            };
                            guard.insert(pending.name.clone());
                            info!(name = %pending.name, "name was unclaimed — auto-claimed before publish");
                            let query_id = dht::get_record(
                                &mut swarm.behaviour_mut().kademlia,
                                format!("site:{}", pending.name),
                            );
                            pending_publish_version_checks.insert(
                                query_id,
                                PendingPublishVersionCheck {
                                    name: pending.name,
                                    site_dir: pending.site_dir,
                                    respond_to: pending.respond_to,
                                    claimed: true,
                                },
                            );
                        }
                        Err(kad::PutRecordError::QuorumFailed {
                            key,
                            success,
                            quorum,
                        }) => {
                            warn!(
                                key = ?key,
                                name = %pending.name,
                                success_count = success.len(),
                                quorum_required = quorum.get(),
                                "kademlia auto-claim quorum failed; record stored locally and publish will continue"
                            );
                            let mut guard = match owned_names.lock() {
                                Ok(guard) => guard,
                                Err(poisoned) => {
                                    error!("owned_names mutex poisoned — recovering");
                                    poisoned.into_inner()
                                }
                            };
                            guard.insert(pending.name.clone());
                            let query_id = dht::get_record(
                                &mut swarm.behaviour_mut().kademlia,
                                format!("site:{}", pending.name),
                            );
                            pending_publish_version_checks.insert(
                                query_id,
                                PendingPublishVersionCheck {
                                    name: pending.name,
                                    site_dir: pending.site_dir,
                                    respond_to: pending.respond_to,
                                    claimed: true,
                                },
                            );
                            let rpc_tx_retry = rpc_tx.clone();
                            tokio::spawn(async move {
                                tokio::time::sleep(Duration::from_secs(30)).await;
                                let _ = rpc_tx_retry.send(RpcCommand::RepublishLocalRecords).await;
                            });
                        }
                        Err(err) => {
                            warn!(name = %pending.name, error = %err, "kademlia auto-claim put_record failed");
                            let _ = pending.respond_to.send(Err(err.to_string()));
                        }
                    }
                } else if let Some(task) = pending_claim_put.remove(&id) {
                    match result {
                        Ok(ok) => {
                            info!(key = ?ok.key, name = %task.name, "kademlia claim_name put_record succeeded");
                            let mut guard = match owned_names.lock() {
                                Ok(guard) => guard,
                                Err(poisoned) => {
                                    error!("owned_names mutex poisoned — recovering");
                                    poisoned.into_inner()
                                }
                            };
                            guard.insert(task.name.clone());
                            let _ = task.respond_to.send(Ok(()));
                        }
                        Err(kad::PutRecordError::QuorumFailed {
                            key,
                            success,
                            quorum,
                        }) => {
                            warn!(
                                key = ?key,
                                name = %task.name,
                                success_count = success.len(),
                                quorum_required = quorum.get(),
                                "kademlia claim_name quorum failed; record stored locally"
                            );
                            let mut guard = match owned_names.lock() {
                                Ok(guard) => guard,
                                Err(poisoned) => {
                                    error!("owned_names mutex poisoned — recovering");
                                    poisoned.into_inner()
                                }
                            };
                            guard.insert(task.name.clone());
                            let _ = task.respond_to.send(Ok(()));
                            let rpc_tx_retry = rpc_tx.clone();
                            tokio::spawn(async move {
                                tokio::time::sleep(Duration::from_secs(30)).await;
                                let _ = rpc_tx_retry.send(RpcCommand::RepublishLocalRecords).await;
                            });
                        }
                        Err(err) => {
                            warn!(name = %task.name, error = %err, "kademlia claim_name put_record failed");
                            let _ = task.respond_to.send(Err(err.to_string()));
                        }
                    }
                } else {
                    match result {
                        Ok(ok) => info!(key = ?ok.key, "republish put_record succeeded"),
                        Err(kad::PutRecordError::QuorumFailed {
                            key,
                            success,
                            quorum,
                        }) => {
                            warn!(
                                key = ?key,
                                success_count = success.len(),
                                quorum_required = quorum.get(),
                                "republish put_record quorum failed"
                            );
                        }
                        Err(err) => warn!(error = %err, "republish put_record failed"),
                    }
                }
            }
            kad::QueryResult::GetRecord(result) => {
                if let Some(mut pending) = pending_get_text.remove(&id) {
                    match result {
                        Ok(kad::GetRecordOk::FoundRecord(record)) => {
                            let publisher_b64 =
                                record_publisher_b64(&pending.key, &record.record.value);
                            if let Some(rule) = hide_record_rule(
                                moderation_engine,
                                &pending.key,
                                publisher_b64.as_deref(),
                            ) {
                                warn!(
                                    rule_id = %rule.id,
                                    rule_kind = ?rule.kind,
                                    matched_value = %rule.value,
                                    action = %action_name(&rule.action),
                                    key = %pending.key,
                                    "record hidden by moderation rule"
                                );
                                let _ = pending.respond_to.send(None);
                                return;
                            }
                            let value = String::from_utf8(record.record.value)
                                .ok()
                                .and_then(|value| normalize_get_record_value(&pending.key, value));
                            info!(
                                key = ?record.record.key,
                                request_key = %pending.key,
                                found = value.is_some(),
                                "kademlia get_record result"
                            );
                            let _ = pending.respond_to.send(value);
                        }
                        Ok(_) => {
                            if pending.attempts < GET_RECORD_MAX_ATTEMPTS {
                                pending.attempts = pending.attempts.saturating_add(1);
                                let query_id = dht::get_record(
                                    &mut swarm.behaviour_mut().kademlia,
                                    pending.key.clone(),
                                );
                                pending_get_text.insert(query_id, pending);
                            } else {
                                info!("kademlia get_record finished without record");
                                let _ = pending.respond_to.send(None);
                            }
                        }
                        Err(err) => {
                            if pending.attempts < GET_RECORD_MAX_ATTEMPTS {
                                warn!(
                                    error = %err,
                                    key = %pending.key,
                                    attempt = pending.attempts,
                                    "kademlia get_record failed; retrying"
                                );
                                pending.attempts = pending.attempts.saturating_add(1);
                                let query_id = dht::get_record(
                                    &mut swarm.behaviour_mut().kademlia,
                                    pending.key.clone(),
                                );
                                pending_get_text.insert(query_id, pending);
                            } else {
                                warn!(error = %err, "kademlia get_record failed");
                                let _ = pending.respond_to.send(None);
                            }
                        }
                    }
                } else if let Some(mut pending) = pending_get_manifest.remove(&id) {
                    match result {
                        Ok(kad::GetRecordOk::FoundRecord(record)) => {
                            if let Some(rule) =
                                hide_record_rule(moderation_engine, &pending.key, None)
                            {
                                warn!(
                                    rule_id = %rule.id,
                                    rule_kind = ?rule.kind,
                                    matched_value = %rule.value,
                                    action = %action_name(&rule.action),
                                    key = %pending.key,
                                    "site manifest hidden by moderation rule"
                                );
                                let _ = pending.respond_to.send(None);
                                return;
                            }
                            let value = String::from_utf8(record.record.value).ok();
                            let response = value.and_then(|manifest_json| {
                                let site_name =
                                    pending.key.strip_prefix("site:").unwrap_or_default().to_string();
                                let publisher_b64 = site_manifest_publisher_b64(&manifest_json);
                                if let Some(rule) = site_manifest_suppression_rule(
                                    moderation_engine,
                                    &pending.key,
                                    publisher_b64.as_deref(),
                                ) {
                                    warn!(
                                        rule_id = %rule.id,
                                        matched_publisher = ?publisher_b64,
                                        site = %site_name,
                                        action = %action_name(&rule.action),
                                        "remote site manifest suppressed by publisher moderation rule"
                                    );
                                    return None;
                                }
                                let trust = site_manifest_trust_state(
                                    local_record_store,
                                    &site_name,
                                    &manifest_json,
                                )
                                .unwrap_or(TrustState {
                                    status: "first_seen".to_string(),
                                    explicitly_trusted: false,
                                    first_seen_at: None,
                                    previous_key: None,
                                });
                                let pinned = local_record_store
                                    .is_site_pinned(&site_name)
                                    .unwrap_or(false);
                                Some(GetSiteManifestResponse { manifest_json, trust, pinned })
                            });
                            let _ = pending.respond_to.send(response);
                        }
                        Ok(_) => {
                            if pending.attempts < GET_RECORD_MAX_ATTEMPTS {
                                pending.attempts = pending.attempts.saturating_add(1);
                                let query_id = dht::get_record(
                                    &mut swarm.behaviour_mut().kademlia,
                                    pending.key.clone(),
                                );
                                pending_get_manifest.insert(query_id, pending);
                            } else {
                                let _ = pending.respond_to.send(None);
                            }
                        }
                        Err(err) => {
                            if pending.attempts < GET_RECORD_MAX_ATTEMPTS {
                                warn!(
                                    error = %err,
                                    key = %pending.key,
                                    attempt = pending.attempts,
                                    "kademlia get_record for site manifest failed; retrying"
                                );
                                pending.attempts = pending.attempts.saturating_add(1);
                                let query_id = dht::get_record(
                                    &mut swarm.behaviour_mut().kademlia,
                                    pending.key.clone(),
                                );
                                pending_get_manifest.insert(query_id, pending);
                            } else {
                                warn!(error = %err, key = %pending.key, "kademlia get_record for site manifest failed");
                                let _ = pending.respond_to.send(None);
                            }
                        }
                    }
                } else if let Some(pending) = pending_publish_checks.remove(&id) {
                    let next_probe_count = pending.probe_count.saturating_add(1);
                    match publish_name_ownership(&pending.name, result, local_pubkey_hex) {
                        Ok(PublishNameOwnership::OwnedByLocal) => {
                            {
                                let mut guard = match owned_names.lock() {
                                    Ok(guard) => guard,
                                    Err(poisoned) => {
                                        error!("owned_names mutex poisoned — recovering");
                                        poisoned.into_inner()
                                    }
                                };
                                guard.insert(pending.name.clone());
                            }

                            let refreshed = NameRecord::new_signed(
                                local_pubkey_hex.to_string(),
                                &pending.name,
                                site_signing_key,
                            );
                            if let Ok(payload) = serde_json::to_vec(&refreshed) {
                                let key = format!("name:{}", pending.name);
                                remember_local_record(
                                    local_record_store,
                                    local_records,
                                    key.clone(),
                                    payload.clone(),
                                );
                                if let Err(err) = maybe_put_record(
                                    &mut swarm.behaviour_mut().kademlia,
                                    moderation_engine,
                                    key.clone(),
                                    payload,
                                )
                                .map(|_| ())
                                {
                                    warn!(
                                        key = %key,
                                        error = %err,
                                        "failed to refresh name heartbeat before publish"
                                    );
                                }
                            } else {
                                warn!(name = %pending.name, "failed to encode name heartbeat before publish");
                            }

                            let query_id = dht::get_record(
                                &mut swarm.behaviour_mut().kademlia,
                                format!("site:{}", pending.name),
                            );
                            pending_publish_version_checks.insert(
                                query_id,
                                PendingPublishVersionCheck {
                                    name: pending.name,
                                    site_dir: pending.site_dir,
                                    respond_to: pending.respond_to,
                                    claimed: false,
                                },
                            );
                        }
                        Ok(PublishNameOwnership::Unclaimed) => {
                            if next_probe_count < PUBLISH_OWNERSHIP_PROBES {
                                let rpc_tx_for_retry = rpc_tx.clone();
                                tokio::spawn(async move {
                                    tokio::time::sleep(Duration::from_secs(
                                        PUBLISH_OWNERSHIP_PROBE_DELAY_SECS,
                                    ))
                                    .await;
                                    let _ = rpc_tx_for_retry
                                        .send(RpcCommand::RetryPublishOwnershipCheck {
                                            name: pending.name,
                                            site_dir: pending.site_dir,
                                            probe_count: next_probe_count,
                                            respond_to: pending.respond_to,
                                        })
                                        .await;
                                });
                            } else {
                                let record = NameRecord::new_signed(
                                    local_pubkey_hex.to_string(),
                                    &pending.name,
                                    site_signing_key,
                                );
                                let payload = match serde_json::to_string(&record) {
                                    Ok(payload) => payload,
                                    Err(err) => {
                                        let _ = pending.respond_to.send(Err(format!(
                                            "failed to encode name record: {err}"
                                        )));
                                        return;
                                    }
                                };
                                let key = format!("name:{}", pending.name);
                                let payload_bytes = payload.into_bytes();
                                let Some(pubkey_b64) = publisher_hex_to_b64(local_pubkey_hex)
                                else {
                                    let _ = pending
                                        .respond_to
                                        .send(Err("invalid claim key".to_string()));
                                    return;
                                };
                                if !local_name_record_owned_by_key(
                                    local_records,
                                    &pending.name,
                                    local_pubkey_hex,
                                ) {
                                    if let Err(err) = local_record_store
                                        .check_and_update_claim_rate_limit(&pubkey_b64, unix_ts())
                                    {
                                        let _ = pending.respond_to.send(Err(err));
                                        return;
                                    }
                                }
                                remember_local_record(
                                    local_record_store,
                                    local_records,
                                    key.clone(),
                                    payload_bytes.clone(),
                                );
                                match maybe_put_record(
                                    &mut swarm.behaviour_mut().kademlia,
                                    moderation_engine,
                                    key,
                                    payload_bytes,
                                ) {
                                    Ok(Some(query_id)) => {
                                        pending_publish_claim_put.insert(
                                            query_id,
                                            PendingPublishClaimPut {
                                                name: pending.name,
                                                site_dir: pending.site_dir,
                                                respond_to: pending.respond_to,
                                            },
                                        );
                                    }
                                    Ok(None) => {
                                        let _ =
                                            pending
                                                .respond_to
                                                .send(Err("publish blocked by moderation rule"
                                                    .to_string()));
                                    }
                                    Err(err) => {
                                        let _ = pending.respond_to.send(Err(err.to_string()));
                                    }
                                }
                            }
                        }
                        Ok(PublishNameOwnership::OwnedByOther) => {
                            let _ = pending
                                .respond_to
                                .send(Err("name already claimed by another key".to_string()));
                        }
                        Err(err) => {
                            let _ = pending.respond_to.send(Err(err));
                        }
                    }
                } else if let Some(pending) = pending_publish_version_checks.remove(&id) {
                    match current_dht_site_version(result) {
                        Ok(dht_version) => {
                            let baseline_version = dht_version.unwrap_or(0);
                            match prepare_publish(
                                &pending.name,
                                Path::new(&pending.site_dir),
                                site_signing_key,
                                baseline_version,
                                mime_policy_strict,
                            ) {
                                Ok(prepared) => {
                                    if let Some(current_version) = dht_version {
                                        if current_version >= prepared.version {
                                            let _ = pending.respond_to.send(Err(format!(
                                                "version must be higher than current version {current_version}"
                                            )));
                                        } else {
                                            let connected_peers_at_start =
                                                swarm.connected_peers().count();
                                            start_publish_task(
                                                &mut swarm.behaviour_mut().kademlia,
                                                connected_peers_at_start,
                                                prepared,
                                                &pending.name,
                                                pending.claimed,
                                                pending.respond_to,
                                                publish_tasks,
                                                publish_query_to_task,
                                                next_publish_task_id,
                                                moderation_engine,
                                                local_record_store,
                                                local_records,
                                            );
                                        }
                                    } else {
                                        let connected_peers_at_start =
                                            swarm.connected_peers().count();
                                        start_publish_task(
                                            &mut swarm.behaviour_mut().kademlia,
                                            connected_peers_at_start,
                                            prepared,
                                            &pending.name,
                                            pending.claimed,
                                            pending.respond_to,
                                            publish_tasks,
                                            publish_query_to_task,
                                            next_publish_task_id,
                                            moderation_engine,
                                            local_record_store,
                                            local_records,
                                        );
                                    }
                                }
                                Err(err) => {
                                    let _ = pending.respond_to.send(Err(err.to_string()));
                                }
                            }
                        }
                        Err(err) => {
                            let _ = pending.respond_to.send(Err(err));
                        }
                    }
                } else if let Some(mut probe) = pending_name_probes.remove(&id) {
                    probe.probe_count = probe.probe_count.saturating_add(1);

                    let mut probe_error: Option<String> = None;
                    let found_active_owner = match &result {
                        Ok(kad::GetRecordOk::FoundRecord(record)) => {
                            if let Ok(value) = String::from_utf8(record.record.value.clone()) {
                                if let Some(existing) =
                                    parse_verified_name_record(&probe.name, &value)
                                {
                                    !existing.is_expired() && existing.key != probe.pubkey_hex
                                } else {
                                    probe_error = Some(
                                        "invalid existing name record; refusing claim".to_string(),
                                    );
                                    false
                                }
                            } else {
                                probe_error = Some(
                                    "invalid existing name record; refusing claim".to_string(),
                                );
                                false
                            }
                        }
                        Err(kad::GetRecordError::NotFound { .. }) => false,
                        Err(err) => {
                            probe_error = Some(format!("name lookup failed: {err}"));
                            false
                        }
                        _ => false,
                    };

                    probe.found_owner |= found_active_owner;

                    if let Some(err) = probe_error {
                        let _ = probe.respond_to.send(Err(err));
                    } else if probe.found_owner {
                        let _ = probe
                            .respond_to
                            .send(Err("name already claimed by another key".to_string()));
                    } else if probe.probe_count >= 3 {
                        handle_claim_name_lookup_result(
                            PendingClaimGet {
                                name: probe.name,
                                pubkey_hex: probe.pubkey_hex,
                                respond_to: probe.respond_to,
                            },
                            result,
                            swarm,
                            pending_claim_put,
                            site_signing_key,
                            moderation_engine,
                            local_record_store,
                            local_records,
                        );
                    } else {
                        let rpc_tx_for_retry = rpc_tx.clone();
                        tokio::spawn(async move {
                            tokio::time::sleep(Duration::from_secs(5)).await;
                            let _ = rpc_tx_for_retry
                                .send(RpcCommand::RetryNameProbe {
                                    name: probe.name,
                                    pubkey_hex: probe.pubkey_hex,
                                    probe_count: probe.probe_count,
                                    respond_to: probe.respond_to,
                                })
                                .await;
                        });
                    }
                } else if let Some(query) = get_site_queries.remove(&id) {
                    handle_get_site_query_result(
                        query,
                        result,
                        swarm,
                        get_site_tasks,
                        get_site_queries,
                        pending_provider_queries,
                        pending_block_requests,
                        moderation_engine,
                        local_record_store,
                        session_block_cache,
                    );
                }
            }
            kad::QueryResult::GetProviders(result) => {
                handle_get_providers_result(
                    id,
                    result,
                    swarm,
                    pending_provider_queries,
                    pending_block_requests,
                    get_site_tasks,
                );
            }
            kad::QueryResult::StartProviding(result) => match result {
                Ok(ok) => info!(key = ?ok.key, "provider announcement succeeded"),
                Err(err) => warn!(error = %err, "provider announcement failed"),
            },
            kad::QueryResult::RepublishProvider(result) => match result {
                Ok(ok) => info!(key = ?ok.key, "provider reannouncement succeeded"),
                Err(err) => warn!(error = %err, "provider reannouncement failed"),
            },
            _ => {}
        },
        libp2p::swarm::SwarmEvent::Behaviour(crate::LatticeBehaviourEvent::BlockFetch(event)) => {
            handle_block_fetch_event(
                event,
                swarm,
                pending_block_requests,
                get_site_tasks,
                pending_provider_queries,
                moderation_engine,
                local_record_store,
                session_block_cache,
            );
        }
        libp2p::swarm::SwarmEvent::Behaviour(crate::LatticeBehaviourEvent::Identify(
            identify::Event::Received { peer_id, info, .. },
        )) => {
            info!(peer = %peer_id, observed_addr = %info.observed_addr, "identify received");
            for addr in info.listen_addrs {
                if addr_is_loopback_or_private(&addr) {
                    continue;
                }
                swarm.behaviour_mut().kademlia.add_address(&peer_id, addr);
            }
        }
        libp2p::swarm::SwarmEvent::Behaviour(crate::LatticeBehaviourEvent::Identify(_)) => {}
        libp2p::swarm::SwarmEvent::Behaviour(crate::LatticeBehaviourEvent::Autonat(event)) => {
            info!(event = ?event, "autonat event");
            if let autonat::Event::StatusChanged { new, .. } = &event {
                info!(status = ?new, "NAT status changed");
            }
        }
        libp2p::swarm::SwarmEvent::Behaviour(crate::LatticeBehaviourEvent::Relay(event)) => {
            info!(event = ?event, "relay event");
        }
        libp2p::swarm::SwarmEvent::Behaviour(crate::LatticeBehaviourEvent::RelayClient(event)) => {
            info!(event = ?event, "relay client event");
            if let relay::client::Event::ReservationReqAccepted { relay_peer_id, .. } = event {
                relay_reservations.insert(relay_peer_id);
                relay_reservation_requests.remove(&relay_peer_id);
                info!(
                    relay = %relay_peer_id,
                    "relay reservation accepted — node reachable via relay"
                );

                let names = {
                    let guard = match owned_names.lock() {
                        Ok(guard) => guard,
                        Err(poisoned) => {
                            error!("owned_names mutex poisoned — recovering");
                            poisoned.into_inner()
                        }
                    };
                    guard.iter().cloned().collect::<Vec<_>>()
                };

                if !names.is_empty() {
                    let rpc_tx_for_refresh = rpc_tx.clone();
                    tokio::spawn(async move {
                        for name in names {
                            let (tx, _rx) = oneshot::channel();
                            let _ = rpc_tx_for_refresh
                                .send(RpcCommand::ClaimName {
                                    name,
                                    pubkey_hex: String::new(),
                                    respond_to: tx,
                                })
                                .await;
                            tokio::time::sleep(Duration::from_millis(100)).await;
                        }
                    });
                }

                info!(relay = %relay_peer_id, "relay reservation accepted — scheduling DHT record re-push");
                let rpc_tx_relay = rpc_tx.clone();
                tokio::spawn(async move {
                    let _ = rpc_tx_relay.send(RpcCommand::RepublishLocalRecords).await;
                });
            }
        }
        libp2p::swarm::SwarmEvent::Behaviour(crate::LatticeBehaviourEvent::Dcutr(event)) => {
            info!(event = ?event, "dcutr event");
        }
        libp2p::swarm::SwarmEvent::Behaviour(crate::LatticeBehaviourEvent::Gossipsub(event)) => {
            info!(event = ?event, "gossipsub event");
        }
        libp2p::swarm::SwarmEvent::IncomingConnectionError { error, .. } => {
            warn!(error = %error, "incoming connection error");
        }
        libp2p::swarm::SwarmEvent::OutgoingConnectionError { peer_id, error, .. } => {
            if let Some(peer_id) = peer_id {
                if bootstrap_peer_ids.contains(&peer_id)
                    && relay_reservation_requests.remove(&peer_id).is_some()
                {
                    warn!(
                        relay = %peer_id,
                        error = %error,
                        "relay reservation failed"
                    );
                }
            }
            warn!(peer = ?peer_id, error = %error, "outgoing connection error");
        }
        libp2p::swarm::SwarmEvent::ExternalAddrConfirmed { address } => {
            info!(addr = %address, "external address confirmed");
        }
        libp2p::swarm::SwarmEvent::ExternalAddrExpired { address } => {
            warn!(addr = %address, "external address expired");
        }
        libp2p::swarm::SwarmEvent::ListenerError { error, .. } => {
            error!(error = %error, "listener error");
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::{local_name_record_owned_by_key, should_rate_limit_existing_name_claim};
    use ed25519_dalek::SigningKey;
    use lattice_daemon::names::NameRecord;
    use lattice_daemon::store::LocalRecordStore;
    use std::collections::HashMap;
    use tempfile::tempdir;

    fn signing_key(seed: u8) -> SigningKey {
        SigningKey::from_bytes(&[seed; 32])
    }

    fn signed_name_record_json(seed: u8, name: &str) -> (String, Vec<u8>) {
        let key = signing_key(seed);
        let pubkey_hex = hex::encode(key.verifying_key().to_bytes());
        let record = NameRecord::new_signed(pubkey_hex.clone(), name, &key);
        (
            pubkey_hex,
            serde_json::to_vec(&record).expect("serialize name record"),
        )
    }

    #[test]
    fn skips_rate_limit_for_existing_owned_name_claim() {
        assert!(!should_rate_limit_existing_name_claim(
            Some("owner"),
            "owner"
        ));
        assert!(should_rate_limit_existing_name_claim(
            Some("other"),
            "owner"
        ));
        assert!(should_rate_limit_existing_name_claim(None, "owner"));
    }

    #[test]
    fn republishing_existing_owned_name_does_not_consume_rate_limit_window() {
        let dir = tempdir().expect("tempdir");
        let store = LocalRecordStore::open(dir.path(), [42; 32]).expect("open store");
        let key_b64 = "owner-key";
        store
            .check_and_update_claim_rate_limit(key_b64, 10_000)
            .expect("first claim");

        let mut local_records = HashMap::new();
        let (pubkey_hex, value) = signed_name_record_json(9, "lattice");
        local_records.insert("name:lattice".to_string(), value);
        assert!(local_name_record_owned_by_key(
            &local_records,
            "lattice",
            &pubkey_hex,
        ));

        let last_claim = store
            .get_last_claim_ts(key_b64)
            .expect("read claim ts")
            .expect("claim ts present");
        assert_eq!(last_claim, 10_000);
    }
}
