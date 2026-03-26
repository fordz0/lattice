use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use base64::Engine as _;
use lattice_core::moderation::{ModerationEngine, RuleAction};
use libp2p::kad;
use libp2p::request_response;
use libp2p::Swarm;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use tokio::sync::oneshot;
use tracing::warn;

use lattice_daemon::block_fetch::{self, BlockFetchRequest, BlockFetchResponse};
use lattice_daemon::cache::{CachePolicy, SessionBlockCache};
use lattice_daemon::mime;
use lattice_daemon::rpc::{GetSiteResponse, SiteFile};
use lattice_daemon::site_helpers::{
    cached_manifest_json, file_block_hashes, hex_encode, site_manifest_key, site_name_from_site_key,
};
use lattice_daemon::store::LocalRecordStore;

use crate::{LatticeBehaviour, MAX_GET_SITE_TOTAL_BYTES};
use lattice_daemon::moderation_helpers::{
    action_name, block_ingest_rule, hide_block_rule, hide_record_rule, quarantine_record,
    site_manifest_publisher_b64,
};

pub struct GetSiteTask {
    pub respond_to: oneshot::Sender<Result<GetSiteResponse, String>>,
    pub requested_name: String,
    pub manifest: Option<lattice_site::manifest::SiteManifest>,
    pub next_file_index: usize,
    pub active_file: Option<ActiveFileDownload>,
    pub total_bytes: u64,
    pub files: Vec<SiteFile>,
}

pub struct ActiveFileDownload {
    pub path: String,
    pub expected_hash: String,
    pub block_hashes: Vec<String>,
    pub next_block_index: usize,
    pub bytes: Vec<u8>,
}

pub enum GetSiteQuery {
    Manifest { task_id: u64 },
    NameOwner { task_id: u64, name: String },
}

pub enum BlockConsumer {
    Rpc {
        respond_to: oneshot::Sender<Option<String>>,
    },
    SiteTask {
        task_id: u64,
    },
}

pub struct PendingProviderQuery {
    pub site_key: String,
    pub site_name: String,
    pub hash: String,
    pub consumer: BlockConsumer,
}

/// Caches recently discovered providers for each site so that subsequent
/// block lookups skip the expensive DHT `get_providers` round-trip.
pub struct SiteProviderCache {
    entries: HashMap<String, (Vec<libp2p::PeerId>, std::time::Instant)>,
}

impl SiteProviderCache {
    pub fn new() -> Self {
        Self {
            entries: HashMap::new(),
        }
    }

    /// Return cached providers if present and younger than 60 seconds.
    pub fn get(&self, site_key: &str) -> Option<&[libp2p::PeerId]> {
        self.entries.get(site_key).and_then(|(peers, ts)| {
            if ts.elapsed() < std::time::Duration::from_secs(60) && !peers.is_empty() {
                Some(peers.as_slice())
            } else {
                None
            }
        })
    }

    pub fn insert(&mut self, site_key: String, peers: Vec<libp2p::PeerId>) {
        self.entries.insert(site_key, (peers, std::time::Instant::now()));
    }
}

pub struct PendingBlockRequest {
    pub site_key: String,
    pub site_name: String,
    pub hash: String,
    pub remaining_peers: Vec<libp2p::PeerId>,
    pub consumer: BlockConsumer,
}

pub fn block_lookup_not_found(
    consumer: BlockConsumer,
    get_site_tasks: &mut HashMap<u64, GetSiteTask>,
    hash: &str,
) {
    match consumer {
        BlockConsumer::Rpc { respond_to } => {
            let _ = respond_to.send(None);
        }
        BlockConsumer::SiteTask { task_id } => {
            if let Some(task) = get_site_tasks.remove(&task_id) {
                let _ = task.respond_to.send(Err(format!("block missing: {hash}")));
            }
        }
    }
}

pub fn block_lookup_failed(
    consumer: BlockConsumer,
    get_site_tasks: &mut HashMap<u64, GetSiteTask>,
    message: String,
) {
    match consumer {
        BlockConsumer::Rpc { respond_to } => {
            let _ = respond_to.send(None);
        }
        BlockConsumer::SiteTask { task_id } => {
            if let Some(task) = get_site_tasks.remove(&task_id) {
                let _ = task.respond_to.send(Err(message));
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub fn start_block_lookup(
    swarm: &mut Swarm<LatticeBehaviour>,
    moderation_engine: &ModerationEngine,
    local_record_store: &LocalRecordStore,
    session_block_cache: &mut SessionBlockCache,
    pending_provider_queries: &mut HashMap<kad::QueryId, PendingProviderQuery>,
    pending_block_requests: &mut HashMap<block_fetch::OutboundRequestId, PendingBlockRequest>,
    get_site_tasks: &mut HashMap<u64, GetSiteTask>,
    hash: String,
    site_key: String,
    consumer: BlockConsumer,
    site_provider_cache: &mut SiteProviderCache,
) {
    if let Some(site_name) = site_name_from_site_key(&site_key) {
        if let Some(rule) = hide_block_rule(moderation_engine, site_name, &hash) {
            warn!(
                rule_id = %rule.id,
                rule_kind = ?rule.kind,
                matched_value = %rule.value,
                action = %action_name(&rule.action),
                site = %site_name,
                hash = %hash,
                "block hidden by moderation rule"
            );
            block_lookup_not_found(consumer, get_site_tasks, &hash);
            return;
        }
    }

    if let Some(bytes) = session_block_cache.get(&hash).cloned() {
        handle_resolved_block(
            consumer,
            hash,
            bytes,
            site_key,
            local_record_store,
            session_block_cache,
            get_site_tasks,
            pending_provider_queries,
            pending_block_requests,
            moderation_engine,
            swarm,
            site_provider_cache,
        );
        return;
    }

    if let Ok(Some(bytes)) = local_record_store.get_block(&hash) {
        let _ = local_record_store.touch_block(&hash);
        handle_resolved_block(
            consumer,
            hash,
            bytes,
            site_key,
            local_record_store,
            session_block_cache,
            get_site_tasks,
            pending_provider_queries,
            pending_block_requests,
            moderation_engine,
            swarm,
            site_provider_cache,
        );
        return;
    }

    let Some(site_name) = site_name_from_site_key(&site_key).map(str::to_string) else {
        block_lookup_failed(
            consumer,
            get_site_tasks,
            "invalid site key for block lookup".to_string(),
        );
        return;
    };

    // Use cached providers if available to skip the expensive DHT query.
    if let Some(cached_peers) = site_provider_cache.get(&site_key) {
        let mut remaining_peers: Vec<_> = cached_peers
            .iter()
            .filter(|peer| *peer != swarm.local_peer_id())
            .cloned()
            .collect();
        if let Some(peer) = remaining_peers.pop() {
            let request_id = swarm.behaviour_mut().block_fetch.send_request(
                &peer,
                BlockFetchRequest {
                    block_hash: hash.clone(),
                    site_key: site_key.clone(),
                },
            );
            pending_block_requests.insert(
                request_id,
                PendingBlockRequest {
                    site_key,
                    site_name,
                    hash,
                    remaining_peers,
                    consumer,
                },
            );
            return;
        }
    }

    let query_id =
        lattice_daemon::dht::get_providers(&mut swarm.behaviour_mut().kademlia, site_key.clone());
    pending_provider_queries.insert(
        query_id,
        PendingProviderQuery {
            site_key,
            site_name,
            hash,
            consumer,
        },
    );
}

pub fn handle_get_providers_result(
    id: kad::QueryId,
    result: kad::GetProvidersResult,
    swarm: &mut Swarm<LatticeBehaviour>,
    pending_provider_queries: &mut HashMap<kad::QueryId, PendingProviderQuery>,
    pending_block_requests: &mut HashMap<block_fetch::OutboundRequestId, PendingBlockRequest>,
    get_site_tasks: &mut HashMap<u64, GetSiteTask>,
    site_provider_cache: &mut SiteProviderCache,
) {
    let Some(pending) = pending_provider_queries.remove(&id) else {
        return;
    };

    match result {
        Ok(kad::GetProvidersOk::FoundProviders { providers, .. }) if !providers.is_empty() => {
            let mut remaining_peers = providers
                .into_iter()
                .filter(|peer| peer != swarm.local_peer_id())
                .collect::<Vec<_>>();
            // Cache providers so subsequent blocks skip the DHT round-trip.
            site_provider_cache.insert(pending.site_key.clone(), remaining_peers.clone());
            if let Some(peer) = remaining_peers.pop() {
                let request_id = swarm.behaviour_mut().block_fetch.send_request(
                    &peer,
                    BlockFetchRequest {
                        block_hash: pending.hash.clone(),
                        site_key: pending.site_key.clone(),
                    },
                );
                pending_block_requests.insert(
                    request_id,
                    PendingBlockRequest {
                        site_key: pending.site_key,
                        site_name: pending.site_name,
                        hash: pending.hash,
                        remaining_peers,
                        consumer: pending.consumer,
                    },
                );
            } else {
                block_lookup_not_found(pending.consumer, get_site_tasks, &pending.hash);
            }
        }
        Ok(_) => block_lookup_not_found(pending.consumer, get_site_tasks, &pending.hash),
        Err(err) => {
            warn!(hash = %pending.hash, error = %err, "provider lookup failed");
            block_lookup_not_found(pending.consumer, get_site_tasks, &pending.hash);
        }
    }
}

pub fn try_next_block_provider(
    swarm: &mut Swarm<LatticeBehaviour>,
    pending_block_requests: &mut HashMap<block_fetch::OutboundRequestId, PendingBlockRequest>,
    mut pending: PendingBlockRequest,
) -> Option<PendingBlockRequest> {
    while let Some(peer) = pending.remaining_peers.pop() {
        if peer == *swarm.local_peer_id() {
            continue;
        }
        let request_id = swarm.behaviour_mut().block_fetch.send_request(
            &peer,
            BlockFetchRequest {
                block_hash: pending.hash.clone(),
                site_key: pending.site_key.clone(),
            },
        );
        pending_block_requests.insert(request_id, pending);
        return None;
    }
    Some(pending)
}

#[allow(clippy::too_many_arguments)]
pub fn handle_resolved_block(
    consumer: BlockConsumer,
    hash: String,
    bytes: Vec<u8>,
    site_key: String,
    local_record_store: &LocalRecordStore,
    session_block_cache: &mut SessionBlockCache,
    get_site_tasks: &mut HashMap<u64, GetSiteTask>,
    pending_provider_queries: &mut HashMap<kad::QueryId, PendingProviderQuery>,
    pending_block_requests: &mut HashMap<block_fetch::OutboundRequestId, PendingBlockRequest>,
    moderation_engine: &ModerationEngine,
    swarm: &mut Swarm<LatticeBehaviour>,
    site_provider_cache: &mut SiteProviderCache,
) {
    match consumer {
        BlockConsumer::Rpc { respond_to } => {
            let _ = respond_to.send(Some(hex_encode(&bytes)));
        }
        BlockConsumer::SiteTask { task_id } => {
            handle_site_task_block_bytes(
                task_id,
                hash,
                bytes,
                site_key,
                swarm,
                get_site_tasks,
                pending_provider_queries,
                pending_block_requests,
                session_block_cache,
                moderation_engine,
                local_record_store,
                site_provider_cache,
            );
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub fn handle_block_fetch_event(
    event: block_fetch::Event,
    swarm: &mut Swarm<LatticeBehaviour>,
    pending_block_requests: &mut HashMap<block_fetch::OutboundRequestId, PendingBlockRequest>,
    get_site_tasks: &mut HashMap<u64, GetSiteTask>,
    pending_provider_queries: &mut HashMap<kad::QueryId, PendingProviderQuery>,
    moderation_engine: &ModerationEngine,
    local_record_store: &LocalRecordStore,
    session_block_cache: &mut SessionBlockCache,
    site_provider_cache: &mut SiteProviderCache,
) {
    match event {
        request_response::Event::Message { message, .. } => match message {
            request_response::Message::Request {
                request, channel, ..
            } => {
                let response = match site_name_from_site_key(&request.site_key) {
                    Some(site_name) => {
                        let publisher_hidden =
                            cached_manifest_json(&mut swarm.behaviour_mut().kademlia, site_name)
                                .and_then(|manifest_json| {
                                    site_manifest_publisher_b64(&manifest_json)
                                })
                                .and_then(|publisher_b64| {
                                    hide_record_rule(
                                        moderation_engine,
                                        &request.site_key,
                                        Some(&publisher_b64),
                                    )
                                })
                                .is_some();
                        if publisher_hidden
                            || hide_block_rule(moderation_engine, site_name, &request.block_hash)
                                .is_some()
                        {
                            BlockFetchResponse {
                                block_hash: request.block_hash,
                                data: None,
                                reason: Some("block hidden".to_string()),
                            }
                        } else if let Some(bytes) =
                            session_block_cache.get(&request.block_hash).cloned()
                        {
                            BlockFetchResponse {
                                block_hash: request.block_hash,
                                data: Some(bytes),
                                reason: None,
                            }
                        } else {
                            match local_record_store.get_block(&request.block_hash) {
                                Ok(Some(bytes)) => {
                                    let _ = local_record_store.touch_block(&request.block_hash);
                                    BlockFetchResponse {
                                        block_hash: request.block_hash,
                                        data: Some(bytes),
                                        reason: None,
                                    }
                                }
                                Ok(None) => BlockFetchResponse {
                                    block_hash: request.block_hash,
                                    data: None,
                                    reason: Some("block not found".to_string()),
                                },
                                Err(err) => BlockFetchResponse {
                                    block_hash: request.block_hash,
                                    data: None,
                                    reason: Some(err.to_string()),
                                },
                            }
                        }
                    }
                    None => {
                        if let Some(bytes) = session_block_cache.get(&request.block_hash).cloned() {
                            BlockFetchResponse {
                                block_hash: request.block_hash,
                                data: Some(bytes),
                                reason: None,
                            }
                        } else {
                            match local_record_store.get_block(&request.block_hash) {
                                Ok(Some(bytes)) => {
                                    let _ = local_record_store.touch_block(&request.block_hash);
                                    BlockFetchResponse {
                                        block_hash: request.block_hash,
                                        data: Some(bytes),
                                        reason: None,
                                    }
                                }
                                Ok(None) => BlockFetchResponse {
                                    block_hash: request.block_hash,
                                    data: None,
                                    reason: Some("block not found".to_string()),
                                },
                                Err(err) => BlockFetchResponse {
                                    block_hash: request.block_hash,
                                    data: None,
                                    reason: Some(err.to_string()),
                                },
                            }
                        }
                    }
                };
                let _ = swarm
                    .behaviour_mut()
                    .block_fetch
                    .send_response(channel, response);
            }
            request_response::Message::Response {
                request_id,
                response,
            } => {
                let Some(pending) = pending_block_requests.remove(&request_id) else {
                    return;
                };

                if let Some(data) = response.data {
                    let actual_hash = hex::encode(Sha256::digest(&data));
                    if actual_hash != pending.hash {
                        warn!(
                            expected = %pending.hash,
                            actual = %actual_hash,
                            "discarded block fetch response with hash mismatch"
                        );
                        if let Some(pending) =
                            try_next_block_provider(swarm, pending_block_requests, pending)
                        {
                            block_lookup_failed(
                                pending.consumer,
                                get_site_tasks,
                                "all block providers returned invalid data".to_string(),
                            );
                        }
                        return;
                    }

                    if let Some(rule) =
                        block_ingest_rule(moderation_engine, &pending.site_name, &pending.hash)
                    {
                        warn!(
                            rule_id = %rule.id,
                            rule_kind = ?rule.kind,
                            matched_value = %rule.value,
                            action = %action_name(&rule.action),
                            site = %pending.site_name,
                            hash = %pending.hash,
                            "moderation rule matched on block ingest"
                        );
                        let _ = local_record_store.remove_block(&pending.hash);
                        if rule.action == RuleAction::Quarantine {
                            quarantine_record(
                                local_record_store,
                                rule,
                                None,
                                None,
                                Some(pending.hash.clone()),
                                Some(pending.site_name.clone()),
                            );
                        }
                        block_lookup_not_found(pending.consumer, get_site_tasks, &pending.hash);
                        return;
                    }

                    match local_record_store.is_site_pinned(&pending.site_name) {
                        Ok(true) => {
                            if let Err(err) = local_record_store.put_block(
                                &pending.hash,
                                &data,
                                &pending.site_name,
                                CachePolicy::Pinned,
                            ) {
                                warn!(hash = %pending.hash, error = %err, "failed to persist pinned block");
                            }
                        }
                        Ok(false) => {
                            session_block_cache.insert(pending.hash.clone(), data.clone());
                        }
                        Err(err) => {
                            warn!(site = %pending.site_name, error = %err, "failed to determine site pin status");
                            session_block_cache.insert(pending.hash.clone(), data.clone());
                        }
                    }

                    handle_resolved_block(
                        pending.consumer,
                        pending.hash,
                        data,
                        pending.site_key,
                        local_record_store,
                        session_block_cache,
                        get_site_tasks,
                        pending_provider_queries,
                        pending_block_requests,
                        moderation_engine,
                        swarm,
                        site_provider_cache,
                    );
                } else if let Some(pending) =
                    try_next_block_provider(swarm, pending_block_requests, pending)
                {
                    block_lookup_not_found(pending.consumer, get_site_tasks, &pending.hash);
                }
            }
        },
        request_response::Event::OutboundFailure {
            request_id, error, ..
        } => {
            if let Some(pending) = pending_block_requests.remove(&request_id) {
                warn!(hash = %pending.hash, error = %error, "block fetch request failed");
                if let Some(pending) =
                    try_next_block_provider(swarm, pending_block_requests, pending)
                {
                    block_lookup_failed(
                        pending.consumer,
                        get_site_tasks,
                        "all block fetch attempts failed".to_string(),
                    );
                }
            }
        }
        request_response::Event::InboundFailure { error, .. } => {
            warn!(error = %error, "block fetch inbound failure");
        }
        request_response::Event::ResponseSent { .. } => {}
    }
}

#[allow(clippy::too_many_arguments)]
pub fn drive_get_site_task(
    task_id: u64,
    mut task: GetSiteTask,
    swarm: &mut Swarm<LatticeBehaviour>,
    get_site_tasks: &mut HashMap<u64, GetSiteTask>,
    pending_provider_queries: &mut HashMap<kad::QueryId, PendingProviderQuery>,
    pending_block_requests: &mut HashMap<block_fetch::OutboundRequestId, PendingBlockRequest>,
    moderation_engine: &ModerationEngine,
    local_record_store: &LocalRecordStore,
    session_block_cache: &mut SessionBlockCache,
    site_provider_cache: &mut SiteProviderCache,
) {
    loop {
        let next_block_hash = match next_task_block_hash(&mut task) {
            Ok(Some(hash)) => hash,
            Ok(None) => {
                let manifest = match task.manifest.as_ref() {
                    Some(manifest) => manifest,
                    None => {
                        let _ = task
                            .respond_to
                            .send(Err("site task missing manifest".to_string()));
                        return;
                    }
                };
                // Announce ourselves as a provider so other nodes can
                // fetch these blocks from us even if the original
                // publisher goes offline.
                let site_key = site_manifest_key(&task.requested_name);
                let _ = lattice_daemon::dht::start_providing(
                    &mut swarm.behaviour_mut().kademlia,
                    site_key,
                );
                let response = GetSiteResponse {
                    name: manifest.name.clone(),
                    version: manifest.version,
                    files: task.files,
                };
                let _ = task.respond_to.send(Ok(response));
                return;
            }
            Err(err) => {
                let _ = task.respond_to.send(Err(err));
                return;
            }
        };

        if let Some(raw_bytes) = session_block_cache.get(&next_block_hash).cloned() {
            if let Err(err) = append_block_to_site_task(&mut task, &next_block_hash, raw_bytes) {
                let _ = task.respond_to.send(Err(err));
                return;
            }
        } else {
            match local_record_store.get_block(&next_block_hash) {
                Ok(Some(raw_bytes)) => {
                    let _ = local_record_store.touch_block(&next_block_hash);
                    if let Err(err) =
                        append_block_to_site_task(&mut task, &next_block_hash, raw_bytes)
                    {
                        let _ = task.respond_to.send(Err(err));
                        return;
                    }
                }
                Ok(None) => {
                    let site_key = site_manifest_key(&task.requested_name);
                    get_site_tasks.insert(task_id, task);
                    start_block_lookup(
                        swarm,
                        moderation_engine,
                        local_record_store,
                        session_block_cache,
                        pending_provider_queries,
                        pending_block_requests,
                        get_site_tasks,
                        next_block_hash,
                        site_key,
                        BlockConsumer::SiteTask { task_id },
                        site_provider_cache,
                    );
                    return;
                }
                Err(err) => {
                    let _ = task.respond_to.send(Err(err.to_string()));
                    return;
                }
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub fn handle_site_task_block_bytes(
    task_id: u64,
    hash: String,
    bytes: Vec<u8>,
    _site_key: String,
    swarm: &mut Swarm<LatticeBehaviour>,
    get_site_tasks: &mut HashMap<u64, GetSiteTask>,
    pending_provider_queries: &mut HashMap<kad::QueryId, PendingProviderQuery>,
    pending_block_requests: &mut HashMap<block_fetch::OutboundRequestId, PendingBlockRequest>,
    session_block_cache: &mut SessionBlockCache,
    moderation_engine: &ModerationEngine,
    local_record_store: &LocalRecordStore,
    site_provider_cache: &mut SiteProviderCache,
) {
    let Some(mut task) = get_site_tasks.remove(&task_id) else {
        return;
    };

    if let Err(err) = append_block_to_site_task(&mut task, &hash, bytes) {
        let _ = task.respond_to.send(Err(err));
        return;
    }

    drive_get_site_task(
        task_id,
        task,
        swarm,
        get_site_tasks,
        pending_provider_queries,
        pending_block_requests,
        moderation_engine,
        local_record_store,
        session_block_cache,
        site_provider_cache,
    );
}

pub fn append_block_to_site_task(
    task: &mut GetSiteTask,
    block_hash: &str,
    raw_bytes: Vec<u8>,
) -> Result<(), String> {
    let actual_hash = hex::encode(Sha256::digest(&raw_bytes));
    if actual_hash != block_hash {
        return Err(format!(
            "block hash mismatch for chunk {}: expected {} got {}",
            block_hash, block_hash, actual_hash
        ));
    }
    let next_total = task.total_bytes.saturating_add(raw_bytes.len() as u64);
    if next_total > MAX_GET_SITE_TOTAL_BYTES {
        return Err("site exceeds maximum total size".to_string());
    }
    task.total_bytes = next_total;

    let Some(active) = task.active_file.as_mut() else {
        return Err("site task missing active file".to_string());
    };
    active.bytes.extend_from_slice(&raw_bytes);

    if active.next_block_index < active.block_hashes.len() {
        return Ok(());
    }

    let Some(finished) = task.active_file.take() else {
        return Err("site task missing active file".to_string());
    };
    let file_hash = hex::encode(Sha256::digest(&finished.bytes));
    if file_hash != finished.expected_hash {
        return Err(format!(
            "file hash mismatch for {}: expected {} got {}",
            finished.path, finished.expected_hash, file_hash
        ));
    }
    task.files.push(SiteFile {
        path: finished.path.clone(),
        contents: BASE64_STANDARD.encode(&finished.bytes),
        mime_type: mime::detect_mime(&finished.path, &finished.bytes),
    });
    Ok(())
}

pub fn next_task_block_hash(task: &mut GetSiteTask) -> std::result::Result<Option<String>, String> {
    if let Some(active) = task.active_file.as_mut() {
        if active.next_block_index < active.block_hashes.len() {
            let hash = active.block_hashes[active.next_block_index].clone();
            active.next_block_index += 1;
            return Ok(Some(hash));
        }
    }
    start_next_file_download(task)
}

pub fn start_next_file_download(
    task: &mut GetSiteTask,
) -> std::result::Result<Option<String>, String> {
    let manifest = task
        .manifest
        .as_ref()
        .ok_or_else(|| "site task missing manifest".to_string())?;

    if task.next_file_index >= manifest.files.len() {
        task.active_file = None;
        return Ok(None);
    }

    let file = manifest.files[task.next_file_index].clone();
    task.next_file_index += 1;

    let block_hashes = file_block_hashes(&file);
    if block_hashes.is_empty() {
        return Err(format!(
            "invalid site manifest: {} has no chunks",
            file.path
        ));
    }
    let first_hash = block_hashes[0].clone();
    task.active_file = Some(ActiveFileDownload {
        path: file.path,
        expected_hash: file.hash,
        block_hashes,
        next_block_index: 1,
        bytes: Vec::new(),
    });
    Ok(Some(first_hash))
}
