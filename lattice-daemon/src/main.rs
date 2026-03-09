use anyhow::{bail, Context, Result};
use aes_gcm::aead::{Aead, KeyInit};
use aes_gcm::{Aes256Gcm, Nonce};
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use base64::Engine as _;
use ed25519_dalek::SigningKey;
use lattice_core::app_namespace::{validate_app_key, validate_fray_dht_key};
use lattice_core::identity::SignedRecord;
use lattice_core::moderation::{ModerationEngine, ModerationRule, RuleAction, RuleKind};
use lattice_daemon::block_fetch::{self, BlockFetchRequest, BlockFetchResponse};
use lattice_daemon::cache::{BlockCacheMeta, CachePolicy, SessionBlockCache};
use lattice_daemon::config::load_or_create_config;
use lattice_daemon::dht;
use lattice_daemon::http_server;
use lattice_daemon::mime;
use lattice_daemon::names::NameRecord;
use lattice_daemon::node::{
    load_or_create_block_cache_key, load_or_create_identity, load_or_create_site_signing_key,
};
use lattice_daemon::proxy_server;
use lattice_daemon::rpc::{
    self, GetSiteManifestResponse, GetSiteResponse, KnownPublisher, KnownPublisherStatus,
    NodeInfoResponse, PublishSiteOk, QuarantineEntryResponse, RpcCommand, SiteFile, TrustState,
    TrustedPublisher,
};
use lattice_daemon::tls;
use lattice_daemon::transport;
use lattice_site::manifest::{hash_bytes, verify_manifest, SiteManifest, DEFAULT_CHUNK_SIZE_BYTES};
use lattice_site::publisher as site_publisher;
use libp2p::autonat;
use libp2p::dcutr;
use libp2p::futures::StreamExt;
use libp2p::gossipsub;
use libp2p::identify;
use libp2p::kad;
use libp2p::kad::store::RecordStore as _;
use libp2p::mdns;
use libp2p::multiaddr::Protocol;
use libp2p::request_response;
use libp2p::relay;
use libp2p::swarm::NetworkBehaviour;
use libp2p::{Multiaddr, Swarm};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::Path;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::{mpsc, oneshot};
use tokio::time::Duration;
use tracing::{error, info, warn};
use uuid::Uuid;
use rand::rngs::OsRng;
use rand::RngCore;

const MAX_CONCURRENT_GET_SITE: usize = 50;
const MAX_CONCURRENT_PUBLISH: usize = 10;
const MAX_GET_SITE_FILES: usize = 1000;
const MAX_GET_SITE_TOTAL_BYTES: u64 = 100 * 1024 * 1024;
const PUBLISH_OWNERSHIP_PROBES: u32 = 3;
const PUBLISH_OWNERSHIP_PROBE_DELAY_SECS: u64 = 5;
const RELAY_RESERVATION_RETRY_SECS: u64 = 60;
const GET_RECORD_MAX_ATTEMPTS: u8 = 3;
const LOCAL_RECORDS_DB_DIR: &str = "records_db";
const LOCAL_RECORD_GC_MAX_AGE_SECS: u64 = 30 * 24 * 60 * 60;
const LOCAL_RECORD_GC_MAX_BYTES: usize = 512 * 1024 * 1024;
const BLOCK_CACHE_MAX_AGE_SECS: u64 = 48 * 60 * 60;

#[derive(NetworkBehaviour)]
pub struct LatticeBehaviour {
    kademlia: kad::Behaviour<kad::store::MemoryStore>,
    block_fetch: block_fetch::Behaviour,
    mdns: mdns::tokio::Behaviour,
    gossipsub: gossipsub::Behaviour,
    identify: identify::Behaviour,
    autonat: autonat::Behaviour,
    relay: relay::Behaviour,
    relay_client: relay::client::Behaviour,
    dcutr: dcutr::Behaviour,
}

struct PublishTask {
    respond_to: oneshot::Sender<Result<PublishSiteOk, String>>,
    remaining: u32,
    failed: Option<String>,
    had_quorum_failed: bool,
    version: u64,
    file_count: usize,
    claimed: bool,
    connected_peers_at_start: usize,
    manifest_record: Option<(String, Vec<u8>)>,
}

struct PublishQuery {
    task_id: u64,
}

struct GetSiteTask {
    respond_to: oneshot::Sender<Result<GetSiteResponse, String>>,
    requested_name: String,
    manifest: Option<SiteManifest>,
    next_file_index: usize,
    active_file: Option<ActiveFileDownload>,
    total_bytes: u64,
    files: Vec<SiteFile>,
}

struct ActiveFileDownload {
    path: String,
    expected_hash: String,
    block_hashes: Vec<String>,
    next_block_index: usize,
    bytes: Vec<u8>,
}

enum GetSiteQuery {
    Manifest { task_id: u64 },
    NameOwner { task_id: u64, name: String },
}

struct PreparedPublish {
    version: u64,
    file_count: usize,
    blocks: Vec<(String, Vec<u8>)>,
    manifest_record: (String, Vec<u8>),
}

struct PendingTextQuery {
    key: String,
    attempts: u8,
    respond_to: oneshot::Sender<Option<String>>,
}

struct PendingManifestQuery {
    key: String,
    attempts: u8,
    respond_to: oneshot::Sender<Option<GetSiteManifestResponse>>,
}

enum BlockConsumer {
    Rpc {
        respond_to: oneshot::Sender<Option<String>>,
    },
    SiteTask {
        task_id: u64,
    },
}

struct PendingProviderQuery {
    site_key: String,
    site_name: String,
    hash: String,
    consumer: BlockConsumer,
}

struct PendingBlockRequest {
    site_key: String,
    site_name: String,
    hash: String,
    remaining_peers: Vec<libp2p::PeerId>,
    consumer: BlockConsumer,
}

struct PendingPut {
    key: String,
    value: Vec<u8>,
    respond_to: oneshot::Sender<Result<(), String>>,
}

struct PendingClaimGet {
    name: String,
    pubkey_hex: String,
    respond_to: oneshot::Sender<Result<(), String>>,
}

struct PendingClaimPut {
    name: String,
    respond_to: oneshot::Sender<Result<(), String>>,
}

struct PendingNameProbe {
    name: String,
    pubkey_hex: String,
    respond_to: oneshot::Sender<Result<(), String>>,
    probe_count: u32,
    found_owner: bool,
}

struct PendingPublishOwnershipCheck {
    name: String,
    site_dir: String,
    probe_count: u32,
    respond_to: oneshot::Sender<Result<PublishSiteOk, String>>,
}

struct PendingPublishVersionCheck {
    name: String,
    site_dir: String,
    respond_to: oneshot::Sender<Result<PublishSiteOk, String>>,
    claimed: bool,
}

struct PendingPublishClaimPut {
    name: String,
    site_dir: String,
    respond_to: oneshot::Sender<Result<PublishSiteOk, String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RecordMeta {
    created_at: u64,
    updated_at: u64,
}

#[derive(Debug, Default)]
struct LocalRecordGcStats {
    removed_records: usize,
    removed_bytes: usize,
}

#[derive(Debug, Default)]
struct BlockCacheGcStats {
    removed_blocks: usize,
    removed_bytes: usize,
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
    block_cipher: Aes256Gcm,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct QuarantineEntry {
    id: String,
    created_at: u64,
    matched_rule_id: String,
    matched_kind: String,
    matched_value: String,
    record_key: Option<String>,
    publisher: Option<String>,
    content_hash: Option<String>,
    site_name: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

    let config = load_or_create_config()?;
    let had_separate_site_key = config.data_dir.join("site_signing.key").exists();
    // site_signing.key signs published names/sites/app records; block_cache.key only encrypts
    // cached site blocks at rest. They are separate node-local keys with different purposes.
    let node_identity = load_or_create_identity(&config.data_dir)?;
    let site_signing_key = load_or_create_site_signing_key(&config.data_dir)?;
    let block_cache_key = load_or_create_block_cache_key(&config.data_dir)?;
    if !had_separate_site_key {
        warn!(
            "first run: generated new site signing key separate from p2p identity. Re-claim your names with: lattice name claim <name>"
        );
    }
    let local_pubkey_hex = hex::encode(site_signing_key.verifying_key().to_bytes());
    info!("site signing key loaded from site_signing.key");
    info!("your publisher key is: {}", local_pubkey_hex);
    let owned_names: Arc<Mutex<HashSet<String>>> = Arc::new(Mutex::new(HashSet::new()));
    let local_records_path = config.data_dir.join(LOCAL_RECORDS_DB_DIR);
    let local_record_store = LocalRecordStore::open(&local_records_path, block_cache_key)?;
    let mut moderation_engine =
        ModerationEngine::load(local_record_store.load_moderation_rules().unwrap_or_default());
    let mut session_block_cache = SessionBlockCache::new(config.session_cache_max_bytes);
    let gc_stats =
        local_record_store.gc_unpinned(LOCAL_RECORD_GC_MAX_AGE_SECS, LOCAL_RECORD_GC_MAX_BYTES)?;
    if gc_stats.removed_records > 0 {
        info!(
            removed_records = gc_stats.removed_records,
            removed_bytes = gc_stats.removed_bytes,
            "local record GC removed stale records"
        );
    }
    let block_gc_stats = local_record_store.gc_ephemeral_blocks(BLOCK_CACHE_MAX_AGE_SECS)?;
    if block_gc_stats.removed_blocks > 0 {
        info!(
            removed_blocks = block_gc_stats.removed_blocks,
            removed_bytes = block_gc_stats.removed_bytes,
            "block cache GC removed stale blocks"
        );
    }
    let mut local_records = local_record_store.load_records()?;

    let peer_id = node_identity.peer_id;

    let mut swarm = transport::build_swarm(
        node_identity.keypair,
        |key, relay_client| -> std::result::Result<
            LatticeBehaviour,
            Box<dyn std::error::Error + Send + Sync>,
        > {
            let mut kademlia = dht::new_kademlia(peer_id);
            dht::add_bootstrap_peers(&mut kademlia, &config.bootstrap_peers);
            let block_fetch = block_fetch::new_behaviour();

            let mdns = mdns::tokio::Behaviour::new(mdns::Config::default(), peer_id)?;
            let gossipsub = gossipsub::Behaviour::new(
                gossipsub::MessageAuthenticity::Signed(key.clone()),
                gossipsub::Config::default(),
            )?;
            let identify = identify::Behaviour::new(identify::Config::new(
                "/lattice/0.1.0".to_string(),
                key.public(),
            ));
            let autonat = autonat::Behaviour::new(peer_id, autonat::Config::default());
            let relay = relay::Behaviour::new(peer_id, relay::Config::default());
            let dcutr = dcutr::Behaviour::new(peer_id);

            Ok(LatticeBehaviour {
                kademlia,
                block_fetch,
                mdns,
                gossipsub,
                identify,
                autonat,
                relay,
                relay_client,
                dcutr,
            })
        },
    )?;

    let listen_addr: Multiaddr = Multiaddr::from_str(&format!(
        "/ip4/{}/tcp/{}",
        config.listen_address, config.listen_port
    ))?;
    swarm.listen_on(listen_addr)?;

    let quic_addr: Multiaddr = Multiaddr::from_str(&format!(
        "/ip4/{}/udp/{}/quic-v1",
        config.listen_address, config.listen_port
    ))?;
    swarm.listen_on(quic_addr)?;

    restore_local_records_to_store(&mut swarm, &local_records);
    reannounce_pinned_sites(&local_record_store, &moderation_engine, &mut swarm);
    let restored_owned_names = owned_names_from_local_records(&local_records, &local_pubkey_hex);
    if !restored_owned_names.is_empty() {
        let mut guard = match owned_names.lock() {
            Ok(guard) => guard,
            Err(poisoned) => {
                error!("owned_names mutex poisoned — recovering");
                poisoned.into_inner()
            }
        };
        guard.extend(restored_owned_names.clone());
        info!(
            count = restored_owned_names.len(),
            "restored owned names from persisted records"
        );
    }

    let bootstrap_peer_ids = build_bootstrap_peer_ids(&config.bootstrap_peers);
    let mut relay_reservations: HashSet<libp2p::PeerId> = HashSet::new();
    let mut relay_reservation_requests: HashMap<libp2p::PeerId, Instant> = HashMap::new();
    let mut relay_connection_counts: HashMap<libp2p::PeerId, usize> = HashMap::new();

    let (rpc_tx, mut rpc_rx) = mpsc::channel::<RpcCommand>(64);
    let _rpc_server = rpc::start_rpc_server(config.rpc_port, rpc_tx.clone()).await?;
    let tls_material = tls::load_or_create_local_tls(&config.data_dir)?;
    info!(
        path = %tls_material.ca_cert_path.display(),
        "local HTTPS CA certificate ready"
    );
    let http_port = config.http_port;
    let https_port = config.https_port;
    let proxy_port = config.proxy_port;
    let mime_policy_strict = config.mime_policy_strict;
    let http_rpc_tx = rpc_tx.clone();
    let http_ca_cert = Some(tls_material.ca_cert_pem.clone());
    let _http_server = tokio::spawn(async move {
        if let Err(err) = http_server::start_http_server(
            http_port,
            http_rpc_tx,
            http_ca_cert,
            mime_policy_strict,
        )
        .await
        {
            error!(error = %err, "http server exited");
        }
    });
    info!(http_port, "HTTP server listening");
    let https_rpc_tx = rpc_tx.clone();
    let https_ca_cert = tls_material.ca_cert_pem.clone();
    let https_cert_path = tls_material.server_cert_path.clone();
    let https_key_path = tls_material.server_key_path.clone();
    let _https_server = tokio::spawn(async move {
        if let Err(err) = http_server::start_https_server(
            https_port,
            https_rpc_tx,
            https_ca_cert,
            https_cert_path,
            https_key_path,
            mime_policy_strict,
        )
        .await
        {
            error!(error = %err, "https server exited");
        }
    });
    info!(https_port, "HTTPS server listening");
    let proxy_http_port = http_port;
    let proxy_ca_key_pem = tls_material.ca_key_pem.clone();
    let _proxy_server = tokio::spawn(async move {
        if let Err(err) =
            proxy_server::start_proxy_server(proxy_port, proxy_http_port, proxy_ca_key_pem).await
        {
            error!(error = %err, "proxy server exited");
        }
    });
    info!(proxy_port, "HTTP proxy server listening");

    info!(peer_id = %peer_id, "lattice daemon started");
    info!(
        port = config.listen_port,
        rpc_port = config.rpc_port,
        http_port = config.http_port,
        https_port = config.https_port,
        proxy_port = config.proxy_port,
        "listening and RPC configured"
    );

    if !local_records.is_empty() {
        info!(
            count = local_records.len(),
            path = %local_records_path.display(),
            "loaded persisted local DHT records"
        );
        let rpc_tx_repush = rpc_tx.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(5)).await;
            let _ = rpc_tx_repush.send(RpcCommand::RepublishLocalRecords).await;
        });
    }

    let heartbeat_rpc_tx = rpc_tx.clone();
    let heartbeat_owned_names = Arc::clone(&owned_names);
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(30)).await;
        loop {
            let (tx, rx) = oneshot::channel();
            let names = if heartbeat_rpc_tx
                .send(RpcCommand::ListNames { respond_to: tx })
                .await
                .is_ok()
            {
                rx.await.unwrap_or_default()
            } else {
                Vec::new()
            };

            let names = if names.is_empty() {
                let guard = match heartbeat_owned_names.lock() {
                    Ok(guard) => guard,
                    Err(poisoned) => {
                        error!("owned_names mutex poisoned — recovering");
                        poisoned.into_inner()
                    }
                };
                guard.iter().cloned().collect::<Vec<_>>()
            } else {
                names
            };

            for name in names {
                let (tx2, rx2) = oneshot::channel();
                if heartbeat_rpc_tx
                    .send(RpcCommand::ClaimName {
                        name: name.clone(),
                        pubkey_hex: String::new(),
                        respond_to: tx2,
                    })
                    .await
                    .is_err()
                {
                    warn!(name = %name, "heartbeat failed to dispatch");
                    continue;
                }

                match rx2.await {
                    Ok(Ok(())) => info!(name = %name, "heartbeat sent"),
                    Ok(Err(err)) => warn!(name = %name, error = %err, "heartbeat failed"),
                    Err(_) => warn!(name = %name, "heartbeat response dropped"),
                }
            }

            tokio::time::sleep(Duration::from_secs(6 * 60 * 60)).await;
        }
    });

    let mut pending_put: HashMap<kad::QueryId, PendingPut> = HashMap::new();
    let mut pending_get_text: HashMap<kad::QueryId, PendingTextQuery> = HashMap::new();
    let mut pending_get_manifest: HashMap<kad::QueryId, PendingManifestQuery> = HashMap::new();
    let mut pending_claim_put: HashMap<kad::QueryId, PendingClaimPut> = HashMap::new();
    let mut pending_name_probes: HashMap<kad::QueryId, PendingNameProbe> = HashMap::new();
    let mut pending_publish_checks: HashMap<kad::QueryId, PendingPublishOwnershipCheck> =
        HashMap::new();
    let mut pending_publish_claim_put: HashMap<kad::QueryId, PendingPublishClaimPut> =
        HashMap::new();
    let mut pending_publish_version_checks: HashMap<kad::QueryId, PendingPublishVersionCheck> =
        HashMap::new();

    let mut publish_tasks: HashMap<u64, PublishTask> = HashMap::new();
    let mut publish_query_to_task: HashMap<kad::QueryId, PublishQuery> = HashMap::new();
    let mut next_publish_task_id: u64 = 1;

    let mut get_site_tasks: HashMap<u64, GetSiteTask> = HashMap::new();
    let mut get_site_queries: HashMap<kad::QueryId, GetSiteQuery> = HashMap::new();
    let mut next_get_site_task_id: u64 = 1;
    let mut pending_provider_queries: HashMap<kad::QueryId, PendingProviderQuery> = HashMap::new();
    let mut pending_block_requests: HashMap<block_fetch::OutboundRequestId, PendingBlockRequest> =
        HashMap::new();

    loop {
        tokio::select! {
            maybe_cmd = rpc_rx.recv() => {
                if let Some(cmd) = maybe_cmd {
                    match cmd {
                        RpcCommand::NodeInfo { respond_to } => {
                            let connected_peer_ids = swarm
                                .connected_peers()
                                .map(ToString::to_string)
                                .collect();
                            let info = NodeInfoResponse {
                                peer_id: peer_id.to_string(),
                                connected_peers: swarm.connected_peers().count() as u32,
                                connected_peer_ids,
                                listen_addrs: swarm.listeners().map(ToString::to_string).collect(),
                            };
                            let _ = respond_to.send(info);
                        }
                        RpcCommand::PutRecord { key, value, respond_to } => {
                            let value_bytes = value.into_bytes();
                            if let Err(err) = validate_put_record_request(&key, &value_bytes) {
                                let _ = respond_to.send(Err(err));
                                continue;
                            }
                            let publisher_b64 = record_publisher_b64(&key, &value_bytes);
                            if let Some(rule) =
                                ingest_rule(&moderation_engine, &key, publisher_b64.as_deref())
                            {
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
                                        &local_record_store,
                                        rule,
                                        Some(key.clone()),
                                        publisher_b64.clone(),
                                        None,
                                        None,
                                    );
                                }
                                let _ = respond_to.send(Ok(()));
                                continue;
                            }
                            remember_local_record(
                                &local_record_store,
                                &mut local_records,
                                key.clone(),
                                value_bytes.clone(),
                            );
                            match maybe_put_record(
                                &mut swarm,
                                &moderation_engine,
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
                            if let Some(bytes) = local_record_value(&mut swarm, &key) {
                                let publisher_b64 = record_publisher_b64(&key, &bytes);
                                if let Some(rule) =
                                    hide_record_rule(&moderation_engine, &key, publisher_b64.as_deref())
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
                                    continue;
                                }
                                if let Some(value) = String::from_utf8(bytes)
                                    .ok()
                                    .and_then(|value| normalize_get_record_value(&key, value))
                                {
                                    let _ = respond_to.send(Some(value));
                                    continue;
                                }
                            }
                            if let Some(rule) = hide_record_rule(&moderation_engine, &key, None) {
                                warn!(
                                    rule_id = %rule.id,
                                    rule_kind = ?rule.kind,
                                    matched_value = %rule.value,
                                    action = %action_name(&rule.action),
                                    key = %key,
                                    "record hidden by moderation rule"
                                );
                                let _ = respond_to.send(None);
                                continue;
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
                        RpcCommand::PublishSite { name, site_dir, respond_to } => {
                            if pending_publish_checks.len()
                                + pending_publish_claim_put.len()
                                + pending_publish_version_checks.len()
                                + publish_tasks.len()
                                >= MAX_CONCURRENT_PUBLISH
                            {
                                let _ = respond_to
                                    .send(Err("too many concurrent publish tasks".to_string()));
                                continue;
                            }
                            if let Err(err) = validate_name(&name) {
                                let _ = respond_to.send(Err(err));
                                continue;
                            }
                            let canonical_site_dir = match validate_site_dir(&site_dir) {
                                Ok(path) => path,
                                Err(err) => {
                                    let _ = respond_to.send(Err(err));
                                    continue;
                                }
                            };
                            let query_id = dht::get_record(
                                &mut swarm.behaviour_mut().kademlia,
                                format!("name:{name}"),
                            );
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
                            if let Some(rule) = hide_record_rule(&moderation_engine, &key, None) {
                                warn!(
                                    rule_id = %rule.id,
                                    rule_kind = ?rule.kind,
                                    matched_value = %rule.value,
                                    action = %action_name(&rule.action),
                                    key = %key,
                                    "site manifest hidden by moderation rule"
                                );
                                let _ = respond_to.send(None);
                                continue;
                            }
                            if let Some(value) = local_record_value(&mut swarm, &key)
                                .and_then(|bytes| String::from_utf8(bytes).ok())
                            {
                                let publisher_b64 = site_manifest_publisher_b64(&value);
                                if let Some(rule) = site_manifest_suppression_rule(
                                    &moderation_engine,
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
                                    continue;
                                }
                                let trust = site_manifest_trust_state(
                                    &local_record_store,
                                    &name,
                                    &value,
                                )
                                .unwrap_or(TrustState {
                                    status: "first_seen".to_string(),
                                    explicitly_trusted: false,
                                    first_seen_at: None,
                                    previous_key: None,
                                });
                                let _ = respond_to.send(Some(GetSiteManifestResponse {
                                    manifest_json: value,
                                    trust,
                                }));
                                continue;
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
                        RpcCommand::GetBlock { hash, site_key, respond_to } => {
                            let consumer = BlockConsumer::Rpc { respond_to };
                            if let Some(site_key) = site_key {
                                if let Some(site_name) = site_name_from_site_key(&site_key) {
                                    if let Some(rule) =
                                        hide_block_rule(&moderation_engine, site_name, &hash)
                                    {
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
                                        continue;
                                    }
                                }
                                start_block_lookup(
                                    &mut swarm,
                                    &moderation_engine,
                                    &local_record_store,
                                    &mut session_block_cache,
                                    &mut pending_provider_queries,
                                    &mut pending_block_requests,
                                    &mut get_site_tasks,
                                    hash,
                                    site_key,
                                    consumer,
                                );
                            // site_key is None: only content-hash hide rules can be evaluated here.
                            // Site-scoped rules require a site_key; callers should prefer passing one.
                            } else if let Some(value) =
                                session_block_cache.get(&hash).cloned().or_else(|| {
                                    local_record_store.get_block(&hash).ok().flatten()
                                })
                            {
                                if let Some(rule) =
                                    moderation_engine.match_rule(RuleKind::ContentHash, &hash)
                                {
                                    if rule.action == RuleAction::Hide {
                                        if let BlockConsumer::Rpc { respond_to } = consumer {
                                            let _ = respond_to.send(None);
                                        }
                                        continue;
                                    }
                                }
                                if local_record_store.get_block(&hash).ok().flatten().is_some() {
                                    let _ = local_record_store.touch_block(&hash);
                                }
                                if let BlockConsumer::Rpc { respond_to } = consumer {
                                    let _ = respond_to.send(Some(hex_encode(&value)));
                                }
                            } else if let BlockConsumer::Rpc { respond_to } = consumer {
                                let _ = respond_to.send(None);
                            }
                        }
                        RpcCommand::GetSite { name, respond_to } => {
                            if get_site_tasks.len() >= MAX_CONCURRENT_GET_SITE {
                                let _ = respond_to
                                    .send(Err("too many concurrent requests".to_string()));
                                continue;
                            }
                            let task_id = next_get_site_task_id;
                            next_get_site_task_id = next_get_site_task_id.saturating_add(1);

                            let task = GetSiteTask {
                                respond_to,
                                requested_name: name.clone(),
                                manifest: None,
                                next_file_index: 0,
                                active_file: None,
                                total_bytes: 0,
                                files: Vec::new(),
                            };

                            let query_id = dht::get_record(&mut swarm.behaviour_mut().kademlia, format!("site:{name}"));
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
                                continue;
                            }

                            let effective_pubkey = if pubkey_hex.is_empty() {
                                local_pubkey_hex.clone()
                            } else if pubkey_hex == local_pubkey_hex {
                                pubkey_hex
                            } else {
                                let _ = respond_to.send(Err(
                                    "name already claimed by another key".to_string(),
                                ));
                                continue;
                            };

                            let query_id = dht::get_record(
                                &mut swarm.behaviour_mut().kademlia,
                                format!("name:{name}"),
                            );
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
                                continue;
                            }
                            let result = cached_manifest_json(&mut swarm, &name)
                                .ok_or_else(|| anyhow::anyhow!("no cached site manifest found for site"))
                                .and_then(|manifest_json| {
                                    let count = pin_cached_site_blocks(
                                        &local_record_store,
                                        &mut session_block_cache,
                                        &name,
                                        &manifest_json,
                                    )?;
                                    if count == 0 {
                                        bail!("no cached blocks found for site");
                                    }
                                    start_providing_site(&mut swarm, &moderation_engine, &name)?;
                                    Ok(())
                                })
                                .map_err(|err| err.to_string());
                            let _ = respond_to.send(result);
                        }
                        RpcCommand::UnpinSite { name, respond_to } => {
                            if let Err(err) = validate_name(&name) {
                                let _ = respond_to.send(Err(err));
                                continue;
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
                                continue;
                            }
                            let result = (|| -> Result<(), String> {
                                if pin {
                                    let manifest_json = cached_manifest_json(&mut swarm, &name)
                                        .ok_or_else(|| "no cached site manifest found for site".to_string())?;
                                    let count = pin_cached_site_blocks(
                                        &local_record_store,
                                        &mut session_block_cache,
                                        &name,
                                        &manifest_json,
                                    )
                                    .map_err(|err| err.to_string())?;
                                    if count == 0 {
                                        return Err("no cached blocks found for site".to_string());
                                    }
                                    start_providing_site(&mut swarm, &moderation_engine, &name)
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
                                continue;
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
                                continue;
                            }
                            let known = local_record_store.get_known_publisher(&name).unwrap_or(None);
                            let _ = respond_to.send(known);
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
                                moderation_engine = ModerationEngine::load(
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
                                        moderation_engine = ModerationEngine::load(
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
                            let rules = local_record_store.load_moderation_rules().unwrap_or_default();
                            let _ = respond_to.send(rules);
                        }
                        RpcCommand::ModPurgeLocal {
                            kind,
                            value,
                            respond_to,
                        } => {
                            let result = parse_rule_kind(&kind).and_then(|kind| {
                                purge_local_matches(
                                    &local_record_store,
                                    &mut local_records,
                                    &mut swarm,
                                    &kind,
                                    &value,
                                )
                            });
                            let _ = respond_to.send(result);
                        }
                        RpcCommand::ModQuarantineList { respond_to } => {
                            let entries =
                                local_record_store.list_quarantine_entries().unwrap_or_default();
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
                            let trusted =
                                local_record_store.list_trusted_publishers().unwrap_or_default();
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
                                continue;
                            }

                            let effective_pubkey = if pubkey_hex.is_empty() {
                                local_pubkey_hex.clone()
                            } else if pubkey_hex == local_pubkey_hex {
                                pubkey_hex
                            } else {
                                let _ = respond_to.send(Err(
                                    "name already claimed by another key".to_string(),
                                ));
                                continue;
                            };

                            let query_id = dht::get_record(
                                &mut swarm.behaviour_mut().kademlia,
                                format!("name:{name}"),
                            );
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
                                continue;
                            }
                            let canonical_site_dir = match validate_site_dir(&site_dir) {
                                Ok(path) => path,
                                Err(err) => {
                                    let _ = respond_to.send(Err(err));
                                    continue;
                                }
                            };
                            let query_id = dht::get_record(
                                &mut swarm.behaviour_mut().kademlia,
                                format!("name:{name}"),
                            );
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
                                        &mut swarm,
                                        &moderation_engine,
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
            }
            event = swarm.select_next_some() => {
                handle_swarm_event(
                    event,
                    &mut swarm,
                    &mut pending_put,
                    &mut pending_get_text,
                    &mut pending_get_manifest,
                    &mut pending_provider_queries,
                    &mut pending_block_requests,
                    &mut session_block_cache,
                    &mut pending_claim_put,
                    &mut pending_name_probes,
                    &mut pending_publish_checks,
                    &mut pending_publish_claim_put,
                    &mut pending_publish_version_checks,
                    &mut publish_tasks,
                    &mut publish_query_to_task,
                    &mut next_publish_task_id,
                    &mut get_site_tasks,
                    &mut get_site_queries,
                    &bootstrap_peer_ids,
                    &mut relay_reservations,
                    &mut relay_reservation_requests,
                    &mut relay_connection_counts,
                    &owned_names,
                    &rpc_tx,
                    &site_signing_key,
                    &local_pubkey_hex,
                    &moderation_engine,
                    config.mime_policy_strict,
                    &local_record_store,
                    &mut local_records,
                );
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn handle_swarm_event(
    event: libp2p::swarm::SwarmEvent<LatticeBehaviourEvent>,
    swarm: &mut Swarm<LatticeBehaviour>,
    pending_put: &mut HashMap<kad::QueryId, PendingPut>,
    pending_get_text: &mut HashMap<kad::QueryId, PendingTextQuery>,
    pending_get_manifest: &mut HashMap<kad::QueryId, PendingManifestQuery>,
    pending_provider_queries: &mut HashMap<kad::QueryId, PendingProviderQuery>,
    pending_block_requests: &mut HashMap<block_fetch::OutboundRequestId, PendingBlockRequest>,
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
        libp2p::swarm::SwarmEvent::Behaviour(LatticeBehaviourEvent::Mdns(
            mdns::Event::Discovered(list),
        )) => {
            for (peer_id, addr) in list {
                info!(peer = %peer_id, address = %addr, "mDNS peer discovered");
                swarm.behaviour_mut().kademlia.add_address(&peer_id, addr);
            }
        }
        libp2p::swarm::SwarmEvent::Behaviour(LatticeBehaviourEvent::Mdns(
            mdns::Event::Expired(list),
        )) => {
            for (peer_id, addr) in list {
                swarm
                    .behaviour_mut()
                    .kademlia
                    .remove_address(&peer_id, &addr);
            }
        }
        libp2p::swarm::SwarmEvent::Behaviour(LatticeBehaviourEvent::Kademlia(
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
                            if task.failed.is_none() {
                                task.failed = Some(format!(
                                    "replication quorum failed for {key:?}: stored on {}/{} peers",
                                    success.len(),
                                    quorum.get()
                                ));
                            }
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
                            let _ =
                                rpc_tx_retry.send(RpcCommand::RepublishLocalRecords).await;
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
                            if let Some(rule) = hide_record_rule(moderation_engine, &pending.key, None) {
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
                                Some(GetSiteManifestResponse { manifest_json, trust })
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
                                    swarm,
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
                                remember_local_record(
                                    local_record_store,
                                    local_records,
                                    key.clone(),
                                    payload_bytes.clone(),
                                );
                                match maybe_put_record(
                                    swarm,
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
                                        let _ = pending.respond_to.send(Err(
                                            "publish blocked by moderation rule".to_string(),
                                        ));
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
                                            start_publish_task(
                                                swarm,
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
                                        start_publish_task(
                                            swarm,
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
        libp2p::swarm::SwarmEvent::Behaviour(LatticeBehaviourEvent::BlockFetch(event)) => {
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
        libp2p::swarm::SwarmEvent::Behaviour(LatticeBehaviourEvent::Identify(
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
        libp2p::swarm::SwarmEvent::Behaviour(LatticeBehaviourEvent::Identify(_)) => {}
        libp2p::swarm::SwarmEvent::Behaviour(LatticeBehaviourEvent::Autonat(event)) => {
            info!(event = ?event, "autonat event");
            if let autonat::Event::StatusChanged { new, .. } = &event {
                info!(status = ?new, "NAT status changed");
            }
        }
        libp2p::swarm::SwarmEvent::Behaviour(LatticeBehaviourEvent::Relay(event)) => {
            info!(event = ?event, "relay event");
        }
        libp2p::swarm::SwarmEvent::Behaviour(LatticeBehaviourEvent::RelayClient(event)) => {
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
        libp2p::swarm::SwarmEvent::Behaviour(LatticeBehaviourEvent::Dcutr(event)) => {
            info!(event = ?event, "dcutr event");
        }
        libp2p::swarm::SwarmEvent::Behaviour(LatticeBehaviourEvent::Gossipsub(event)) => {
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
            let manifest: SiteManifest = serde_json::from_str(&value)
                .map_err(|_| "invalid site manifest record".to_string())?;
            Ok(Some(manifest.version))
        }
        Ok(_) => Ok(None),
        Err(kad::GetRecordError::NotFound { .. }) => Ok(None),
        Err(err) => Err(format!("failed to read current site version: {err}")),
    }
}

#[allow(clippy::too_many_arguments)]
fn start_publish_task(
    swarm: &mut Swarm<LatticeBehaviour>,
    prepared: PreparedPublish,
    site_name: &str,
    claimed: bool,
    respond_to: oneshot::Sender<Result<PublishSiteOk, String>>,
    publish_tasks: &mut HashMap<u64, PublishTask>,
    publish_query_to_task: &mut HashMap<kad::QueryId, PublishQuery>,
    next_publish_task_id: &mut u64,
    moderation_engine: &ModerationEngine,
    local_record_store: &LocalRecordStore,
    local_records: &mut HashMap<String, Vec<u8>>,
) {
    let task_id = *next_publish_task_id;
    *next_publish_task_id = (*next_publish_task_id).saturating_add(1);

    remember_local_record(
        local_record_store,
        local_records,
        prepared.manifest_record.0.clone(),
        prepared.manifest_record.1.clone(),
    );

    let mut task = PublishTask {
        respond_to,
        remaining: 0,
        failed: None,
        had_quorum_failed: false,
        version: prepared.version,
        file_count: prepared.file_count,
        claimed,
        connected_peers_at_start: swarm.connected_peers().count(),
        manifest_record: Some(prepared.manifest_record),
    };

    if task.connected_peers_at_start == 0 {
        warn!(
            "publishing with no connected peers; records are local-only until peers connect and replication occurs"
        );
    }

    for (hash, value) in prepared.blocks {
        if let Err(err) = local_record_store.put_block(&hash, &value, site_name, CachePolicy::Pinned)
        {
            if task.failed.is_none() {
                task.failed = Some(err.to_string());
            }
        }
    }

    if let Err(err) = start_providing_site(swarm, moderation_engine, site_name) {
        if task.failed.is_none() {
            task.failed = Some(err.to_string());
        }
    }

    if let Some(err) = task.failed {
        let _ = task.respond_to.send(Err(err));
        return;
    }

    let Some((manifest_key, manifest_value)) = task.manifest_record.take() else {
        let _ = task.respond_to.send(Err(
            "internal publish error: missing manifest record".to_string()
        ));
        return;
    };

    match maybe_put_record(
        swarm,
        moderation_engine,
        manifest_key,
        manifest_value,
    ) {
        Ok(Some(query_id)) => {
            task.remaining = 1;
            publish_query_to_task.insert(query_id, PublishQuery { task_id });
            publish_tasks.insert(task_id, task);
        }
        Ok(None) => {
            let _ = task
                .respond_to
                .send(Err("publish blocked by moderation rule".to_string()));
        }
        Err(err) => {
            let _ = task.respond_to.send(Err(err.to_string()));
        }
    }
}

fn reannounce_pinned_sites(
    local_record_store: &LocalRecordStore,
    moderation_engine: &ModerationEngine,
    swarm: &mut Swarm<LatticeBehaviour>,
) {
    match local_record_store.list_pinned_sites() {
        Ok(sites) => {
            for site in sites {
                if let Err(err) = start_providing_site(swarm, moderation_engine, &site) {
                    warn!(site = %site, error = %err, "failed to reannounce pinned site provider");
                }
            }
        }
        Err(err) => warn!(error = %err, "failed to list pinned sites on startup"),
    }
}

fn site_name_from_site_key(site_key: &str) -> Option<&str> {
    site_key.strip_prefix("site:")
}

fn site_manifest_key(site_name: &str) -> String {
    format!("site:{site_name}")
}

fn block_lookup_not_found(
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

fn block_lookup_failed(
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

fn start_block_lookup(
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

    let query_id = dht::get_providers(&mut swarm.behaviour_mut().kademlia, site_key.clone());
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

fn handle_get_providers_result(
    id: kad::QueryId,
    result: kad::GetProvidersResult,
    swarm: &mut Swarm<LatticeBehaviour>,
    pending_provider_queries: &mut HashMap<kad::QueryId, PendingProviderQuery>,
    pending_block_requests: &mut HashMap<block_fetch::OutboundRequestId, PendingBlockRequest>,
    get_site_tasks: &mut HashMap<u64, GetSiteTask>,
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

fn try_next_block_provider(
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

fn handle_resolved_block(
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
            );
        }
    }
}

fn handle_block_fetch_event(
    event: block_fetch::Event,
    swarm: &mut Swarm<LatticeBehaviour>,
    pending_block_requests: &mut HashMap<block_fetch::OutboundRequestId, PendingBlockRequest>,
    get_site_tasks: &mut HashMap<u64, GetSiteTask>,
    pending_provider_queries: &mut HashMap<kad::QueryId, PendingProviderQuery>,
    moderation_engine: &ModerationEngine,
    local_record_store: &LocalRecordStore,
    session_block_cache: &mut SessionBlockCache,
) {
    match event {
        request_response::Event::Message { message, .. } => match message {
            request_response::Message::Request {
                request, channel, ..
            } => {
                let response = match site_name_from_site_key(&request.site_key) {
                    Some(site_name) => {
                        let publisher_hidden = cached_manifest_json(swarm, site_name)
                            .and_then(|manifest_json| site_manifest_publisher_b64(&manifest_json))
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
                let _ = swarm.behaviour_mut().block_fetch.send_response(channel, response);
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

fn parse_verified_name_record(name: &str, value: &str) -> Option<NameRecord> {
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

fn validate_name(name: &str) -> Result<(), String> {
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

fn validate_site_dir(site_dir: &str) -> Result<std::path::PathBuf, String> {
    let path = Path::new(site_dir);
    if !path.is_absolute() {
        return Err("site_dir must be an absolute path".to_string());
    }
    let canonical = path
        .canonicalize()
        .map_err(|e| format!("invalid site_dir: {e}"))?;
    if !canonical.is_dir() {
        return Err("site_dir must be a directory".to_string());
    }
    Ok(canonical)
}

fn addr_is_loopback_or_private(addr: &Multiaddr) -> bool {
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

fn build_bootstrap_peer_ids(bootstrap_peers: &[String]) -> HashSet<libp2p::PeerId> {
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

fn build_relay_reservation_addr(peer_addr: &Multiaddr, peer_id: libp2p::PeerId) -> Multiaddr {
    let mut relay_addr = peer_addr.clone();
    if let Some(Protocol::P2p(_)) = relay_addr.iter().last() {
        relay_addr.pop();
    }
    relay_addr.push(Protocol::P2p(peer_id));
    relay_addr.push(Protocol::P2pCircuit);
    relay_addr
}

fn normalize_get_record_value(key: &str, value: String) -> Option<String> {
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
    remember_local_record(
        local_record_store,
        local_records,
        key.clone(),
        payload_bytes.clone(),
    );

    match maybe_put_record(swarm, moderation_engine, key, payload_bytes) {
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

fn handle_get_site_query_result(
    query: GetSiteQuery,
    result: Result<kad::GetRecordOk, kad::GetRecordError>,
    swarm: &mut Swarm<LatticeBehaviour>,
    get_site_tasks: &mut HashMap<u64, GetSiteTask>,
    get_site_queries: &mut HashMap<kad::QueryId, GetSiteQuery>,
    pending_provider_queries: &mut HashMap<kad::QueryId, PendingProviderQuery>,
    pending_block_requests: &mut HashMap<block_fetch::OutboundRequestId, PendingBlockRequest>,
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
                    let manifest = match serde_json::from_str::<SiteManifest>(&value) {
                        Ok(manifest) => manifest,
                        Err(_) => {
                            let _ = task
                                .respond_to
                                .send(Err("invalid site manifest".to_string()));
                            return;
                        }
                    };
                    if let Err(err) = verify_manifest(&manifest) {
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
            if manifest.files.len() > MAX_GET_SITE_FILES {
                let _ = task
                    .respond_to
                    .send(Err("site exceeds maximum file count".to_string()));
                return;
            }
            let declared_bytes = manifest
                .files
                .iter()
                .fold(0_u64, |acc, file| acc.saturating_add(file.size));
            if declared_bytes > MAX_GET_SITE_TOTAL_BYTES {
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
                let response = GetSiteResponse {
                    name: manifest_name,
                    version: manifest_version,
                    files: Vec::new(),
                };
                let _ = task.respond_to.send(Ok(response));
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
            );
        }
    }
}

fn drive_get_site_task(
    task_id: u64,
    mut task: GetSiteTask,
    swarm: &mut Swarm<LatticeBehaviour>,
    get_site_tasks: &mut HashMap<u64, GetSiteTask>,
    pending_provider_queries: &mut HashMap<kad::QueryId, PendingProviderQuery>,
    pending_block_requests: &mut HashMap<block_fetch::OutboundRequestId, PendingBlockRequest>,
    moderation_engine: &ModerationEngine,
    local_record_store: &LocalRecordStore,
    session_block_cache: &mut SessionBlockCache,
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

fn handle_site_task_block_bytes(
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
    );
}

fn append_block_to_site_task(task: &mut GetSiteTask, block_hash: &str, raw_bytes: Vec<u8>) -> Result<(), String> {
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

fn next_task_block_hash(task: &mut GetSiteTask) -> std::result::Result<Option<String>, String> {
    if let Some(active) = task.active_file.as_mut() {
        if active.next_block_index < active.block_hashes.len() {
            let hash = active.block_hashes[active.next_block_index].clone();
            active.next_block_index += 1;
            return Ok(Some(hash));
        }
    }
    start_next_file_download(task)
}

fn start_next_file_download(task: &mut GetSiteTask) -> std::result::Result<Option<String>, String> {
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

fn file_block_hashes(file: &lattice_site::manifest::FileEntry) -> Vec<String> {
    if !file.chunks.is_empty() {
        return file.chunks.clone();
    }
    vec![file.hash.clone()]
}

pub fn validate_site_file_mime_policy(
    path: &str,
    contents: &[u8],
    mime_policy_strict: bool,
) -> Result<()> {
    let detected_mime = mime::detect_mime(path, contents);
    if let Some(reason) = mime::violation_reason(&detected_mime, contents.len()) {
        warn!(
            filename = %path,
            detected_mime = %detected_mime,
            file_size = contents.len(),
            reason = %reason,
            "MIME policy violation while publishing site file"
        );
        if mime_policy_strict {
            bail!(
                "rejected: {path} (detected: {detected_mime}, size: {} bytes, reason: {reason})",
                contents.len()
            );
        }
    }
    Ok(())
}

fn prepare_publish(
    name: &str,
    site_dir: &Path,
    signing_key: &SigningKey,
    dht_baseline_version: u64,
    mime_policy_strict: bool,
) -> Result<PreparedPublish> {
    let (local_existing_version, rating) = match site_publisher::load_manifest(site_dir) {
        Ok(existing) => {
            let rating = if existing.rating.is_empty() {
                "general".to_string()
            } else {
                existing.rating
            };
            (existing.version, rating)
        }
        Err(_) => (0, "general".to_string()),
    };

    let existing_version = local_existing_version.max(dht_baseline_version);
    let manifest =
        site_publisher::build_manifest(name, site_dir, signing_key, &rating, existing_version)?;
    verify_manifest(&manifest)?;
    site_publisher::save_manifest(&manifest, site_dir)?;

    let mut blocks = Vec::new();
    let mut seen_block_hashes: HashSet<String> = HashSet::new();
    for file in &manifest.files {
        let file_path = site_dir.join(&file.path);
        let contents = fs::read(&file_path)
            .with_context(|| format!("failed to read site file {}", file_path.display()))?;
        validate_site_file_mime_policy(&file.path, &contents, mime_policy_strict)?;

        let actual_hash = hash_bytes(&contents);
        if actual_hash != file.hash {
            bail!(
                "hash mismatch for {}: manifest={}, actual={}",
                file.path,
                file.hash,
                actual_hash
            );
        }

        let block_hashes = file_block_hashes(file);
        if block_hashes.len() == 1 {
            let block_hash = block_hashes[0].clone();
            let actual_block_hash = hash_bytes(&contents);
            if actual_block_hash != block_hash {
                bail!(
                    "block hash mismatch for {}: manifest={}, actual={}",
                    file.path,
                    block_hash,
                    actual_block_hash
                );
            }
            if seen_block_hashes.insert(block_hash.clone()) {
                blocks.push((block_hash, contents));
            }
            continue;
        }

        let chunk_size = file
            .chunk_size
            .and_then(|v| usize::try_from(v).ok())
            .unwrap_or(DEFAULT_CHUNK_SIZE_BYTES);
        if chunk_size == 0 {
            bail!("invalid chunk_size for {}", file.path);
        }
        let chunks: Vec<&[u8]> = contents.chunks(chunk_size).collect();
        if chunks.len() != block_hashes.len() {
            bail!(
                "chunk count mismatch for {}: manifest={}, actual={}",
                file.path,
                block_hashes.len(),
                chunks.len()
            );
        }

        for (i, chunk_hash) in block_hashes.iter().enumerate() {
            let chunk = chunks[i];
            let actual_chunk_hash = hash_bytes(chunk);
            if actual_chunk_hash != *chunk_hash {
                bail!(
                    "chunk hash mismatch for {} chunk {}: manifest={}, actual={}",
                    file.path,
                    i,
                    chunk_hash,
                    actual_chunk_hash
                );
            }
            if seen_block_hashes.insert(chunk_hash.clone()) {
                blocks.push((chunk_hash.clone(), chunk.to_vec()));
            }
        }
    }

    let manifest_json =
        serde_json::to_string(&manifest).context("failed to serialize site manifest")?;
    let manifest_record = (format!("site:{name}"), manifest_json.into_bytes());

    Ok(PreparedPublish {
        version: manifest.version,
        file_count: manifest.files.len(),
        blocks,
        manifest_record,
    })
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        out.push(HEX[(b >> 4) as usize] as char);
        out.push(HEX[(b & 0x0f) as usize] as char);
    }
    out
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
                    warn!("local records db contained non-utf8 key; skipping");
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
        self.db
            .flush()
            .context("failed to flush local records db")?;
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

    fn touch_block(&self, hash: &str) -> Result<()> {
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

    fn list_site_block_hashes(&self, site_name: &str) -> Result<Vec<String>> {
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

    fn set_site_cache_policy(&self, site_name: &str, policy: CachePolicy) -> Result<usize> {
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

    fn list_pinned_sites(&self) -> Result<Vec<String>> {
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

    fn is_site_pinned(&self, site_name: &str) -> Result<bool> {
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

    fn load_moderation_rules(&self) -> Result<Vec<ModerationRule>> {
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

    fn insert_moderation_rule(&self, rule: &ModerationRule) -> Result<()> {
        let encoded = serde_json::to_vec(rule).context("failed to encode moderation rule")?;
        self.mod_rules
            .insert(rule.id.as_bytes(), encoded)
            .context("failed to persist moderation rule")?;
        self.db.flush().context("failed to flush moderation rule write")?;
        Ok(())
    }

    fn remove_moderation_rule(&self, id: &str) -> Result<bool> {
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

    fn insert_quarantine_entry(&self, entry: &QuarantineEntry) -> Result<()> {
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
            let encoded =
                serde_json::to_vec(&known).context("failed to encode known publisher")?;
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

    fn gc_ephemeral_blocks(&self, max_age_secs: u64) -> Result<BlockCacheGcStats> {
        let _ = max_age_secs;
        // Ephemeral blocks are session-only now; disk-backed block GC is superseded by the
        // in-memory session cache and pinned blocks are managed explicitly.
        Ok(BlockCacheGcStats::default())
    }

    fn gc_unpinned(&self, max_age_secs: u64, max_total_bytes: usize) -> Result<LocalRecordGcStats> {
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

fn remember_local_record(
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

fn key_should_be_pinned(key: &str) -> bool {
    key.starts_with("name:") || key.starts_with("site:")
}

fn parse_rule_kind(kind: &str) -> Result<RuleKind, String> {
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

fn parse_rule_action(action: &str) -> Result<RuleAction, String> {
    match action {
        "Hide" | "hide" => Ok(RuleAction::Hide),
        "RejectIngest" | "reject_ingest" => Ok(RuleAction::RejectIngest),
        "PurgeLocal" | "purge_local" => Ok(RuleAction::PurgeLocal),
        "RefuseRepublish" | "refuse_republish" => Ok(RuleAction::RefuseRepublish),
        "Quarantine" | "quarantine" => Ok(RuleAction::Quarantine),
        _ => Err(format!("unknown moderation action: {action}")),
    }
}

fn action_name(action: &RuleAction) -> String {
    format!("{action:?}")
}

fn publisher_hex_to_b64(value: &str) -> Option<String> {
    let bytes = hex::decode(value).ok()?;
    Some(BASE64_STANDARD.encode(bytes))
}

fn site_manifest_publisher_b64(manifest_json: &str) -> Option<String> {
    let manifest: SiteManifest = serde_json::from_str(manifest_json).ok()?;
    publisher_hex_to_b64(&manifest.publisher_key)
}

fn site_manifest_suppression_rule<'a>(
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

fn trust_state_from_status(known: &KnownPublisher, status: KnownPublisherStatus) -> TrustState {
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

fn site_manifest_trust_state(
    local_record_store: &LocalRecordStore,
    site_name: &str,
    manifest_json: &str,
) -> Result<TrustState> {
    let manifest: SiteManifest =
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

fn cached_manifest_json(swarm: &mut Swarm<LatticeBehaviour>, site_name: &str) -> Option<String> {
    local_record_value(swarm, &site_manifest_key(site_name))
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
    let manifest: SiteManifest =
        serde_json::from_str(manifest_json).context("failed to decode site manifest for pinning")?;
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

fn signed_record_publisher_b64(value: &[u8]) -> Option<String> {
    let raw = std::str::from_utf8(value).ok()?;
    let signed: SignedRecord = serde_json::from_str(raw).ok()?;
    Some(signed.publisher_b64())
}

fn record_publisher_b64(key: &str, value: &[u8]) -> Option<String> {
    if key.starts_with("app:") {
        return signed_record_publisher_b64(value);
    }
    None
}

fn match_rule<'a>(
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
    swarm: &mut Swarm<LatticeBehaviour>,
    kind: &RuleKind,
    value: &str,
) -> Result<(), String> {
    match kind {
        RuleKind::RecordKey => {
            local_records.remove(value);
            let record_key = kad::RecordKey::new(&value);
            swarm.behaviour_mut().kademlia.store_mut().remove(&record_key);
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
                swarm.behaviour_mut().kademlia.store_mut().remove(&record_key);
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
                swarm.behaviour_mut().kademlia.store_mut().remove(&record_key);
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

fn maybe_put_record(
    swarm: &mut Swarm<LatticeBehaviour>,
    moderation_engine: &ModerationEngine,
    key: String,
    value: Vec<u8>,
) -> Result<Option<kad::QueryId>> {
    let publisher_b64 = record_publisher_b64(&key, &value);
    let site_name = key.strip_prefix("site:");
    if let Some(rule) = republish_rule(
        moderation_engine,
        &key,
        publisher_b64.as_deref(),
        site_name,
    ) {
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
    let query_id = dht::put_record_bytes(&mut swarm.behaviour_mut().kademlia, key, value)
        .map_err(|err| anyhow::anyhow!(err.to_string()))?;
    Ok(Some(query_id))
}

fn start_providing_site(
    swarm: &mut Swarm<LatticeBehaviour>,
    moderation_engine: &ModerationEngine,
    site_name: &str,
) -> Result<()> {
    let site_key = site_manifest_key(site_name);
    let publisher_b64 = cached_manifest_json(swarm, site_name)
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
    dht::start_providing(&mut swarm.behaviour_mut().kademlia, site_key)?;
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

fn unix_ts() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|dur| dur.as_secs())
        .unwrap_or(0)
}

pub fn build_test_swarm() -> Result<Swarm<LatticeBehaviour>> {
    let identity = libp2p::identity::Keypair::generate_ed25519();
    let peer_id = identity.public().to_peer_id();
    transport::build_swarm(
        identity,
        |key, relay_client| -> std::result::Result<
            LatticeBehaviour,
            Box<dyn std::error::Error + Send + Sync>,
        > {
            let kademlia = dht::new_kademlia(peer_id);
            let block_fetch = block_fetch::new_behaviour();
            let mdns = mdns::tokio::Behaviour::new(mdns::Config::default(), peer_id)?;
            let gossipsub = gossipsub::Behaviour::new(
                gossipsub::MessageAuthenticity::Signed(key.clone()),
                gossipsub::Config::default(),
            )?;
            let identify = identify::Behaviour::new(identify::Config::new(
                "/lattice/test".to_string(),
                key.public(),
            ));
            let autonat = autonat::Behaviour::new(peer_id, autonat::Config::default());
            let relay = relay::Behaviour::new(peer_id, relay::Config::default());
            let dcutr = dcutr::Behaviour::new(peer_id);

            Ok(LatticeBehaviour {
                kademlia,
                block_fetch,
                mdns,
                gossipsub,
                identify,
                autonat,
                relay,
                relay_client,
                dcutr,
            })
        },
    )
}

fn local_record_value(swarm: &mut Swarm<LatticeBehaviour>, key: &str) -> Option<Vec<u8>> {
    let target = kad::RecordKey::new(&key.as_bytes());
    for record in swarm.behaviour_mut().kademlia.store_mut().records() {
        let record = record.as_ref();
        if record.key == target {
            return Some(record.value.clone());
        }
    }
    None
}

fn restore_local_records_to_store(
    swarm: &mut Swarm<LatticeBehaviour>,
    local_records: &HashMap<String, Vec<u8>>,
) {
    for (key, value) in local_records {
        let record = kad::Record::new(kad::RecordKey::new(key), value.clone());
        if let Err(err) = swarm.behaviour_mut().kademlia.store_mut().put(record) {
            warn!(
                key = %key,
                error = %err,
                "failed to restore persisted record to local store"
            );
        }
    }
}

fn owned_names_from_local_records(
    local_records: &HashMap<String, Vec<u8>>,
    local_pubkey_hex: &str,
) -> HashSet<String> {
    let mut names = HashSet::new();
    for (key, value) in local_records {
        let Some(name) = key.strip_prefix("name:") else {
            continue;
        };

        let value_str = match std::str::from_utf8(value) {
            Ok(value_str) => value_str,
            Err(_) => continue,
        };

        if let Some(record) = parse_verified_name_record(name, value_str) {
            if record.key == local_pubkey_hex {
                names.insert(name.to_string());
            }
        }
    }
    names
}
