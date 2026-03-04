use anyhow::{bail, Context, Result};
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use base64::Engine as _;
use ed25519_dalek::SigningKey;
use lattice_daemon::config::load_or_create_config;
use lattice_daemon::dht;
use lattice_daemon::http_server;
use lattice_daemon::names::NameRecord;
use lattice_daemon::node::{load_or_create_identity, load_or_create_site_signing_key};
use lattice_daemon::rpc::{
    self, GetSiteResponse, NodeInfoResponse, PublishSiteOk, RpcCommand, SiteFile,
};
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

#[derive(NetworkBehaviour)]
struct LatticeBehaviour {
    kademlia: kad::Behaviour<kad::store::MemoryStore>,
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

#[derive(Clone, Copy)]
enum PublishQueryKind {
    Block,
    Manifest,
}

struct PublishQuery {
    task_id: u64,
    kind: PublishQueryKind,
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
    FileBlock { task_id: u64, block_hash: String },
}

struct PreparedPublish {
    version: u64,
    file_count: usize,
    block_records: Vec<(String, Vec<u8>)>,
    manifest_record: (String, Vec<u8>),
}

struct PendingTextQuery {
    key: String,
    attempts: u8,
    respond_to: oneshot::Sender<Option<String>>,
}

struct PendingBlockQuery {
    key: String,
    hash: String,
    attempts: u8,
    respond_to: oneshot::Sender<Option<String>>,
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
    pinned: bool,
    updated_at: u64,
}

#[derive(Debug, Default)]
struct LocalRecordGcStats {
    removed_records: usize,
    removed_bytes: usize,
}

struct LocalRecordStore {
    db: sled::Db,
    records: sled::Tree,
    meta: sled::Tree,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let config = load_or_create_config()?;
    let had_separate_site_key = config.data_dir.join("site_signing.key").exists();
    let node_identity = load_or_create_identity(&config.data_dir)?;
    let site_signing_key = load_or_create_site_signing_key(&config.data_dir)?;
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
    let local_record_store = LocalRecordStore::open(&local_records_path)?;
    let gc_stats =
        local_record_store.gc_unpinned(LOCAL_RECORD_GC_MAX_AGE_SECS, LOCAL_RECORD_GC_MAX_BYTES)?;
    if gc_stats.removed_records > 0 {
        info!(
            removed_records = gc_stats.removed_records,
            removed_bytes = gc_stats.removed_bytes,
            "local record GC removed stale records"
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
    let http_port = config.http_port;
    let http_rpc_tx = rpc_tx.clone();
    let _http_server = tokio::spawn(async move {
        if let Err(err) = http_server::start_http_server(http_port, http_rpc_tx).await {
            error!(error = %err, "http server exited");
        }
    });
    info!(http_port, "HTTP server listening");

    info!(peer_id = %peer_id, "lattice daemon started");
    info!(
        port = config.listen_port,
        rpc_port = config.rpc_port,
        http_port = config.http_port,
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
    let mut pending_get_block: HashMap<kad::QueryId, PendingBlockQuery> = HashMap::new();
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

    loop {
        tokio::select! {
            maybe_cmd = rpc_rx.recv() => {
                if let Some(cmd) = maybe_cmd {
                    match cmd {
                        RpcCommand::NodeInfo { respond_to } => {
                            let info = NodeInfoResponse {
                                peer_id: peer_id.to_string(),
                                connected_peers: swarm.connected_peers().count() as u32,
                                listen_addrs: swarm.listeners().map(ToString::to_string).collect(),
                            };
                            let _ = respond_to.send(info);
                        }
                        RpcCommand::PutRecord { key, value, respond_to } => {
                            let value_bytes = value.into_bytes();
                            remember_local_record(
                                &local_record_store,
                                &mut local_records,
                                key.clone(),
                                value_bytes.clone(),
                            );
                            match dht::put_record_bytes(
                                &mut swarm.behaviour_mut().kademlia,
                                key.clone(),
                                value_bytes.clone(),
                            ) {
                                Ok(query_id) => {
                                    pending_put.insert(
                                        query_id,
                                        PendingPut {
                                            key,
                                            value: value_bytes,
                                            respond_to,
                                        },
                                    );
                                }
                                Err(err) => {
                                    let _ = respond_to.send(Err(err.to_string()));
                                }
                            }
                        }
                        RpcCommand::GetRecord { key, respond_to } => {
                            if let Some(value) = local_record_value(&mut swarm, &key)
                                .and_then(|bytes| String::from_utf8(bytes).ok())
                                .and_then(|value| normalize_get_record_value(&key, value))
                            {
                                let _ = respond_to.send(Some(value));
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
                            if let Some(value) = local_record_value(&mut swarm, &key)
                                .and_then(|bytes| String::from_utf8(bytes).ok())
                            {
                                let _ = respond_to.send(Some(value));
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
                        RpcCommand::GetBlock { hash, respond_to } => {
                            let key = format!("block:{hash}");
                            if let Some(value) = local_record_value(&mut swarm, &key) {
                                let _ = respond_to.send(Some(hex_encode(&value)));
                                continue;
                            }
                            let query_id = dht::get_record_bytes(&mut swarm.behaviour_mut().kademlia, key.clone());
                            pending_get_block.insert(
                                query_id,
                                PendingBlockQuery {
                                    key,
                                    hash,
                                    attempts: 1,
                                    respond_to,
                                },
                            );
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
                                    let record =
                                        kad::Record::new(kad::RecordKey::new(key), value.clone());
                                    if let Err(err) =
                                        swarm.behaviour_mut().kademlia.store_mut().put(record.clone())
                                    {
                                        warn!(key = %key, error = %err, "failed to restore record to local store before republish");
                                    }
                                    if let Err(err) = swarm
                                        .behaviour_mut()
                                        .kademlia
                                        .put_record(record, kad::Quorum::One)
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
                    &mut pending_get_block,
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
    pending_get_block: &mut HashMap<kad::QueryId, PendingBlockQuery>,
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

                    match publish_query.kind {
                        PublishQueryKind::Block => {
                            if let Some(err) = task.failed {
                                let _ = task.respond_to.send(Err(err));
                                return;
                            }

                            let Some((manifest_key, manifest_value)) = task.manifest_record.take()
                            else {
                                let _ = task
                                    .respond_to
                                    .send(Err("internal publish error: missing manifest record"
                                        .to_string()));
                                return;
                            };

                            match dht::put_record_bytes(
                                &mut swarm.behaviour_mut().kademlia,
                                manifest_key,
                                manifest_value,
                            ) {
                                Ok(query_id) => {
                                    task.remaining = 1;
                                    publish_query_to_task.insert(
                                        query_id,
                                        PublishQuery {
                                            task_id,
                                            kind: PublishQueryKind::Manifest,
                                        },
                                    );
                                    publish_tasks.insert(task_id, task);
                                }
                                Err(err) => {
                                    let _ = task.respond_to.send(Err(err.to_string()));
                                }
                            }
                        }
                        PublishQueryKind::Manifest => {
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

                            // Even when we return an error for quorum failures, keep trying
                            // to replicate local records in the background once peers settle.
                            if succeeded || task.had_quorum_failed {
                                let rpc_tx_retry = rpc_tx.clone();
                                tokio::spawn(async move {
                                    tokio::time::sleep(Duration::from_secs(30)).await;
                                    let _ =
                                        rpc_tx_retry.send(RpcCommand::RepublishLocalRecords).await;
                                });
                            }
                        }
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
                    // Untracked put_record (e.g. from RepublishLocalRecords).
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
                } else if let Some(mut pending) = pending_get_block.remove(&id) {
                    match result {
                        Ok(kad::GetRecordOk::FoundRecord(record)) => {
                            let value = hex_encode(&record.record.value);
                            info!(key = ?record.record.key, bytes = record.record.value.len(), "kademlia get_block result");
                            let _ = pending.respond_to.send(Some(value));
                        }
                        Ok(_) => {
                            if pending.attempts < GET_RECORD_MAX_ATTEMPTS {
                                pending.attempts = pending.attempts.saturating_add(1);
                                let query_id = dht::get_record_bytes(
                                    &mut swarm.behaviour_mut().kademlia,
                                    pending.key.clone(),
                                );
                                pending_get_block.insert(query_id, pending);
                            } else {
                                info!(
                                    hash = %pending.hash,
                                    "kademlia get_block finished without record"
                                );
                                let _ = pending.respond_to.send(None);
                            }
                        }
                        Err(err) => {
                            if pending.attempts < GET_RECORD_MAX_ATTEMPTS {
                                warn!(
                                    error = %err,
                                    hash = %pending.hash,
                                    attempt = pending.attempts,
                                    "kademlia get_block failed; retrying"
                                );
                                pending.attempts = pending.attempts.saturating_add(1);
                                let query_id = dht::get_record_bytes(
                                    &mut swarm.behaviour_mut().kademlia,
                                    pending.key.clone(),
                                );
                                pending_get_block.insert(query_id, pending);
                            } else {
                                warn!(error = %err, hash = %pending.hash, "kademlia get_block failed");
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
                                if let Err(err) = dht::put_record_bytes(
                                    &mut swarm.behaviour_mut().kademlia,
                                    key.clone(),
                                    payload,
                                ) {
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
                                match dht::put_record_bytes(
                                    &mut swarm.behaviour_mut().kademlia,
                                    key,
                                    payload_bytes,
                                ) {
                                    Ok(query_id) => {
                                        pending_publish_claim_put.insert(
                                            query_id,
                                            PendingPublishClaimPut {
                                                name: pending.name,
                                                site_dir: pending.site_dir,
                                                respond_to: pending.respond_to,
                                            },
                                        );
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
                                                pending.claimed,
                                                pending.respond_to,
                                                publish_tasks,
                                                publish_query_to_task,
                                                next_publish_task_id,
                                                local_record_store,
                                                local_records,
                                            );
                                        }
                                    } else {
                                        start_publish_task(
                                            swarm,
                                            prepared,
                                            pending.claimed,
                                            pending.respond_to,
                                            publish_tasks,
                                            publish_query_to_task,
                                            next_publish_task_id,
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
                    );
                }
            }
            _ => {}
        },
        libp2p::swarm::SwarmEvent::Behaviour(LatticeBehaviourEvent::Identify(
            identify::Event::Received { peer_id, info, .. },
        )) => {
            info!(peer = %peer_id, observed_addr = %info.observed_addr, "identify received");
            for addr in info.listen_addrs {
                // Skip loopback and private addresses — adding them to
                // Kademlia causes dial attempts to 127.0.0.1 which connect
                // back to ourselves instead of the remote peer.
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

                // Re-push all locally stored records now that we have a relay address.
                // This ensures blocks and manifests published before the relay was ready
                // get replicated to the DHT and are reachable by other peers.
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

            if let Some(legacy_owner_key) = parse_legacy_name_owner(&value) {
                if legacy_owner_key == local_pubkey_hex {
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
    claimed: bool,
    respond_to: oneshot::Sender<Result<PublishSiteOk, String>>,
    publish_tasks: &mut HashMap<u64, PublishTask>,
    publish_query_to_task: &mut HashMap<kad::QueryId, PublishQuery>,
    next_publish_task_id: &mut u64,
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

    for (key, value) in prepared.block_records {
        remember_local_record(
            local_record_store,
            local_records,
            key.clone(),
            value.clone(),
        );
        match dht::put_record_bytes(&mut swarm.behaviour_mut().kademlia, key, value) {
            Ok(query_id) => {
                task.remaining = task.remaining.saturating_add(1);
                publish_query_to_task.insert(
                    query_id,
                    PublishQuery {
                        task_id,
                        kind: PublishQueryKind::Block,
                    },
                );
            }
            Err(err) => {
                if task.failed.is_none() {
                    task.failed = Some(err.to_string());
                }
            }
        }
    }

    if task.remaining == 0 {
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

        match dht::put_record_bytes(
            &mut swarm.behaviour_mut().kademlia,
            manifest_key,
            manifest_value,
        ) {
            Ok(query_id) => {
                task.remaining = 1;
                publish_query_to_task.insert(
                    query_id,
                    PublishQuery {
                        task_id,
                        kind: PublishQueryKind::Manifest,
                    },
                );
                publish_tasks.insert(task_id, task);
            }
            Err(err) => {
                let _ = task.respond_to.send(Err(err.to_string()));
            }
        }
    } else {
        publish_tasks.insert(task_id, task);
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

fn parse_legacy_name_owner(value: &str) -> Option<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return None;
    }

    let candidate = if trimmed.starts_with('"') {
        serde_json::from_str::<String>(trimmed).ok()?
    } else {
        trimmed.to_string()
    };

    if candidate.len() == 64 && candidate.chars().all(|c| c.is_ascii_hexdigit()) {
        Some(candidate.to_ascii_lowercase())
    } else {
        None
    }
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

/// Returns true if the multiaddr contains a loopback (127.x) or private
/// (10.x, 172.16-31.x, 192.168.x) IP address.  These must not be added to
/// Kademlia for remote peers — dialling 127.0.0.1 connects back to ourselves.
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

    if let Some(legacy_owner_key) = parse_legacy_name_owner(&value) {
        return Some(legacy_owner_key);
    }

    None
}

fn handle_claim_name_lookup_result(
    claim: PendingClaimGet,
    result: Result<kad::GetRecordOk, kad::GetRecordError>,
    swarm: &mut Swarm<LatticeBehaviour>,
    pending_claim_put: &mut HashMap<kad::QueryId, PendingClaimPut>,
    site_signing_key: &SigningKey,
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
        } else if let Some(legacy_owner_key) = parse_legacy_name_owner(&existing_value) {
            if legacy_owner_key != pubkey_hex {
                let _ = respond_to.send(Err("name already claimed".to_string()));
                return;
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

    match dht::put_record_bytes(&mut swarm.behaviour_mut().kademlia, key, payload_bytes) {
        Ok(query_id) => {
            pending_claim_put.insert(query_id, PendingClaimPut { name, respond_to });
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
            let mut task = if let Some(task) = get_site_tasks.remove(&task_id) {
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
                    if let Ok(value) = String::from_utf8(record.record.value) {
                        if let Some(owner) = parse_verified_name_record(&name, &value) {
                            if !owner.is_expired() && owner.key != manifest_publisher_key {
                                let _ =
                                    task.respond_to
                                        .send(Err("manifest publisher does not match name owner"
                                            .to_string()));
                                return;
                            }
                        } else if let Some(legacy_owner_key) = parse_legacy_name_owner(&value) {
                            if legacy_owner_key != manifest_publisher_key {
                                let _ =
                                    task.respond_to
                                        .send(Err("manifest publisher does not match name owner"
                                            .to_string()));
                                return;
                            }
                            warn!(
                                name = %name,
                                "using legacy unsigned name owner record for compatibility"
                            );
                        } else {
                            warn!(name = %name, "invalid name ownership record; treating as unclaimed");
                        }
                    } else {
                        warn!(name = %name, "invalid name record bytes; treating as unclaimed");
                    }
                }
                Ok(_) | Err(kad::GetRecordError::NotFound { .. }) => {
                    warn!(name = %name, "name record not found; serving unclaimed site");
                }
                Err(err) => {
                    warn!(
                        name = %name,
                        error = %err,
                        "failed to resolve name ownership; serving site without ownership confirmation"
                    );
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

            let next_block_hash = match start_next_file_download(&mut task) {
                Ok(Some(hash)) => hash,
                Ok(None) => {
                    let response = GetSiteResponse {
                        name: manifest_name,
                        version: manifest_version,
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

            let query_id = dht::get_record_bytes(
                &mut swarm.behaviour_mut().kademlia,
                format!("block:{next_block_hash}"),
            );
            get_site_queries.insert(
                query_id,
                GetSiteQuery::FileBlock {
                    task_id,
                    block_hash: next_block_hash,
                },
            );
            get_site_tasks.insert(task_id, task);
        }
        GetSiteQuery::FileBlock {
            task_id,
            block_hash,
        } => {
            let mut task = if let Some(task) = get_site_tasks.remove(&task_id) {
                task
            } else {
                return;
            };

            let stored = match result {
                Ok(kad::GetRecordOk::FoundRecord(record)) => record.record.value,
                _ => {
                    let _ = task
                        .respond_to
                        .send(Err(format!("block missing: {block_hash}")));
                    return;
                }
            };

            let raw_bytes = resolve_block_bytes(&stored, &block_hash);
            let actual_hash = hex::encode(Sha256::digest(&raw_bytes));
            if actual_hash != block_hash {
                let _ = task.respond_to.send(Err(format!(
                    "block hash mismatch for chunk {}: expected {} got {}",
                    block_hash, block_hash, actual_hash
                )));
                return;
            }
            let next_total = task.total_bytes.saturating_add(raw_bytes.len() as u64);
            if next_total > MAX_GET_SITE_TOTAL_BYTES {
                let _ = task
                    .respond_to
                    .send(Err("site exceeds maximum total size".to_string()));
                return;
            }
            task.total_bytes = next_total;

            let mut next_block_to_fetch: Option<String> = None;
            let mut completed_file: Option<SiteFile> = None;
            {
                let Some(active) = task.active_file.as_mut() else {
                    let _ = task
                        .respond_to
                        .send(Err("site task missing active file".to_string()));
                    return;
                };
                active.bytes.extend_from_slice(&raw_bytes);

                if active.next_block_index < active.block_hashes.len() {
                    next_block_to_fetch =
                        Some(active.block_hashes[active.next_block_index].clone());
                    active.next_block_index += 1;
                } else {
                    let finished = task.active_file.take().expect("active file exists");
                    let file_hash = hex::encode(Sha256::digest(&finished.bytes));
                    if file_hash != finished.expected_hash {
                        let _ = task.respond_to.send(Err(format!(
                            "file hash mismatch for {}: expected {} got {}",
                            finished.path, finished.expected_hash, file_hash
                        )));
                        return;
                    }
                    completed_file = Some(SiteFile {
                        path: finished.path.clone(),
                        contents: BASE64_STANDARD.encode(finished.bytes),
                        mime_type: infer_mime_type(&finished.path).to_string(),
                    });
                }
            }

            if let Some(file) = completed_file {
                task.files.push(file);
            }

            let next_block_hash = if let Some(next_hash) = next_block_to_fetch {
                next_hash
            } else {
                match start_next_file_download(&mut task) {
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
                }
            };

            let query_id = dht::get_record_bytes(
                &mut swarm.behaviour_mut().kademlia,
                format!("block:{next_block_hash}"),
            );
            get_site_queries.insert(
                query_id,
                GetSiteQuery::FileBlock {
                    task_id,
                    block_hash: next_block_hash,
                },
            );
            get_site_tasks.insert(task_id, task);
        }
    }
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

fn infer_mime_type(path: &str) -> &'static str {
    let ext = Path::new(path)
        .extension()
        .and_then(|e| e.to_str())
        .map(|e| e.to_ascii_lowercase());

    match ext.as_deref() {
        Some("html") => "text/html",
        Some("css") => "text/css",
        Some("js") => "application/javascript",
        Some("png") => "image/png",
        Some("jpg") | Some("jpeg") => "image/jpeg",
        Some("gif") => "image/gif",
        Some("svg") => "image/svg+xml",
        Some("ico") => "image/x-icon",
        _ => "application/octet-stream",
    }
}

fn decode_block_storage(stored: &[u8]) -> Option<Vec<u8>> {
    let hex = std::str::from_utf8(stored).ok()?.trim();
    decode_hex(hex).ok()
}

fn resolve_block_bytes(stored: &[u8], expected_hash: &str) -> Vec<u8> {
    if let Some(decoded) = decode_block_storage(stored) {
        let decoded_hash = hex::encode(Sha256::digest(&decoded));
        if decoded_hash == expected_hash {
            return decoded;
        }
    }
    stored.to_vec()
}

fn decode_hex(input: &str) -> Result<Vec<u8>> {
    if !input.len().is_multiple_of(2) {
        bail!("hex input length must be even");
    }

    let mut out = Vec::with_capacity(input.len() / 2);
    let bytes = input.as_bytes();
    for i in (0..bytes.len()).step_by(2) {
        let hi = hex_nibble(bytes[i])?;
        let lo = hex_nibble(bytes[i + 1])?;
        out.push((hi << 4) | lo);
    }
    Ok(out)
}

fn hex_nibble(byte: u8) -> Result<u8> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        _ => bail!("invalid hex character"),
    }
}

fn prepare_publish(
    name: &str,
    site_dir: &Path,
    signing_key: &SigningKey,
    dht_baseline_version: u64,
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

    let mut block_records = Vec::new();
    let mut seen_block_hashes: HashSet<String> = HashSet::new();
    for file in &manifest.files {
        let file_path = site_dir.join(&file.path);
        let contents = fs::read(&file_path)
            .with_context(|| format!("failed to read site file {}", file_path.display()))?;

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
                block_records.push((format!("block:{block_hash}"), contents));
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
                block_records.push((format!("block:{chunk_hash}"), chunk.to_vec()));
            }
        }
    }

    let manifest_json =
        serde_json::to_string(&manifest).context("failed to serialize site manifest")?;
    let manifest_record = (format!("site:{name}"), manifest_json.into_bytes());

    Ok(PreparedPublish {
        version: manifest.version,
        file_count: manifest.files.len(),
        block_records,
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
    fn open(path: &Path) -> Result<Self> {
        fs::create_dir_all(path)
            .with_context(|| format!("failed to create local records db dir {}", path.display()))?;
        let db = sled::open(path)
            .with_context(|| format!("failed to open local records db at {}", path.display()))?;
        let records = db
            .open_tree("records")
            .context("failed to open records tree")?;
        let meta = db.open_tree("meta").context("failed to open meta tree")?;
        Ok(Self { db, records, meta })
    }

    fn load_records(&self) -> Result<HashMap<String, Vec<u8>>> {
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

    fn put_record(&self, key: &str, value: &[u8], pinned: bool) -> Result<()> {
        self.records
            .insert(key.as_bytes(), value)
            .context("failed to persist local record value")?;

        let existing_pinned = self
            .meta
            .get(key.as_bytes())
            .context("failed to read local record metadata")?
            .and_then(|raw| serde_json::from_slice::<RecordMeta>(&raw).ok())
            .map(|meta| meta.pinned)
            .unwrap_or(false);

        let meta = RecordMeta {
            pinned: pinned || existing_pinned,
            updated_at: unix_ts(),
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
                    pinned: key_should_be_pinned(&key_str),
                    updated_at: 0,
                });
            if !meta.pinned {
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
    key.starts_with("name:") || key.starts_with("site:") || key.starts_with("block:")
}

fn unix_ts() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|dur| dur.as_secs())
        .unwrap_or(0)
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
            continue;
        }

        if let Some(legacy_key) = parse_legacy_name_owner(value_str) {
            if legacy_key == local_pubkey_hex {
                names.insert(name.to_string());
            }
        }
    }
    names
}
