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
use lattice_site::manifest::{hash_bytes, verify_manifest, SiteManifest};
use lattice_site::publisher as site_publisher;
use libp2p::autonat;
use libp2p::dcutr;
use libp2p::futures::StreamExt;
use libp2p::gossipsub;
use libp2p::identify;
use libp2p::kad;
use libp2p::mdns;
use libp2p::relay;
use libp2p::swarm::NetworkBehaviour;
use libp2p::{Multiaddr, Swarm};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::Path;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use tokio::sync::{mpsc, oneshot};
use tokio::time::Duration;
use tracing::{error, info, warn};

const MAX_CONCURRENT_GET_SITE: usize = 50;
const MAX_CONCURRENT_PUBLISH: usize = 10;
const MAX_GET_SITE_FILES: usize = 1000;
const MAX_GET_SITE_TOTAL_BYTES: u64 = 100 * 1024 * 1024;

#[derive(NetworkBehaviour)]
struct LatticeBehaviour {
    kademlia: kad::Behaviour<kad::store::MemoryStore>,
    mdns: mdns::tokio::Behaviour,
    gossipsub: gossipsub::Behaviour,
    identify: identify::Behaviour,
    autonat: autonat::Behaviour,
    relay: relay::Behaviour,
    dcutr: dcutr::Behaviour,
}

struct PublishTask {
    respond_to: oneshot::Sender<Result<PublishSiteOk, String>>,
    remaining: u32,
    failed: Option<String>,
    version: u64,
    file_count: usize,
    claimed: bool,
}

struct GetSiteTask {
    respond_to: oneshot::Sender<Result<GetSiteResponse, String>>,
    requested_name: String,
    manifest: Option<SiteManifest>,
    next_index: usize,
    total_bytes: u64,
    files: Vec<SiteFile>,
}

enum GetSiteQuery {
    Manifest {
        task_id: u64,
    },
    NameOwner {
        task_id: u64,
        name: String,
    },
    Block {
        task_id: u64,
        hash: String,
        path: String,
    },
}

struct PreparedPublish {
    version: u64,
    file_count: usize,
    records: Vec<(String, Vec<u8>)>,
}

struct PendingTextQuery {
    key: String,
    respond_to: oneshot::Sender<Option<String>>,
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

    let peer_id = node_identity.peer_id;

    let mut swarm = transport::build_swarm(
        node_identity.keypair,
        |key| -> std::result::Result<LatticeBehaviour, Box<dyn std::error::Error + Send + Sync>> {
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
                dcutr,
            })
        },
    )?;

    let listen_addr: Multiaddr =
        Multiaddr::from_str(&format!("/ip4/0.0.0.0/tcp/{}", config.listen_port))?;
    swarm.listen_on(listen_addr)?;

    let quic_addr: Multiaddr =
        Multiaddr::from_str(&format!("/ip4/0.0.0.0/udp/{}/quic-v1", config.listen_port))?;
    swarm.listen_on(quic_addr)?;

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

    let mut pending_put: HashMap<kad::QueryId, oneshot::Sender<Result<(), String>>> =
        HashMap::new();
    let mut pending_get_text: HashMap<kad::QueryId, PendingTextQuery> = HashMap::new();
    let mut pending_get_block: HashMap<kad::QueryId, oneshot::Sender<Option<String>>> =
        HashMap::new();
    let mut pending_claim_put: HashMap<kad::QueryId, PendingClaimPut> = HashMap::new();
    let mut pending_name_probes: HashMap<kad::QueryId, PendingNameProbe> = HashMap::new();
    let mut pending_publish_checks: HashMap<kad::QueryId, PendingPublishOwnershipCheck> =
        HashMap::new();
    let mut pending_publish_claim_put: HashMap<kad::QueryId, PendingPublishClaimPut> =
        HashMap::new();
    let mut pending_publish_version_checks: HashMap<kad::QueryId, PendingPublishVersionCheck> =
        HashMap::new();

    let mut publish_tasks: HashMap<u64, PublishTask> = HashMap::new();
    let mut publish_query_to_task: HashMap<kad::QueryId, u64> = HashMap::new();
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
                            match dht::put_record(&mut swarm.behaviour_mut().kademlia, key, value) {
                                Ok(query_id) => {
                                    pending_put.insert(query_id, respond_to);
                                }
                                Err(err) => {
                                    let _ = respond_to.send(Err(err.to_string()));
                                }
                            }
                        }
                        RpcCommand::GetRecord { key, respond_to } => {
                            let query_id = dht::get_record(&mut swarm.behaviour_mut().kademlia, key.clone());
                            pending_get_text.insert(
                                query_id,
                                PendingTextQuery {
                                    key,
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
                                    respond_to,
                                },
                            );
                        }
                        RpcCommand::GetSiteManifest { name, respond_to } => {
                            let key = format!("site:{name}");
                            let query_id = dht::get_record(&mut swarm.behaviour_mut().kademlia, key.clone());
                            pending_get_text.insert(
                                query_id,
                                PendingTextQuery {
                                    key,
                                    respond_to,
                                },
                            );
                        }
                        RpcCommand::GetBlock { hash, respond_to } => {
                            let query_id = dht::get_record_bytes(&mut swarm.behaviour_mut().kademlia, format!("block:{hash}"));
                            pending_get_block.insert(query_id, respond_to);
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
                                next_index: 0,
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
                    &owned_names,
                    &rpc_tx,
                    &site_signing_key,
                    &local_pubkey_hex,
                );
            }
        }
    }
}

fn handle_swarm_event(
    event: libp2p::swarm::SwarmEvent<LatticeBehaviourEvent>,
    swarm: &mut Swarm<LatticeBehaviour>,
    pending_put: &mut HashMap<kad::QueryId, oneshot::Sender<Result<(), String>>>,
    pending_get_text: &mut HashMap<kad::QueryId, PendingTextQuery>,
    pending_get_block: &mut HashMap<kad::QueryId, oneshot::Sender<Option<String>>>,
    pending_claim_put: &mut HashMap<kad::QueryId, PendingClaimPut>,
    pending_name_probes: &mut HashMap<kad::QueryId, PendingNameProbe>,
    pending_publish_checks: &mut HashMap<kad::QueryId, PendingPublishOwnershipCheck>,
    pending_publish_claim_put: &mut HashMap<kad::QueryId, PendingPublishClaimPut>,
    pending_publish_version_checks: &mut HashMap<kad::QueryId, PendingPublishVersionCheck>,
    publish_tasks: &mut HashMap<u64, PublishTask>,
    publish_query_to_task: &mut HashMap<kad::QueryId, u64>,
    next_publish_task_id: &mut u64,
    get_site_tasks: &mut HashMap<u64, GetSiteTask>,
    get_site_queries: &mut HashMap<kad::QueryId, GetSiteQuery>,
    owned_names: &Arc<Mutex<HashSet<String>>>,
    rpc_tx: &mpsc::Sender<RpcCommand>,
    site_signing_key: &SigningKey,
    local_pubkey_hex: &str,
) {
    match event {
        libp2p::swarm::SwarmEvent::ConnectionEstablished {
            peer_id, endpoint, ..
        } => {
            info!(peer = %peer_id, address = ?endpoint.get_remote_address(), "new peer connected");
        }
        libp2p::swarm::SwarmEvent::ConnectionClosed { peer_id, .. } => {
            info!(peer = %peer_id, "peer disconnected");
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
                if let Some(ch) = pending_put.remove(&id) {
                    match result {
                        Ok(ok) => {
                            info!(key = ?ok.key, "kademlia put_record succeeded");
                            let _ = ch.send(Ok(()));
                        }
                        Err(err) => {
                            warn!(error = %err, "kademlia put_record failed");
                            let _ = ch.send(Err(err.to_string()));
                        }
                    }
                } else if let Some(task_id) = publish_query_to_task.remove(&id) {
                    if let Some(task) = publish_tasks.get_mut(&task_id) {
                        task.remaining = task.remaining.saturating_sub(1);
                        match result {
                            Ok(ok) => {
                                info!(key = ?ok.key, "kademlia publish put_record succeeded");
                            }
                            Err(err) => {
                                let err_str = err.to_string();
                                if err_str.contains("quorum failed") {
                                    warn!(
                                        task_id,
                                        error = %err,
                                        "put_record quorum failed but record stored locally — continuing"
                                    );
                                } else {
                                    warn!(task_id, error = %err, "kademlia publish put_record failed");
                                    if task.failed.is_none() {
                                        task.failed = Some(err_str);
                                    }
                                }
                            }
                        }

                        if task.remaining == 0 {
                            let task = match publish_tasks.remove(&task_id) {
                                Some(task) => task,
                                None => {
                                    error!(
                                        task_id,
                                        "publish task missing from map — internal error"
                                    );
                                    return;
                                }
                            };
                            let response = match task.failed {
                                Some(err) => Err(err),
                                None => Ok(PublishSiteOk {
                                    version: task.version,
                                    file_count: task.file_count,
                                    claimed: task.claimed,
                                }),
                            };
                            let _ = task.respond_to.send(response);
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
                }
            }
            kad::QueryResult::GetRecord(result) => {
                if let Some(pending) = pending_get_text.remove(&id) {
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
                            info!("kademlia get_record finished without record");
                            let _ = pending.respond_to.send(None);
                        }
                        Err(err) => {
                            warn!(error = %err, "kademlia get_record failed");
                            let _ = pending.respond_to.send(None);
                        }
                    }
                } else if let Some(ch) = pending_get_block.remove(&id) {
                    match result {
                        Ok(kad::GetRecordOk::FoundRecord(record)) => {
                            let value = hex_encode(&record.record.value);
                            info!(key = ?record.record.key, bytes = record.record.value.len(), "kademlia get_block result");
                            let _ = ch.send(Some(value));
                        }
                        Ok(_) => {
                            info!("kademlia get_block finished without record");
                            let _ = ch.send(None);
                        }
                        Err(err) => {
                            warn!(error = %err, "kademlia get_block failed");
                            let _ = ch.send(None);
                        }
                    }
                } else if let Some(pending) = pending_publish_checks.remove(&id) {
                    match publish_name_ownership(&pending.name, result, local_pubkey_hex) {
                        Ok(PublishNameOwnership::OwnedByLocal) => {
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
                            let record = NameRecord::new_signed(
                                local_pubkey_hex.to_string(),
                                &pending.name,
                                site_signing_key,
                            );
                            let payload = match serde_json::to_string(&record) {
                                Ok(payload) => payload,
                                Err(err) => {
                                    let _ = pending
                                        .respond_to
                                        .send(Err(format!("failed to encode name record: {err}")));
                                    return;
                                }
                            };
                            match dht::put_record(
                                &mut swarm.behaviour_mut().kademlia,
                                format!("name:{}", pending.name),
                                payload,
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
            info!(peer = %peer_id, "identify received");
            for addr in info.listen_addrs {
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
            warn!(peer = ?peer_id, error = %error, "outgoing connection error");
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

fn start_publish_task(
    swarm: &mut Swarm<LatticeBehaviour>,
    prepared: PreparedPublish,
    claimed: bool,
    respond_to: oneshot::Sender<Result<PublishSiteOk, String>>,
    publish_tasks: &mut HashMap<u64, PublishTask>,
    publish_query_to_task: &mut HashMap<kad::QueryId, u64>,
    next_publish_task_id: &mut u64,
) {
    let task_id = *next_publish_task_id;
    *next_publish_task_id = (*next_publish_task_id).saturating_add(1);

    let mut task = PublishTask {
        respond_to,
        remaining: 0,
        failed: None,
        version: prepared.version,
        file_count: prepared.file_count,
        claimed,
    };

    for (key, value) in prepared.records {
        match dht::put_record_bytes(&mut swarm.behaviour_mut().kademlia, key, value) {
            Ok(query_id) => {
                task.remaining = task.remaining.saturating_add(1);
                publish_query_to_task.insert(query_id, task_id);
            }
            Err(err) => {
                if task.failed.is_none() {
                    task.failed = Some(err.to_string());
                }
            }
        }
    }

    if task.remaining == 0 {
        let response = match task.failed {
            Some(err) => Err(err),
            None => Ok(PublishSiteOk {
                version: task.version,
                file_count: task.file_count,
                claimed: task.claimed,
            }),
        };
        let _ = task.respond_to.send(response);
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

    match dht::put_record(
        &mut swarm.behaviour_mut().kademlia,
        format!("name:{name}"),
        payload,
    ) {
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

            let manifest = match task.manifest.as_ref() {
                Some(manifest) => manifest,
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
                            if !owner.is_expired() && owner.key != manifest.publisher_key {
                                let _ =
                                    task.respond_to
                                        .send(Err("manifest publisher does not match name owner"
                                            .to_string()));
                                return;
                            }
                        } else {
                            let _ = task
                                .respond_to
                                .send(Err("invalid name ownership record".to_string()));
                            return;
                        }
                    } else {
                        let _ = task
                            .respond_to
                            .send(Err("invalid name ownership record".to_string()));
                        return;
                    }
                }
                Ok(_) | Err(kad::GetRecordError::NotFound { .. }) => {
                    warn!(name = %name, "name record not found; serving unclaimed site");
                }
                Err(err) => {
                    let _ = task
                        .respond_to
                        .send(Err(format!("failed to resolve name ownership: {err}")));
                    return;
                }
            }

            if manifest.files.is_empty() {
                let response = GetSiteResponse {
                    name: manifest.name.clone(),
                    version: manifest.version,
                    files: Vec::new(),
                };
                let _ = task.respond_to.send(Ok(response));
                return;
            }

            let first = manifest.files[0].clone();
            task.next_index = 1;

            let query_id = dht::get_record_bytes(
                &mut swarm.behaviour_mut().kademlia,
                format!("block:{}", first.hash),
            );
            get_site_queries.insert(
                query_id,
                GetSiteQuery::Block {
                    task_id,
                    hash: first.hash,
                    path: first.path,
                },
            );
            get_site_tasks.insert(task_id, task);
        }
        GetSiteQuery::Block {
            task_id,
            hash,
            path,
        } => {
            let mut task = if let Some(task) = get_site_tasks.remove(&task_id) {
                task
            } else {
                return;
            };

            let stored = match result {
                Ok(kad::GetRecordOk::FoundRecord(record)) => record.record.value,
                _ => {
                    let _ = task.respond_to.send(Err(format!("block missing: {hash}")));
                    return;
                }
            };

            let raw_bytes = decode_block_storage(&stored).unwrap_or(stored);
            let actual_hash = hex::encode(Sha256::digest(&raw_bytes));
            if actual_hash != hash {
                let _ = task.respond_to.send(Err(format!(
                    "block hash mismatch for {}: expected {} got {}",
                    path, hash, actual_hash
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

            task.files.push(SiteFile {
                path: path.clone(),
                contents: BASE64_STANDARD.encode(raw_bytes),
                mime_type: infer_mime_type(&path).to_string(),
            });

            let manifest = match task.manifest.as_ref() {
                Some(manifest) => manifest,
                None => {
                    let _ = task
                        .respond_to
                        .send(Err("site task missing manifest".to_string()));
                    return;
                }
            };

            if task.next_index >= manifest.files.len() {
                let response = GetSiteResponse {
                    name: manifest.name.clone(),
                    version: manifest.version,
                    files: task.files,
                };
                let _ = task.respond_to.send(Ok(response));
                return;
            }

            let next_file = manifest.files[task.next_index].clone();
            task.next_index += 1;

            let query_id = dht::get_record_bytes(
                &mut swarm.behaviour_mut().kademlia,
                format!("block:{}", next_file.hash),
            );
            get_site_queries.insert(
                query_id,
                GetSiteQuery::Block {
                    task_id,
                    hash: next_file.hash,
                    path: next_file.path,
                },
            );
            get_site_tasks.insert(task_id, task);
        }
    }
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

fn decode_hex(input: &str) -> Result<Vec<u8>> {
    if input.len() % 2 != 0 {
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

    let mut records = Vec::new();
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

        records.push((format!("block:{}", file.hash), contents));
    }

    let manifest_json =
        serde_json::to_string(&manifest).context("failed to serialize site manifest")?;
    records.push((format!("site:{name}"), manifest_json.into_bytes()));

    Ok(PreparedPublish {
        version: manifest.version,
        file_count: manifest.files.len(),
        records,
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
