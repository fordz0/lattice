use anyhow::{bail, Context, Result};
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use base64::Engine as _;
use ed25519_dalek::SigningKey;
use lattice_daemon::config::load_or_create_config;
use lattice_daemon::dht;
use lattice_daemon::http_server;
use lattice_daemon::node::load_or_create_identity;
use lattice_daemon::rpc::{self, GetSiteResponse, NodeInfoResponse, PublishSiteOk, RpcCommand, SiteFile};
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
use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::str::FromStr;
use tokio::sync::{mpsc, oneshot};
use tracing::{error, info, warn};

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
    file_count: u32,
}

struct GetSiteTask {
    respond_to: oneshot::Sender<Result<GetSiteResponse, String>>,
    manifest: Option<SiteManifest>,
    next_index: usize,
    files: Vec<SiteFile>,
}

enum GetSiteQuery {
    Manifest {
        task_id: u64,
    },
    Block {
        task_id: u64,
        hash: String,
        path: String,
    },
}

struct PreparedPublish {
    version: u64,
    file_count: u32,
    records: Vec<(String, Vec<u8>)>,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let config = load_or_create_config()?;
    let node_identity = load_or_create_identity(&config.data_dir)?;
    let site_signing_key = load_site_signing_key(&config.data_dir)?;

    let peer_id = node_identity.peer_id;

    let mut swarm = transport::build_swarm(node_identity.keypair, |key| -> std::result::Result<
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
            dcutr,
        })
    })?;

    let listen_addr: Multiaddr = Multiaddr::from_str(&format!("/ip4/0.0.0.0/tcp/{}", config.listen_port))?;
    swarm.listen_on(listen_addr)?;

    let quic_addr: Multiaddr = Multiaddr::from_str(&format!("/ip4/0.0.0.0/udp/{}/quic-v1", config.listen_port))?;
    swarm.listen_on(quic_addr)?;

    let (rpc_tx, mut rpc_rx) = mpsc::channel::<RpcCommand>(64);
    let _rpc_server = rpc::start_rpc_server(config.rpc_port, rpc_tx.clone()).await?;
    let http_rpc_tx = rpc_tx.clone();
    let _http_server = tokio::spawn(async move {
        if let Err(err) = http_server::start_http_server(7781, http_rpc_tx).await {
            error!(error = %err, "http server exited");
        }
    });
    info!("HTTP server listening on port 7781");

    info!(peer_id = %peer_id, "lattice daemon started");
    info!(port = config.listen_port, rpc_port = config.rpc_port, "listening and RPC configured");

    let mut pending_put: HashMap<kad::QueryId, oneshot::Sender<Result<(), String>>> = HashMap::new();
    let mut pending_get_text: HashMap<kad::QueryId, oneshot::Sender<Option<String>>> = HashMap::new();
    let mut pending_get_block: HashMap<kad::QueryId, oneshot::Sender<Option<String>>> = HashMap::new();

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
                            let query_id = dht::get_record(&mut swarm.behaviour_mut().kademlia, key);
                            pending_get_text.insert(query_id, respond_to);
                        }
                        RpcCommand::PublishSite { name, site_dir, respond_to } => {
                            match prepare_publish(&name, Path::new(&site_dir), &site_signing_key) {
                                Ok(prepared) => {
                                    let task_id = next_publish_task_id;
                                    next_publish_task_id = next_publish_task_id.saturating_add(1);

                                    let mut task = PublishTask {
                                        respond_to,
                                        remaining: 0,
                                        failed: None,
                                        version: prepared.version,
                                        file_count: prepared.file_count,
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
                                            }),
                                        };
                                        let _ = task.respond_to.send(response);
                                    } else {
                                        publish_tasks.insert(task_id, task);
                                    }
                                }
                                Err(err) => {
                                    let _ = respond_to.send(Err(err.to_string()));
                                }
                            }
                        }
                        RpcCommand::GetSiteManifest { name, respond_to } => {
                            let query_id = dht::get_record(&mut swarm.behaviour_mut().kademlia, format!("site:{name}"));
                            pending_get_text.insert(query_id, respond_to);
                        }
                        RpcCommand::GetBlock { hash, respond_to } => {
                            let query_id = dht::get_record_bytes(&mut swarm.behaviour_mut().kademlia, format!("block:{hash}"));
                            pending_get_block.insert(query_id, respond_to);
                        }
                        RpcCommand::GetSite { name, respond_to } => {
                            let task_id = next_get_site_task_id;
                            next_get_site_task_id = next_get_site_task_id.saturating_add(1);

                            let task = GetSiteTask {
                                respond_to,
                                manifest: None,
                                next_index: 0,
                                files: Vec::new(),
                            };

                            let query_id = dht::get_record(&mut swarm.behaviour_mut().kademlia, format!("site:{name}"));
                            get_site_queries.insert(query_id, GetSiteQuery::Manifest { task_id });
                            get_site_tasks.insert(task_id, task);
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
                    &mut publish_tasks,
                    &mut publish_query_to_task,
                    &mut get_site_tasks,
                    &mut get_site_queries,
                );
            }
        }
    }
}

fn handle_swarm_event(
    event: libp2p::swarm::SwarmEvent<LatticeBehaviourEvent>,
    swarm: &mut Swarm<LatticeBehaviour>,
    pending_put: &mut HashMap<kad::QueryId, oneshot::Sender<Result<(), String>>>,
    pending_get_text: &mut HashMap<kad::QueryId, oneshot::Sender<Option<String>>>,
    pending_get_block: &mut HashMap<kad::QueryId, oneshot::Sender<Option<String>>>,
    publish_tasks: &mut HashMap<u64, PublishTask>,
    publish_query_to_task: &mut HashMap<kad::QueryId, u64>,
    get_site_tasks: &mut HashMap<u64, GetSiteTask>,
    get_site_queries: &mut HashMap<kad::QueryId, GetSiteQuery>,
) {
    match event {
        libp2p::swarm::SwarmEvent::ConnectionEstablished { peer_id, endpoint, .. } => {
            info!(peer = %peer_id, address = ?endpoint.get_remote_address(), "new peer connected");
        }
        libp2p::swarm::SwarmEvent::ConnectionClosed { peer_id, .. } => {
            info!(peer = %peer_id, "peer disconnected");
        }
        libp2p::swarm::SwarmEvent::NewListenAddr { address, .. } => {
            info!(address = %address, "node listening");
        }
        libp2p::swarm::SwarmEvent::Behaviour(LatticeBehaviourEvent::Mdns(mdns::Event::Discovered(list))) => {
            for (peer_id, addr) in list {
                info!(peer = %peer_id, address = %addr, "mDNS peer discovered");
                swarm.behaviour_mut().kademlia.add_address(&peer_id, addr);
            }
        }
        libp2p::swarm::SwarmEvent::Behaviour(LatticeBehaviourEvent::Mdns(mdns::Event::Expired(list))) => {
            for (peer_id, addr) in list {
                swarm.behaviour_mut().kademlia.remove_address(&peer_id, &addr);
            }
        }
        libp2p::swarm::SwarmEvent::Behaviour(LatticeBehaviourEvent::Kademlia(kad::Event::OutboundQueryProgressed { id, result, .. })) => {
            match result {
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
                                    warn!(error = %err, "kademlia publish put_record failed");
                                    if task.failed.is_none() {
                                        task.failed = Some(err.to_string());
                                    }
                                }
                            }

                            if task.remaining == 0 {
                                let task = publish_tasks.remove(&task_id).expect("publish task should exist");
                                let response = match task.failed {
                                    Some(err) => Err(err),
                                    None => Ok(PublishSiteOk {
                                        version: task.version,
                                        file_count: task.file_count,
                                    }),
                                };
                                let _ = task.respond_to.send(response);
                            }
                        }
                    }
                }
                kad::QueryResult::GetRecord(result) => {
                    if let Some(ch) = pending_get_text.remove(&id) {
                        match result {
                            Ok(kad::GetRecordOk::FoundRecord(record)) => {
                                let value = String::from_utf8(record.record.value).ok();
                                info!(key = ?record.record.key, found = value.is_some(), "kademlia get_record result");
                                let _ = ch.send(value);
                            }
                            Ok(_) => {
                                info!("kademlia get_record finished without record");
                                let _ = ch.send(None);
                            }
                            Err(err) => {
                                warn!(error = %err, "kademlia get_record failed");
                                let _ = ch.send(None);
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
                    } else if let Some(query) = get_site_queries.remove(&id) {
                        handle_get_site_query_result(query, result, swarm, get_site_tasks, get_site_queries);
                    }
                }
                _ => {}
            }
        }
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
                    match String::from_utf8(record.record.value)
                        .ok()
                        .and_then(|json| serde_json::from_str::<SiteManifest>(&json).ok())
                    {
                        Some(manifest) => manifest,
                        None => {
                            let _ = task.respond_to.send(Err("invalid site manifest".to_string()));
                            return;
                        }
                    }
                }
                _ => {
                    let _ = task.respond_to.send(Err("site not found".to_string()));
                    return;
                }
            };

            if manifest.files.is_empty() {
                let response = GetSiteResponse {
                    name: manifest.name,
                    version: manifest.version,
                    files: Vec::new(),
                };
                let _ = task.respond_to.send(Ok(response));
                return;
            }

            let first = manifest.files[0].clone();
            task.manifest = Some(manifest);
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
        GetSiteQuery::Block { task_id, hash, path } => {
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
            task.files.push(SiteFile {
                path: path.clone(),
                contents: BASE64_STANDARD.encode(raw_bytes),
                mime_type: infer_mime_type(&path).to_string(),
            });

            let manifest = match task.manifest.as_ref() {
                Some(manifest) => manifest,
                None => {
                    let _ = task.respond_to.send(Err("site task missing manifest".to_string()));
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

fn load_site_signing_key(data_dir: &Path) -> Result<SigningKey> {
    let key_path = data_dir.join("identity.key");
    let bytes = fs::read(&key_path)
        .with_context(|| format!("failed to read identity key {}", key_path.display()))?;

    if bytes.len() != 32 {
        bail!(
            "invalid identity key length in {}: expected 32 bytes, got {}",
            key_path.display(),
            bytes.len()
        );
    }

    let mut secret = [0_u8; 32];
    secret.copy_from_slice(&bytes);
    Ok(SigningKey::from_bytes(&secret))
}

fn prepare_publish(name: &str, site_dir: &Path, signing_key: &SigningKey) -> Result<PreparedPublish> {
    let (existing_version, rating) = match site_publisher::load_manifest(site_dir) {
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

    let manifest = site_publisher::build_manifest(name, site_dir, signing_key, &rating, existing_version)?;
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

    let manifest_json = serde_json::to_string(&manifest).context("failed to serialize site manifest")?;
    records.push((format!("site:{name}"), manifest_json.into_bytes()));

    Ok(PreparedPublish {
        version: manifest.version,
        file_count: manifest.files.len() as u32,
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
