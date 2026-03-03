use anyhow::Result;
use lattice_daemon::config::load_or_create_config;
use lattice_daemon::dht;
use lattice_daemon::node::load_or_create_identity;
use lattice_daemon::rpc::{self, NodeInfoResponse, RpcCommand};
use lattice_daemon::transport;
use libp2p::futures::StreamExt;
use libp2p::gossipsub;
use libp2p::identify;
use libp2p::kad;
use libp2p::mdns;
use libp2p::swarm::NetworkBehaviour;
use libp2p::{Multiaddr, Swarm};
use std::collections::HashMap;
use std::str::FromStr;
use tokio::sync::{mpsc, oneshot};
use tracing::{error, info, warn};

#[derive(NetworkBehaviour)]
struct LatticeBehaviour {
    kademlia: kad::Behaviour<kad::store::MemoryStore>,
    mdns: mdns::tokio::Behaviour,
    gossipsub: gossipsub::Behaviour,
    identify: identify::Behaviour,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let config = load_or_create_config()?;
    let node_identity = load_or_create_identity(&config.data_dir)?;

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

        Ok(LatticeBehaviour {
            kademlia,
            mdns,
            gossipsub,
            identify,
        })
    })?;

    let listen_addr: Multiaddr = Multiaddr::from_str(&format!("/ip4/0.0.0.0/tcp/{}", config.listen_port))?;
    swarm.listen_on(listen_addr)?;

    let quic_addr: Multiaddr = Multiaddr::from_str(&format!("/ip4/0.0.0.0/udp/{}/quic-v1", config.listen_port))?;
    swarm.listen_on(quic_addr)?;

    let (rpc_tx, mut rpc_rx) = mpsc::channel::<RpcCommand>(64);
    let _rpc_server = rpc::start_rpc_server(config.rpc_port, rpc_tx.clone()).await?;

    info!(peer_id = %peer_id, "lattice daemon started");
    info!(port = config.listen_port, rpc_port = config.rpc_port, "listening and RPC configured");

    let mut pending_put: HashMap<kad::QueryId, oneshot::Sender<Result<(), String>>> = HashMap::new();
    let mut pending_get: HashMap<kad::QueryId, oneshot::Sender<Option<String>>> = HashMap::new();

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
                            pending_get.insert(query_id, respond_to);
                        }
                    }
                }
            }
            event = swarm.select_next_some() => {
                handle_swarm_event(event, &mut swarm, &mut pending_put, &mut pending_get);
            }
        }
    }
}

fn handle_swarm_event(
    event: libp2p::swarm::SwarmEvent<LatticeBehaviourEvent>,
    swarm: &mut Swarm<LatticeBehaviour>,
    pending_put: &mut HashMap<kad::QueryId, oneshot::Sender<Result<(), String>>>,
    pending_get: &mut HashMap<kad::QueryId, oneshot::Sender<Option<String>>>,
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
                    }
                }
                kad::QueryResult::GetRecord(result) => {
                    if let Some(ch) = pending_get.remove(&id) {
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
