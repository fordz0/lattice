use anyhow::{Context, Result};
use libp2p::kad::store::MemoryStore;
use libp2p::kad::store::RecordStore;
use libp2p::kad::{self, Behaviour, QueryId, Quorum, Record};
use libp2p::multiaddr::Protocol;
use libp2p::{Multiaddr, PeerId};
use std::num::NonZeroUsize;
use std::time::Duration;

pub fn new_kademlia(local_peer_id: PeerId) -> Behaviour<MemoryStore> {
    let mut store_config = kad::store::MemoryStoreConfig::default();
    // Allow block records up to 1 MiB (default 65 KiB is too small for site files).
    store_config.max_value_bytes = 1024 * 1024;
    let store = MemoryStore::with_config(local_peer_id, store_config);
    let mut config = kad::Config::default();
    config.set_record_ttl(Some(Duration::from_secs(48 * 60 * 60)));
    config.set_replication_interval(Some(Duration::from_secs(30 * 60)));
    config.set_provider_record_ttl(Some(Duration::from_secs(48 * 60 * 60)));
    // Use a high replication factor so put_record targets enough peers to
    // include bootstrap/relay nodes even when other peers are XOR-closer
    // to the key.  Critical for NAT-traversed nodes that can only reach
    // bootstrap peers — the default K=20 can miss them entirely.
    config.set_replication_factor(NonZeroUsize::new(200).unwrap());
    let mut kad = Behaviour::with_config(local_peer_id, store, config);
    kad.set_mode(Some(kad::Mode::Server));
    kad
}

pub fn add_bootstrap_peers(kad: &mut Behaviour<MemoryStore>, peers: &[String]) {
    for entry in peers {
        if let Ok(mut addr) = entry.parse::<Multiaddr>() {
            if let Some(Protocol::P2p(peer_id)) = addr.pop() {
                kad.add_address(&peer_id, addr);
            }
        }
    }
    // Trigger a bootstrap query so the routing table is populated
    // (FIND_NODE exchange with bootstrap peers) before the first publish.
    let _ = kad.bootstrap();
}

pub fn put_record_bytes(
    kad: &mut Behaviour<MemoryStore>,
    key: String,
    value: Vec<u8>,
) -> Result<QueryId> {
    let record = Record::new(key.into_bytes(), value);
    kad.store_mut()
        .put(record.clone())
        .context("failed to store record locally")?;

    let query_id = kad
        .put_record(record, Quorum::One)
        .context("failed to start put_record query")?;

    Ok(query_id)
}

pub fn put_record(kad: &mut Behaviour<MemoryStore>, key: String, value: String) -> Result<QueryId> {
    put_record_bytes(kad, key, value.into_bytes())
}

pub fn get_record(kad: &mut Behaviour<MemoryStore>, key: String) -> QueryId {
    kad.get_record(kad::RecordKey::new(&key))
}

pub fn get_record_bytes(kad: &mut Behaviour<MemoryStore>, key: String) -> QueryId {
    get_record(kad, key)
}
