use anyhow::{Context, Result};
use libp2p::kad::store::MemoryStore;
use libp2p::kad::{self, Behaviour, QueryId, Quorum, Record};
use libp2p::multiaddr::Protocol;
use libp2p::{Multiaddr, PeerId};

pub fn new_kademlia(local_peer_id: PeerId) -> Behaviour<MemoryStore> {
    let store = MemoryStore::new(local_peer_id);
    let config = kad::Config::default();
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
}

pub fn put_record(kad: &mut Behaviour<MemoryStore>, key: String, value: String) -> Result<QueryId> {
    let record = Record::new(key.into_bytes(), value.into_bytes());
    kad.put_record(record, Quorum::One)
        .context("failed to start put_record query")
}

pub fn get_record(kad: &mut Behaviour<MemoryStore>, key: String) -> QueryId {
    kad.get_record(kad::RecordKey::new(&key))
}
