use libp2p::request_response::{self, ProtocolSupport};
use libp2p::StreamProtocol;
use serde::{Deserialize, Serialize};
use std::iter;
use std::time::Duration;

pub const BLOCK_FETCH_PROTOCOL: &str = "/lattice/block-fetch/1.0.0";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BlockFetchRequest {
    pub block_hash: String,
    pub site_key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BlockFetchResponse {
    pub block_hash: String,
    pub data: Option<Vec<u8>>,
    pub reason: Option<String>,
}

pub type Behaviour = request_response::cbor::Behaviour<BlockFetchRequest, BlockFetchResponse>;
pub type Event = request_response::Event<BlockFetchRequest, BlockFetchResponse>;
pub type ResponseChannel = request_response::ResponseChannel<BlockFetchResponse>;
pub type OutboundRequestId = request_response::OutboundRequestId;

pub fn new_behaviour() -> Behaviour {
    let protocols = iter::once((
        StreamProtocol::new(BLOCK_FETCH_PROTOCOL),
        ProtocolSupport::Full,
    ));
    let config = request_response::Config::default().with_request_timeout(Duration::from_secs(20));
    request_response::cbor::Behaviour::new(protocols, config)
}
