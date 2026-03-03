#![allow(dead_code)]

// This module will handle fetching content blocks from peers via the daemon RPC
// by resolving site manifests and requesting block hashes from the network.

use anyhow::Result;

pub struct SiteBlock {
    pub path: String,
    pub contents: Vec<u8>,
}

pub async fn fetch_site(_name: &str) -> Result<Vec<SiteBlock>> {
    todo!("fetching site blocks from peers is not implemented yet");
}
