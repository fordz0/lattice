use anyhow::Result;
use libp2p::swarm::{NetworkBehaviour, Swarm};
use libp2p::{identity, noise, tcp, yamux, SwarmBuilder};
use std::error::Error;
use std::time::Duration;

pub fn build_swarm<B, F>(local_key: identity::Keypair, behaviour_builder: F) -> Result<Swarm<B>>
where
    B: NetworkBehaviour,
    F: FnOnce(&identity::Keypair) -> std::result::Result<B, Box<dyn Error + Send + Sync>>,
{
    let swarm = SwarmBuilder::with_existing_identity(local_key)
        .with_tokio()
        .with_tcp(
            tcp::Config::default(),
            noise::Config::new,
            yamux::Config::default,
        )?
        .with_quic()
        .with_behaviour(|key| behaviour_builder(key))?
        .with_swarm_config(|c| c.with_idle_connection_timeout(Duration::from_secs(60)))
        .build();

    Ok(swarm)
}
