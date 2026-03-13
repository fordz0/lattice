mod event_loop;
mod fetch;

use anyhow::Result;
use clap::Parser;
use lattice_core::moderation::ModerationEngine;
use lattice_daemon::app_registry::AppRegistry;
use lattice_daemon::block_fetch;
use lattice_daemon::cache::SessionBlockCache;
use lattice_daemon::config::{load_or_create_config_with_overrides, Config, ConfigOverrides};
use lattice_daemon::dht;
use lattice_daemon::http_server;
use lattice_daemon::node::{
    load_or_create_block_cache_key, load_or_create_identity, load_or_create_site_signing_key,
};
use lattice_daemon::proxy_server;
use lattice_daemon::rpc::{self, RpcCommand};
use lattice_daemon::site_helpers::{
    build_bootstrap_peer_ids, owned_names_from_local_records, reannounce_pinned_sites,
    restore_local_records_to_store,
};
use lattice_daemon::store::LocalRecordStore;
use lattice_daemon::tls;
use lattice_daemon::transport;
use libp2p::autonat;
use libp2p::dcutr;
use libp2p::futures::StreamExt;
use libp2p::gossipsub;
use libp2p::identify;
use libp2p::kad;
use libp2p::mdns;
use libp2p::relay;
use libp2p::swarm::behaviour::toggle::Toggle;
use libp2p::swarm::NetworkBehaviour;
use libp2p::Multiaddr;
use std::collections::{HashMap, HashSet};
use std::future::pending;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::time::Duration;
use tracing::{error, info, warn};

#[cfg(windows)]
use std::ffi::OsString;
#[cfg(windows)]
use std::sync::OnceLock;
#[cfg(windows)]
use windows_service::service::{
    ServiceControl, ServiceControlAccept, ServiceExitCode, ServiceState, ServiceStatus, ServiceType,
};
#[cfg(windows)]
use windows_service::service_control_handler::{self, ServiceControlHandlerResult};
#[cfg(windows)]
use windows_service::service_dispatcher;
#[cfg(windows)]
windows_service::define_windows_service!(ffi_service_main, windows_service_entry);

use crate::event_loop::{
    handle_rpc_command, handle_swarm_event, PendingClaimPut, PendingManifestQuery,
    PendingNameProbe, PendingPublishClaimPut, PendingPublishOwnershipCheck,
    PendingPublishVersionCheck, PendingPut, PendingTextQuery,
};
use crate::fetch::{GetSiteQuery, GetSiteTask, PendingBlockRequest, PendingProviderQuery};
use lattice_daemon::publish::{PublishQuery, PublishTask};

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
#[cfg(windows)]
const WINDOWS_SERVICE_NAME: &str = "lattice-daemon";
#[cfg(windows)]
static WINDOWS_SERVICE_OVERRIDES: OnceLock<ConfigOverrides> = OnceLock::new();

#[derive(Parser, Debug, Clone)]
#[command(name = "lattice-daemon")]
#[command(about = "Lattice daemon")]
struct Cli {
    #[arg(long)]
    listen_port: Option<u16>,
    #[arg(long)]
    rpc_port: Option<u16>,
    #[arg(long)]
    http_port: Option<u16>,
    #[arg(long)]
    https_port: Option<u16>,
    #[arg(long)]
    proxy_port: Option<u16>,
    #[arg(long)]
    data_dir: Option<PathBuf>,
    #[cfg_attr(not(windows), allow(dead_code))]
    #[arg(long, hide = true)]
    service: bool,
}

impl Cli {
    fn config_overrides(&self) -> ConfigOverrides {
        ConfigOverrides {
            listen_port: self.listen_port,
            rpc_port: self.rpc_port,
            http_port: self.http_port,
            https_port: self.https_port,
            proxy_port: self.proxy_port,
            data_dir: self.data_dir.clone(),
        }
    }
}

#[derive(NetworkBehaviour)]
pub struct LatticeBehaviour {
    kademlia: kad::Behaviour<kad::store::MemoryStore>,
    block_fetch: block_fetch::Behaviour,
    mdns: Toggle<mdns::tokio::Behaviour>,
    gossipsub: gossipsub::Behaviour,
    identify: identify::Behaviour,
    autonat: autonat::Behaviour,
    relay: relay::Behaviour,
    relay_client: relay::client::Behaviour,
    dcutr: dcutr::Behaviour,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

    let cli = Cli::parse();

    #[cfg(windows)]
    if cli.service {
        return run_windows_service(cli.config_overrides());
    }

    let config = load_or_create_config_with_overrides(
        ConfigOverrides::from_env()?.merge(cli.config_overrides()),
    )?;
    run_daemon(config, None).await
}

async fn run_daemon(
    config: Config,
    mut shutdown_signal: Option<watch::Receiver<bool>>,
) -> Result<()> {
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
    let app_registry = AppRegistry::new();
    let local_records_path = config.data_dir.join(LOCAL_RECORDS_DB_DIR);
    let local_record_store = LocalRecordStore::open(&local_records_path, block_cache_key)?;
    let mut moderation_engine = ModerationEngine::load(
        local_record_store
            .load_moderation_rules()
            .unwrap_or_default(),
    );
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
            let mdns = if config.mdns_enabled {
                Some(mdns::tokio::Behaviour::new(mdns::Config::default(), peer_id)?)
            } else {
                None
            };
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
                mdns: mdns.into(),
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

    restore_local_records_to_store(&mut swarm.behaviour_mut().kademlia, &local_records);
    reannounce_pinned_sites(
        &local_record_store,
        &moderation_engine,
        &mut swarm.behaviour_mut().kademlia,
    );

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
    let http_app_registry = app_registry.clone();
    let _http_server = tokio::spawn(async move {
        if let Err(err) = http_server::start_http_server(
            http_port,
            http_rpc_tx,
            http_ca_cert,
            mime_policy_strict,
            http_app_registry,
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
    let https_app_registry = app_registry.clone();
    let _https_server = tokio::spawn(async move {
        if let Err(err) = http_server::start_https_server(
            https_port,
            https_rpc_tx,
            https_ca_cert,
            https_cert_path,
            https_key_path,
            mime_policy_strict,
            https_app_registry,
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
            () = wait_for_shutdown(&mut shutdown_signal) => {
                info!("shutdown requested");
                break;
            }
            maybe_cmd = rpc_rx.recv() => {
                if let Some(cmd) = maybe_cmd {
                    handle_rpc_command(
                        cmd,
                        &mut swarm,
                        peer_id,
                        &local_pubkey_hex,
                        &mut moderation_engine,
                        &local_record_store,
                        &mut local_records,
                        &mut session_block_cache,
                        &mut pending_put,
                        &mut pending_get_text,
                        &mut pending_get_manifest,
                        &mut pending_name_probes,
                        &mut pending_publish_checks,
                        &mut pending_publish_claim_put,
                        &mut pending_publish_version_checks,
                        &mut publish_tasks,
                        &mut get_site_tasks,
                        &mut next_get_site_task_id,
                        &mut get_site_queries,
                        &mut pending_provider_queries,
                        &mut pending_block_requests,
                        &owned_names,
                        &app_registry,
                    );
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
                    mime_policy_strict,
                    &local_record_store,
                    &mut local_records,
                );
            }
        }
    }

    Ok(())
}

async fn wait_for_shutdown(shutdown_signal: &mut Option<watch::Receiver<bool>>) {
    match shutdown_signal.as_mut() {
        Some(receiver) => {
            let _ = receiver.changed().await;
        }
        None => pending::<()>().await,
    }
}

#[cfg(windows)]
fn run_windows_service(overrides: ConfigOverrides) -> Result<()> {
    let _ = WINDOWS_SERVICE_OVERRIDES.set(overrides);
    service_dispatcher::start(WINDOWS_SERVICE_NAME, ffi_service_main)?;
    Ok(())
}

#[cfg(windows)]
fn windows_service_entry(_args: Vec<OsString>) {
    if let Err(err) = windows_service_main() {
        eprintln!("windows service failed: {err:#}");
    }
}

#[cfg(windows)]
fn windows_service_main() -> Result<()> {
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let status_handle =
        service_control_handler::register(WINDOWS_SERVICE_NAME, move |control_event| {
            match control_event {
                ServiceControl::Stop | ServiceControl::Shutdown => {
                    let _ = shutdown_tx.send(true);
                    ServiceControlHandlerResult::NoError
                }
                ServiceControl::Interrogate => ServiceControlHandlerResult::NoError,
                _ => ServiceControlHandlerResult::NotImplemented,
            }
        })?;

    status_handle.set_service_status(ServiceStatus {
        service_type: ServiceType::OWN_PROCESS,
        current_state: ServiceState::StartPending,
        controls_accepted: ServiceControlAccept::empty(),
        exit_code: ServiceExitCode::Win32(0),
        checkpoint: 0,
        wait_hint: Duration::from_secs(10),
        process_id: None,
    })?;

    let overrides = ConfigOverrides::from_env()?
        .merge(WINDOWS_SERVICE_OVERRIDES.get().cloned().unwrap_or_default());
    let config = load_or_create_config_with_overrides(overrides)?;

    status_handle.set_service_status(ServiceStatus {
        service_type: ServiceType::OWN_PROCESS,
        current_state: ServiceState::Running,
        controls_accepted: ServiceControlAccept::STOP | ServiceControlAccept::SHUTDOWN,
        exit_code: ServiceExitCode::Win32(0),
        checkpoint: 0,
        wait_hint: Duration::default(),
        process_id: None,
    })?;

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    let result = runtime.block_on(run_daemon(config, Some(shutdown_rx)));
    let exit_code = if result.is_ok() { 0 } else { 1 };

    status_handle.set_service_status(ServiceStatus {
        service_type: ServiceType::OWN_PROCESS,
        current_state: ServiceState::Stopped,
        controls_accepted: ServiceControlAccept::empty(),
        exit_code: ServiceExitCode::Win32(exit_code),
        checkpoint: 0,
        wait_hint: Duration::default(),
        process_id: None,
    })?;

    result
}
