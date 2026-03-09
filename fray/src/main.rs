use anyhow::{Context, Result};
use directories::BaseDirs;
use fray::api::{app, AppState};
use fray::store::FrayStore;
use ed25519_dalek::SigningKey;
use std::path::PathBuf;
use std::sync::Arc;
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let port = std::env::var("FRAY_PORT")
        .ok()
        .and_then(|raw| raw.parse::<u16>().ok())
        .unwrap_or(8890);
    let data_dir = fray_data_dir()?;
    let lattice_rpc_port = std::env::var("FRAY_LATTICE_RPC_PORT")
        .ok()
        .and_then(|raw| raw.parse::<u16>().ok())
        .unwrap_or(7780);
    let signing_key = Arc::new(load_signing_key()?);
    std::fs::create_dir_all(&data_dir)
        .with_context(|| format!("failed to create {}", data_dir.display()))?;

    let store = FrayStore::open(&data_dir)?;
    let app = app(AppState {
        store,
        lattice_rpc_port,
        signing_key,
    });
    let listen_addr = format!("127.0.0.1:{port}");
    let listener = tokio::net::TcpListener::bind(&listen_addr)
        .await
        .with_context(|| format!("failed to bind fray api on {listen_addr}"))?;
    info!(
        port,
        lattice_rpc_port,
        data_dir = %data_dir.display(),
        "fray api started"
    );
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .context("fray api stopped unexpectedly")?;
    Ok(())
}

async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
}

fn fray_data_dir() -> Result<PathBuf> {
    if let Ok(dir) = std::env::var("FRAY_DATA_DIR") {
        return Ok(PathBuf::from(dir));
    }
    let base_dirs =
        BaseDirs::new().ok_or_else(|| anyhow::anyhow!("failed to resolve user home directory"))?;
    Ok(base_dirs.home_dir().join(".lattice").join("fray"))
}

fn load_signing_key() -> Result<SigningKey> {
    let key_path = if let Ok(path) = std::env::var("FRAY_SIGNING_KEY_PATH") {
        PathBuf::from(path)
    } else {
        let base_dirs = BaseDirs::new()
            .ok_or_else(|| anyhow::anyhow!("failed to resolve user home directory"))?;
        base_dirs.home_dir().join(".lattice").join("site_signing.key")
    };
    let bytes = std::fs::read(&key_path)
        .with_context(|| format!("failed to read signing key {}", key_path.display()))?;
    let key_bytes: [u8; 32] = bytes
        .try_into()
        .map_err(|_| anyhow::anyhow!("invalid signing key length in {}", key_path.display()))?;
    Ok(SigningKey::from_bytes(&key_bytes))
}
