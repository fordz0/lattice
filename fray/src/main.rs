use anyhow::{Context, Result};
use directories::BaseDirs;
use ed25519_dalek::SigningKey;
use fray::api::{app, AppState};
use fray::blocklist::ContentBlocklist;
use fray::store::FrayStore;
use serde_json::json;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tracing::{info, warn};

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
    let pid = std::process::id();
    std::fs::create_dir_all(&data_dir)
        .with_context(|| format!("failed to create {}", data_dir.display()))?;

    let store = FrayStore::open(&data_dir)?;
    let blocklist_path = data_dir.join("blocklist.txt");
    let blocklist = ContentBlocklist::load_from_file(&blocklist_path)
        .with_context(|| format!("failed to load blocklist {}", blocklist_path.display()))?;
    let app = app(AppState {
        store,
        lattice_rpc_port,
        signing_key,
        blocklist,
        blocklist_path,
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
    if let Err(err) = register_local_app(lattice_rpc_port, port, pid).await {
        warn!(error = %err, "failed to register Fray as a local app");
    }
    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .with_graceful_shutdown(shutdown_signal(lattice_rpc_port, pid))
    .await
    .context("fray api stopped unexpectedly")?;
    Ok(())
}

async fn shutdown_signal(lattice_rpc_port: u16, pid: u32) {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};

        let mut terminate =
            signal(SignalKind::terminate()).expect("failed to register SIGTERM handler");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = terminate.recv() => {}
        }
    }

    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }

    if let Err(err) = unregister_local_app(lattice_rpc_port, pid).await {
        warn!(error = %err, "failed to unregister Fray local app");
    }
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
        base_dirs
            .home_dir()
            .join(".lattice")
            .join("site_signing.key")
    };
    let bytes = std::fs::read(&key_path)
        .with_context(|| format!("failed to read signing key {}", key_path.display()))?;
    let key_bytes: [u8; 32] = bytes
        .try_into()
        .map_err(|_| anyhow::anyhow!("invalid signing key length in {}", key_path.display()))?;
    Ok(SigningKey::from_bytes(&key_bytes))
}

async fn register_local_app(lattice_rpc_port: u16, proxy_port: u16, pid: u32) -> Result<()> {
    let result = daemon_rpc_call(
        lattice_rpc_port,
        "app_register",
        json!({
            "site_name": "fray",
            "proxy_port": proxy_port,
            "proxy_paths": ["/api"],
            "pid": pid,
        }),
    )
    .await?;
    let status = result
        .get("status")
        .and_then(|value| value.as_str())
        .unwrap_or("err");
    if status != "ok" {
        let error = result
            .get("error")
            .and_then(|value| value.as_str())
            .unwrap_or("app_register failed");
        anyhow::bail!(error.to_string());
    }
    Ok(())
}

async fn unregister_local_app(lattice_rpc_port: u16, pid: u32) -> Result<()> {
    let result = daemon_rpc_call(
        lattice_rpc_port,
        "app_unregister",
        json!({
            "site_name": "fray",
            "pid": pid,
        }),
    )
    .await?;
    let status = result
        .get("status")
        .and_then(|value| value.as_str())
        .unwrap_or("err");
    if status != "ok" {
        let error = result
            .get("error")
            .and_then(|value| value.as_str())
            .unwrap_or("app_unregister failed");
        anyhow::bail!(error.to_string());
    }
    Ok(())
}

async fn daemon_rpc_call(
    lattice_rpc_port: u16,
    method: &str,
    params: serde_json::Value,
) -> Result<serde_json::Value> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .context("failed to build daemon RPC client")?;
    let response = client
        .post(format!("http://127.0.0.1:{lattice_rpc_port}"))
        .json(&json!({
            "jsonrpc": "2.0",
            "id": format!("fray-main:{method}"),
            "method": method,
            "params": params,
        }))
        .send()
        .await
        .with_context(|| format!("failed to reach lattice daemon RPC on {lattice_rpc_port}"))?;
    let envelope: serde_json::Value = response
        .json()
        .await
        .context("failed to decode daemon RPC response")?;
    if let Some(error) = envelope.get("error") {
        anyhow::bail!("rpc error: {error}");
    }
    envelope
        .get("result")
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("rpc result missing"))
}
