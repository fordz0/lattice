use anyhow::{Context, Result};
use directories::BaseDirs;
use serde::{Deserialize, Serialize};
use std::env;
use std::fs;
use std::path::PathBuf;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub listen_port: u16,
    pub rpc_port: u16,
    #[serde(default = "default_http_port")]
    pub http_port: u16,
    pub data_dir: PathBuf,
    pub bootstrap_peers: Vec<String>,
}

impl Default for Config {
    fn default() -> Self {
        let data_dir = BaseDirs::new()
            .map(|base_dirs| base_dirs.home_dir().join(".lattice"))
            .or_else(|| {
                std::env::var_os("HOME")
                    .map(PathBuf::from)
                    .map(|h| h.join(".lattice"))
            })
            .unwrap_or_else(|| PathBuf::from(".lattice"));

        Self {
            listen_port: 7779,
            rpc_port: 7780,
            http_port: default_http_port(),
            data_dir,
            bootstrap_peers: Config::default_bootstrap_peers(),
        }
    }
}

impl Config {
    pub fn default_bootstrap_peers() -> Vec<String> {
        vec![
            "/ip4/188.245.245.179/tcp/7779/p2p/12D3KooWQQw51zoUZuVKoraBuAqkts7gX8qe2yQ1ZgTAoFVfCQFD".to_string(),
        ]
    }
}

pub fn load_or_create_config() -> Result<Config> {
    let default_cfg = Config::default();
    let config_path = default_cfg.data_dir.join("config.toml");

    if !config_path.exists() {
        fs::create_dir_all(&default_cfg.data_dir).with_context(|| {
            format!(
                "failed to create data dir {}",
                default_cfg.data_dir.display()
            )
        })?;

        let toml =
            toml::to_string_pretty(&default_cfg).context("failed to serialize default config")?;
        fs::write(&config_path, toml).with_context(|| {
            format!(
                "failed to write default config to {}",
                config_path.display()
            )
        })?;
    }

    let config_contents = fs::read_to_string(&config_path)
        .with_context(|| format!("failed to read config from {}", config_path.display()))?;
    let mut config: Config =
        toml::from_str(&config_contents).context("failed to parse config.toml")?;

    if let Ok(port) = env::var("LATTICE_PORT") {
        config.listen_port = port.parse().context("invalid LATTICE_PORT value")?;
    }

    if let Ok(port) = env::var("LATTICE_RPC_PORT") {
        config.rpc_port = port.parse().context("invalid LATTICE_RPC_PORT value")?;
    }

    if let Ok(port) = env::var("LATTICE_HTTP_PORT") {
        config.http_port = port.parse().context("invalid LATTICE_HTTP_PORT")?;
    }

    if let Ok(dir) = env::var("LATTICE_DATA_DIR") {
        config.data_dir = PathBuf::from(dir);
        fs::create_dir_all(&config.data_dir).with_context(|| {
            format!(
                "failed to create LATTICE_DATA_DIR {}",
                config.data_dir.display()
            )
        })?;
    }

    if config.bootstrap_peers.is_empty() {
        config.bootstrap_peers = Config::default_bootstrap_peers();
    }

    fs::create_dir_all(&config.data_dir).with_context(|| {
        format!(
            "failed to ensure data_dir {} exists",
            config.data_dir.display()
        )
    })?;

    Ok(config)
}

fn default_http_port() -> u16 {
    7781
}
