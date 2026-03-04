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
    /// IP address to listen on.  Defaults to 0.0.0.0 (all interfaces).
    /// Set to a specific IP (e.g. the public IP on a VPS) to prevent
    /// loopback/private addresses from being advertised to the network.
    #[serde(default = "default_listen_address")]
    pub listen_address: String,
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
            listen_address: default_listen_address(),
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
    let overrides = EnvOverrides::from_env()?;
    load_or_create_config_with_overrides(overrides)
}

#[derive(Debug, Clone, Default)]
struct EnvOverrides {
    listen_port: Option<u16>,
    rpc_port: Option<u16>,
    http_port: Option<u16>,
    data_dir: Option<PathBuf>,
}

impl EnvOverrides {
    fn from_env() -> Result<Self> {
        Ok(Self {
            listen_port: parse_env_port("LATTICE_PORT")?,
            rpc_port: parse_env_port("LATTICE_RPC_PORT")?,
            http_port: parse_env_port("LATTICE_HTTP_PORT")?,
            data_dir: env::var("LATTICE_DATA_DIR").ok().map(PathBuf::from),
        })
    }
}

fn parse_env_port(var_name: &str) -> Result<Option<u16>> {
    match env::var(var_name) {
        Ok(raw) => {
            let parsed = raw
                .parse()
                .with_context(|| format!("invalid {var_name} value"))?;
            Ok(Some(parsed))
        }
        Err(env::VarError::NotPresent) => Ok(None),
        Err(err) => Err(anyhow::anyhow!("{var_name} error: {err}")),
    }
}

fn load_or_create_config_with_overrides(overrides: EnvOverrides) -> Result<Config> {
    let mut default_cfg = Config::default();
    if let Some(dir) = overrides.data_dir.as_ref() {
        default_cfg.data_dir = dir.clone();
    }
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

    if let Some(port) = overrides.listen_port {
        config.listen_port = port;
    }

    if let Some(port) = overrides.rpc_port {
        config.rpc_port = port;
    }

    if let Some(port) = overrides.http_port {
        config.http_port = port;
    }

    if let Some(dir) = overrides.data_dir {
        config.data_dir = dir;
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

fn default_listen_address() -> String {
    "0.0.0.0".to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::process;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_path(label: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time went backwards")
            .as_nanos();
        std::env::temp_dir().join(format!("lattice-config-{label}-{}-{nanos}", process::id()))
    }

    #[test]
    fn uses_lattice_data_dir_for_config_lookup() {
        let default_dir = temp_path("default");
        let override_dir = temp_path("override");
        fs::create_dir_all(&default_dir).expect("create default dir");
        fs::create_dir_all(&override_dir).expect("create override dir");

        let default_cfg = Config {
            listen_port: 10000,
            rpc_port: 10001,
            http_port: 10002,
            listen_address: "127.0.0.1".to_string(),
            data_dir: default_dir.clone(),
            bootstrap_peers: vec!["/ip4/1.1.1.1/tcp/7779/p2p/default".to_string()],
        };
        let override_cfg = Config {
            listen_port: 19000,
            rpc_port: 19001,
            http_port: 19002,
            listen_address: "127.0.0.1".to_string(),
            data_dir: override_dir.clone(),
            bootstrap_peers: vec!["/ip4/127.0.0.1/tcp/19000/p2p/override".to_string()],
        };
        fs::write(
            default_dir.join("config.toml"),
            toml::to_string_pretty(&default_cfg).expect("serialize default cfg"),
        )
        .expect("write default cfg");
        fs::write(
            override_dir.join("config.toml"),
            toml::to_string_pretty(&override_cfg).expect("serialize override cfg"),
        )
        .expect("write override cfg");

        let config = load_or_create_config_with_overrides(EnvOverrides {
            data_dir: Some(override_dir.clone()),
            ..EnvOverrides::default()
        })
        .expect("load config");

        assert_eq!(config.listen_port, 19000);
        assert_eq!(config.rpc_port, 19001);
        assert_eq!(config.http_port, 19002);
        assert_eq!(config.data_dir, override_dir);
        assert_eq!(
            config.bootstrap_peers,
            vec!["/ip4/127.0.0.1/tcp/19000/p2p/override".to_string()]
        );

        let _ = fs::remove_dir_all(&default_dir);
        let _ = fs::remove_dir_all(&config.data_dir);
    }

    #[test]
    fn creates_config_file_in_overridden_data_dir() {
        let override_dir = temp_path("create");

        let config = load_or_create_config_with_overrides(EnvOverrides {
            data_dir: Some(override_dir.clone()),
            ..EnvOverrides::default()
        })
        .expect("load config");

        assert_eq!(config.data_dir, override_dir);
        assert!(config.data_dir.join("config.toml").exists());

        let _ = fs::remove_dir_all(&config.data_dir);
    }
}
