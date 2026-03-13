mod rpc;

use anyhow::{anyhow, bail, Context, Result};
use clap::{Parser, Subcommand};
use directories::BaseDirs;
use ed25519_dalek::SigningKey;
use lattice_core::app_namespace::APP_REGISTRY_PREFIX;
use lattice_core::app_registry_record::{validate_app_registry_record, AppRegistryRecord};
use lattice_core::identity::{canonical_json_bytes, SignedRecord};
use lattice_core::registry::is_registry_operator;
use lattice_site::manifest::{
    hash_bytes, hash_file, verify_manifest, FileEntry, SiteManifest, DEFAULT_CHUNK_SIZE_BYTES,
};
use lattice_site::publisher as site_publisher;
use rpc::{DaemonNotRunning, RpcClient};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::error::Error;
use std::fmt;
use std::fs;
use std::io::{Read, Write};
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
#[cfg(windows)]
use std::os::windows::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Command as ProcessCommand, Stdio};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[cfg(windows)]
const WINDOWS_DAEMON_CREATION_FLAGS: u32 = 0x00000008 | 0x00000200 | 0x08000000;
#[cfg(windows)]
const WINDOWS_DAEMON_SERVICE_NAME: &str = "lattice-daemon";
const MACOS_DAEMON_LABEL: &str = "dev.benjf.lattice-daemon";
const MACOS_APP_LABEL_PREFIX: &str = "dev.benjf.lattice.app";

#[derive(Parser)]
#[command(name = "lattice")]
#[command(about = "CLI client for lattice-daemon JSON-RPC")]
struct Cli {
    #[arg(long, global = true, default_value_t = rpc::DEFAULT_RPC_PORT)]
    rpc_port: u16,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    Up,
    Down,
    Service {
        #[command(subcommand)]
        command: ServiceCommand,
    },
    Status,
    Peers,
    Put {
        key: String,
        value: String,
    },
    Get {
        key: String,
    },
    Keygen,
    Name {
        #[command(subcommand)]
        command: NameCommand,
    },
    Init {
        #[arg(long)]
        name: Option<String>,
        #[arg(long, default_value = "general")]
        rating: String,
    },
    Publish {
        #[arg(long)]
        dir: Option<PathBuf>,
        #[arg(long)]
        name: Option<String>,
    },
    Fetch {
        name: String,
        #[arg(long)]
        out: Option<PathBuf>,
    },
    PublishApp {
        app_id: String,
        #[arg(long)]
        version: String,
        #[arg(long)]
        description: String,
        #[arg(long = "linux-x86-64")]
        linux_x86_64: Option<String>,
        #[arg(long = "linux-x86-64-sha256")]
        linux_x86_64_sha256: Option<String>,
        #[arg(long = "linux-aarch64")]
        linux_aarch64: Option<String>,
        #[arg(long = "linux-aarch64-sha256")]
        linux_aarch64_sha256: Option<String>,
        #[arg(long = "macos-aarch64")]
        macos_aarch64: Option<String>,
        #[arg(long = "macos-aarch64-sha256")]
        macos_aarch64_sha256: Option<String>,
        #[arg(long = "macos-x86-64")]
        macos_x86_64: Option<String>,
        #[arg(long = "macos-x86-64-sha256")]
        macos_x86_64_sha256: Option<String>,
    },
    Install {
        app_id: String,
    },
    Update {
        app_id: Option<String>,
        #[arg(long)]
        all: bool,
    },
    Uninstall {
        app_id: String,
    },
    Apps,
}

#[derive(Subcommand)]
enum NameCommand {
    Claim { name: String },
    Info { name: String },
    List,
}

#[derive(Subcommand)]
enum ServiceCommand {
    Install,
    Uninstall {
        #[arg(long)]
        purge_data: bool,
    },
    Start,
    Stop,
    Restart,
    Status,
}

#[derive(Debug)]
struct NameClaimedByOther {
    name: String,
}

impl fmt::Display for NameClaimedByOther {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.loom is already claimed by another key", self.name)
    }
}

impl Error for NameClaimedByOther {}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct InstalledAppMeta {
    app_id: String,
    version: String,
    description: String,
}

const MAX_FETCH_SITE_FILES: usize = 1000;
const MAX_FETCH_SITE_BYTES: u64 = 100 * 1024 * 1024;

#[tokio::main]
async fn main() {
    let exit_code = match run().await {
        Ok(()) => 0,
        Err(err) => {
            if err.downcast_ref::<DaemonNotRunning>().is_some() {
                eprintln!("lattice daemon is not running. Start it with: lattice up");
            } else if let Some(claimed) = err.downcast_ref::<NameClaimedByOther>() {
                println!(
                    "Error: {}.loom is already claimed by another key",
                    claimed.name
                );
                println!("Claim the name first: lattice name claim {}", claimed.name);
            } else {
                eprintln!("{err:#}");
            }
            1
        }
    };

    if exit_code != 0 {
        std::process::exit(exit_code);
    }
}

async fn run() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Command::Up => {
            up(cli.rpc_port).await?;
        }
        Command::Down => {
            down()?;
        }
        Command::Service { command } => {
            service_command(command, cli.rpc_port).await?;
        }
        Command::Status => {
            let client = RpcClient::new(cli.rpc_port);
            let info = client.node_info().await?;
            print_status(&info);
        }
        Command::Peers => {
            let client = RpcClient::new(cli.rpc_port);
            let info = client.node_info().await?;
            print_peers(&info);
        }
        Command::Put { key, value } => {
            let client = RpcClient::new(cli.rpc_port);
            let result = client.put_record(&key, &value).await?;
            print_put_result(&result);
        }
        Command::Get { key } => {
            let client = RpcClient::new(cli.rpc_port);
            let result = client.get_record(&key).await?;
            if result.is_null() {
                println!("not found");
            } else if let Some(value) = result.as_str() {
                println!("{value}");
            } else {
                println!("{result}");
            }
        }
        Command::Keygen => {
            keygen()?;
        }
        Command::Name { command } => match command {
            NameCommand::Claim { name } => {
                let client = RpcClient::new(cli.rpc_port);
                let result = client.claim_name(&name, "").await?;

                let status = result
                    .get("status")
                    .and_then(Value::as_str)
                    .unwrap_or("err");
                if status == "ok" {
                    println!("claimed {name}.loom");
                } else {
                    let error = result
                        .get("error")
                        .and_then(Value::as_str)
                        .unwrap_or("unknown error");
                    println!("{error}");
                }
            }
            NameCommand::Info { name } => {
                let client = RpcClient::new(cli.rpc_port);
                let result = client.get_record(&format!("name:{name}")).await?;
                let owner = if result.is_null() {
                    "unclaimed".to_string()
                } else if let Some(value) = result.as_str() {
                    value.to_string()
                } else {
                    result.to_string()
                };

                println!("Name:      {name}.loom");
                println!("Owner key: {owner}");
            }
            NameCommand::List => {
                let client = RpcClient::new(cli.rpc_port);
                let names = client.list_names().await?;
                if names.is_empty() {
                    println!("No names claimed on this node");
                } else {
                    for name in names {
                        println!("{name}.loom");
                    }
                }
            }
        },
        Command::Init { name, rating } => {
            init_site(name, &rating)?;
        }
        Command::Publish { dir, name } => {
            let site_dir =
                dir.unwrap_or(std::env::current_dir().context("failed to get current directory")?);
            let canonical_dir = site_dir
                .canonicalize()
                .with_context(|| format!("failed to resolve {}", site_dir.display()))?;
            let name = match name {
                Some(name) if !name.trim().is_empty() => name,
                Some(_) => {
                    bail!("no name specified — use --name <name> or add \"name\" to lattice.json")
                }
                None => site_name_for_dir(&canonical_dir)?,
            };

            println!("Publishing {name}.loom...");

            let client = RpcClient::new(cli.rpc_port);
            let result = client
                .publish_site(&name, &canonical_dir.to_string_lossy())
                .await?;

            let status = result
                .get("status")
                .and_then(Value::as_str)
                .unwrap_or("err");
            if status != "ok" {
                let error = result
                    .get("error")
                    .and_then(Value::as_str)
                    .unwrap_or("publish_site failed");
                if error == "name already claimed by another key" {
                    return Err(anyhow!(NameClaimedByOther { name }));
                }
                bail!("{error}");
            }

            let file_count = result
                .get("file_count")
                .and_then(Value::as_u64)
                .unwrap_or(0);
            let version = result.get("version").and_then(Value::as_u64).unwrap_or(0);
            let claimed = result
                .get("claimed")
                .and_then(Value::as_bool)
                .unwrap_or(false);

            if claimed {
                println!("Auto-claimed {name}.loom");
            }
            println!("Published {name}.loom v{version} ({file_count} files)");
        }
        Command::Fetch { name, out } => {
            let out_dir = out.unwrap_or_else(|| PathBuf::from(&name));
            let client = RpcClient::new(cli.rpc_port);

            let manifest_result = client.get_site_manifest(&name).await?;
            let manifest_json = manifest_result
                .get("manifest_json")
                .and_then(Value::as_str)
                .ok_or_else(|| anyhow!("site {}.loom not found", name))?;

            let manifest: SiteManifest = serde_json::from_str(manifest_json)
                .with_context(|| format!("failed to parse site manifest for {}.loom", name))?;

            verify_manifest(&manifest)?;
            if manifest.name != name {
                bail!(
                    "manifest name mismatch: expected {}.loom, got {}.loom",
                    name,
                    manifest.name
                );
            }
            if manifest.files.len() > MAX_FETCH_SITE_FILES {
                bail!("site exceeds maximum file count");
            }
            let declared_bytes = manifest
                .files
                .iter()
                .fold(0_u64, |acc, file| acc.saturating_add(file.size));
            if declared_bytes > MAX_FETCH_SITE_BYTES {
                bail!("site exceeds maximum total size");
            }

            let owner_result = client.get_record(&format!("name:{name}")).await?;
            let owner_key = owner_result
                .as_str()
                .ok_or_else(|| anyhow!("name owner record missing or invalid for {}.loom", name))?;
            if owner_key != manifest.publisher_key {
                bail!(
                    "manifest publisher does not match name owner for {}.loom",
                    name
                );
            }

            fs::create_dir_all(&out_dir)
                .with_context(|| format!("failed to create output dir {}", out_dir.display()))?;

            let site_key = format!("site:{name}");
            for file in &manifest.files {
                let mut contents = Vec::new();
                let block_hashes = file_block_hashes(file);
                for block_hash in block_hashes {
                    let block_result = client.get_block(&block_hash, Some(&site_key)).await?;
                    let hex_contents = block_result.as_str().ok_or_else(|| {
                        anyhow!("missing content block {} for {}", block_hash, file.path)
                    })?;
                    let block_contents = decode_hex(hex_contents)
                        .with_context(|| format!("invalid block hex for {}", file.path))?;
                    let actual_block_hash = hash_bytes(&block_contents);
                    if actual_block_hash != block_hash {
                        bail!(
                            "chunk hash mismatch for {}: expected {}, got {}",
                            file.path,
                            block_hash,
                            actual_block_hash
                        );
                    }
                    contents.extend_from_slice(&block_contents);
                }

                let actual_hash = hash_bytes(&contents);
                if actual_hash != file.hash {
                    bail!(
                        "hash mismatch for {}: expected {}, got {}",
                        file.path,
                        file.hash,
                        actual_hash
                    );
                }

                let output_path = safe_join(&out_dir, &file.path)?;
                if let Some(parent) = output_path.parent() {
                    fs::create_dir_all(parent)
                        .with_context(|| format!("failed to create {}", parent.display()))?;
                }
                fs::write(&output_path, &contents)
                    .with_context(|| format!("failed to write {}", output_path.display()))?;

                println!("Fetched {}", file.path);
            }

            println!(
                "Fetched {}.loom v{} — {} files",
                name,
                manifest.version,
                manifest.files.len()
            );
        }
        Command::PublishApp {
            app_id,
            version,
            description,
            linux_x86_64,
            linux_x86_64_sha256,
            linux_aarch64,
            linux_aarch64_sha256,
            macos_aarch64,
            macos_aarch64_sha256,
            macos_x86_64,
            macos_x86_64_sha256,
        } => {
            let record = AppRegistryRecord {
                app_id: app_id.clone(),
                version: version.clone(),
                description,
                linux_x86_64_url: linux_x86_64,
                linux_x86_64_sha256,
                linux_aarch64_url: linux_aarch64,
                linux_aarch64_sha256,
                macos_aarch64_url: macos_aarch64,
                macos_aarch64_sha256,
                macos_x86_64_url: macos_x86_64,
                macos_x86_64_sha256,
                published_at: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map_err(|e| anyhow!("system clock error: {e}"))?
                    .as_secs(),
            };
            validate_app_registry_record(&record)
                .map_err(|err| anyhow!("invalid app registry record: {err}"))?;
            let signing_key = load_site_signing_key()?;
            let payload =
                canonical_json_bytes(&record).context("failed to encode app registry record")?;
            let signed = SignedRecord::sign(&signing_key, payload);
            let value =
                serde_json::to_string(&signed).context("failed to encode signed app record")?;
            let key = format!("{APP_REGISTRY_PREFIX}{app_id}");
            let client = RpcClient::new(cli.rpc_port);
            let result = client.put_record(&key, &value).await?;
            let status = result
                .get("status")
                .and_then(Value::as_str)
                .unwrap_or("err");
            if status != "ok" {
                let error = result
                    .get("error")
                    .and_then(Value::as_str)
                    .unwrap_or("publish-app failed");
                if error == "app registry records may only be published by the Lattice operator" {
                    bail!("only the Lattice operator key can publish to the app registry");
                }
                bail!("{error}");
            }
            println!("published app registry record for {app_id} v{version}");
        }
        Command::Install { app_id } => {
            install_app(cli.rpc_port, &app_id).await?;
        }
        Command::Update { app_id, all } => {
            update_apps(cli.rpc_port, app_id.as_deref(), all).await?;
        }
        Command::Uninstall { app_id } => {
            uninstall_app(&app_id)?;
        }
        Command::Apps => {
            list_installed_apps()?;
        }
    }

    Ok(())
}

fn print_status(info: &Value) {
    let peer_id = info
        .get("peer_id")
        .and_then(Value::as_str)
        .unwrap_or("unknown");
    let connected = info
        .get("connected_peers")
        .and_then(Value::as_u64)
        .unwrap_or(0);

    println!("Peer ID:         {peer_id}");
    println!("Connected peers: {connected}");
    println!("Listening on:");
    if let Some(addrs) = info.get("listen_addrs").and_then(Value::as_array) {
        for addr in addrs {
            if let Some(addr) = addr.as_str() {
                println!("  {addr}");
            }
        }
    }
}

fn print_peers(info: &Value) {
    let connected = info
        .get("connected_peers")
        .and_then(Value::as_u64)
        .unwrap_or(0);

    println!("Connected peers: {connected}");
    if let Some(addrs) = info.get("listen_addrs").and_then(Value::as_array) {
        for addr in addrs {
            if let Some(addr) = addr.as_str() {
                println!("{addr}");
            }
        }
    }
}

fn print_put_result(result: &Value) {
    let status = result.get("status").and_then(Value::as_str);
    if status == Some("ok") {
        println!("ok");
        return;
    }

    if let Some(error) = result.get("error").and_then(Value::as_str) {
        println!("{error}");
        return;
    }

    println!("{result}");
}

fn keygen() -> Result<()> {
    let mut secret = [0_u8; 32];
    getrandom::getrandom(&mut secret)
        .map_err(|e| anyhow!("failed to generate random key bytes: {e}"))?;
    let signing_key = SigningKey::from_bytes(&secret);
    let public_key_hex = hex_encode(&signing_key.verifying_key().to_bytes());

    let keys_dir = lattice_data_dir()?.join("named_keys");
    fs::create_dir_all(&keys_dir)
        .with_context(|| format!("failed to create key directory {}", keys_dir.display()))?;

    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|e| anyhow!("system clock error: {e}"))?
        .as_millis();
    let key_path = keys_dir.join(format!("{timestamp}.key"));

    fs::write(&key_path, signing_key.to_bytes())
        .with_context(|| format!("failed to save key to {}", key_path.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&key_path, fs::Permissions::from_mode(0o600))
            .context("failed to set key file permissions")?;
    }

    println!("Public key: {public_key_hex}");
    println!("Saved secret key: {}", key_path.display());

    Ok(())
}

fn init_site(name: Option<String>, rating: &str) -> Result<()> {
    let cwd = std::env::current_dir().context("failed to get current directory")?;
    let site_name = match name {
        Some(name) => name,
        None => cwd
            .file_name()
            .and_then(|n| n.to_str())
            .ok_or_else(|| anyhow!("failed to derive site name from current directory"))?
            .to_string(),
    };

    let index_path = cwd.join("index.html");
    let index_html = format!(
        "<!doctype html>\n<html>\n<head>\n  <meta charset=\"utf-8\">\n  <meta name=\"viewport\" content=\"width=device-width,initial-scale=1\">\n  <title>{0}.loom</title>\n</head>\n<body>\n  <h1>Welcome to {0}.loom - powered by Lattice</h1>\n</body>\n</html>\n",
        site_name
    );
    fs::write(&index_path, index_html)
        .with_context(|| format!("failed to write {}", index_path.display()))?;

    let publisher_key = load_identity_public_key_hex().unwrap_or_default();
    let file_hash = hash_file(&index_path)?;
    let file_size = fs::metadata(&index_path)
        .with_context(|| format!("failed to read metadata for {}", index_path.display()))?
        .len();

    let manifest = SiteManifest {
        name: site_name.clone(),
        version: 0,
        publisher_key,
        rating: rating.to_string(),
        app: None,
        files: vec![FileEntry {
            path: "index.html".to_string(),
            hash: file_hash.clone(),
            size: file_size,
            chunks: vec![file_hash],
            chunk_size: Some(DEFAULT_CHUNK_SIZE_BYTES as u64),
        }],
        signature: String::new(),
    };

    site_publisher::save_manifest(&manifest, &cwd)?;
    println!("Initialised {}.loom in current directory", site_name);

    Ok(())
}

fn site_name_for_dir(site_dir: &Path) -> Result<String> {
    if let Ok(manifest) = site_publisher::load_manifest(site_dir) {
        if !manifest.name.trim().is_empty() {
            return Ok(manifest.name);
        }
    }

    bail!("no name specified — use --name <name> or add \"name\" to lattice.json")
}

fn load_identity_public_key_hex() -> Result<String> {
    let identity_path = lattice_data_dir()?.join("identity.key");
    let bytes = fs::read(&identity_path)
        .with_context(|| format!("failed to read {}", identity_path.display()))?;

    if bytes.len() != 32 {
        bail!(
            "invalid identity key length in {}: expected 32 bytes, got {}",
            identity_path.display(),
            bytes.len()
        );
    }

    let mut secret = [0_u8; 32];
    secret.copy_from_slice(&bytes);
    let signing_key = SigningKey::from_bytes(&secret);
    Ok(hex_encode(&signing_key.verifying_key().to_bytes()))
}

fn load_site_signing_key() -> Result<SigningKey> {
    let key_path = lattice_data_dir()?.join("site_signing.key");
    let bytes =
        fs::read(&key_path).with_context(|| format!("failed to read {}", key_path.display()))?;
    let key_bytes: [u8; 32] = bytes
        .try_into()
        .map_err(|_| anyhow!("invalid site signing key length in {}", key_path.display()))?;
    Ok(SigningKey::from_bytes(&key_bytes))
}

fn lattice_data_dir() -> Result<PathBuf> {
    if let Ok(dir) = std::env::var("LATTICE_DATA_DIR") {
        return Ok(PathBuf::from(dir));
    }

    let base_dirs =
        BaseDirs::new().ok_or_else(|| anyhow!("failed to locate user home directory"))?;
    Ok(base_dirs.home_dir().join(".lattice"))
}

fn lattice_apps_dir() -> Result<PathBuf> {
    Ok(lattice_data_dir()?.join("apps"))
}

fn daemon_pid_path() -> Result<PathBuf> {
    Ok(lattice_data_dir()?.join("daemon.pid"))
}

fn macos_launch_agents_dir() -> Result<PathBuf> {
    let base_dirs =
        BaseDirs::new().ok_or_else(|| anyhow!("failed to locate user home directory"))?;
    Ok(base_dirs.home_dir().join("Library").join("LaunchAgents"))
}

fn macos_daemon_launch_agent_path() -> Result<PathBuf> {
    Ok(macos_launch_agents_dir()?.join(format!("{MACOS_DAEMON_LABEL}.plist")))
}

fn installed_app_path(app_id: &str) -> Result<PathBuf> {
    Ok(lattice_apps_dir()?.join(app_id))
}

fn installed_app_meta_path(app_id: &str) -> Result<PathBuf> {
    Ok(lattice_apps_dir()?.join(format!("{app_id}.json")))
}

fn read_installed_app_meta(app_id: &str) -> Result<Option<InstalledAppMeta>> {
    let path = installed_app_meta_path(app_id)?;
    if !path.exists() {
        return Ok(None);
    }
    let bytes = fs::read(&path).with_context(|| format!("failed to read {}", path.display()))?;
    let meta = serde_json::from_slice::<InstalledAppMeta>(&bytes)
        .with_context(|| format!("failed to decode {}", path.display()))?;
    Ok(Some(meta))
}

fn installed_app_ids() -> Result<Vec<String>> {
    let apps_dir = lattice_apps_dir()?;
    if !apps_dir.exists() {
        return Ok(Vec::new());
    }

    let mut app_ids = Vec::new();
    for entry in
        fs::read_dir(&apps_dir).with_context(|| format!("failed to read {}", apps_dir.display()))?
    {
        let entry = entry?;
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        let Some(file_name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        if file_name.ends_with(".json") {
            continue;
        }
        app_ids.push(file_name.to_string());
    }
    app_ids.sort();
    app_ids.dedup();
    Ok(app_ids)
}

#[derive(Debug, Clone)]
enum ServiceMode {
    System { service_path: PathBuf },
    User { service_path: PathBuf },
}

impl ServiceMode {
    fn service_path(&self) -> &Path {
        match self {
            ServiceMode::System { service_path } | ServiceMode::User { service_path } => {
                service_path.as_path()
            }
        }
    }

    fn systemctl_args<'a>(&self, args: &[&'a str]) -> Vec<&'a str> {
        match self {
            ServiceMode::System { .. } => args.to_vec(),
            ServiceMode::User { .. } => {
                let mut out = Vec::with_capacity(args.len() + 1);
                out.push("--user");
                out.extend_from_slice(args);
                out
            }
        }
    }
}

fn system_service_file_path(app_id: &str) -> PathBuf {
    PathBuf::from(format!("/etc/systemd/system/lattice-{app_id}.service"))
}

fn user_service_file_path(app_id: &str) -> Result<PathBuf> {
    let base_dirs =
        BaseDirs::new().ok_or_else(|| anyhow!("failed to locate user home directory"))?;
    Ok(base_dirs
        .config_dir()
        .join("systemd")
        .join("user")
        .join(format!("lattice-{app_id}.service")))
}

fn daemon_system_service_file_path() -> PathBuf {
    system_service_file_path("daemon")
}

fn packaged_daemon_user_service_file_path() -> PathBuf {
    PathBuf::from("/usr/lib/systemd/user/lattice-daemon.service")
}

fn daemon_user_service_file_path() -> Result<PathBuf> {
    user_service_file_path("daemon")
}

fn detect_existing_macos_daemon_launch_agent() -> Result<Option<PathBuf>> {
    let path = macos_daemon_launch_agent_path()?;
    if path.exists() {
        Ok(Some(path))
    } else {
        Ok(None)
    }
}

fn macos_app_label(app_id: &str) -> String {
    let suffix: String = app_id
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_') {
                ch
            } else {
                '-'
            }
        })
        .collect();
    format!("{MACOS_APP_LABEL_PREFIX}.{suffix}")
}

fn macos_app_launch_agent_path(app_id: &str) -> Result<PathBuf> {
    Ok(macos_launch_agents_dir()?.join(format!("{}.plist", macos_app_label(app_id))))
}

fn detect_existing_macos_app_launch_agent(app_id: &str) -> Result<Option<PathBuf>> {
    let path = macos_app_launch_agent_path(app_id)?;
    if path.exists() {
        Ok(Some(path))
    } else {
        Ok(None)
    }
}

fn detect_service_mode(app_id: &str) -> Result<Option<ServiceMode>> {
    let system_path = system_service_file_path(app_id);
    let daemon_system_service = Path::new("/etc/systemd/system/lattice-daemon.service");
    if daemon_system_service.exists() {
        return Ok(Some(ServiceMode::System {
            service_path: system_path,
        }));
    }

    let user_path = user_service_file_path(app_id)?;
    if let Some(parent) = user_path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
        return Ok(Some(ServiceMode::User {
            service_path: user_path,
        }));
    }

    Ok(None)
}

fn detect_daemon_service_mode() -> Result<Option<ServiceMode>> {
    let system_path = daemon_system_service_file_path();
    if system_path.exists() {
        return Ok(Some(ServiceMode::System {
            service_path: system_path,
        }));
    }

    let packaged_user_path = packaged_daemon_user_service_file_path();
    if packaged_user_path.exists() {
        return Ok(Some(ServiceMode::User {
            service_path: packaged_user_path,
        }));
    }

    let user_path = daemon_user_service_file_path()?;
    if let Some(parent) = user_path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
        return Ok(Some(ServiceMode::User {
            service_path: user_path,
        }));
    }

    Ok(None)
}

fn detect_existing_daemon_service_mode() -> Result<Option<ServiceMode>> {
    let system_path = daemon_system_service_file_path();
    if system_path.exists() {
        return Ok(Some(ServiceMode::System {
            service_path: system_path,
        }));
    }

    let packaged_user_path = packaged_daemon_user_service_file_path();
    if packaged_user_path.exists() {
        return Ok(Some(ServiceMode::User {
            service_path: packaged_user_path,
        }));
    }

    let user_path = daemon_user_service_file_path()?;
    if user_path.exists() {
        return Ok(Some(ServiceMode::User {
            service_path: user_path,
        }));
    }

    Ok(None)
}

fn installed_manual_instructions(install_path: &Path, rpc_port: u16) {
    println!("binary installed at {}", install_path.display());
    println!(
        "to start manually: LATTICE_RPC_PORT={} {}",
        rpc_port,
        install_path.display()
    );
    println!(
        "to start on login: add {} to your shell profile",
        install_path.display()
    );
}

fn install_macos_app_launch_agent(app_id: &str, install_path: &Path, rpc_port: u16) -> Result<()> {
    let plist_path = macos_app_launch_agent_path(app_id)?;
    if let Some(parent) = plist_path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }

    let plist = render_app_launchd_plist(app_id, install_path, rpc_port)?;
    fs::write(&plist_path, plist)
        .with_context(|| format!("failed to write {}", plist_path.display()))?;

    let domain = macos_launchctl_domain()?;
    let plist_arg = plist_path.to_string_lossy().to_string();
    let _ = run_launchctl(["bootout", &domain, &plist_arg].as_slice());
    run_launchctl(["bootstrap", &domain, &plist_arg].as_slice())?;
    let service_target = macos_app_launchctl_service_target(app_id)?;
    let _ = run_launchctl(["kickstart", "-k", &service_target].as_slice());
    Ok(())
}

fn uninstall_macos_app_launch_agent(app_id: &str) -> Result<()> {
    let Some(plist_path) = detect_existing_macos_app_launch_agent(app_id)? else {
        return Ok(());
    };

    let domain = macos_launchctl_domain()?;
    let plist_arg = plist_path.to_string_lossy().to_string();
    let _ = run_launchctl(["bootout", &domain, &plist_arg].as_slice());
    fs::remove_file(&plist_path)
        .with_context(|| format!("failed to remove {}", plist_path.display()))?;
    Ok(())
}

fn render_daemon_systemd_service(mode: &ServiceMode, daemon_path: &Path, rpc_port: u16) -> String {
    match mode {
        ServiceMode::System { .. } => {
            let user = std::env::var("USER").unwrap_or_else(|_| "root".to_string());
            format!(
                "[Unit]\nDescription=Lattice Daemon\nAfter=network.target\n\n[Service]\nType=simple\nUser={user}\nExecStart={} --rpc-port {}\nRestart=on-failure\nRestartSec=5\n\n[Install]\nWantedBy=multi-user.target\n",
                daemon_path.display(),
                rpc_port
            )
        }
        ServiceMode::User { .. } => format!(
            "[Unit]\nDescription=Lattice Daemon\nAfter=network.target\n\n[Service]\nType=simple\nExecStart={} --rpc-port {}\nRestart=on-failure\nRestartSec=5\n\n[Install]\nWantedBy=default.target\n",
            daemon_path.display(),
            rpc_port
        ),
    }
}

fn render_daemon_launchd_plist(daemon_path: &Path, rpc_port: u16) -> Result<String> {
    let data_dir = lattice_data_dir()?;
    let daemon_path = xml_escape_path(daemon_path);
    let data_dir = xml_escape_path(&data_dir);

    Ok(format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
<!DOCTYPE plist PUBLIC \"-//Apple//DTD PLIST 1.0//EN\" \"http://www.apple.com/DTDs/PropertyList-1.0.dtd\">\n\
<plist version=\"1.0\">\n\
  <dict>\n\
    <key>Label</key>\n\
    <string>{MACOS_DAEMON_LABEL}</string>\n\
    <key>ProgramArguments</key>\n\
    <array>\n\
      <string>{daemon_path}</string>\n\
      <string>--rpc-port</string>\n\
      <string>{rpc_port}</string>\n\
    </array>\n\
    <key>RunAtLoad</key>\n\
    <true/>\n\
    <key>KeepAlive</key>\n\
    <true/>\n\
    <key>WorkingDirectory</key>\n\
    <string>{data_dir}</string>\n\
  </dict>\n\
</plist>\n"
    ))
}

fn render_app_launchd_plist(app_id: &str, install_path: &Path, rpc_port: u16) -> Result<String> {
    let data_dir = lattice_data_dir()?;
    let app_label = macos_app_label(app_id);
    let install_path = xml_escape_path(install_path);
    let data_dir = xml_escape_path(&data_dir);

    Ok(format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
<!DOCTYPE plist PUBLIC \"-//Apple//DTD PLIST 1.0//EN\" \"http://www.apple.com/DTDs/PropertyList-1.0.dtd\">\n\
<plist version=\"1.0\">\n\
  <dict>\n\
    <key>Label</key>\n\
    <string>{app_label}</string>\n\
    <key>ProgramArguments</key>\n\
    <array>\n\
      <string>{install_path}</string>\n\
    </array>\n\
    <key>EnvironmentVariables</key>\n\
    <dict>\n\
      <key>LATTICE_RPC_PORT</key>\n\
      <string>{rpc_port}</string>\n\
    </dict>\n\
    <key>RunAtLoad</key>\n\
    <true/>\n\
    <key>KeepAlive</key>\n\
    <true/>\n\
    <key>WorkingDirectory</key>\n\
    <string>{data_dir}</string>\n\
  </dict>\n\
</plist>\n"
    ))
}

fn xml_escape_path(path: &Path) -> String {
    path.display()
        .to_string()
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

fn macos_launchctl_domain() -> Result<String> {
    let output = ProcessCommand::new("id")
        .arg("-u")
        .output()
        .context("failed to run id -u")?;
    if !output.status.success() {
        bail!("id -u failed");
    }
    let uid = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if uid.is_empty() {
        bail!("failed to determine user id for launchctl");
    }
    Ok(format!("gui/{uid}"))
}

fn run_launchctl(args: &[&str]) -> Result<()> {
    let status = ProcessCommand::new("launchctl")
        .args(args)
        .status()
        .with_context(|| format!("failed to run launchctl {}", args.join(" ")))?;
    if !status.success() {
        bail!("launchctl {} failed", args.join(" "));
    }
    Ok(())
}

#[cfg(target_os = "macos")]
fn launchctl_output(args: &[&str]) -> Result<std::process::Output> {
    ProcessCommand::new("launchctl")
        .args(args)
        .output()
        .with_context(|| format!("failed to run launchctl {}", args.join(" ")))
}

fn macos_launchctl_service_target() -> Result<String> {
    Ok(format!(
        "{}/{}",
        macos_launchctl_domain()?,
        MACOS_DAEMON_LABEL
    ))
}

fn macos_app_launchctl_service_target(app_id: &str) -> Result<String> {
    Ok(format!(
        "{}/{}",
        macos_launchctl_domain()?,
        macos_app_label(app_id)
    ))
}

fn install_linux_daemon_service_definition(rpc_port: u16) -> Result<()> {
    let daemon_path = daemon_binary_path()?;
    let Some(mode) = detect_daemon_service_mode()? else {
        bail!("systemd service mode is unavailable");
    };
    let service_path = mode.service_path().to_path_buf();
    if matches!(mode, ServiceMode::User { .. })
        && service_path == packaged_daemon_user_service_file_path()
    {
        return Ok(());
    }
    let service = render_daemon_systemd_service(&mode, &daemon_path, rpc_port);
    fs::write(&service_path, service)
        .with_context(|| format!("failed to write {}", service_path.display()))?;
    let args = mode.systemctl_args(["daemon-reload"].as_slice());
    run_systemctl(&args)?;
    Ok(())
}

fn start_linux_daemon_service() -> Result<()> {
    let Some(mode) = detect_daemon_service_mode()? else {
        bail!("lattice-daemon service is not available");
    };
    for args in [
        mode.systemctl_args(["enable", "lattice-daemon.service"].as_slice()),
        mode.systemctl_args(["start", "lattice-daemon.service"].as_slice()),
    ] {
        run_systemctl(&args)?;
    }
    Ok(())
}

fn stop_linux_daemon_service() -> Result<()> {
    let Some(mode) = detect_existing_daemon_service_mode()? else {
        bail!("lattice-daemon service is not installed");
    };
    let args = mode.systemctl_args(["stop", "lattice-daemon.service"].as_slice());
    run_systemctl(&args)?;
    Ok(())
}

fn restart_linux_daemon_service() -> Result<()> {
    let Some(mode) = detect_existing_daemon_service_mode()? else {
        bail!("lattice-daemon service is not installed");
    };
    let args = mode.systemctl_args(["restart", "lattice-daemon.service"].as_slice());
    run_systemctl(&args)?;
    Ok(())
}

fn linux_daemon_service_status() -> Result<Option<String>> {
    let Some(mode) = detect_existing_daemon_service_mode()? else {
        return Ok(None);
    };
    let args = mode.systemctl_args(["is-active", "lattice-daemon.service"].as_slice());
    let output = ProcessCommand::new("systemctl").args(&args).output()?;
    let status = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if status.is_empty() {
        Ok(Some("inactive".to_string()))
    } else {
        Ok(Some(status))
    }
}

fn uninstall_linux_daemon_service() -> Result<()> {
    let Some(mode) = detect_existing_daemon_service_mode()? else {
        return Ok(());
    };

    let _ = run_systemctl(&mode.systemctl_args(["stop", "lattice-daemon.service"].as_slice()));
    let _ = run_systemctl(&mode.systemctl_args(["disable", "lattice-daemon.service"].as_slice()));

    if mode.service_path() != packaged_daemon_user_service_file_path().as_path()
        && mode.service_path().exists()
    {
        let _ = fs::remove_file(mode.service_path());
        let _ = run_systemctl(&mode.systemctl_args(["daemon-reload"].as_slice()));
    }

    Ok(())
}

#[cfg(windows)]
fn windows_service_data_dir() -> PathBuf {
    let base = std::env::var_os("PROGRAMDATA")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(r"C:\ProgramData"));
    base.join("Lattice")
}

#[cfg(windows)]
fn windows_service_bin_path(daemon_path: &Path, rpc_port: u16) -> String {
    format!(
        "\"{}\" --service --rpc-port {} --data-dir \"{}\"",
        daemon_path.display(),
        rpc_port,
        windows_service_data_dir().display()
    )
}

#[cfg(windows)]
fn detect_existing_windows_daemon_service() -> Result<bool> {
    let output = ProcessCommand::new("sc.exe")
        .args(["query", WINDOWS_DAEMON_SERVICE_NAME])
        .output()
        .context("failed to run sc.exe query lattice-daemon")?;
    if output.status.success() {
        return Ok(true);
    }

    let stderr = String::from_utf8_lossy(&output.stderr);
    let stdout = String::from_utf8_lossy(&output.stdout);
    if stderr.contains("1060") || stdout.contains("1060") {
        return Ok(false);
    }

    bail!(
        "failed to query Windows service {}: {}{}",
        WINDOWS_DAEMON_SERVICE_NAME,
        stdout,
        stderr
    );
}

#[cfg(windows)]
fn run_sc(args: &[String]) -> Result<()> {
    let output = ProcessCommand::new("sc.exe")
        .args(args)
        .output()
        .with_context(|| format!("failed to run sc.exe {}", args.join(" ")))?;
    if !output.status.success() {
        bail!(
            "sc.exe {} failed: {}{}",
            args.join(" "),
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
    Ok(())
}

#[cfg(windows)]
fn install_or_update_windows_daemon_service(daemon_path: &Path, rpc_port: u16) -> Result<()> {
    let bin_path = windows_service_bin_path(daemon_path, rpc_port);
    if detect_existing_windows_daemon_service()? {
        run_sc(&[
            "config".to_string(),
            WINDOWS_DAEMON_SERVICE_NAME.to_string(),
            "binPath=".to_string(),
            bin_path,
            "start=".to_string(),
            "auto".to_string(),
            "DisplayName=".to_string(),
            "Lattice Daemon".to_string(),
        ])?;
    } else {
        run_sc(&[
            "create".to_string(),
            WINDOWS_DAEMON_SERVICE_NAME.to_string(),
            "binPath=".to_string(),
            bin_path,
            "start=".to_string(),
            "auto".to_string(),
            "DisplayName=".to_string(),
            "Lattice Daemon".to_string(),
        ])?;
    }
    Ok(())
}

#[cfg(windows)]
fn windows_daemon_service_status() -> Result<Option<String>> {
    let output = ProcessCommand::new("sc.exe")
        .args(["query", WINDOWS_DAEMON_SERVICE_NAME])
        .output()
        .context("failed to run sc.exe query lattice-daemon")?;

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    if !output.status.success() {
        if stdout.contains("1060") || stderr.contains("1060") {
            return Ok(None);
        }
        bail!(
            "failed to query Windows service {}: {}{}",
            WINDOWS_DAEMON_SERVICE_NAME,
            stdout,
            stderr
        );
    }

    for line in stdout.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with("STATE") {
            return Ok(Some(trimmed.to_string()));
        }
    }

    Ok(Some("STATE: unknown".to_string()))
}

#[cfg(windows)]
fn delete_windows_daemon_service() -> Result<()> {
    if !detect_existing_windows_daemon_service()? {
        return Ok(());
    }

    let _ = run_sc(&["stop".to_string(), WINDOWS_DAEMON_SERVICE_NAME.to_string()]);
    run_sc(&[
        "delete".to_string(),
        WINDOWS_DAEMON_SERVICE_NAME.to_string(),
    ])?;
    Ok(())
}

fn find_executable_in_path(name: &str) -> Option<PathBuf> {
    let path = std::env::var_os("PATH")?;
    for dir in std::env::split_paths(&path) {
        let candidate = dir.join(name);
        if candidate.is_file() {
            return Some(candidate);
        }
    }
    None
}

fn daemon_binary_path() -> Result<PathBuf> {
    if let Ok(current) = std::env::current_exe() {
        if let Some(parent) = current.parent() {
            #[cfg(windows)]
            {
                let sibling = parent.join("lattice-daemon.exe");
                if sibling.is_file() {
                    return Ok(sibling);
                }
            }

            let sibling = parent.join("lattice-daemon");
            if sibling.is_file() {
                return Ok(sibling);
            }
        }
    }

    #[cfg(windows)]
    if let Some(path) = find_executable_in_path("lattice-daemon.exe") {
        return Ok(path);
    }

    find_executable_in_path("lattice-daemon")
        .ok_or_else(|| anyhow!("lattice-daemon not found in PATH or next to lattice"))
}

fn purge_data_dir(path: &Path) -> Result<bool> {
    if !path.exists() {
        return Ok(false);
    }
    fs::remove_dir_all(path).with_context(|| format!("failed to remove {}", path.display()))?;
    Ok(true)
}

async fn wait_for_daemon(rpc_port: u16, timeout: Duration) -> Result<Value> {
    let client = RpcClient::new(rpc_port);
    let start = std::time::Instant::now();
    loop {
        match client.node_info().await {
            Ok(info) => return Ok(info),
            Err(err) if err.downcast_ref::<DaemonNotRunning>().is_some() => {}
            Err(err) => return Err(err),
        }

        if start.elapsed() >= timeout {
            bail!("timed out waiting for lattice-daemon to become ready");
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

fn write_daemon_pid(pid: u32) -> Result<()> {
    let pid_path = daemon_pid_path()?;
    if let Some(parent) = pid_path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    fs::write(&pid_path, pid.to_string())
        .with_context(|| format!("failed to write {}", pid_path.display()))?;
    Ok(())
}

fn pid_looks_like_lattice_daemon(pid: &str) -> bool {
    #[cfg(not(any(unix, windows)))]
    let _ = pid;

    #[cfg(unix)]
    {
        if let Ok(output) = ProcessCommand::new("ps")
            .args(["-p", pid, "-o", "command="])
            .output()
        {
            if output.status.success() {
                let command = String::from_utf8_lossy(&output.stdout);
                return command.contains("lattice-daemon");
            }
        }
    }

    #[cfg(windows)]
    {
        if let Ok(output) = ProcessCommand::new("tasklist")
            .args(["/FI", &format!("PID eq {pid}"), "/FO", "CSV", "/NH"])
            .output()
        {
            if output.status.success() {
                let listing = String::from_utf8_lossy(&output.stdout);
                return listing.contains("lattice-daemon.exe");
            }
        }
    }

    false
}

fn clear_daemon_pid() -> Result<()> {
    let pid_path = daemon_pid_path()?;
    if pid_path.exists() {
        fs::remove_file(&pid_path)
            .with_context(|| format!("failed to remove {}", pid_path.display()))?;
    }
    Ok(())
}

fn stop_manual_daemon() -> Result<bool> {
    let pid_path = daemon_pid_path()?;
    if !pid_path.exists() {
        return Ok(false);
    }
    let pid = fs::read_to_string(&pid_path)
        .with_context(|| format!("failed to read {}", pid_path.display()))?
        .trim()
        .to_string();
    if pid.is_empty() {
        clear_daemon_pid()?;
        return Ok(false);
    }

    if !pid_looks_like_lattice_daemon(&pid) {
        clear_daemon_pid()?;
        return Ok(false);
    }

    #[cfg(windows)]
    let status = ProcessCommand::new("taskkill")
        .args(["/PID", &pid, "/T", "/F"])
        .status()
        .with_context(|| format!("failed to run taskkill /PID {pid}"))?;

    #[cfg(not(windows))]
    let status = ProcessCommand::new("kill")
        .arg(&pid)
        .status()
        .with_context(|| format!("failed to run kill {pid}"))?;

    clear_daemon_pid()?;
    if !status.success() {
        bail!("failed to stop daemon process {pid}");
    }
    Ok(true)
}

fn has_manual_daemon_pid() -> Result<bool> {
    Ok(daemon_pid_path()?.exists())
}

fn start_manual_daemon(rpc_port: u16) -> Result<()> {
    let daemon_path = daemon_binary_path()?;
    let mut command = ProcessCommand::new(&daemon_path);
    command
        .arg("--rpc-port")
        .arg(rpc_port.to_string())
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());

    #[cfg(windows)]
    {
        command.creation_flags(WINDOWS_DAEMON_CREATION_FLAGS);
    }

    let child = command
        .spawn()
        .with_context(|| format!("failed to start {}", daemon_path.display()))?;
    write_daemon_pid(child.id())
}

async fn restart_daemon_if_managed(rpc_port: u16) -> Result<()> {
    if RpcClient::new(rpc_port).node_info().await.is_err() {
        return Ok(());
    }

    #[cfg(windows)]
    {
        if detect_existing_windows_daemon_service()? {
            run_sc(&["stop".to_string(), WINDOWS_DAEMON_SERVICE_NAME.to_string()])?;
            run_sc(&["start".to_string(), WINDOWS_DAEMON_SERVICE_NAME.to_string()])?;
            let _ = wait_for_daemon(rpc_port, Duration::from_secs(15)).await?;
            println!("lattice-daemon restarted");
            return Ok(());
        }
    }

    if std::env::consts::OS == "linux" {
        if let Some(mode) = detect_existing_daemon_service_mode()? {
            let args = mode.systemctl_args(["restart", "lattice-daemon.service"].as_slice());
            run_systemctl(&args)?;
            let _ = wait_for_daemon(rpc_port, Duration::from_secs(10)).await?;
            println!("lattice-daemon restarted");
            return Ok(());
        }
    }

    if std::env::consts::OS == "macos" {
        if detect_existing_macos_daemon_launch_agent()?.is_some() {
            let service_target = macos_launchctl_service_target()?;
            run_launchctl(["kickstart", "-k", &service_target].as_slice())?;
            let _ = wait_for_daemon(rpc_port, Duration::from_secs(10)).await?;
            println!("lattice-daemon restarted");
            return Ok(());
        }
    }

    if has_manual_daemon_pid()? {
        stop_manual_daemon()?;
        start_manual_daemon(rpc_port)?;
        let _ = wait_for_daemon(rpc_port, Duration::from_secs(10)).await?;
        println!("lattice-daemon restarted");
        return Ok(());
    }

    eprintln!(
        "warning: lattice-daemon is running but not managed by `lattice up`; restart it manually to pick up app changes"
    );
    Ok(())
}

async fn service_command(command: ServiceCommand, rpc_port: u16) -> Result<()> {
    #[cfg(windows)]
    {
        let daemon_path = daemon_binary_path()?;
        match command {
            ServiceCommand::Install => {
                install_or_update_windows_daemon_service(&daemon_path, rpc_port)?;
                println!("lattice-daemon service installed");
            }
            ServiceCommand::Uninstall { purge_data } => {
                delete_windows_daemon_service()?;
                clear_daemon_pid()?;
                if purge_data {
                    let data_dir = windows_service_data_dir();
                    if purge_data_dir(&data_dir)? {
                        println!("removed {}", data_dir.display());
                    } else {
                        println!("no data found at {}", data_dir.display());
                    }
                }
                println!("lattice-daemon service removed");
            }
            ServiceCommand::Start => {
                install_or_update_windows_daemon_service(&daemon_path, rpc_port)?;
                run_sc(&["start".to_string(), WINDOWS_DAEMON_SERVICE_NAME.to_string()])?;
                let _ = wait_for_daemon(rpc_port, Duration::from_secs(15)).await?;
                println!("lattice-daemon service started");
            }
            ServiceCommand::Stop => {
                if !detect_existing_windows_daemon_service()? {
                    bail!("lattice-daemon service is not installed");
                }
                run_sc(&["stop".to_string(), WINDOWS_DAEMON_SERVICE_NAME.to_string()])?;
                clear_daemon_pid()?;
                println!("lattice-daemon service stopped");
            }
            ServiceCommand::Restart => {
                if !detect_existing_windows_daemon_service()? {
                    bail!("lattice-daemon service is not installed");
                }
                let _ = run_sc(&["stop".to_string(), WINDOWS_DAEMON_SERVICE_NAME.to_string()]);
                run_sc(&["start".to_string(), WINDOWS_DAEMON_SERVICE_NAME.to_string()])?;
                let _ = wait_for_daemon(rpc_port, Duration::from_secs(15)).await?;
                println!("lattice-daemon service restarted");
            }
            ServiceCommand::Status => match windows_daemon_service_status()? {
                Some(status) => println!("{status}"),
                None => println!("lattice-daemon service is not installed"),
            },
        }
        return Ok(());
    }

    #[cfg(target_os = "linux")]
    {
        match command {
            ServiceCommand::Install => {
                install_linux_daemon_service_definition(rpc_port)?;
                println!("lattice-daemon service installed");
            }
            ServiceCommand::Uninstall { purge_data } => {
                uninstall_linux_daemon_service()?;
                clear_daemon_pid()?;
                if purge_data {
                    let data_dir = lattice_data_dir()?;
                    if purge_data_dir(&data_dir)? {
                        println!("removed {}", data_dir.display());
                    } else {
                        println!("no data found at {}", data_dir.display());
                    }
                }
                println!("lattice-daemon service removed");
            }
            ServiceCommand::Start => {
                install_linux_daemon_service_definition(rpc_port)?;
                start_linux_daemon_service()?;
                clear_daemon_pid()?;
                let _ = wait_for_daemon(rpc_port, Duration::from_secs(10)).await?;
                println!("lattice-daemon service started");
            }
            ServiceCommand::Stop => {
                stop_linux_daemon_service()?;
                clear_daemon_pid()?;
                println!("lattice-daemon service stopped");
            }
            ServiceCommand::Restart => {
                restart_linux_daemon_service()?;
                clear_daemon_pid()?;
                let _ = wait_for_daemon(rpc_port, Duration::from_secs(10)).await?;
                println!("lattice-daemon service restarted");
            }
            ServiceCommand::Status => match linux_daemon_service_status()? {
                Some(status) => println!("{status}"),
                None => println!("lattice-daemon service is not installed"),
            },
        }
        return Ok(());
    }

    #[cfg(target_os = "macos")]
    {
        let daemon_path = daemon_binary_path()?;
        let plist_path = macos_daemon_launch_agent_path()?;
        match command {
            ServiceCommand::Install => {
                if let Some(parent) = plist_path.parent() {
                    fs::create_dir_all(parent)
                        .with_context(|| format!("failed to create {}", parent.display()))?;
                }
                let plist = render_daemon_launchd_plist(&daemon_path, rpc_port)?;
                fs::write(&plist_path, plist)
                    .with_context(|| format!("failed to write {}", plist_path.display()))?;
                println!("lattice-daemon service installed");
            }
            ServiceCommand::Uninstall { purge_data } => {
                if let Some(existing) = detect_existing_macos_daemon_launch_agent()? {
                    let domain = macos_launchctl_domain()?;
                    let plist_arg = existing.to_string_lossy().to_string();
                    let _ = run_launchctl(["bootout", &domain, &plist_arg].as_slice());
                    fs::remove_file(&existing)
                        .with_context(|| format!("failed to remove {}", existing.display()))?;
                }
                clear_daemon_pid()?;
                if purge_data {
                    let data_dir = lattice_data_dir()?;
                    if purge_data_dir(&data_dir)? {
                        println!("removed {}", data_dir.display());
                    } else {
                        println!("no data found at {}", data_dir.display());
                    }
                }
                println!("lattice-daemon service removed");
            }
            ServiceCommand::Start => {
                if let Some(parent) = plist_path.parent() {
                    fs::create_dir_all(parent)
                        .with_context(|| format!("failed to create {}", parent.display()))?;
                }
                let plist = render_daemon_launchd_plist(&daemon_path, rpc_port)?;
                fs::write(&plist_path, plist)
                    .with_context(|| format!("failed to write {}", plist_path.display()))?;
                let domain = macos_launchctl_domain()?;
                let plist_arg = plist_path.to_string_lossy().to_string();
                let _ = run_launchctl(["bootout", &domain, &plist_arg].as_slice());
                run_launchctl(["bootstrap", &domain, &plist_arg].as_slice())?;
                let service_target = macos_launchctl_service_target()?;
                let _ = run_launchctl(["kickstart", "-k", &service_target].as_slice());
                clear_daemon_pid()?;
                let _ = wait_for_daemon(rpc_port, Duration::from_secs(10)).await?;
                println!("lattice-daemon service started");
            }
            ServiceCommand::Stop => {
                let Some(existing) = detect_existing_macos_daemon_launch_agent()? else {
                    bail!("lattice-daemon service is not installed");
                };
                let domain = macos_launchctl_domain()?;
                let plist_arg = existing.to_string_lossy().to_string();
                run_launchctl(["bootout", &domain, &plist_arg].as_slice())?;
                clear_daemon_pid()?;
                println!("lattice-daemon service stopped");
            }
            ServiceCommand::Restart => {
                if detect_existing_macos_daemon_launch_agent()?.is_none() {
                    bail!("lattice-daemon service is not installed");
                }
                let service_target = macos_launchctl_service_target()?;
                run_launchctl(["kickstart", "-k", &service_target].as_slice())?;
                clear_daemon_pid()?;
                let _ = wait_for_daemon(rpc_port, Duration::from_secs(10)).await?;
                println!("lattice-daemon service restarted");
            }
            ServiceCommand::Status => {
                if detect_existing_macos_daemon_launch_agent()?.is_none() {
                    println!("lattice-daemon service is not installed");
                } else {
                    let service_target = macos_launchctl_service_target()?;
                    let output = launchctl_output(["print", &service_target].as_slice())?;
                    if output.status.success() {
                        println!("loaded");
                    } else {
                        println!("installed");
                    }
                }
            }
        }
        return Ok(());
    }

    #[cfg(not(any(windows, target_os = "linux", target_os = "macos")))]
    {
        let _ = command;
        let _ = rpc_port;
        bail!("`lattice service` is not supported on this platform yet");
    }
}

async fn up(rpc_port: u16) -> Result<()> {
    if let Ok(info) = RpcClient::new(rpc_port).node_info().await {
        println!("lattice-daemon is already running");
        print_status(&info);
        return Ok(());
    }

    if std::env::consts::OS == "linux" {
        let daemon_path = daemon_binary_path()?;
        if let Some(mode) = detect_daemon_service_mode()? {
            let service_path = mode.service_path().to_path_buf();
            if matches!(mode, ServiceMode::User { .. })
                && service_path == daemon_user_service_file_path()?
            {
                let service = render_daemon_systemd_service(&mode, &daemon_path, rpc_port);
                fs::write(&service_path, service)
                    .with_context(|| format!("failed to write {}", service_path.display()))?;
            }
            let unit = "lattice-daemon.service";
            let mut service_ok = true;
            for args in [
                vec!["daemon-reload"],
                vec!["enable", unit],
                vec!["start", unit],
            ] {
                let systemctl_args = mode.systemctl_args(&args);
                if let Err(err) = run_systemctl(&systemctl_args) {
                    service_ok = false;
                    eprintln!("warning: {err}");
                    break;
                }
            }
            if service_ok {
                clear_daemon_pid()?;
                let info = wait_for_daemon(rpc_port, Duration::from_secs(10)).await?;
                println!("lattice-daemon enabled and started");
                print_status(&info);
                return Ok(());
            }
        }
    }

    if std::env::consts::OS == "macos" {
        let daemon_path = daemon_binary_path()?;
        let plist_path = macos_daemon_launch_agent_path()?;
        if let Some(parent) = plist_path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("failed to create {}", parent.display()))?;
        }
        let plist = render_daemon_launchd_plist(&daemon_path, rpc_port)?;
        fs::write(&plist_path, plist)
            .with_context(|| format!("failed to write {}", plist_path.display()))?;

        let domain = macos_launchctl_domain()?;
        let plist_arg = plist_path.to_string_lossy().to_string();
        let _ = run_launchctl(["bootout", &domain, &plist_arg].as_slice());
        run_launchctl(["bootstrap", &domain, &plist_arg].as_slice())?;
        let _ = run_launchctl(
            ["kickstart", "-k", &format!("{domain}/{MACOS_DAEMON_LABEL}")].as_slice(),
        );

        clear_daemon_pid()?;
        let info = wait_for_daemon(rpc_port, Duration::from_secs(10)).await?;
        println!("lattice-daemon enabled and started");
        print_status(&info);
        return Ok(());
    }

    #[cfg(windows)]
    {
        let daemon_path = daemon_binary_path()?;
        match install_or_update_windows_daemon_service(&daemon_path, rpc_port)
            .and_then(|_| run_sc(&["start".to_string(), WINDOWS_DAEMON_SERVICE_NAME.to_string()]))
        {
            Ok(()) => {
                clear_daemon_pid()?;
                let info = wait_for_daemon(rpc_port, Duration::from_secs(15)).await?;
                println!("lattice-daemon enabled and started");
                print_status(&info);
                return Ok(());
            }
            Err(err) => {
                eprintln!("warning: {err}");
                eprintln!(
                    "warning: falling back to manual daemon startup because Windows service management is unavailable"
                );
            }
        }
    }

    start_manual_daemon(rpc_port)?;
    let info = wait_for_daemon(rpc_port, Duration::from_secs(10)).await?;
    println!("lattice-daemon started");
    print_status(&info);
    Ok(())
}

fn down() -> Result<()> {
    #[cfg(windows)]
    {
        if detect_existing_windows_daemon_service()? {
            run_sc(&["stop".to_string(), WINDOWS_DAEMON_SERVICE_NAME.to_string()])?;
            clear_daemon_pid()?;
            println!("lattice-daemon stopped");
            return Ok(());
        }
    }

    if std::env::consts::OS == "linux" {
        if let Some(mode) = detect_existing_daemon_service_mode()? {
            let args = mode.systemctl_args(["stop", "lattice-daemon.service"].as_slice());
            run_systemctl(&args)?;
            clear_daemon_pid()?;
            println!("lattice-daemon stopped");
            return Ok(());
        }
    }

    if std::env::consts::OS == "macos" {
        if let Some(plist_path) = detect_existing_macos_daemon_launch_agent()? {
            let domain = macos_launchctl_domain()?;
            let plist_arg = plist_path.to_string_lossy().to_string();
            run_launchctl(["bootout", &domain, &plist_arg].as_slice())?;
            clear_daemon_pid()?;
            println!("lattice-daemon stopped");
            return Ok(());
        }
    }

    if stop_manual_daemon()? {
        println!("lattice-daemon stopped");
        return Ok(());
    }

    println!("lattice-daemon is not running");
    Ok(())
}

async fn install_app(rpc_port: u16, app_id: &str) -> Result<()> {
    install_or_update_app(rpc_port, app_id, false).await
}

async fn update_apps(rpc_port: u16, app_id: Option<&str>, all: bool) -> Result<()> {
    if all && app_id.is_some() {
        bail!("use either `lattice update <app>` or `lattice update --all`");
    }
    if !all && app_id.is_none() {
        bail!("specify an app id or use --all");
    }

    if all {
        let app_ids = installed_app_ids()?;
        if app_ids.is_empty() {
            println!("No apps installed");
            return Ok(());
        }
        for app_id in app_ids {
            install_or_update_app(rpc_port, &app_id, true).await?;
        }
        return Ok(());
    }

    let app_id = app_id.expect("validated above");
    if read_installed_app_meta(app_id)?.is_none() && !installed_app_path(app_id)?.exists() {
        bail!("app {app_id} is not installed");
    }
    install_or_update_app(rpc_port, app_id, true).await
}

async fn install_or_update_app(rpc_port: u16, app_id: &str, update_only: bool) -> Result<()> {
    let record = fetch_app_registry_record(rpc_port, app_id).await?;

    let Some((url, sha256)) = platform_asset(&record) else {
        println!("no binary available for your platform");
        return Ok(());
    };

    let installed_meta = read_installed_app_meta(app_id)?;
    if update_only {
        if let Some(meta) = &installed_meta {
            if meta.version == record.version {
                println!("{app_id} is already up to date ({})", meta.version);
                return Ok(());
            }
        }
        println!("updating {app_id} to v{}...", record.version);
    } else {
        println!("installing {app_id} v{}...", record.version);
    }

    fs::create_dir_all(lattice_apps_dir()?).context("failed to create app install directory")?;

    let temp_path = std::env::temp_dir().join(format!(
        "lattice-install-{app_id}-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis())
            .unwrap_or(0)
    ));
    download_with_progress(url, &temp_path)?;
    verify_sha256_file(&temp_path, sha256)?;

    let install_path = installed_app_path(app_id)?;
    if install_path.exists() {
        fs::remove_file(&install_path)
            .with_context(|| format!("failed to replace {}", install_path.display()))?;
    }
    fs::rename(&temp_path, &install_path).or_else(|_| {
        fs::copy(&temp_path, &install_path)
            .map(|_| ())
            .with_context(|| format!("failed to install {}", install_path.display()))
    })?;
    let _ = fs::remove_file(&temp_path);

    #[cfg(unix)]
    {
        fs::set_permissions(&install_path, fs::Permissions::from_mode(0o755))
            .with_context(|| format!("failed to mark {} executable", install_path.display()))?;
    }

    let meta = InstalledAppMeta {
        app_id: app_id.to_string(),
        version: record.version.clone(),
        description: record.description.clone(),
    };
    fs::write(
        installed_app_meta_path(app_id)?,
        serde_json::to_vec_pretty(&meta).context("failed to encode app metadata")?,
    )
    .context("failed to write app metadata")?;

    if std::env::consts::OS == "linux" {
        match detect_service_mode(app_id)? {
            Some(mode) => {
                let service_path = mode.service_path().to_path_buf();
                let service_contents =
                    render_systemd_service(&mode, app_id, &install_path, rpc_port);
                match fs::write(&service_path, service_contents) {
                    Ok(()) => {
                        let mut setup_ok = true;
                        for args in [
                            ["daemon-reload"].as_slice(),
                            ["enable", &format!("lattice-{app_id}.service")].as_slice(),
                            ["start", &format!("lattice-{app_id}.service")].as_slice(),
                        ] {
                            let systemctl_args = mode.systemctl_args(args);
                            if let Err(err) = run_systemctl(&systemctl_args) {
                                setup_ok = false;
                                eprintln!("warning: {err}");
                                eprintln!("service file written to {}", service_path.display());
                                break;
                            }
                        }
                        if setup_ok {
                            println!("{app_id} installed and started");
                        } else {
                            installed_manual_instructions(&install_path, rpc_port);
                        }
                    }
                    Err(err) => {
                        eprintln!(
                            "warning: failed to write service file {}: {err}",
                            service_path.display()
                        );
                        installed_manual_instructions(&install_path, rpc_port);
                    }
                }
            }
            None => installed_manual_instructions(&install_path, rpc_port),
        }
    } else if std::env::consts::OS == "macos" {
        match install_macos_app_launch_agent(app_id, &install_path, rpc_port) {
            Ok(()) => println!("{app_id} installed and started"),
            Err(err) => {
                eprintln!("warning: {err}");
                installed_manual_instructions(&install_path, rpc_port);
            }
        }
    } else {
        println!("installed at {}", install_path.display());
        println!(
            "run it manually: set LATTICE_RPC_PORT={} and start {}",
            rpc_port,
            install_path.display()
        );
    }

    restart_daemon_if_managed(rpc_port).await?;

    Ok(())
}

async fn fetch_app_registry_record(rpc_port: u16, app_id: &str) -> Result<AppRegistryRecord> {
    let value = RpcClient::new(rpc_port)
        .get_record(&format!("{APP_REGISTRY_PREFIX}{app_id}"))
        .await?;

    if value.is_null() {
        bail!("app {app_id} not found in registry");
    }

    let value = value
        .as_str()
        .ok_or_else(|| anyhow!("app registry record was not a string"))?;
    let signed: SignedRecord =
        serde_json::from_str(value).context("failed to decode signed app registry record")?;
    if !signed.verify() {
        bail!("invalid signed app registry record");
    }
    if !is_registry_operator(&signed.publisher_b64()) {
        bail!("registry record has invalid operator signature");
    }
    let record: AppRegistryRecord = signed
        .payload_json()
        .context("failed to decode app registry payload")?;
    validate_app_registry_record(&record)
        .map_err(|err| anyhow!("invalid app registry record: {err}"))?;
    if record.app_id != app_id {
        bail!("registry app id mismatch");
    }
    Ok(record)
}

fn uninstall_app(app_id: &str) -> Result<()> {
    if std::env::consts::OS == "linux" {
        let unit = format!("lattice-{app_id}.service");
        if let Some(mode) = detect_existing_service_mode(app_id)? {
            let stop_args = mode.systemctl_args(["stop", &unit].as_slice());
            let disable_args = mode.systemctl_args(["disable", &unit].as_slice());
            let reload_args = mode.systemctl_args(["daemon-reload"].as_slice());
            let _ = run_systemctl(&stop_args);
            let _ = run_systemctl(&disable_args);
            if mode.service_path().exists() {
                let _ = fs::remove_file(mode.service_path());
            }
            let _ = run_systemctl(&reload_args);
        }
    } else if std::env::consts::OS == "macos" {
        let _ = uninstall_macos_app_launch_agent(app_id);
    }

    let install_path = installed_app_path(app_id)?;
    if install_path.exists() {
        fs::remove_file(&install_path)
            .with_context(|| format!("failed to delete {}", install_path.display()))?;
    }

    let meta_path = installed_app_meta_path(app_id)?;
    if meta_path.exists() {
        let _ = fs::remove_file(&meta_path);
    }

    println!("{app_id} uninstalled");
    Ok(())
}

fn list_installed_apps() -> Result<()> {
    let apps_dir = lattice_apps_dir()?;
    if !apps_dir.exists() {
        println!("No apps installed");
        return Ok(());
    }

    let mut found = false;
    for entry in
        fs::read_dir(&apps_dir).with_context(|| format!("failed to read {}", apps_dir.display()))?
    {
        let entry = entry?;
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        let Some(file_name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        if file_name.ends_with(".json") {
            continue;
        }
        found = true;
        let app_id = file_name.to_string();
        let meta = installed_app_meta_path(&app_id)
            .ok()
            .and_then(|path| fs::read(&path).ok())
            .and_then(|bytes| serde_json::from_slice::<InstalledAppMeta>(&bytes).ok());
        let version = meta
            .as_ref()
            .map(|meta| meta.version.as_str())
            .unwrap_or("unknown");
        let status = service_status(&app_id);
        println!("{app_id} {version} {status}");
    }

    if !found {
        println!("No apps installed");
    }

    Ok(())
}

fn platform_asset(record: &AppRegistryRecord) -> Option<(&str, &str)> {
    match (std::env::consts::OS, std::env::consts::ARCH) {
        ("linux", "x86_64") => pair(
            record.linux_x86_64_url.as_deref(),
            record.linux_x86_64_sha256.as_deref(),
        ),
        ("linux", "aarch64") => pair(
            record.linux_aarch64_url.as_deref(),
            record.linux_aarch64_sha256.as_deref(),
        ),
        ("macos", "aarch64") => pair(
            record.macos_aarch64_url.as_deref(),
            record.macos_aarch64_sha256.as_deref(),
        ),
        ("macos", "x86_64") => pair(
            record.macos_x86_64_url.as_deref(),
            record.macos_x86_64_sha256.as_deref(),
        ),
        _ => None,
    }
}

fn pair<'a>(url: Option<&'a str>, sha256: Option<&'a str>) -> Option<(&'a str, &'a str)> {
    match (url, sha256) {
        (Some(url), Some(sha256)) => Some((url, sha256)),
        _ => None,
    }
}

fn download_with_progress(url: &str, output: &Path) -> Result<()> {
    let client = reqwest::blocking::Client::new();
    let mut response = client
        .get(url)
        .send()
        .with_context(|| format!("failed to download {url}"))?
        .error_for_status()
        .with_context(|| format!("download failed for {url}"))?;
    let mut file = fs::File::create(output)
        .with_context(|| format!("failed to create {}", output.display()))?;
    let mut downloaded = 0_u64;
    let mut next_report = 1_048_576_u64;
    let mut buffer = [0_u8; 8192];
    loop {
        let read = response
            .read(&mut buffer)
            .context("failed to read download response")?;
        if read == 0 {
            break;
        }
        file.write_all(&buffer[..read])
            .with_context(|| format!("failed to write {}", output.display()))?;
        downloaded = downloaded.saturating_add(read as u64);
        if downloaded >= next_report {
            println!("{downloaded} bytes downloaded");
            next_report = next_report.saturating_add(1_048_576);
        }
    }
    Ok(())
}

fn verify_sha256_file(path: &Path, expected_hex: &str) -> Result<()> {
    let mut file =
        fs::File::open(path).with_context(|| format!("failed to open {}", path.display()))?;
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 8192];
    loop {
        let read = file
            .read(&mut buffer)
            .with_context(|| format!("failed to read {}", path.display()))?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    let actual = hex_encode(&hasher.finalize());
    if actual != expected_hex {
        let _ = fs::remove_file(path);
        bail!("checksum mismatch, aborting");
    }
    Ok(())
}

fn detect_existing_service_mode(app_id: &str) -> Result<Option<ServiceMode>> {
    let system_path = system_service_file_path(app_id);
    if system_path.exists() {
        return Ok(Some(ServiceMode::System {
            service_path: system_path,
        }));
    }

    let user_path = user_service_file_path(app_id)?;
    if user_path.exists() {
        return Ok(Some(ServiceMode::User {
            service_path: user_path,
        }));
    }

    Ok(None)
}

fn render_systemd_service(
    mode: &ServiceMode,
    app_id: &str,
    install_path: &Path,
    rpc_port: u16,
) -> String {
    match mode {
        ServiceMode::System { .. } => {
            let user = std::env::var("USER").unwrap_or_else(|_| "root".to_string());
            format!(
                "[Unit]\nDescription=Lattice App: {app_id}\nAfter=network.target lattice-daemon.service\nRequires=lattice-daemon.service\n\n[Service]\nType=simple\nUser={user}\nExecStart={}\nRestart=on-failure\nRestartSec=5\nEnvironment=LATTICE_RPC_PORT={rpc_port}\n\n[Install]\nWantedBy=multi-user.target\n",
                install_path.display(),
            )
        }
        ServiceMode::User { .. } => format!(
            "[Unit]\nDescription=Lattice App: {app_id}\nAfter=network.target\n\n[Service]\nType=simple\nExecStart={}\nRestart=on-failure\nRestartSec=5\nEnvironment=LATTICE_RPC_PORT={rpc_port}\n\n[Install]\nWantedBy=default.target\n",
            install_path.display(),
        ),
    }
}

fn run_systemctl(args: &[&str]) -> Result<()> {
    let status = ProcessCommand::new("systemctl")
        .args(args)
        .status()
        .with_context(|| format!("failed to run systemctl {}", args.join(" ")))?;
    if !status.success() {
        bail!("systemctl {} failed", args.join(" "));
    }
    Ok(())
}

fn service_status(app_id: &str) -> String {
    if std::env::consts::OS == "macos" {
        return match detect_existing_macos_app_launch_agent(app_id) {
            Ok(Some(_)) => "launchd".to_string(),
            Ok(None) => "manual".to_string(),
            Err(_) => "unknown".to_string(),
        };
    }

    if std::env::consts::OS != "linux" {
        return "manual".to_string();
    }
    let unit = format!("lattice-{app_id}.service");
    let mode = match detect_existing_service_mode(app_id) {
        Ok(Some(mode)) => mode,
        Ok(None) => return "manual".to_string(),
        Err(_) => return "unknown".to_string(),
    };
    let args = mode.systemctl_args(["is-active", &unit].as_slice());
    match ProcessCommand::new("systemctl").args(&args).output() {
        Ok(output) if output.status.success() => {
            String::from_utf8_lossy(&output.stdout).trim().to_string()
        }
        Ok(output) => {
            let status = String::from_utf8_lossy(&output.stdout).trim().to_string();
            if status.is_empty() {
                "inactive".to_string()
            } else {
                status
            }
        }
        Err(_) => "unknown".to_string(),
    }
}

fn safe_join(base: &Path, untrusted: &str) -> Result<PathBuf> {
    if untrusted.contains('\0') {
        bail!("unsafe path in manifest: {}", untrusted);
    }
    if untrusted.contains('\\') {
        bail!("unsafe path separator in manifest: {}", untrusted);
    }

    let path = Path::new(untrusted);
    if path.is_absolute() {
        bail!("unsafe path in manifest: {}", untrusted);
    }

    for component in path.components() {
        if matches!(
            component,
            std::path::Component::ParentDir
                | std::path::Component::CurDir
                | std::path::Component::RootDir
                | std::path::Component::Prefix(_)
        ) {
            bail!("path traversal in manifest: {}", untrusted);
        }
    }

    Ok(base.join(path))
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        out.push(HEX[(b >> 4) as usize] as char);
        out.push(HEX[(b & 0x0f) as usize] as char);
    }
    out
}

fn decode_hex(input: &str) -> Result<Vec<u8>> {
    if !input.len().is_multiple_of(2) {
        bail!("hex input length must be even");
    }

    let mut out = Vec::with_capacity(input.len() / 2);
    let bytes = input.as_bytes();
    for i in (0..bytes.len()).step_by(2) {
        let hi = hex_nibble(bytes[i])?;
        let lo = hex_nibble(bytes[i + 1])?;
        out.push((hi << 4) | lo);
    }
    Ok(out)
}

fn hex_nibble(byte: u8) -> Result<u8> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        _ => bail!("invalid hex character: {}", byte as char),
    }
}

fn file_block_hashes(file: &FileEntry) -> Vec<String> {
    if !file.chunks.is_empty() {
        return file.chunks.clone();
    }
    vec![file.hash.clone()]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn safe_join_rejects_parent_dir() {
        let base = Path::new("/tmp/out");
        assert!(safe_join(base, "../secret").is_err());
    }

    #[test]
    fn safe_join_rejects_curdir_segments() {
        let base = Path::new("/tmp/out");
        assert!(safe_join(base, "./index.html").is_err());
    }

    #[test]
    fn safe_join_rejects_backslashes() {
        let base = Path::new("/tmp/out");
        assert!(safe_join(base, "a\\b.txt").is_err());
    }

    #[test]
    fn safe_join_accepts_normal_relative_path() {
        let base = Path::new("/tmp/out");
        let joined = safe_join(base, "assets/app.js").expect("join path");
        assert_eq!(joined, base.join("assets/app.js"));
    }

    #[cfg(windows)]
    #[test]
    fn windows_service_bin_path_uses_programdata_lattice() {
        let daemon = Path::new(r"C:\Program Files\Lattice\lattice-daemon.exe");
        let bin_path = windows_service_bin_path(daemon, 7780);
        assert!(bin_path.contains(r#"--data-dir "C:\ProgramData\Lattice""#));
        assert!(bin_path.contains(r#"--rpc-port 7780"#));
    }
}
