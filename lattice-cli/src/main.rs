mod rpc;

use anyhow::{anyhow, bail, Context, Result};
use clap::{Parser, Subcommand};
use directories::BaseDirs;
use ed25519_dalek::SigningKey;
use lattice_site::manifest::{
    hash_bytes, hash_file, verify_manifest, FileEntry, SiteManifest, DEFAULT_CHUNK_SIZE_BYTES,
};
use lattice_site::publisher as site_publisher;
use rpc::{DaemonNotRunning, RpcClient};
use serde_json::Value;
use std::error::Error;
use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Parser)]
#[command(name = "lattice-cli")]
#[command(about = "CLI client for lattice-daemon JSON-RPC")]
struct Cli {
    #[arg(long, global = true, default_value_t = 7780)]
    rpc_port: u16,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
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
}

#[derive(Subcommand)]
enum NameCommand {
    Claim { name: String },
    Info { name: String },
    List,
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

const MAX_FETCH_SITE_FILES: usize = 1000;
const MAX_FETCH_SITE_BYTES: u64 = 100 * 1024 * 1024;

#[tokio::main]
async fn main() {
    let exit_code = match run().await {
        Ok(()) => 0,
        Err(err) => {
            if err.downcast_ref::<DaemonNotRunning>().is_some() {
                eprintln!("lattice daemon is not running. Start it with: lattice-daemon");
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

fn lattice_data_dir() -> Result<PathBuf> {
    if let Ok(dir) = std::env::var("LATTICE_DATA_DIR") {
        return Ok(PathBuf::from(dir));
    }

    let base_dirs =
        BaseDirs::new().ok_or_else(|| anyhow!("failed to locate user home directory"))?;
    Ok(base_dirs.home_dir().join(".lattice"))
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
}
