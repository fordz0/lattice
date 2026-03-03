mod rpc;

use anyhow::{anyhow, bail, Context, Result};
use clap::{Parser, Subcommand};
use directories::BaseDirs;
use ed25519_dalek::SigningKey;
use rpc::{DaemonNotRunning, RpcClient};
use serde_json::Value;
use std::fs;
use std::path::PathBuf;
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
    Put { key: String, value: String },
    Get { key: String },
    Keygen,
    Name {
        #[command(subcommand)]
        command: NameCommand,
    },
}

#[derive(Subcommand)]
enum NameCommand {
    Claim { name: String },
    Info { name: String },
}

#[tokio::main]
async fn main() {
    let exit_code = match run().await {
        Ok(()) => 0,
        Err(err) => {
            if err.downcast_ref::<DaemonNotRunning>().is_some() {
                eprintln!(
                    "lattice daemon is not running. Start it with: lattice-daemon"
                );
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
                let owner_key = load_identity_public_key_hex()?;
                let result = client
                    .put_record(&format!("name:{name}"), &owner_key)
                    .await?;

                let status = result.get("status").and_then(Value::as_str).unwrap_or("err");
                if status == "ok" {
                    println!("claimed {name}.lat");
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

                println!("Name:      {name}.lat");
                println!("Owner key: {owner}");
            }
        },
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

    println!("Public key: {public_key_hex}");
    println!("Saved secret key: {}", key_path.display());

    Ok(())
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
    let base_dirs = BaseDirs::new().ok_or_else(|| anyhow!("failed to locate user home directory"))?;
    Ok(base_dirs.home_dir().join(".lattice"))
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
