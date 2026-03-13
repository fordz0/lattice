// NOTE: breaking data-dir change:
// Site publishing now uses a separate key file in the active Lattice data dir.
// Existing installs using identity.key for publishing must re-claim names with:
// `lattice name claim <name>`.
use anyhow::{anyhow, Context, Result};
use ed25519_dalek::SigningKey;
use libp2p::identity;
use libp2p::PeerId;
use rand::rngs::OsRng;
use rand::RngCore;
use std::fs;
use std::path::Path;
use tracing::info;

pub struct NodeIdentity {
    pub keypair: identity::Keypair,
    pub peer_id: PeerId,
}

pub fn load_or_create_identity(data_dir: &Path) -> Result<NodeIdentity> {
    let identity_path = data_dir.join("identity.key");

    let keypair = if identity_path.exists() {
        let bytes = fs::read(&identity_path)
            .with_context(|| format!("failed to read identity key {}", identity_path.display()))?;

        if bytes.len() != 32 {
            anyhow::bail!(
                "invalid identity key length in {}: expected 32 bytes, got {}",
                identity_path.display(),
                bytes.len()
            );
        }

        let mut secret_bytes = [0_u8; 32];
        secret_bytes.copy_from_slice(&bytes);
        let secret_key = identity::ed25519::SecretKey::try_from_bytes(secret_bytes)
            .map_err(|e| anyhow::anyhow!("failed to parse identity secret key: {e}"))?;
        let ed25519_keypair = identity::ed25519::Keypair::from(secret_key);
        identity::Keypair::from(ed25519_keypair)
    } else {
        fs::create_dir_all(data_dir)
            .with_context(|| format!("failed to create data dir {}", data_dir.display()))?;

        let ed25519_keypair = identity::ed25519::Keypair::generate();
        fs::write(&identity_path, ed25519_keypair.secret().as_ref())
            .with_context(|| format!("failed to write identity key {}", identity_path.display()))?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&identity_path, fs::Permissions::from_mode(0o600)).with_context(
                || {
                    format!(
                        "failed to set permissions on identity key {}",
                        identity_path.display()
                    )
                },
            )?;
        }
        identity::Keypair::from(ed25519_keypair)
    };

    let peer_id = keypair.public().to_peer_id();
    Ok(NodeIdentity { keypair, peer_id })
}

pub fn load_or_create_site_signing_key(data_dir: &Path) -> Result<SigningKey> {
    let key_path = data_dir.join("site_signing.key");

    if key_path.exists() {
        let bytes = fs::read(&key_path).context("failed to read site signing key")?;
        let key_bytes: [u8; 32] = bytes
            .try_into()
            .map_err(|_| anyhow!("invalid site signing key length"))?;
        return Ok(SigningKey::from_bytes(&key_bytes));
    }

    fs::create_dir_all(data_dir)
        .with_context(|| format!("failed to create data dir {}", data_dir.display()))?;

    let mut rng = OsRng;
    let mut secret = [0_u8; 32];
    rng.fill_bytes(&mut secret);
    let signing_key = SigningKey::from_bytes(&secret);
    fs::write(&key_path, signing_key.to_bytes())
        .with_context(|| format!("failed to write site signing key {}", key_path.display()))?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&key_path, fs::Permissions::from_mode(0o600)).with_context(|| {
            format!(
                "failed to set permissions on site signing key {}",
                key_path.display()
            )
        })?;
    }

    info!(path = ?key_path, "generated new site signing key");
    Ok(signing_key)
}

pub fn load_or_create_block_cache_key(data_dir: &Path) -> Result<[u8; 32]> {
    let key_path = data_dir.join("block_cache.key");

    if key_path.exists() {
        let bytes = fs::read(&key_path).context("failed to read block cache key")?;
        let key_bytes: [u8; 32] = bytes
            .try_into()
            .map_err(|_| anyhow!("invalid block cache key length"))?;
        return Ok(key_bytes);
    }

    fs::create_dir_all(data_dir)
        .with_context(|| format!("failed to create data dir {}", data_dir.display()))?;

    let mut rng = OsRng;
    let mut secret = [0_u8; 32];
    rng.fill_bytes(&mut secret);
    fs::write(&key_path, secret)
        .with_context(|| format!("failed to write block cache key {}", key_path.display()))?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&key_path, fs::Permissions::from_mode(0o600)).with_context(|| {
            format!(
                "failed to set permissions on block cache key {}",
                key_path.display()
            )
        })?;
    }

    info!(path = ?key_path, "generated new block cache encryption key");
    Ok(secret)
}
