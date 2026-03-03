use anyhow::{Context, Result};
use libp2p::identity;
use libp2p::PeerId;
use std::fs;
use std::path::Path;

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
