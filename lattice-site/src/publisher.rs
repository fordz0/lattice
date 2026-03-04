use crate::manifest::{hash_file, sign_manifest, FileEntry, SiteManifest};
use anyhow::{Context, Result};
use ed25519_dalek::SigningKey;
use std::path::Path;
use walkdir::WalkDir;

pub fn build_manifest(
    name: &str,
    site_dir: &Path,
    keypair: &SigningKey,
    rating: &str,
    existing_version: u64,
) -> Result<SiteManifest> {
    let mut files = Vec::new();

    for entry in WalkDir::new(site_dir) {
        let entry = entry.with_context(|| format!("failed walking {}", site_dir.display()))?;
        if !entry.file_type().is_file() {
            continue;
        }

        let path = entry.path();
        if path.file_name().and_then(|n| n.to_str()) == Some("lattice.json") {
            continue;
        }

        let relative = path
            .strip_prefix(site_dir)
            .with_context(|| format!("failed to make relative path for {}", path.display()))?
            .to_string_lossy()
            .replace('\\', "/");

        let hash = hash_file(path)?;
        let size = entry
            .metadata()
            .with_context(|| format!("failed to read metadata for {}", path.display()))?
            .len();

        files.push(FileEntry {
            path: relative,
            hash,
            size,
        });
    }

    files.sort_by(|a, b| a.path.cmp(&b.path));

    let mut manifest = SiteManifest {
        name: name.to_string(),
        version: existing_version + 1,
        publisher_key: hex::encode(keypair.verifying_key().to_bytes()),
        rating: rating.to_string(),
        files,
        signature: String::new(),
    };

    sign_manifest(&mut manifest, keypair)?;
    Ok(manifest)
}

pub fn save_manifest(manifest: &SiteManifest, site_dir: &Path) -> Result<()> {
    let output_path = site_dir.join("lattice.json");
    let json =
        serde_json::to_string_pretty(manifest).context("failed to serialize lattice.json")?;
    std::fs::write(&output_path, json)
        .with_context(|| format!("failed to write {}", output_path.display()))?;
    Ok(())
}

pub fn load_manifest(site_dir: &Path) -> Result<SiteManifest> {
    let input_path = site_dir.join("lattice.json");
    let json = std::fs::read_to_string(&input_path)
        .with_context(|| format!("failed to read {}", input_path.display()))?;
    let manifest = serde_json::from_str(&json)
        .with_context(|| format!("failed to parse {}", input_path.display()))?;
    Ok(manifest)
}
