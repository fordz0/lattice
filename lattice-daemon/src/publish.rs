use anyhow::{bail, Context, Result};
use ed25519_dalek::SigningKey;
use lattice_core::moderation::ModerationEngine;
use libp2p::kad;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use tokio::sync::oneshot;
use tracing::warn;

use crate::cache::CachePolicy;
use crate::mime;
use crate::rpc::PublishSiteOk;
use crate::site_helpers::{
    file_block_hashes, maybe_put_record, remember_local_record, start_providing_site,
};
use crate::store::LocalRecordStore;
use lattice_site::manifest::{hash_bytes, verify_manifest, DEFAULT_CHUNK_SIZE_BYTES};
use lattice_site::publisher as site_publisher;

pub struct PublishTask {
    pub respond_to: oneshot::Sender<Result<PublishSiteOk, String>>,
    pub remaining: u32,
    pub failed: Option<String>,
    pub had_quorum_failed: bool,
    pub version: u64,
    pub file_count: usize,
    pub claimed: bool,
    pub connected_peers_at_start: usize,
    pub manifest_record: Option<(String, Vec<u8>)>,
}

pub struct PublishQuery {
    pub task_id: u64,
}

pub struct PreparedPublish {
    pub version: u64,
    pub file_count: usize,
    pub blocks: Vec<(String, Vec<u8>)>,
    pub manifest_record: (String, Vec<u8>),
}

pub fn validate_site_dir(site_dir: &str) -> Result<PathBuf, String> {
    let path = Path::new(site_dir);
    if !path.is_absolute() {
        return Err("site_dir must be an absolute path".to_string());
    }
    let canonical = path
        .canonicalize()
        .map_err(|e| format!("invalid site_dir: {e}"))?;
    if !canonical.is_dir() {
        return Err("site_dir must be a directory".to_string());
    }
    Ok(canonical)
}

pub fn validate_site_file_mime_policy(
    path: &str,
    contents: &[u8],
    mime_policy_strict: bool,
) -> Result<()> {
    let detected_mime = mime::detect_mime(path, contents);
    if let Some(reason) = mime::violation_reason(&detected_mime, contents.len()) {
        tracing::warn!(
            filename = %path,
            detected_mime = %detected_mime,
            file_size = contents.len(),
            reason = %reason,
            "MIME policy violation while publishing site file"
        );
        if mime_policy_strict {
            bail!(
                "rejected: {path} (detected: {detected_mime}, size: {} bytes, reason: {reason})",
                contents.len()
            );
        }
    }
    Ok(())
}

pub fn prepare_publish(
    name: &str,
    site_dir: &Path,
    signing_key: &SigningKey,
    dht_baseline_version: u64,
    mime_policy_strict: bool,
) -> Result<PreparedPublish> {
    let (local_existing_version, rating, app) = match site_publisher::load_manifest(site_dir) {
        Ok(existing) => {
            let rating = if existing.rating.is_empty() {
                "general".to_string()
            } else {
                existing.rating
            };
            (existing.version, rating, existing.app)
        }
        Err(_) => (0, "general".to_string(), None),
    };

    let existing_version = local_existing_version.max(dht_baseline_version);
    let manifest = site_publisher::build_manifest(
        name,
        site_dir,
        signing_key,
        &rating,
        app,
        existing_version,
    )?;
    verify_manifest(&manifest)?;
    site_publisher::save_manifest(&manifest, site_dir)?;

    let mut blocks = Vec::new();
    let mut seen_block_hashes: HashSet<String> = HashSet::new();
    for file in &manifest.files {
        let file_path = site_dir.join(&file.path);
        let contents = fs::read(&file_path)
            .with_context(|| format!("failed to read site file {}", file_path.display()))?;
        validate_site_file_mime_policy(&file.path, &contents, mime_policy_strict)?;

        let actual_hash = hash_bytes(&contents);
        if actual_hash != file.hash {
            bail!(
                "hash mismatch for {}: manifest={}, actual={}",
                file.path,
                file.hash,
                actual_hash
            );
        }

        let block_hashes = file_block_hashes(file);
        if block_hashes.len() == 1 {
            let block_hash = block_hashes[0].clone();
            let actual_block_hash = hash_bytes(&contents);
            if actual_block_hash != block_hash {
                bail!(
                    "block hash mismatch for {}: manifest={}, actual={}",
                    file.path,
                    block_hash,
                    actual_block_hash
                );
            }
            if seen_block_hashes.insert(block_hash.clone()) {
                blocks.push((block_hash, contents));
            }
            continue;
        }

        let chunk_size = file
            .chunk_size
            .and_then(|v| usize::try_from(v).ok())
            .unwrap_or(DEFAULT_CHUNK_SIZE_BYTES);
        if chunk_size == 0 {
            bail!("invalid chunk_size for {}", file.path);
        }
        let chunks: Vec<&[u8]> = contents.chunks(chunk_size).collect();
        if chunks.len() != block_hashes.len() {
            bail!(
                "chunk count mismatch for {}: manifest={}, actual={}",
                file.path,
                block_hashes.len(),
                chunks.len()
            );
        }

        for (i, chunk_hash) in block_hashes.iter().enumerate() {
            let chunk = chunks[i];
            let actual_chunk_hash = hash_bytes(chunk);
            if actual_chunk_hash != *chunk_hash {
                bail!(
                    "chunk hash mismatch for {} chunk {}: manifest={}, actual={}",
                    file.path,
                    i,
                    chunk_hash,
                    actual_chunk_hash
                );
            }
            if seen_block_hashes.insert(chunk_hash.clone()) {
                blocks.push((chunk_hash.clone(), chunk.to_vec()));
            }
        }
    }

    let manifest_json =
        serde_json::to_string(&manifest).context("failed to serialize site manifest")?;
    let manifest_record = (format!("site:{name}"), manifest_json.into_bytes());

    Ok(PreparedPublish {
        version: manifest.version,
        file_count: manifest.files.len(),
        blocks,
        manifest_record,
    })
}

#[allow(clippy::too_many_arguments)]
pub fn start_publish_task(
    kademlia: &mut kad::Behaviour<kad::store::MemoryStore>,
    connected_peers_at_start: usize,
    prepared: PreparedPublish,
    site_name: &str,
    claimed: bool,
    respond_to: oneshot::Sender<Result<PublishSiteOk, String>>,
    publish_tasks: &mut HashMap<u64, PublishTask>,
    publish_query_to_task: &mut HashMap<kad::QueryId, PublishQuery>,
    next_publish_task_id: &mut u64,
    moderation_engine: &ModerationEngine,
    local_record_store: &LocalRecordStore,
    local_records: &mut HashMap<String, Vec<u8>>,
) {
    let task_id = *next_publish_task_id;
    *next_publish_task_id = (*next_publish_task_id).saturating_add(1);

    remember_local_record(
        local_record_store,
        local_records,
        prepared.manifest_record.0.clone(),
        prepared.manifest_record.1.clone(),
    );

    let mut task = PublishTask {
        respond_to,
        remaining: 0,
        failed: None,
        had_quorum_failed: false,
        version: prepared.version,
        file_count: prepared.file_count,
        claimed,
        connected_peers_at_start,
        manifest_record: Some(prepared.manifest_record),
    };

    if task.connected_peers_at_start == 0 {
        warn!(
            "publishing with no connected peers; records are local-only until peers connect and replication occurs"
        );
    }

    for (hash, value) in prepared.blocks {
        if let Err(err) =
            local_record_store.put_block(&hash, &value, site_name, CachePolicy::Pinned)
        {
            if task.failed.is_none() {
                task.failed = Some(err.to_string());
            }
        }
    }

    if let Err(err) = start_providing_site(kademlia, moderation_engine, site_name) {
        if task.failed.is_none() {
            task.failed = Some(err.to_string());
        }
    }

    if let Some(err) = task.failed {
        let _ = task.respond_to.send(Err(err));
        return;
    }

    let Some((manifest_key, manifest_value)) = task.manifest_record.take() else {
        let _ = task.respond_to.send(Err(
            "internal publish error: missing manifest record".to_string()
        ));
        return;
    };

    match maybe_put_record(kademlia, moderation_engine, manifest_key, manifest_value) {
        Ok(Some(query_id)) => {
            task.remaining = 1;
            publish_query_to_task.insert(query_id, PublishQuery { task_id });
            publish_tasks.insert(task_id, task);
        }
        Ok(None) => {
            let _ = task
                .respond_to
                .send(Err("publish blocked by moderation rule".to_string()));
        }
        Err(err) => {
            let _ = task.respond_to.send(Err(err.to_string()));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::prepare_publish;
    use ed25519_dalek::SigningKey;
    use lattice_site::manifest::{AppManifest, SiteManifest};
    use lattice_site::publisher as site_publisher;
    use std::fs;
    use tempfile::tempdir;

    #[test]
    fn prepare_publish_preserves_existing_app_block() {
        let site_dir = tempdir().expect("tempdir");
        let index_path = site_dir.path().join("index.html");
        fs::write(&index_path, "<!doctype html><title>fray</title>").expect("write index");

        let app = AppManifest {
            proxy_port: 8890,
            proxy_paths: vec!["/api".to_string()],
        };
        let manifest = SiteManifest {
            name: "fray".to_string(),
            version: 3,
            publisher_key: String::new(),
            rating: "general".to_string(),
            app: Some(app.clone()),
            files: Vec::new(),
            signature: String::new(),
        };
        site_publisher::save_manifest(&manifest, site_dir.path()).expect("save manifest");

        let signing_key = SigningKey::from_bytes(&[7_u8; 32]);
        let prepared = prepare_publish("fray", site_dir.path(), &signing_key, 0, false)
            .expect("prepare publish");
        let manifest_json =
            String::from_utf8(prepared.manifest_record.1).expect("manifest record utf8");
        let published: SiteManifest =
            serde_json::from_str(&manifest_json).expect("parse published manifest");

        assert_eq!(published.app, Some(app));
    }
}
