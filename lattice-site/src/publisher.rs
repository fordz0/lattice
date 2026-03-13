use crate::manifest::{
    hash_bytes, sign_manifest, AppManifest, FileEntry, SiteManifest, DEFAULT_CHUNK_SIZE_BYTES,
};
use anyhow::{anyhow, bail, Context, Result};
use ed25519_dalek::SigningKey;
use globset::{Glob, GlobSet, GlobSetBuilder};
use std::fs;
use std::path::{Component, Path};
use walkdir::WalkDir;

const MAX_SITE_FILES: usize = 1000;
const MAX_SITE_BYTES: u64 = 100 * 1024 * 1024;
const IGNORE_FILE_NAME: &str = ".latticeignore";

pub fn build_manifest(
    name: &str,
    site_dir: &Path,
    keypair: &SigningKey,
    rating: &str,
    app: Option<AppManifest>,
    existing_version: u64,
) -> Result<SiteManifest> {
    let mut files = Vec::new();
    let mut total_bytes: u64 = 0;
    let ignore_matcher = PublishIgnoreMatcher::load(site_dir)?;

    for entry in WalkDir::new(site_dir) {
        let entry = entry.with_context(|| format!("failed walking {}", site_dir.display()))?;
        if should_skip_path(site_dir, entry.path(), &ignore_matcher)? {
            continue;
        }
        if !entry.file_type().is_file() {
            continue;
        }

        let path = entry.path();
        let relative = path
            .strip_prefix(site_dir)
            .with_context(|| format!("failed to make relative path for {}", path.display()))?
            .to_string_lossy()
            .replace('\\', "/");

        let contents =
            std::fs::read(path).with_context(|| format!("failed to read {}", path.display()))?;
        let hash = hash_bytes(&contents);
        let size = contents.len() as u64;
        let chunks = chunk_hashes(&contents);
        total_bytes = total_bytes.saturating_add(size);
        if total_bytes > MAX_SITE_BYTES {
            bail!("site exceeds maximum total size of 100MB");
        }

        if files.len() >= MAX_SITE_FILES {
            bail!("site exceeds maximum file count of {MAX_SITE_FILES}");
        }
        files.push(FileEntry {
            path: relative,
            hash,
            size,
            chunk_size: Some(DEFAULT_CHUNK_SIZE_BYTES as u64),
            chunks,
        });
    }

    files.sort_by(|a, b| a.path.cmp(&b.path));

    let version = existing_version
        .checked_add(1)
        .ok_or_else(|| anyhow!("site version overflow"))?;

    let mut manifest = SiteManifest {
        name: name.to_string(),
        version,
        publisher_key: hex::encode(keypair.verifying_key().to_bytes()),
        rating: rating.to_string(),
        app,
        files,
        signature: String::new(),
    };

    sign_manifest(&mut manifest, keypair)?;
    Ok(manifest)
}

fn chunk_hashes(contents: &[u8]) -> Vec<String> {
    let mut hashes = Vec::new();
    for chunk in contents.chunks(DEFAULT_CHUNK_SIZE_BYTES) {
        hashes.push(hash_bytes(chunk));
    }
    if hashes.is_empty() {
        hashes.push(hash_bytes(&[]));
    }
    hashes
}

#[derive(Debug, Default)]
struct PublishIgnoreMatcher {
    globs: Option<GlobSet>,
}

impl PublishIgnoreMatcher {
    fn load(site_dir: &Path) -> Result<Self> {
        let path = site_dir.join(IGNORE_FILE_NAME);
        if !path.exists() {
            return Ok(Self::default());
        }

        let contents = fs::read_to_string(&path)
            .with_context(|| format!("failed to read {}", path.display()))?;
        let mut builder = GlobSetBuilder::new();
        let mut has_patterns = false;

        for raw_line in contents.lines() {
            let line = raw_line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }

            let normalized = line.replace('\\', "/");
            let candidates = normalized_ignore_patterns(&normalized);
            for candidate in candidates {
                builder.add(Glob::new(&candidate).with_context(|| {
                    format!("invalid pattern `{normalized}` in {IGNORE_FILE_NAME}")
                })?);
                has_patterns = true;
            }
        }

        if !has_patterns {
            return Ok(Self::default());
        }

        let globs = builder
            .build()
            .context("failed to build .latticeignore matcher")?;
        Ok(Self { globs: Some(globs) })
    }

    fn matches(&self, relative_path: &str) -> bool {
        self.globs
            .as_ref()
            .is_some_and(|globs| globs.is_match(relative_path))
    }
}

fn normalized_ignore_patterns(pattern: &str) -> Vec<String> {
    let trimmed = pattern.trim_start_matches("./");
    let directory_pattern = trimmed.strip_suffix('/').map(|value| format!("{value}/**"));

    let mut patterns = Vec::new();
    if let Some(directory_pattern) = directory_pattern {
        patterns.push(directory_pattern.clone());
        if !trimmed.contains('/') {
            patterns.push(format!("**/{directory_pattern}"));
        }
        return patterns;
    }

    patterns.push(trimmed.to_string());
    if !trimmed.contains('/') {
        patterns.push(format!("**/{trimmed}"));
    }
    patterns
}

fn should_skip_path(
    site_dir: &Path,
    path: &Path,
    ignore_matcher: &PublishIgnoreMatcher,
) -> Result<bool> {
    let relative = path
        .strip_prefix(site_dir)
        .with_context(|| format!("failed to make relative path for {}", path.display()))?;

    if relative.as_os_str().is_empty() {
        return Ok(false);
    }

    if relative.components().any(|component| {
        matches!(
            component,
            Component::Normal(name) if name == ".git"
        )
    }) {
        return Ok(true);
    }

    let relative_path = relative.to_string_lossy().replace('\\', "/");
    if ignore_matcher.matches(&relative_path) {
        return Ok(true);
    }

    let file_name = path.file_name().and_then(|name| name.to_str());
    Ok(matches!(
        file_name,
        Some("lattice.json" | ".DS_Store" | "Thumbs.db" | IGNORE_FILE_NAME)
    ))
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

#[cfg(test)]
mod tests {
    use super::{build_manifest, IGNORE_FILE_NAME};
    use ed25519_dalek::SigningKey;
    use std::fs;
    use tempfile::tempdir;

    #[test]
    fn build_manifest_skips_git_and_os_junk() {
        let dir = tempdir().expect("tempdir");
        fs::write(dir.path().join("index.html"), "<h1>ok</h1>").expect("write index");
        fs::write(dir.path().join(".DS_Store"), "junk").expect("write ds store");
        fs::create_dir_all(dir.path().join(".git")).expect("create git dir");
        fs::write(dir.path().join(".git").join("config"), "[core]").expect("write git config");

        let key = SigningKey::from_bytes(&[7; 32]);
        let manifest =
            build_manifest("lattice", dir.path(), &key, "general", None, 0).expect("manifest");

        let paths: Vec<&str> = manifest
            .files
            .iter()
            .map(|file| file.path.as_str())
            .collect();
        assert_eq!(paths, vec!["index.html"]);
    }

    #[test]
    fn build_manifest_keeps_well_known_content() {
        let dir = tempdir().expect("tempdir");
        fs::create_dir_all(dir.path().join(".well-known")).expect("create well-known dir");
        fs::write(dir.path().join(".well-known").join("assetlinks.json"), "{}")
            .expect("write assetlinks");

        let key = SigningKey::from_bytes(&[8; 32]);
        let manifest =
            build_manifest("lattice", dir.path(), &key, "general", None, 0).expect("manifest");

        let paths: Vec<&str> = manifest
            .files
            .iter()
            .map(|file| file.path.as_str())
            .collect();
        assert_eq!(paths, vec![".well-known/assetlinks.json"]);
    }

    #[test]
    fn build_manifest_respects_latticeignore() {
        let dir = tempdir().expect("tempdir");
        fs::create_dir_all(dir.path().join("build")).expect("create build dir");
        fs::create_dir_all(dir.path().join("nested/private")).expect("create nested private dir");
        fs::create_dir_all(dir.path().join("nested/public")).expect("create nested public dir");

        fs::write(dir.path().join("index.html"), "<h1>ok</h1>").expect("write index");
        fs::write(dir.path().join("secret.txt"), "hidden").expect("write secret");
        fs::write(dir.path().join("build").join("bundle.js"), "ignored").expect("write bundle");
        fs::write(dir.path().join("notes.bak"), "ignored").expect("write notes");
        fs::write(dir.path().join("nested/private").join("key.txt"), "ignored")
            .expect("write private");
        fs::write(dir.path().join("nested/public").join("post.txt"), "kept").expect("write public");
        fs::write(
            dir.path().join(IGNORE_FILE_NAME),
            "secret.txt\nbuild/\n*.bak\nnested/private/**\n",
        )
        .expect("write ignore file");

        let key = SigningKey::from_bytes(&[9; 32]);
        let manifest =
            build_manifest("lattice", dir.path(), &key, "general", None, 0).expect("manifest");

        let paths: Vec<&str> = manifest
            .files
            .iter()
            .map(|file| file.path.as_str())
            .collect();
        assert_eq!(paths, vec!["index.html", "nested/public/post.txt"]);
    }
}
