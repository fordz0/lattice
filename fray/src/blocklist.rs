use anyhow::{anyhow, Context, Result};
use std::collections::HashSet;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::Path;
use std::sync::{Arc, Mutex};

#[derive(Clone, Default)]
pub struct ContentBlocklist {
    inner: Arc<Mutex<HashSet<String>>>,
}

impl ContentBlocklist {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add(&self, hash_hex: &str) -> Result<()> {
        validate_hash(hash_hex)?;
        let mut guard = self
            .inner
            .lock()
            .map_err(|_| anyhow!("blocklist mutex poisoned"))?;
        guard.insert(hash_hex.to_ascii_lowercase());
        Ok(())
    }

    pub fn contains(&self, hash_hex: &str) -> bool {
        self.inner
            .lock()
            .map(|guard| guard.contains(&hash_hex.to_ascii_lowercase()))
            .unwrap_or(false)
    }

    pub fn load_from_file(path: &Path) -> Result<Self> {
        if !path.exists() {
            return Ok(Self::new());
        }
        let out = Self::new();
        let contents = fs::read_to_string(path)
            .with_context(|| format!("failed to read blocklist {}", path.display()))?;
        for line in contents.lines() {
            let trimmed = line.trim();
            if trimmed.is_empty() || trimmed.starts_with('#') {
                continue;
            }
            out.add(trimmed)?;
        }
        Ok(out)
    }

    pub fn append_to_file(&self, path: &Path, hash_hex: &str) -> Result<()> {
        self.add(hash_hex)?;
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .with_context(|| format!("failed to open blocklist file {}", path.display()))?;
        writeln!(file, "{}", hash_hex.to_ascii_lowercase())
            .with_context(|| format!("failed to append blocklist file {}", path.display()))?;
        Ok(())
    }
}

fn validate_hash(hash_hex: &str) -> Result<()> {
    let value = hash_hex.trim();
    if value.len() != 64 {
        return Err(anyhow!("blocklist hash must be 64 hex characters"));
    }
    if !value.chars().all(|c| c.is_ascii_hexdigit()) {
        return Err(anyhow!("blocklist hash must be valid hex"));
    }
    Ok(())
}
