use crate::blocklist::ContentBlocklist;
use crate::directory::{
    sign_directory, validate_directory, verify_signed_directory, FrayDirectory, SignedFrayDirectory,
};
use crate::handle::{validate_handle_record, FrayHandleRecord};
use crate::model::{Comment, Post};
use crate::store::FrayStore;
use crate::trust::{
    sign_trust_record, validate_trust_record, verify_signed_trust_record, FrayTrustRecord,
    KeyStanding, SignedTrustRecord,
};
use anyhow::{anyhow, bail, Context, Result};
use blake3::Hasher;
use ed25519_dalek::SigningKey;
use lattice_core::identity::{canonical_json_bytes, SignedRecord};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::time::Duration;

const FRAY_FEED_VERSION: u8 = 1;
const MAX_FEED_BYTES: usize = 256 * 1024;
const MAX_FEED_POSTS: usize = 64;
const MAX_FEED_COMMENTS: usize = 256;
const TRUST_RECORD_KEY_PREFIX: &str = "app:fray:trust:";
const HANDLE_RECORD_KEY_PREFIX: &str = "app:fray:identity:";
const DIRECTORY_RECORD_KEY: &str = "app:fray:directory";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FrayFeedRecord {
    // Field order is canonical — do not reorder.
    pub version: u8,
    pub fray: String,
    pub generated_at: u64,
    pub posts: Vec<Post>,
    pub comments: Vec<Comment>,
}

pub struct SignedFrayFeedRecord {
    pub signed: SignedRecord,
    pub feed: FrayFeedRecord,
}

pub struct SignedFrayHandleRecord {
    pub signed: SignedRecord,
    pub record: FrayHandleRecord,
}

#[derive(Debug, Deserialize)]
struct GetSiteManifestResult {
    trust: SiteTrustState,
    pinned: bool,
}

#[derive(Debug, Deserialize)]
struct SiteTrustState {
    status: String,
    explicitly_trusted: bool,
}

pub async fn publish_feed(
    rpc_port: u16,
    signing_key: &SigningKey,
    fray: &str,
    posts: Vec<Post>,
    comments: Vec<Comment>,
    generated_at: u64,
) -> Result<()> {
    let record = FrayFeedRecord {
        version: FRAY_FEED_VERSION,
        fray: fray.to_string(),
        generated_at,
        posts,
        comments,
    };
    validate_feed_record(&record)?;
    let canonical_payload =
        canonical_json_bytes(&record).context("failed to canonicalize fray feed record")?;
    let signed_record = SignedRecord::sign(signing_key, canonical_payload);
    let payload = serde_json::to_string(&signed_record)
        .context("failed to encode signed fray feed record")?;
    if payload.len() > MAX_FEED_BYTES {
        bail!("fray feed exceeds maximum payload size");
    }

    put_record(rpc_port, &format!("app:fray:feed:{fray}"), payload).await
}

pub async fn fetch_feed(rpc_port: u16, fray: &str) -> Result<Option<SignedFrayFeedRecord>> {
    let key = format!("app:fray:feed:{fray}");
    let Some(raw) = get_record(rpc_port, &key).await? else {
        return Ok(None);
    };
    if raw.len() > MAX_FEED_BYTES {
        bail!("fray feed payload is too large");
    }

    let signed: SignedRecord =
        serde_json::from_str(&raw).context("failed to decode signed fray feed record")?;
    let decoded: FrayFeedRecord = signed
        .payload_json()
        .context("failed to decode fray feed payload")?;
    if decoded.version != FRAY_FEED_VERSION {
        bail!("unsupported fray feed version");
    }
    if decoded.fray != fray {
        bail!("fray feed record mismatch");
    }
    validate_feed_record(&decoded)?;
    Ok(Some(SignedFrayFeedRecord {
        signed,
        feed: decoded,
    }))
}

pub async fn check_frayloom_stake(lattice_rpc_port: u16) -> Result<bool> {
    let known = match rpc_call(
        lattice_rpc_port,
        "known_publisher_status",
        json!({ "name": "fray" }),
    )
    .await
    {
        Ok(value) => value,
        Err(_) => return Ok(false),
    };
    if known.is_null() {
        return Ok(false);
    }
    let explicitly_trusted = known
        .get("explicitly_trusted")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    if !explicitly_trusted {
        return Ok(false);
    }

    let manifest = match rpc_call(
        lattice_rpc_port,
        "get_site_manifest",
        json!({ "name": "fray" }),
    )
    .await
    {
        Ok(value) => value,
        Err(_) => return Ok(false),
    };
    if manifest.is_null() {
        return Ok(false);
    }
    let manifest: GetSiteManifestResult = match serde_json::from_value(manifest) {
        Ok(value) => value,
        Err(_) => return Ok(false),
    };
    if manifest.trust.status != "matches" || !manifest.trust.explicitly_trusted {
        return Ok(false);
    }
    Ok(manifest.pinned)
}

pub async fn publish_trust_record(
    fray: &str,
    record: &FrayTrustRecord,
    signing_key: &SigningKey,
    lattice_rpc_port: u16,
) -> Result<()> {
    validate_trust_record(record).map_err(anyhow::Error::msg)?;
    let signed = sign_trust_record(record, signing_key)?;
    let payload = serde_json::to_string(&signed).context("failed to encode trust record")?;
    put_record(
        lattice_rpc_port,
        &format!("{TRUST_RECORD_KEY_PREFIX}{fray}"),
        payload,
    )
    .await
}

pub async fn publish_handle_record(
    handle: &str,
    record: &FrayHandleRecord,
    signing_key: &SigningKey,
    lattice_rpc_port: u16,
) -> Result<()> {
    validate_handle_record(record).map_err(anyhow::Error::msg)?;
    let payload = canonical_json_bytes(record).context("failed to encode handle record")?;
    let signed = SignedRecord::sign(signing_key, payload);
    let value = serde_json::to_string(&signed).context("failed to encode signed handle record")?;
    put_record(
        lattice_rpc_port,
        &format!("{HANDLE_RECORD_KEY_PREFIX}{handle}"),
        value,
    )
    .await
}

pub async fn fetch_handle_record(
    lattice_rpc_port: u16,
    handle: &str,
) -> Result<Option<SignedFrayHandleRecord>> {
    let Some(raw) = get_record(
        lattice_rpc_port,
        &format!("{HANDLE_RECORD_KEY_PREFIX}{handle}"),
    )
    .await?
    else {
        return Ok(None);
    };
    let signed: SignedRecord =
        serde_json::from_str(&raw).context("failed to decode signed handle record")?;
    if !signed.verify() {
        bail!("handle record signature verification failed");
    }
    let record: FrayHandleRecord = signed
        .payload_json()
        .context("failed to decode fray handle payload")?;
    validate_handle_record(&record).map_err(anyhow::Error::msg)?;
    if record.handle != handle {
        bail!("handle record mismatch");
    }
    Ok(Some(SignedFrayHandleRecord { signed, record }))
}

pub async fn publisher_owns_handle(
    lattice_rpc_port: u16,
    handle: &str,
    key_b64: &str,
) -> Result<bool> {
    let Some(record) = fetch_handle_record(lattice_rpc_port, handle).await? else {
        return Ok(false);
    };
    if record.record.claimed_at == 0 {
        return Ok(false);
    }
    Ok(record.signed.publisher_b64() == key_b64)
}

pub async fn fetch_trust_record(
    fray: &str,
    lattice_rpc_port: u16,
) -> Result<Option<SignedTrustRecord>> {
    let key = format!("{TRUST_RECORD_KEY_PREFIX}{fray}");
    let Some(raw) = get_record(lattice_rpc_port, &key).await? else {
        return Ok(None);
    };
    let signed: SignedTrustRecord =
        serde_json::from_str(&raw).context("failed to decode signed trust record")?;
    verify_signed_trust_record(&signed)?;
    Ok(Some(signed))
}

pub async fn publish_directory(
    dir: &FrayDirectory,
    signing_key: &SigningKey,
    lattice_rpc_port: u16,
) -> Result<()> {
    validate_directory(dir).map_err(anyhow::Error::msg)?;
    let signed = sign_directory(dir, signing_key)?;
    let payload = serde_json::to_string(&signed).context("failed to encode directory")?;
    put_record(lattice_rpc_port, DIRECTORY_RECORD_KEY, payload).await
}

pub async fn fetch_directory(lattice_rpc_port: u16) -> Result<Option<SignedFrayDirectory>> {
    let Some(raw) = get_record(lattice_rpc_port, DIRECTORY_RECORD_KEY).await? else {
        return Ok(None);
    };
    let signed: SignedFrayDirectory =
        serde_json::from_str(&raw).context("failed to decode fray directory")?;
    verify_signed_directory(&signed)?;
    Ok(Some(signed))
}

pub async fn moderation_check_many(
    rpc_port: u16,
    checks: Vec<(String, String)>,
) -> Result<Option<String>> {
    let checks = checks
        .into_iter()
        .map(|(kind, value)| json!({ "kind": kind, "value": value }))
        .collect::<Vec<_>>();
    let result = rpc_call(
        rpc_port,
        "mod_check_many",
        json!({
            "checks": checks,
        }),
    )
    .await?;
    if result.is_null() {
        return Ok(None);
    }
    let action = result
        .as_str()
        .ok_or_else(|| anyhow!("mod_check_many response was not a string"))?;
    Ok(Some(action.to_string()))
}

pub fn import_trust_record(store: &FrayStore, signed: &SignedTrustRecord) -> Result<()> {
    verify_signed_trust_record(signed)?;
    store.store_trust_record(&signed.record.fray, signed)?;
    for entry in &signed.record.entries {
        store.store_key_record(&signed.record.fray, entry.clone())?;
    }
    Ok(())
}

pub fn standing_map(record: Option<&SignedTrustRecord>) -> HashMap<String, KeyStanding> {
    let mut out = HashMap::new();
    if let Some(record) = record {
        for entry in &record.record.entries {
            out.insert(entry.key_b64.clone(), entry.standing.clone());
        }
    }
    out
}

pub fn standing_hides_publisher(
    standings: &HashMap<String, KeyStanding>,
    publisher_b64: &str,
) -> bool {
    matches!(
        standings.get(publisher_b64),
        Some(KeyStanding::Restricted { .. })
    )
}

pub fn content_hash_hex(body: &str) -> String {
    let mut hasher = Hasher::new();
    hasher.update(body.as_bytes());
    hasher.finalize().to_hex().to_string()
}

pub fn post_should_drop(blocklist: &ContentBlocklist, post: &Post) -> bool {
    blocklist.contains(&content_hash_hex(&post.body))
}

pub fn comment_should_drop(blocklist: &ContentBlocklist, comment: &Comment) -> bool {
    blocklist.contains(&content_hash_hex(&comment.body))
}

fn validate_feed_record(feed: &FrayFeedRecord) -> Result<()> {
    if feed.posts.len() > MAX_FEED_POSTS {
        bail!("too many posts in feed");
    }
    if feed.comments.len() > MAX_FEED_COMMENTS {
        bail!("too many comments in feed");
    }
    for post in &feed.posts {
        if post.fray != feed.fray {
            bail!("post fray mismatch in feed");
        }
    }
    for comment in &feed.comments {
        if comment.fray != feed.fray {
            bail!("comment fray mismatch in feed");
        }
    }
    Ok(())
}

pub async fn put_record(rpc_port: u16, key: &str, value: String) -> Result<()> {
    let result = rpc_call(
        rpc_port,
        "put_record",
        json!({
            "key": key,
            "value": value,
        }),
    )
    .await?;
    let status = result
        .get("status")
        .and_then(Value::as_str)
        .unwrap_or("err");
    if status != "ok" {
        let reason = result
            .get("error")
            .and_then(Value::as_str)
            .unwrap_or("put_record failed");
        bail!("{reason}");
    }
    Ok(())
}

pub async fn get_record(rpc_port: u16, key: &str) -> Result<Option<String>> {
    let result = rpc_call(
        rpc_port,
        "get_record",
        json!({
            "key": key,
        }),
    )
    .await?;
    if result.is_null() {
        return Ok(None);
    }
    let raw = result
        .as_str()
        .ok_or_else(|| anyhow!("record is not a string"))?;
    Ok(Some(raw.to_string()))
}

pub async fn rpc_call(rpc_port: u16, method: &str, params: Value) -> Result<Value> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .context("failed to build fray RPC client")?;
    let body = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": method,
        "params": params,
    });
    let response = client
        .post(format!("http://127.0.0.1:{rpc_port}"))
        .json(&body)
        .send()
        .await
        .with_context(|| format!("failed to reach lattice daemon RPC on {rpc_port}"))?;
    let envelope: Value = response
        .json()
        .await
        .context("failed to decode RPC response")?;
    if let Some(error) = envelope.get("error") {
        bail!("rpc error: {}", error);
    }
    envelope
        .get("result")
        .cloned()
        .ok_or_else(|| anyhow!("rpc result missing"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::trust::{unix_ts, FrayTrustRecord};
    use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
    use base64::Engine as _;

    fn sample_key(seed: u8) -> SigningKey {
        SigningKey::from_bytes(&[seed; 32])
    }

    #[tokio::test]
    async fn stake_check_returns_false_when_rpc_is_unreachable() {
        assert!(!check_frayloom_stake(9).await.expect("stake check"));
    }

    #[test]
    fn restricted_posts_are_hidden_after_trust_import() {
        let owner = sample_key(1);
        let restricted = BASE64_STANDARD.encode(sample_key(2).verifying_key().as_bytes());
        let signed = sign_trust_record(
            &FrayTrustRecord {
                version: 1,
                fray: "lattice".into(),
                owner_key_b64: BASE64_STANDARD.encode(owner.verifying_key().as_bytes()),
                moderator_keys: Vec::new(),
                entries: vec![crate::trust::KeyRecord {
                    key_b64: restricted.clone(),
                    standing: KeyStanding::Restricted { reason: None },
                    label: None,
                    updated_at: unix_ts(),
                }],
                generated_at: unix_ts(),
            },
            &owner,
        )
        .expect("sign trust");
        let standings = standing_map(Some(&signed));
        assert!(standing_hides_publisher(&standings, &restricted));
    }

    #[test]
    fn trusted_posts_are_not_hidden_by_trust_record_alone() {
        let trusted = BASE64_STANDARD.encode(sample_key(3).verifying_key().as_bytes());
        let signed = sign_trust_record(
            &FrayTrustRecord {
                version: 1,
                fray: "lattice".into(),
                owner_key_b64: BASE64_STANDARD.encode(sample_key(1).verifying_key().as_bytes()),
                moderator_keys: Vec::new(),
                entries: vec![crate::trust::KeyRecord {
                    key_b64: trusted.clone(),
                    standing: KeyStanding::Trusted,
                    label: None,
                    updated_at: unix_ts(),
                }],
                generated_at: unix_ts(),
            },
            &sample_key(1),
        )
        .expect("sign");
        let standings = standing_map(Some(&signed));
        let standing = standings.get(&trusted).cloned();
        assert_eq!(standing, Some(KeyStanding::Trusted));
        assert!(!standing_hides_publisher(&standings, &trusted));
    }

    #[test]
    fn blocklist_drops_matching_post_bodies() {
        let blocklist = ContentBlocklist::new();
        let body = "blocked body";
        let hash = content_hash_hex(body);
        blocklist.add(&hash).expect("add blocklist");
        let post = Post {
            id: "abc123-abcdef".into(),
            fray: "lattice".into(),
            author: "fordz0".into(),
            title: "hello".into(),
            body: body.into(),
            created_at: unix_ts(),
            key_b64: None,
            signature_b64: None,
            hidden: false,
        };
        assert!(post_should_drop(&blocklist, &post));
    }
}
