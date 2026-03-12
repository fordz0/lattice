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

pub struct SignedFrayTrustRecord {
    pub signed: SignedRecord,
    pub record: SignedTrustRecord,
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
    let payload = encode_trust_record_value(signing_key, &signed)?;
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
) -> Result<Option<SignedFrayTrustRecord>> {
    let key = format!("{TRUST_RECORD_KEY_PREFIX}{fray}");
    let Some(raw) = get_record(lattice_rpc_port, &key).await? else {
        return Ok(None);
    };
    Ok(Some(decode_trust_record_value(fray, &raw)?))
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
    store.replace_key_records(&signed.record.fray, &signed.record.entries)?;
    Ok(())
}

fn encode_trust_record_value(
    signing_key: &SigningKey,
    record: &SignedTrustRecord,
) -> Result<String> {
    let payload =
        canonical_json_bytes(record).context("failed to canonicalize signed trust record")?;
    let signed = SignedRecord::sign(signing_key, payload);
    serde_json::to_string(&signed).context("failed to encode wrapped trust record")
}

fn decode_trust_record_value(fray: &str, raw: &str) -> Result<SignedFrayTrustRecord> {
    let signed: SignedRecord =
        serde_json::from_str(raw).context("failed to decode wrapped trust record")?;
    if !signed.verify() {
        bail!("trust record wrapper signature verification failed");
    }
    let record: SignedTrustRecord = signed
        .payload_json()
        .context("failed to decode signed trust payload")?;
    verify_signed_trust_record(&record)?;
    if record.record.fray != fray {
        bail!("trust record mismatch");
    }
    if signed.publisher_b64() != record.record.owner_key_b64 {
        bail!("trust record owner does not match wrapper signer");
    }
    Ok(SignedFrayTrustRecord { signed, record })
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
    use axum::extract::State;
    use axum::routing::post;
    use axum::{Json, Router};
    use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
    use base64::Engine as _;
    use serde_json::Value;
    use std::collections::HashMap;
    use std::sync::Arc;
    use tokio::sync::{oneshot, Mutex};

    fn sample_key(seed: u8) -> SigningKey {
        SigningKey::from_bytes(&[seed; 32])
    }

    fn sample_trust_record() -> FrayTrustRecord {
        FrayTrustRecord {
            version: 1,
            fray: "lattice".into(),
            owner_key_b64: BASE64_STANDARD.encode(sample_key(1).verifying_key().as_bytes()),
            moderator_keys: Vec::new(),
            entries: vec![crate::trust::KeyRecord {
                key_b64: BASE64_STANDARD.encode(sample_key(2).verifying_key().as_bytes()),
                standing: KeyStanding::Trusted,
                label: Some("peer".into()),
                updated_at: unix_ts(),
            }],
            generated_at: unix_ts(),
        }
    }

    async fn spawn_mock_rpc_server() -> (
        u16,
        Arc<Mutex<HashMap<String, String>>>,
        oneshot::Sender<()>,
    ) {
        let records = Arc::new(Mutex::new(HashMap::new()));
        let app = Router::new()
            .route("/", post(mock_rpc))
            .with_state(records.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let port = listener.local_addr().expect("local addr").port();
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        tokio::spawn(async move {
            axum::serve(listener, app)
                .with_graceful_shutdown(async move {
                    let _ = shutdown_rx.await;
                })
                .await
                .expect("serve mock rpc");
        });
        (port, records, shutdown_tx)
    }

    async fn mock_rpc(
        State(records): State<Arc<Mutex<HashMap<String, String>>>>,
        Json(request): Json<Value>,
    ) -> Json<Value> {
        let method = request
            .get("method")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let params = request.get("params").cloned().unwrap_or(Value::Null);
        let result = match method {
            "put_record" => {
                let key = params
                    .get("key")
                    .and_then(Value::as_str)
                    .expect("put_record key");
                let value = params
                    .get("value")
                    .and_then(Value::as_str)
                    .expect("put_record value");
                records
                    .lock()
                    .await
                    .insert(key.to_string(), value.to_string());
                json!({ "status": "ok" })
            }
            "get_record" => {
                let key = params
                    .get("key")
                    .and_then(Value::as_str)
                    .expect("get_record key");
                records
                    .lock()
                    .await
                    .get(key)
                    .map(|value| Value::String(value.clone()))
                    .unwrap_or(Value::Null)
            }
            _ => Value::Null,
        };
        Json(json!({
            "jsonrpc": "2.0",
            "id": request.get("id").cloned().unwrap_or(json!(1)),
            "result": result,
        }))
    }

    #[tokio::test]
    async fn stake_check_returns_false_when_rpc_is_unreachable() {
        assert!(!check_frayloom_stake(9).await.expect("stake check"));
    }

    #[tokio::test]
    async fn publish_trust_record_wraps_signed_record_payload() {
        let (port, records, shutdown_tx) = spawn_mock_rpc_server().await;
        let owner = sample_key(1);
        let record = sample_trust_record();
        publish_trust_record("lattice", &record, &owner, port)
            .await
            .expect("publish trust record");

        let stored = records
            .lock()
            .await
            .get("app:fray:trust:lattice")
            .cloned()
            .expect("stored trust record");
        let outer: SignedRecord =
            serde_json::from_str(&stored).expect("decode outer signed record");
        assert!(outer.verify());
        let inner: SignedTrustRecord = outer.payload_json().expect("decode inner trust record");
        verify_signed_trust_record(&inner).expect("verify inner trust record");
        assert_eq!(inner.record, record);

        let _ = shutdown_tx.send(());
    }

    #[tokio::test]
    async fn fetch_trust_record_roundtrips_wrapped_record() {
        let (port, _records, shutdown_tx) = spawn_mock_rpc_server().await;
        let owner = sample_key(1);
        let record = sample_trust_record();
        publish_trust_record("lattice", &record, &owner, port)
            .await
            .expect("publish trust record");

        let fetched = fetch_trust_record("lattice", port)
            .await
            .expect("fetch trust record")
            .expect("wrapped trust record");
        assert!(fetched.signed.verify());
        verify_signed_trust_record(&fetched.record).expect("verify signed trust record");
        assert_eq!(fetched.record.record, record);

        let _ = shutdown_tx.send(());
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
            author: "alice".into(),
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
