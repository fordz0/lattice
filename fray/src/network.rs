use crate::model::{Comment, Post};
use anyhow::{anyhow, bail, Context, Result};
use ed25519_dalek::SigningKey;
use lattice_core::identity::{canonical_json_bytes, SignedRecord};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::time::Duration;

const FRAY_FEED_VERSION: u8 = 1;
const MAX_FEED_BYTES: usize = 256 * 1024;
const MAX_FEED_POSTS: usize = 64;
const MAX_FEED_COMMENTS: usize = 256;

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
    let payload =
        serde_json::to_string(&signed_record).context("failed to encode signed fray feed record")?;
    if payload.len() > MAX_FEED_BYTES {
        bail!("fray feed exceeds maximum payload size");
    }

    let key = format!("app:fray:feed:{fray}");
    let result = rpc_call(
        rpc_port,
        "put_record",
        json!({
            "key": key,
            "value": payload,
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

pub async fn fetch_feed(rpc_port: u16, fray: &str) -> Result<Option<SignedFrayFeedRecord>> {
    let key = format!("app:fray:feed:{fray}");
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
        .ok_or_else(|| anyhow!("fray feed record is not a string"))?;
    if raw.len() > MAX_FEED_BYTES {
        bail!("fray feed payload is too large");
    }

    let signed: SignedRecord =
        serde_json::from_str(raw).context("failed to decode signed fray feed record")?;
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

async fn rpc_call(rpc_port: u16, method: &str, params: Value) -> Result<Value> {
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
