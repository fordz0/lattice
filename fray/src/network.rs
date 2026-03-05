use crate::model::Post;
use anyhow::{anyhow, bail, Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

const FRAY_FEED_VERSION: u8 = 1;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FrayFeedRecord {
    pub version: u8,
    pub fray: String,
    pub generated_at: u64,
    pub posts: Vec<Post>,
}

pub async fn publish_feed(
    rpc_port: u16,
    fray: &str,
    posts: Vec<Post>,
    generated_at: u64,
) -> Result<()> {
    let record = FrayFeedRecord {
        version: FRAY_FEED_VERSION,
        fray: fray.to_string(),
        generated_at,
        posts,
    };
    let payload = serde_json::to_string(&record).context("failed to encode fray feed record")?;
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

pub async fn fetch_feed(rpc_port: u16, fray: &str) -> Result<Option<FrayFeedRecord>> {
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
    let decoded: FrayFeedRecord =
        serde_json::from_str(raw).context("failed to decode fray feed record")?;
    if decoded.version != FRAY_FEED_VERSION {
        bail!("unsupported fray feed version");
    }
    if decoded.fray != fray {
        bail!("fray feed record mismatch");
    }
    Ok(Some(decoded))
}

async fn rpc_call(rpc_port: u16, method: &str, params: Value) -> Result<Value> {
    let client = reqwest::Client::new();
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
