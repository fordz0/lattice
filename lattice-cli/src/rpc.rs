use anyhow::{anyhow, Context, Result};
use reqwest::Client;
use serde_json::{json, Value};
use std::error::Error;
use std::fmt;

#[derive(Debug)]
pub struct DaemonNotRunning;

impl fmt::Display for DaemonNotRunning {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "lattice daemon is not running")
    }
}

impl Error for DaemonNotRunning {}

pub struct RpcClient {
    pub base_url: String,
    http: Client,
}

impl RpcClient {
    pub fn new(port: u16) -> Self {
        Self {
            base_url: format!("http://127.0.0.1:{port}"),
            http: Client::new(),
        }
    }

    pub async fn call(&self, method: &str, params: Value) -> Result<Value> {
        let payload = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": method,
            "params": params,
        });

        let response = self
            .http
            .post(&self.base_url)
            .json(&payload)
            .send()
            .await
            .map_err(|err| {
                if err.is_connect() {
                    anyhow::Error::new(DaemonNotRunning)
                } else {
                    anyhow!("failed to send JSON-RPC request: {err}")
                }
            })?
            .error_for_status()
            .context("JSON-RPC endpoint returned HTTP error")?;

        let body: Value = response
            .json()
            .await
            .context("failed to decode JSON-RPC response")?;

        if let Some(err) = body.get("error") {
            let message = err
                .get("message")
                .and_then(Value::as_str)
                .unwrap_or("unknown JSON-RPC error");
            return Err(anyhow!("JSON-RPC error: {message}"));
        }

        body.get("result")
            .cloned()
            .ok_or_else(|| anyhow!("JSON-RPC response did not include result"))
    }

    pub async fn node_info(&self) -> Result<Value> {
        self.call("node_info", json!([])).await
    }

    pub async fn put_record(&self, key: &str, value: &str) -> Result<Value> {
        self.call("put_record", json!({ "key": key, "value": value }))
            .await
    }

    pub async fn get_record(&self, key: &str) -> Result<Value> {
        self.call("get_record", json!({ "key": key })).await
    }
}
