use anyhow::Result;
use jsonrpsee::server::{ServerBuilder, ServerHandle};
use jsonrpsee::types::ErrorObjectOwned;
use jsonrpsee::RpcModule;
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, oneshot};

const RESERVED_PREFIXES: &[&str] = &["name:", "site:", "block:"];

#[derive(Debug, Clone, Serialize)]
pub struct NodeInfoResponse {
    pub peer_id: String,
    pub connected_peers: u32,
    pub connected_peer_ids: Vec<String>,
    pub listen_addrs: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct PublishSiteOk {
    pub version: u64,
    pub file_count: usize,
    pub claimed: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct SiteFile {
    pub path: String,
    pub contents: String,
    pub mime_type: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct GetSiteResponse {
    pub name: String,
    pub version: u64,
    pub files: Vec<SiteFile>,
}

pub enum RpcCommand {
    NodeInfo {
        respond_to: oneshot::Sender<NodeInfoResponse>,
    },
    PutRecord {
        key: String,
        value: String,
        respond_to: oneshot::Sender<Result<(), String>>,
    },
    GetRecord {
        key: String,
        respond_to: oneshot::Sender<Option<String>>,
    },
    PublishSite {
        name: String,
        site_dir: String,
        respond_to: oneshot::Sender<Result<PublishSiteOk, String>>,
    },
    GetSiteManifest {
        name: String,
        respond_to: oneshot::Sender<Option<String>>,
    },
    GetBlock {
        hash: String,
        respond_to: oneshot::Sender<Option<String>>,
    },
    GetSite {
        name: String,
        respond_to: oneshot::Sender<Result<GetSiteResponse, String>>,
    },
    ClaimName {
        name: String,
        pubkey_hex: String,
        respond_to: oneshot::Sender<Result<(), String>>,
    },
    ListNames {
        respond_to: oneshot::Sender<Vec<String>>,
    },
    RetryNameProbe {
        name: String,
        pubkey_hex: String,
        probe_count: u32,
        respond_to: oneshot::Sender<Result<(), String>>,
    },
    RetryPublishOwnershipCheck {
        name: String,
        site_dir: String,
        probe_count: u32,
        respond_to: oneshot::Sender<Result<PublishSiteOk, String>>,
    },
    RepublishLocalRecords,
}

#[derive(Debug, Deserialize)]
struct PutRecordParams {
    key: String,
    value: String,
}

#[derive(Debug, Deserialize)]
struct GetRecordParams {
    key: String,
}

#[derive(Debug, Deserialize)]
struct PublishSiteParams {
    name: String,
    site_dir: String,
}

#[derive(Debug, Deserialize)]
struct GetSiteManifestParams {
    name: String,
}

#[derive(Debug, Deserialize)]
struct GetBlockParams {
    hash: String,
}

#[derive(Debug, Deserialize)]
struct GetSiteParams {
    name: String,
}

#[derive(Debug, Deserialize)]
struct ClaimNameParams {
    name: String,
    pubkey_hex: String,
}

#[derive(Debug, Clone, Serialize)]
struct PutRecordResponse {
    status: String,
    error: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
struct PublishSiteResponse {
    status: String,
    version: Option<u64>,
    file_count: Option<usize>,
    claimed: Option<bool>,
    error: Option<String>,
}

pub async fn start_rpc_server(
    port: u16,
    command_tx: mpsc::Sender<RpcCommand>,
) -> Result<ServerHandle> {
    let addr = format!("127.0.0.1:{port}");
    let server = ServerBuilder::default().build(&addr).await?;

    let mut module = RpcModule::new(command_tx);

    module.register_async_method("node_info", |_, ctx, _| async move {
        let (resp_tx, resp_rx) = oneshot::channel();
        ctx.send(RpcCommand::NodeInfo {
            respond_to: resp_tx,
        })
        .await
        .map_err(|e| internal_error(format!("failed to query node info: {e}")))?;

        let info = resp_rx
            .await
            .map_err(|e| internal_error(format!("node info response dropped: {e}")))?;
        Ok::<_, ErrorObjectOwned>(info)
    })?;

    module.register_async_method("put_record", |params, ctx, _| async move {
        let PutRecordParams { key, value } = params.parse()?;

        if let Some(name) = key.strip_prefix("name:") {
            if let Err(err) = validate_name(name) {
                return Err(internal_error(err));
            }
        }
        if RESERVED_PREFIXES.iter().any(|p| key.starts_with(p)) {
            return Err(internal_error(
                "cannot write to reserved key prefix via put_record; use dedicated RPCs (claim_name, publish_site)"
                    .to_string(),
            ));
        }

        let (resp_tx, resp_rx) = oneshot::channel();

        ctx.send(RpcCommand::PutRecord {
            key,
            value,
            respond_to: resp_tx,
        })
        .await
        .map_err(|e| internal_error(format!("failed to dispatch put_record: {e}")))?;

        let result = resp_rx
            .await
            .map_err(|e| internal_error(format!("put_record response dropped: {e}")))?;

        let response = match result {
            Ok(()) => PutRecordResponse {
                status: "ok".to_string(),
                error: None,
            },
            Err(err) => PutRecordResponse {
                status: "err".to_string(),
                error: Some(err),
            },
        };

        Ok::<_, ErrorObjectOwned>(response)
    })?;

    module.register_async_method("get_record", |params, ctx, _| async move {
        let GetRecordParams { key } = params.parse()?;
        let (resp_tx, resp_rx) = oneshot::channel();

        ctx.send(RpcCommand::GetRecord {
            key,
            respond_to: resp_tx,
        })
        .await
        .map_err(|e| internal_error(format!("failed to dispatch get_record: {e}")))?;

        resp_rx
            .await
            .map_err(|e| internal_error(format!("get_record response dropped: {e}")))
    })?;

    module.register_async_method("publish_site", |params, ctx, _| async move {
        let PublishSiteParams { name, site_dir } = params.parse()?;
        let (resp_tx, resp_rx) = oneshot::channel();

        ctx.send(RpcCommand::PublishSite {
            name,
            site_dir,
            respond_to: resp_tx,
        })
        .await
        .map_err(|e| internal_error(format!("failed to dispatch publish_site: {e}")))?;

        let result = resp_rx
            .await
            .map_err(|e| internal_error(format!("publish_site response dropped: {e}")))?;

        let response = match result {
            Ok(ok) => PublishSiteResponse {
                status: "ok".to_string(),
                version: Some(ok.version),
                file_count: Some(ok.file_count),
                claimed: Some(ok.claimed),
                error: None,
            },
            Err(err) => PublishSiteResponse {
                status: "err".to_string(),
                version: None,
                file_count: None,
                claimed: None,
                error: Some(err),
            },
        };

        Ok::<_, ErrorObjectOwned>(response)
    })?;

    module.register_async_method("get_site_manifest", |params, ctx, _| async move {
        let GetSiteManifestParams { name } = params.parse()?;
        let (resp_tx, resp_rx) = oneshot::channel();

        ctx.send(RpcCommand::GetSiteManifest {
            name,
            respond_to: resp_tx,
        })
        .await
        .map_err(|e| internal_error(format!("failed to dispatch get_site_manifest: {e}")))?;

        resp_rx
            .await
            .map_err(|e| internal_error(format!("get_site_manifest response dropped: {e}")))
    })?;

    module.register_async_method("get_block", |params, ctx, _| async move {
        let GetBlockParams { hash } = params.parse()?;
        let (resp_tx, resp_rx) = oneshot::channel();

        ctx.send(RpcCommand::GetBlock {
            hash,
            respond_to: resp_tx,
        })
        .await
        .map_err(|e| internal_error(format!("failed to dispatch get_block: {e}")))?;

        resp_rx
            .await
            .map_err(|e| internal_error(format!("get_block response dropped: {e}")))
    })?;

    module.register_async_method("get_site", |params, ctx, _| async move {
        let GetSiteParams { name } = params.parse()?;
        let (resp_tx, resp_rx) = oneshot::channel();

        ctx.send(RpcCommand::GetSite {
            name,
            respond_to: resp_tx,
        })
        .await
        .map_err(|e| internal_error(format!("failed to dispatch get_site: {e}")))?;

        let result = resp_rx
            .await
            .map_err(|e| internal_error(format!("get_site response dropped: {e}")))?;

        match result {
            Ok(response) => Ok::<_, ErrorObjectOwned>(response),
            Err(err) => Err(internal_error(err)),
        }
    })?;

    module.register_async_method("claim_name", |params, ctx, _| async move {
        let ClaimNameParams { name, pubkey_hex } = params.parse()?;
        let (resp_tx, resp_rx) = oneshot::channel();

        ctx.send(RpcCommand::ClaimName {
            name,
            pubkey_hex,
            respond_to: resp_tx,
        })
        .await
        .map_err(|e| internal_error(format!("failed to dispatch claim_name: {e}")))?;

        let result = resp_rx
            .await
            .map_err(|e| internal_error(format!("claim_name response dropped: {e}")))?;

        let response = match result {
            Ok(()) => PutRecordResponse {
                status: "ok".to_string(),
                error: None,
            },
            Err(err) => PutRecordResponse {
                status: "err".to_string(),
                error: Some(err),
            },
        };

        Ok::<_, ErrorObjectOwned>(response)
    })?;

    module.register_async_method("list_names", |_, ctx, _| async move {
        let (resp_tx, resp_rx) = oneshot::channel();
        ctx.send(RpcCommand::ListNames {
            respond_to: resp_tx,
        })
        .await
        .map_err(|e| internal_error(format!("failed to dispatch list_names: {e}")))?;

        let names = resp_rx
            .await
            .map_err(|e| internal_error(format!("list_names response dropped: {e}")))?;
        Ok::<_, ErrorObjectOwned>(names)
    })?;

    let handle = server.start(module);
    Ok(handle)
}

fn internal_error(message: String) -> ErrorObjectOwned {
    ErrorObjectOwned::owned(-32000, message, None::<()>)
}

fn validate_name(name: &str) -> Result<(), String> {
    if name.is_empty() {
        return Err("name cannot be empty".to_string());
    }
    if name.len() > 63 {
        return Err("name must be 63 characters or fewer".to_string());
    }
    if !name
        .chars()
        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
    {
        return Err("name may only contain lowercase letters, digits, and hyphens".to_string());
    }
    if name.starts_with('-') || name.ends_with('-') {
        return Err("name cannot start or end with a hyphen".to_string());
    }
    Ok(())
}
