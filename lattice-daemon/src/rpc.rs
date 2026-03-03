use anyhow::Result;
use jsonrpsee::server::{ServerBuilder, ServerHandle};
use jsonrpsee::types::ErrorObjectOwned;
use jsonrpsee::RpcModule;
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, oneshot};

#[derive(Debug, Clone, Serialize)]
pub struct NodeInfoResponse {
    pub peer_id: String,
    pub connected_peers: u32,
    pub listen_addrs: Vec<String>,
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

#[derive(Debug, Clone, Serialize)]
struct PutRecordResponse {
    status: String,
    error: Option<String>,
}

pub async fn start_rpc_server(port: u16, command_tx: mpsc::Sender<RpcCommand>) -> Result<ServerHandle> {
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

    let handle = server.start(module);
    Ok(handle)
}

fn internal_error(message: String) -> ErrorObjectOwned {
    ErrorObjectOwned::owned(-32000, message, None::<()>)
}
