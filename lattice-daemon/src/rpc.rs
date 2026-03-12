use crate::app_registry::LocalAppRegistration;
use anyhow::Result;
use jsonrpsee::server::{ServerBuilder, ServerHandle};
use jsonrpsee::types::ErrorObjectOwned;
use jsonrpsee::RpcModule;
use lattice_core::moderation::ModerationRule;
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, oneshot};

const RESERVED_PREFIXES: &[&str] = &["name:", "site:", "block:"];
const MAX_PUT_KEY_BYTES: usize = 256;
const MAX_PUT_VALUE_BYTES: usize = 256 * 1024;

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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetSiteManifestResponse {
    pub manifest_json: String,
    pub trust: TrustState,
    pub pinned: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TrustState {
    pub status: String,
    pub explicitly_trusted: bool,
    pub first_seen_at: Option<u64>,
    pub previous_key: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModerationCheck {
    pub kind: String,
    pub value: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrustedPublisher {
    pub publisher_b64: String,
    pub label: String,
    pub added_at: u64,
    pub note: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct KnownPublisher {
    pub site_name: String,
    pub publisher_b64: String,
    pub first_seen_at: u64,
    pub explicitly_trusted: bool,
    pub explicitly_trusted_at: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum KnownPublisherStatus {
    FirstSeen,
    Matches,
    KeyChanged {
        previous_key: String,
        first_seen_at: u64,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuarantineEntryResponse {
    pub id: String,
    pub created_at: u64,
    pub matched_rule_id: String,
    pub matched_kind: String,
    pub matched_value: String,
    pub record_key: Option<String>,
    pub publisher: Option<String>,
    pub content_hash: Option<String>,
    pub site_name: Option<String>,
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
        respond_to: oneshot::Sender<Option<GetSiteManifestResponse>>,
    },
    GetBlock {
        hash: String,
        site_key: Option<String>,
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
    PinSite {
        name: String,
        respond_to: oneshot::Sender<Result<(), String>>,
    },
    UnpinSite {
        name: String,
        respond_to: oneshot::Sender<Result<(), String>>,
    },
    ListPinned {
        respond_to: oneshot::Sender<Vec<String>>,
    },
    ModAddRule {
        kind: String,
        value: String,
        action: String,
        note: Option<String>,
        respond_to: oneshot::Sender<Result<String, String>>,
    },
    ModRemoveRule {
        id: String,
        respond_to: oneshot::Sender<Result<(), String>>,
    },
    ModListRules {
        respond_to: oneshot::Sender<Vec<ModerationRule>>,
    },
    ModPurgeLocal {
        kind: String,
        value: String,
        respond_to: oneshot::Sender<Result<(), String>>,
    },
    ModQuarantineList {
        respond_to: oneshot::Sender<Vec<QuarantineEntryResponse>>,
    },
    ModCheck {
        kind: String,
        value: String,
        respond_to: oneshot::Sender<Option<String>>,
    },
    ModCheckMany {
        checks: Vec<ModerationCheck>,
        respond_to: oneshot::Sender<Option<String>>,
    },
    TrustAdd {
        publisher_b64: String,
        label: String,
        note: Option<String>,
        respond_to: oneshot::Sender<Result<(), String>>,
    },
    TrustRemove {
        publisher_b64: String,
        respond_to: oneshot::Sender<Result<(), String>>,
    },
    TrustList {
        respond_to: oneshot::Sender<Vec<TrustedPublisher>>,
    },
    TrustCheck {
        publisher_b64: String,
        respond_to: oneshot::Sender<Option<TrustedPublisher>>,
    },
    TrustSite {
        name: String,
        pin: bool,
        respond_to: oneshot::Sender<Result<(), String>>,
    },
    UntrustSite {
        name: String,
        unpin: bool,
        respond_to: oneshot::Sender<Result<(), String>>,
    },
    KnownPublisherStatus {
        name: String,
        respond_to: oneshot::Sender<Option<KnownPublisher>>,
    },
    AppRegister {
        site_name: String,
        proxy_port: u16,
        proxy_paths: Vec<String>,
        pid: u32,
        respond_to: oneshot::Sender<Result<(), String>>,
    },
    AppUnregister {
        site_name: String,
        pid: u32,
        respond_to: oneshot::Sender<Result<(), String>>,
    },
    AppList {
        respond_to: oneshot::Sender<Vec<LocalAppRegistration>>,
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
    site_key: Option<String>,
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

#[derive(Debug, Deserialize)]
struct SiteNameParams {
    name: String,
}

#[derive(Debug, Deserialize)]
struct ModAddRuleParams {
    kind: String,
    value: String,
    action: String,
    note: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ModRemoveRuleParams {
    id: String,
}

#[derive(Debug, Deserialize)]
struct ModPurgeLocalParams {
    kind: String,
    value: String,
}

#[derive(Debug, Deserialize)]
struct ModCheckParams {
    kind: String,
    value: String,
}

#[derive(Debug, Deserialize)]
struct ModCheckManyParams {
    checks: Vec<ModerationCheck>,
}

#[derive(Debug, Deserialize)]
struct TrustAddParams {
    publisher_b64: String,
    label: String,
    note: Option<String>,
}

#[derive(Debug, Deserialize)]
struct TrustPublisherParams {
    publisher_b64: String,
}

#[derive(Debug, Deserialize)]
struct TrustSiteParams {
    name: String,
    pin: bool,
}

#[derive(Debug, Deserialize)]
struct AppRegisterParams {
    site_name: String,
    proxy_port: u16,
    proxy_paths: Vec<String>,
    pid: u32,
}

#[derive(Debug, Deserialize)]
struct AppUnregisterParams {
    site_name: String,
    pid: u32,
}

#[derive(Debug, Deserialize)]
struct UntrustSiteParams {
    name: String,
    unpin: bool,
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

        if let Err(err) = validate_record_key(&key) {
            return Err(internal_error(err));
        }
        if value.len() > MAX_PUT_VALUE_BYTES {
            return Err(internal_error(format!(
                "value exceeds maximum size of {} bytes",
                MAX_PUT_VALUE_BYTES
            )));
        }

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
        let GetBlockParams { hash, site_key } = params.parse()?;
        let (resp_tx, resp_rx) = oneshot::channel();

        ctx.send(RpcCommand::GetBlock {
            hash,
            site_key,
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

    module.register_async_method("pin_site", |params, ctx, _| async move {
        let SiteNameParams { name } = params.parse()?;
        let (resp_tx, resp_rx) = oneshot::channel();
        ctx.send(RpcCommand::PinSite {
            name,
            respond_to: resp_tx,
        })
        .await
        .map_err(|e| internal_error(format!("failed to dispatch pin_site: {e}")))?;

        let result = resp_rx
            .await
            .map_err(|e| internal_error(format!("pin_site response dropped: {e}")))?;
        match result {
            Ok(()) => Ok::<_, ErrorObjectOwned>(PutRecordResponse {
                status: "ok".to_string(),
                error: None,
            }),
            Err(err) => Ok::<_, ErrorObjectOwned>(PutRecordResponse {
                status: "err".to_string(),
                error: Some(err),
            }),
        }
    })?;

    module.register_async_method("unpin_site", |params, ctx, _| async move {
        let SiteNameParams { name } = params.parse()?;
        let (resp_tx, resp_rx) = oneshot::channel();
        ctx.send(RpcCommand::UnpinSite {
            name,
            respond_to: resp_tx,
        })
        .await
        .map_err(|e| internal_error(format!("failed to dispatch unpin_site: {e}")))?;

        let result = resp_rx
            .await
            .map_err(|e| internal_error(format!("unpin_site response dropped: {e}")))?;
        match result {
            Ok(()) => Ok::<_, ErrorObjectOwned>(PutRecordResponse {
                status: "ok".to_string(),
                error: None,
            }),
            Err(err) => Ok::<_, ErrorObjectOwned>(PutRecordResponse {
                status: "err".to_string(),
                error: Some(err),
            }),
        }
    })?;

    module.register_async_method("list_pinned", |_, ctx, _| async move {
        let (resp_tx, resp_rx) = oneshot::channel();
        ctx.send(RpcCommand::ListPinned {
            respond_to: resp_tx,
        })
        .await
        .map_err(|e| internal_error(format!("failed to dispatch list_pinned: {e}")))?;

        let sites = resp_rx
            .await
            .map_err(|e| internal_error(format!("list_pinned response dropped: {e}")))?;
        Ok::<_, ErrorObjectOwned>(sites)
    })?;

    module.register_async_method("mod_add_rule", |params, ctx, _| async move {
        let ModAddRuleParams {
            kind,
            value,
            action,
            note,
        } = params.parse()?;
        let (resp_tx, resp_rx) = oneshot::channel();
        ctx.send(RpcCommand::ModAddRule {
            kind,
            value,
            action,
            note,
            respond_to: resp_tx,
        })
        .await
        .map_err(|e| internal_error(format!("failed to dispatch mod_add_rule: {e}")))?;

        match resp_rx
            .await
            .map_err(|e| internal_error(format!("mod_add_rule response dropped: {e}")))?
        {
            Ok(rule_id) => Ok::<_, ErrorObjectOwned>(serde_json::json!({
                "status": "ok",
                "id": rule_id,
            })),
            Err(err) => Ok::<_, ErrorObjectOwned>(serde_json::json!({
                "status": "err",
                "error": err,
            })),
        }
    })?;

    module.register_async_method("mod_remove_rule", |params, ctx, _| async move {
        let ModRemoveRuleParams { id } = params.parse()?;
        let (resp_tx, resp_rx) = oneshot::channel();
        ctx.send(RpcCommand::ModRemoveRule {
            id,
            respond_to: resp_tx,
        })
        .await
        .map_err(|e| internal_error(format!("failed to dispatch mod_remove_rule: {e}")))?;

        match resp_rx
            .await
            .map_err(|e| internal_error(format!("mod_remove_rule response dropped: {e}")))?
        {
            Ok(()) => Ok::<_, ErrorObjectOwned>(PutRecordResponse {
                status: "ok".to_string(),
                error: None,
            }),
            Err(err) => Ok::<_, ErrorObjectOwned>(PutRecordResponse {
                status: "err".to_string(),
                error: Some(err),
            }),
        }
    })?;

    module.register_async_method("mod_list_rules", |_, ctx, _| async move {
        let (resp_tx, resp_rx) = oneshot::channel();
        ctx.send(RpcCommand::ModListRules {
            respond_to: resp_tx,
        })
        .await
        .map_err(|e| internal_error(format!("failed to dispatch mod_list_rules: {e}")))?;

        let rules = resp_rx
            .await
            .map_err(|e| internal_error(format!("mod_list_rules response dropped: {e}")))?;
        Ok::<_, ErrorObjectOwned>(rules)
    })?;

    module.register_async_method("mod_purge_local", |params, ctx, _| async move {
        let ModPurgeLocalParams { kind, value } = params.parse()?;
        let (resp_tx, resp_rx) = oneshot::channel();
        ctx.send(RpcCommand::ModPurgeLocal {
            kind,
            value,
            respond_to: resp_tx,
        })
        .await
        .map_err(|e| internal_error(format!("failed to dispatch mod_purge_local: {e}")))?;

        match resp_rx
            .await
            .map_err(|e| internal_error(format!("mod_purge_local response dropped: {e}")))?
        {
            Ok(()) => Ok::<_, ErrorObjectOwned>(PutRecordResponse {
                status: "ok".to_string(),
                error: None,
            }),
            Err(err) => Ok::<_, ErrorObjectOwned>(PutRecordResponse {
                status: "err".to_string(),
                error: Some(err),
            }),
        }
    })?;

    module.register_async_method("mod_quarantine_list", |_, ctx, _| async move {
        let (resp_tx, resp_rx) = oneshot::channel();
        ctx.send(RpcCommand::ModQuarantineList {
            respond_to: resp_tx,
        })
        .await
        .map_err(|e| internal_error(format!("failed to dispatch mod_quarantine_list: {e}")))?;

        let quarantined = resp_rx
            .await
            .map_err(|e| internal_error(format!("mod_quarantine_list response dropped: {e}")))?;
        Ok::<_, ErrorObjectOwned>(quarantined)
    })?;

    module.register_async_method("mod_check", |params, ctx, _| async move {
        let ModCheckParams { kind, value } = params.parse()?;
        let (resp_tx, resp_rx) = oneshot::channel();
        ctx.send(RpcCommand::ModCheck {
            kind,
            value,
            respond_to: resp_tx,
        })
        .await
        .map_err(|e| internal_error(format!("failed to dispatch mod_check: {e}")))?;

        let result = resp_rx
            .await
            .map_err(|e| internal_error(format!("mod_check response dropped: {e}")))?;
        Ok::<_, ErrorObjectOwned>(result)
    })?;

    module.register_async_method("mod_check_many", |params, ctx, _| async move {
        let ModCheckManyParams { checks } = params.parse()?;
        let (resp_tx, resp_rx) = oneshot::channel();
        ctx.send(RpcCommand::ModCheckMany {
            checks,
            respond_to: resp_tx,
        })
        .await
        .map_err(|e| internal_error(format!("failed to dispatch mod_check_many: {e}")))?;

        let result = resp_rx
            .await
            .map_err(|e| internal_error(format!("mod_check_many response dropped: {e}")))?;
        Ok::<_, ErrorObjectOwned>(result)
    })?;

    module.register_async_method("trust_add", |params, ctx, _| async move {
        let TrustAddParams {
            publisher_b64,
            label,
            note,
        } = params.parse()?;
        let (resp_tx, resp_rx) = oneshot::channel();
        ctx.send(RpcCommand::TrustAdd {
            publisher_b64,
            label,
            note,
            respond_to: resp_tx,
        })
        .await
        .map_err(|e| internal_error(format!("failed to dispatch trust_add: {e}")))?;

        match resp_rx
            .await
            .map_err(|e| internal_error(format!("trust_add response dropped: {e}")))?
        {
            Ok(()) => Ok::<_, ErrorObjectOwned>(PutRecordResponse {
                status: "ok".to_string(),
                error: None,
            }),
            Err(err) => Ok::<_, ErrorObjectOwned>(PutRecordResponse {
                status: "err".to_string(),
                error: Some(err),
            }),
        }
    })?;

    module.register_async_method("trust_remove", |params, ctx, _| async move {
        let TrustPublisherParams { publisher_b64 } = params.parse()?;
        let (resp_tx, resp_rx) = oneshot::channel();
        ctx.send(RpcCommand::TrustRemove {
            publisher_b64,
            respond_to: resp_tx,
        })
        .await
        .map_err(|e| internal_error(format!("failed to dispatch trust_remove: {e}")))?;

        match resp_rx
            .await
            .map_err(|e| internal_error(format!("trust_remove response dropped: {e}")))?
        {
            Ok(()) => Ok::<_, ErrorObjectOwned>(PutRecordResponse {
                status: "ok".to_string(),
                error: None,
            }),
            Err(err) => Ok::<_, ErrorObjectOwned>(PutRecordResponse {
                status: "err".to_string(),
                error: Some(err),
            }),
        }
    })?;

    module.register_async_method("trust_list", |_, ctx, _| async move {
        let (resp_tx, resp_rx) = oneshot::channel();
        ctx.send(RpcCommand::TrustList {
            respond_to: resp_tx,
        })
        .await
        .map_err(|e| internal_error(format!("failed to dispatch trust_list: {e}")))?;

        let trusted = resp_rx
            .await
            .map_err(|e| internal_error(format!("trust_list response dropped: {e}")))?;
        Ok::<_, ErrorObjectOwned>(trusted)
    })?;

    module.register_async_method("trust_check", |params, ctx, _| async move {
        let TrustPublisherParams { publisher_b64 } = params.parse()?;
        let (resp_tx, resp_rx) = oneshot::channel();
        ctx.send(RpcCommand::TrustCheck {
            publisher_b64,
            respond_to: resp_tx,
        })
        .await
        .map_err(|e| internal_error(format!("failed to dispatch trust_check: {e}")))?;

        let trusted = resp_rx
            .await
            .map_err(|e| internal_error(format!("trust_check response dropped: {e}")))?;
        Ok::<_, ErrorObjectOwned>(trusted)
    })?;

    module.register_async_method("trust_site", |params, ctx, _| async move {
        let TrustSiteParams { name, pin } = params.parse()?;
        let (resp_tx, resp_rx) = oneshot::channel();
        ctx.send(RpcCommand::TrustSite {
            name,
            pin,
            respond_to: resp_tx,
        })
        .await
        .map_err(|e| internal_error(format!("failed to dispatch trust_site: {e}")))?;

        match resp_rx
            .await
            .map_err(|e| internal_error(format!("trust_site response dropped: {e}")))?
        {
            Ok(()) => Ok::<_, ErrorObjectOwned>(PutRecordResponse {
                status: "ok".to_string(),
                error: None,
            }),
            Err(err) => Ok::<_, ErrorObjectOwned>(PutRecordResponse {
                status: "err".to_string(),
                error: Some(err),
            }),
        }
    })?;

    module.register_async_method("untrust_site", |params, ctx, _| async move {
        let UntrustSiteParams { name, unpin } = params.parse()?;
        let (resp_tx, resp_rx) = oneshot::channel();
        ctx.send(RpcCommand::UntrustSite {
            name,
            unpin,
            respond_to: resp_tx,
        })
        .await
        .map_err(|e| internal_error(format!("failed to dispatch untrust_site: {e}")))?;

        match resp_rx
            .await
            .map_err(|e| internal_error(format!("untrust_site response dropped: {e}")))?
        {
            Ok(()) => Ok::<_, ErrorObjectOwned>(PutRecordResponse {
                status: "ok".to_string(),
                error: None,
            }),
            Err(err) => Ok::<_, ErrorObjectOwned>(PutRecordResponse {
                status: "err".to_string(),
                error: Some(err),
            }),
        }
    })?;

    module.register_async_method("known_publisher_status", |params, ctx, _| async move {
        let SiteNameParams { name } = params.parse()?;
        let (resp_tx, resp_rx) = oneshot::channel();
        ctx.send(RpcCommand::KnownPublisherStatus {
            name,
            respond_to: resp_tx,
        })
        .await
        .map_err(|e| internal_error(format!("failed to dispatch known_publisher_status: {e}")))?;

        let known = resp_rx
            .await
            .map_err(|e| internal_error(format!("known_publisher_status response dropped: {e}")))?;
        Ok::<_, ErrorObjectOwned>(known)
    })?;

    module.register_async_method("app_register", |params, ctx, _| async move {
        let AppRegisterParams {
            site_name,
            proxy_port,
            proxy_paths,
            pid,
        } = params.parse()?;
        let (resp_tx, resp_rx) = oneshot::channel();
        ctx.send(RpcCommand::AppRegister {
            site_name,
            proxy_port,
            proxy_paths,
            pid,
            respond_to: resp_tx,
        })
        .await
        .map_err(|e| internal_error(format!("failed to dispatch app_register: {e}")))?;

        match resp_rx
            .await
            .map_err(|e| internal_error(format!("app_register response dropped: {e}")))?
        {
            Ok(()) => Ok::<_, ErrorObjectOwned>(PutRecordResponse {
                status: "ok".to_string(),
                error: None,
            }),
            Err(err) => Ok::<_, ErrorObjectOwned>(PutRecordResponse {
                status: "err".to_string(),
                error: Some(err),
            }),
        }
    })?;

    module.register_async_method("app_unregister", |params, ctx, _| async move {
        let AppUnregisterParams { site_name, pid } = params.parse()?;
        let (resp_tx, resp_rx) = oneshot::channel();
        ctx.send(RpcCommand::AppUnregister {
            site_name,
            pid,
            respond_to: resp_tx,
        })
        .await
        .map_err(|e| internal_error(format!("failed to dispatch app_unregister: {e}")))?;

        match resp_rx
            .await
            .map_err(|e| internal_error(format!("app_unregister response dropped: {e}")))?
        {
            Ok(()) => Ok::<_, ErrorObjectOwned>(PutRecordResponse {
                status: "ok".to_string(),
                error: None,
            }),
            Err(err) => Ok::<_, ErrorObjectOwned>(PutRecordResponse {
                status: "err".to_string(),
                error: Some(err),
            }),
        }
    })?;

    module.register_async_method("app_list", |_, ctx, _| async move {
        let (resp_tx, resp_rx) = oneshot::channel();
        ctx.send(RpcCommand::AppList {
            respond_to: resp_tx,
        })
        .await
        .map_err(|e| internal_error(format!("failed to dispatch app_list: {e}")))?;

        let apps: Vec<LocalAppRegistration> = resp_rx
            .await
            .map_err(|e| internal_error(format!("app_list response dropped: {e}")))?;
        Ok::<_, ErrorObjectOwned>(apps)
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

fn validate_record_key(key: &str) -> Result<(), String> {
    if key.is_empty() {
        return Err("key cannot be empty".to_string());
    }
    if key.len() > MAX_PUT_KEY_BYTES {
        return Err(format!(
            "key exceeds maximum length of {} bytes",
            MAX_PUT_KEY_BYTES
        ));
    }
    if key.contains('\0') || key.contains('\n') || key.contains('\r') || key.contains('\\') {
        return Err("key contains invalid characters".to_string());
    }
    if !key
        .chars()
        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == ':' || c == '-' || c == '_')
    {
        return Err(
            "key may only contain lowercase letters, digits, colon, hyphen, and underscore"
                .to_string(),
        );
    }
    Ok(())
}
