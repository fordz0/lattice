use anyhow::{Context, Result};
use axum::body::{Body, Bytes};
use axum::extract::{Host, OriginalUri, State};
use axum::http::{header, HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::Router;
use sha2::{Digest, Sha256};
use std::io;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::{mpsc, oneshot};
use tokio::time::{timeout, Duration};
use tokio_stream::wrappers::ReceiverStream;
use tower_http::cors::{Any, CorsLayer};
use tracing::warn;

use lattice_site::manifest::{verify_manifest, FileEntry, SiteManifest, DEFAULT_CHUNK_SIZE_BYTES};

use crate::rpc::RpcCommand;

const RPC_SEND_TIMEOUT: Duration = Duration::from_secs(10);
const RPC_RESPONSE_TIMEOUT: Duration = Duration::from_secs(20);
const MAX_MANIFEST_BYTES: usize = 1024 * 1024;
const MAX_MANIFEST_FILES: usize = 1000;
const MAX_MANIFEST_TOTAL_BYTES: u64 = 100 * 1024 * 1024;
const MAX_HTTP_RESPONSE_BYTES: u64 = 100 * 1024 * 1024;

static HTTP_REQUESTS_TOTAL: AtomicU64 = AtomicU64::new(0);
static HTTP_ERRORS_TOTAL: AtomicU64 = AtomicU64::new(0);
static HTTP_RANGE_REQUESTS_TOTAL: AtomicU64 = AtomicU64::new(0);
static HTTP_RANGE_416_TOTAL: AtomicU64 = AtomicU64::new(0);
static HTTP_BLOCK_FETCH_TOTAL: AtomicU64 = AtomicU64::new(0);
static HTTP_BLOCK_FETCH_ERRORS_TOTAL: AtomicU64 = AtomicU64::new(0);
static HTTP_OWNER_MISMATCH_TOTAL: AtomicU64 = AtomicU64::new(0);

pub async fn start_http_server(port: u16, rpc_tx: mpsc::Sender<RpcCommand>) -> Result<()> {
    let app = Router::new()
        .route("/__lattice_metrics", get(metrics_endpoint))
        .route("/", get(serve_site))
        .route("/*path", get(serve_site))
        .with_state(rpc_tx)
        .layer(CorsLayer::new().allow_origin(Any));

    let listen_addr = format!("127.0.0.1:{port}");
    let listener = tokio::net::TcpListener::bind(&listen_addr)
        .await
        .with_context(|| format!("failed to bind HTTP server on {listen_addr}"))?;

    axum::serve(listener, app)
        .await
        .context("HTTP server stopped unexpectedly")?;

    Ok(())
}

async fn serve_site(
    Host(host): Host,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
    State(rpc_tx): State<mpsc::Sender<RpcCommand>>,
) -> impl IntoResponse {
    HTTP_REQUESTS_TOTAL.fetch_add(1, Ordering::Relaxed);
    if headers.contains_key(header::RANGE) {
        HTTP_RANGE_REQUESTS_TOTAL.fetch_add(1, Ordering::Relaxed);
    }

    let fail = |response: Response| -> Response {
        record_http_error(response.status());
        response
    };

    let site_name = match extract_site_name(&host, &uri) {
        Some(name) => name,
        None => {
            return fail(plain(
                StatusCode::NOT_FOUND,
                "site not found on Lattice (expected *.lat host)",
            ));
        }
    };

    let request_path = match normalize_path(uri.path()) {
        Some(path) => path,
        None => return fail(plain(StatusCode::BAD_REQUEST, "invalid path")),
    };

    let manifest = match fetch_manifest(&rpc_tx, &site_name).await {
        Ok(manifest) => manifest,
        Err(response) => return fail(response),
    };

    let file = match manifest.files.iter().find(|f| f.path == request_path) {
        Some(file) => file,
        None => return fail(plain(StatusCode::NOT_FOUND, "site not found on Lattice")),
    };

    let requested_range = match parse_requested_range(&headers, file.size) {
        Ok(range) => range,
        Err(response) => return fail(response),
    };

    let full_range = if file.size == 0 {
        ByteRange { start: 0, end: 0 }
    } else {
        ByteRange {
            start: 0,
            end: file.size.saturating_sub(1),
        }
    };
    let effective_range = requested_range.unwrap_or(full_range);

    let content_length = if file.size == 0 {
        0
    } else {
        effective_range.end - effective_range.start + 1
    };
    let body = match stream_file_range(rpc_tx.clone(), file.clone(), effective_range) {
        Ok(body) => body,
        Err(response) => return fail(response),
    };

    let mime_type = infer_mime_type(&file.path);
    if let Some(range) = requested_range {
        file_response(mime_type, body, content_length, file.size, Some(range))
    } else {
        file_response(mime_type, body, content_length, file.size, None)
    }
}

async fn fetch_manifest(
    rpc_tx: &mpsc::Sender<RpcCommand>,
    site_name: &str,
) -> std::result::Result<SiteManifest, Response> {
    let (resp_tx, resp_rx) = oneshot::channel();
    let send_result = timeout(
        RPC_SEND_TIMEOUT,
        rpc_tx.send(RpcCommand::GetSiteManifest {
            name: site_name.to_string(),
            respond_to: resp_tx,
        }),
    )
    .await;
    match send_result {
        Ok(Ok(())) => {}
        Ok(Err(_)) => return Err(plain(StatusCode::BAD_GATEWAY, "lattice daemon error")),
        Err(_) => return Err(plain(StatusCode::GATEWAY_TIMEOUT, "lattice daemon timeout")),
    }

    let manifest_json = match timeout(RPC_RESPONSE_TIMEOUT, resp_rx).await {
        Err(_) => return Err(plain(StatusCode::GATEWAY_TIMEOUT, "lattice daemon timeout")),
        Ok(Ok(Some(manifest_json))) => manifest_json,
        Ok(Ok(None)) => return Err(plain(StatusCode::NOT_FOUND, "site not found on Lattice")),
        Ok(Err(_)) => return Err(plain(StatusCode::BAD_GATEWAY, "lattice daemon error")),
    };
    if manifest_json.len() > MAX_MANIFEST_BYTES {
        return Err(plain(StatusCode::BAD_GATEWAY, "site manifest too large"));
    }

    let manifest: SiteManifest = match serde_json::from_str(&manifest_json) {
        Ok(manifest) => manifest,
        Err(_) => return Err(plain(StatusCode::BAD_GATEWAY, "invalid site manifest")),
    };

    if manifest.name != site_name {
        return Err(plain(StatusCode::BAD_GATEWAY, "manifest name mismatch"));
    }
    if manifest.files.len() > MAX_MANIFEST_FILES {
        return Err(plain(
            StatusCode::BAD_GATEWAY,
            "site exceeds maximum file count",
        ));
    }

    if let Err(err) = verify_manifest(&manifest) {
        warn!(name = %site_name, error = %err, "manifest signature invalid");
        return Err(plain(StatusCode::BAD_GATEWAY, "manifest signature invalid"));
    }

    for file in &manifest.files {
        if !is_safe_manifest_path(&file.path) {
            return Err(plain(
                StatusCode::BAD_GATEWAY,
                "invalid path in site manifest",
            ));
        }
    }
    let declared_bytes = manifest
        .files
        .iter()
        .fold(0_u64, |acc, file| acc.saturating_add(file.size));
    if declared_bytes > MAX_MANIFEST_TOTAL_BYTES {
        return Err(plain(
            StatusCode::BAD_GATEWAY,
            "site exceeds maximum total size",
        ));
    }

    let owner_key = match fetch_name_owner(rpc_tx, site_name).await? {
        Some(owner_key) => owner_key,
        None => return Err(plain(StatusCode::NOT_FOUND, "name owner record missing")),
    };
    if owner_key != manifest.publisher_key {
        HTTP_OWNER_MISMATCH_TOTAL.fetch_add(1, Ordering::Relaxed);
        warn!(
            name = %site_name,
            owner_key = %owner_key,
            publisher_key = %manifest.publisher_key,
            "manifest publisher does not match name owner"
        );
        return Err(plain(
            StatusCode::BAD_GATEWAY,
            "manifest publisher does not match name owner",
        ));
    }

    Ok(manifest)
}

async fn fetch_name_owner(
    rpc_tx: &mpsc::Sender<RpcCommand>,
    site_name: &str,
) -> std::result::Result<Option<String>, Response> {
    let (resp_tx, resp_rx) = oneshot::channel();
    let send_result = timeout(
        RPC_SEND_TIMEOUT,
        rpc_tx.send(RpcCommand::GetRecord {
            key: format!("name:{site_name}"),
            respond_to: resp_tx,
        }),
    )
    .await;
    match send_result {
        Ok(Ok(())) => {}
        Ok(Err(_)) => return Err(plain(StatusCode::BAD_GATEWAY, "lattice daemon error")),
        Err(_) => return Err(plain(StatusCode::GATEWAY_TIMEOUT, "lattice daemon timeout")),
    }

    match timeout(RPC_RESPONSE_TIMEOUT, resp_rx).await {
        Err(_) => Err(plain(StatusCode::GATEWAY_TIMEOUT, "lattice daemon timeout")),
        Ok(Ok(owner)) => Ok(owner),
        Ok(Err(_)) => Err(plain(StatusCode::BAD_GATEWAY, "lattice daemon error")),
    }
}

#[derive(Clone, Copy)]
struct ByteRange {
    start: u64,
    end: u64,
}

#[allow(clippy::result_large_err)]
fn parse_requested_range(
    headers: &HeaderMap,
    file_size: u64,
) -> std::result::Result<Option<ByteRange>, Response> {
    let Some(raw) = headers.get(header::RANGE) else {
        return Ok(None);
    };

    let range_str = match raw.to_str() {
        Ok(v) => v,
        Err(_) => return Err(range_not_satisfiable(file_size)),
    };

    if file_size == 0 {
        return Err(range_not_satisfiable(0));
    }

    let Some(spec) = range_str.strip_prefix("bytes=") else {
        return Err(range_not_satisfiable(file_size));
    };

    if spec.contains(',') {
        return Err(range_not_satisfiable(file_size));
    }

    let Some((start_raw, end_raw)) = spec.split_once('-') else {
        return Err(range_not_satisfiable(file_size));
    };

    let (start, end) = if start_raw.is_empty() {
        let suffix_len = match end_raw.parse::<u64>() {
            Ok(v) if v > 0 => v,
            _ => return Err(range_not_satisfiable(file_size)),
        };
        let start = file_size.saturating_sub(suffix_len);
        (start, file_size - 1)
    } else {
        let start = match start_raw.parse::<u64>() {
            Ok(v) => v,
            Err(_) => return Err(range_not_satisfiable(file_size)),
        };

        let end = if end_raw.is_empty() {
            file_size - 1
        } else {
            match end_raw.parse::<u64>() {
                Ok(v) => v,
                Err(_) => return Err(range_not_satisfiable(file_size)),
            }
        };
        (start, end)
    };

    if start >= file_size || end < start {
        return Err(range_not_satisfiable(file_size));
    }

    Ok(Some(ByteRange {
        start,
        end: end.min(file_size - 1),
    }))
}

#[derive(Clone)]
struct ChunkSlice {
    block_hash: String,
    start: usize,
    end: usize,
}

#[allow(clippy::result_large_err)]
fn plan_range_slices(
    file: &FileEntry,
    range: ByteRange,
) -> std::result::Result<Vec<ChunkSlice>, Response> {
    if file.size == 0 {
        return Ok(Vec::new());
    }

    let block_hashes = file_block_hashes(file);
    if block_hashes.is_empty() {
        return Err(plain(StatusCode::BAD_GATEWAY, "invalid site manifest"));
    }

    let chunk_size = file
        .chunk_size
        .and_then(|size| usize::try_from(size).ok())
        .filter(|size| *size > 0)
        .unwrap_or(DEFAULT_CHUNK_SIZE_BYTES);

    let expected_len_u64 = range.end.saturating_sub(range.start).saturating_add(1);
    if expected_len_u64 > MAX_HTTP_RESPONSE_BYTES {
        return Err(plain(
            StatusCode::PAYLOAD_TOO_LARGE,
            "requested range too large",
        ));
    }
    let expected_len = usize::try_from(expected_len_u64)
        .map_err(|_| plain(StatusCode::PAYLOAD_TOO_LARGE, "requested range too large"))?;

    let mut slices = Vec::new();
    let mut planned_len: usize = 0;
    for (chunk_index, block_hash) in block_hashes.iter().enumerate() {
        let chunk_start = if block_hashes.len() == 1 {
            0
        } else {
            chunk_index as u64 * chunk_size as u64
        };

        if chunk_start >= file.size {
            break;
        }

        let chunk_end_from_manifest = if block_hashes.len() == 1 {
            file.size.saturating_sub(1)
        } else {
            let capped_end = chunk_start
                .saturating_add(chunk_size as u64)
                .saturating_sub(1);
            capped_end.min(file.size.saturating_sub(1))
        };

        if chunk_end_from_manifest < range.start || chunk_start > range.end {
            continue;
        }

        let within_start = range.start.saturating_sub(chunk_start) as usize;
        let within_end = if range.end < chunk_end_from_manifest {
            range.end.saturating_sub(chunk_start) as usize
        } else {
            usize::try_from(chunk_end_from_manifest.saturating_sub(chunk_start)).unwrap_or(0)
        };

        let declared_chunk_len_u64 = chunk_end_from_manifest
            .saturating_sub(chunk_start)
            .saturating_add(1);
        let declared_chunk_len = usize::try_from(declared_chunk_len_u64)
            .map_err(|_| plain(StatusCode::BAD_GATEWAY, "chunk bounds mismatch"))?;
        if within_start >= declared_chunk_len
            || within_end >= declared_chunk_len
            || within_start > within_end
        {
            return Err(plain(StatusCode::BAD_GATEWAY, "chunk bounds mismatch"));
        }

        planned_len = planned_len.saturating_add(within_end.saturating_sub(within_start) + 1);
        slices.push(ChunkSlice {
            block_hash: block_hash.clone(),
            start: within_start,
            end: within_end,
        });
    }

    if planned_len != expected_len {
        return Err(plain(StatusCode::BAD_GATEWAY, "incomplete file range"));
    }

    Ok(slices)
}

#[allow(clippy::result_large_err)]
fn stream_file_range(
    rpc_tx: mpsc::Sender<RpcCommand>,
    file: FileEntry,
    range: ByteRange,
) -> std::result::Result<Body, Response> {
    if file.size == 0 {
        return Ok(Body::empty());
    }
    let slices = plan_range_slices(&file, range)?;
    let (tx, rx) = tokio::sync::mpsc::channel::<std::result::Result<Bytes, io::Error>>(8);

    tokio::spawn(async move {
        let verify_full_file = range.start == 0 && range.end.saturating_add(1) == file.size;
        let mut full_file_hasher = verify_full_file.then(Sha256::new);

        for slice in slices {
            let chunk_bytes = match fetch_block_bytes(&rpc_tx, &slice.block_hash).await {
                Ok(bytes) => bytes,
                Err(_) => {
                    let _ = tx
                        .send(Err(io::Error::other("failed to fetch block bytes")))
                        .await;
                    break;
                }
            };

            let actual_hash = hex::encode(Sha256::digest(&chunk_bytes));
            if actual_hash != slice.block_hash {
                let _ = tx.send(Err(io::Error::other("block hash mismatch"))).await;
                break;
            }

            if slice.start >= chunk_bytes.len()
                || slice.end >= chunk_bytes.len()
                || slice.start > slice.end
            {
                let _ = tx
                    .send(Err(io::Error::other("chunk bounds mismatch")))
                    .await;
                break;
            }

            let slice_bytes = &chunk_bytes[slice.start..=slice.end];
            if let Some(hasher) = full_file_hasher.as_mut() {
                hasher.update(slice_bytes);
            }
            if tx
                .send(Ok(Bytes::copy_from_slice(slice_bytes)))
                .await
                .is_err()
            {
                break;
            }
        }

        if let Some(hasher) = full_file_hasher.take() {
            let actual_file_hash = hex::encode(hasher.finalize());
            if actual_file_hash != file.hash {
                let _ = tx.send(Err(io::Error::other("file hash mismatch"))).await;
            }
        }
    });

    Ok(Body::from_stream(ReceiverStream::new(rx)))
}

async fn fetch_block_bytes(
    rpc_tx: &mpsc::Sender<RpcCommand>,
    block_hash: &str,
) -> std::result::Result<Vec<u8>, Response> {
    HTTP_BLOCK_FETCH_TOTAL.fetch_add(1, Ordering::Relaxed);
    let (resp_tx, resp_rx) = oneshot::channel();
    let send_result = timeout(
        RPC_SEND_TIMEOUT,
        rpc_tx.send(RpcCommand::GetBlock {
            hash: block_hash.to_string(),
            respond_to: resp_tx,
        }),
    )
    .await;

    match send_result {
        Ok(Ok(())) => {}
        Ok(Err(_)) => {
            HTTP_BLOCK_FETCH_ERRORS_TOTAL.fetch_add(1, Ordering::Relaxed);
            return Err(plain(StatusCode::BAD_GATEWAY, "lattice daemon error"));
        }
        Err(_) => {
            HTTP_BLOCK_FETCH_ERRORS_TOTAL.fetch_add(1, Ordering::Relaxed);
            return Err(plain(StatusCode::GATEWAY_TIMEOUT, "lattice daemon timeout"));
        }
    }

    let encoded = match timeout(RPC_RESPONSE_TIMEOUT, resp_rx).await {
        Err(_) => {
            HTTP_BLOCK_FETCH_ERRORS_TOTAL.fetch_add(1, Ordering::Relaxed);
            return Err(plain(StatusCode::GATEWAY_TIMEOUT, "lattice daemon timeout"));
        }
        Ok(Ok(Some(encoded))) => encoded,
        Ok(Ok(None)) => {
            HTTP_BLOCK_FETCH_ERRORS_TOTAL.fetch_add(1, Ordering::Relaxed);
            return Err(plain_owned(
                StatusCode::NOT_FOUND,
                format!("block missing: {block_hash}"),
            ));
        }
        Ok(Err(_)) => {
            HTTP_BLOCK_FETCH_ERRORS_TOTAL.fetch_add(1, Ordering::Relaxed);
            return Err(plain(StatusCode::BAD_GATEWAY, "lattice daemon error"));
        }
    };

    let stored_bytes = match hex::decode(encoded.trim()) {
        Ok(bytes) => bytes,
        Err(_) => {
            HTTP_BLOCK_FETCH_ERRORS_TOTAL.fetch_add(1, Ordering::Relaxed);
            return Err(plain(StatusCode::BAD_GATEWAY, "invalid block encoding"));
        }
    };

    Ok(resolve_block_bytes(&stored_bytes, block_hash))
}

fn file_block_hashes(file: &FileEntry) -> Vec<String> {
    if !file.chunks.is_empty() {
        return file.chunks.clone();
    }
    vec![file.hash.clone()]
}

fn decode_block_storage(stored: &[u8]) -> Option<Vec<u8>> {
    let hex = std::str::from_utf8(stored).ok()?.trim();
    hex::decode(hex).ok()
}

fn resolve_block_bytes(stored: &[u8], expected_hash: &str) -> Vec<u8> {
    if let Some(decoded) = decode_block_storage(stored) {
        let decoded_hash = hex::encode(Sha256::digest(&decoded));
        if decoded_hash == expected_hash {
            return decoded;
        }
    }
    stored.to_vec()
}

fn normalize_path(path: &str) -> Option<String> {
    if path.is_empty() || path == "/" {
        return Some("index.html".to_string());
    }

    let trimmed = path.trim_start_matches('/');
    if trimmed.is_empty() {
        return Some("index.html".to_string());
    }

    let normalized = if trimmed.ends_with('/') {
        format!("{trimmed}index.html")
    } else {
        trimmed.to_string()
    };

    let normalized_path = std::path::Path::new(&normalized);
    if normalized_path.is_absolute() {
        return None;
    }
    for component in normalized_path.components() {
        if matches!(
            component,
            std::path::Component::ParentDir
                | std::path::Component::CurDir
                | std::path::Component::RootDir
                | std::path::Component::Prefix(_)
        ) {
            return None;
        }
    }
    Some(normalized)
}

fn extract_site_name(host: &str, uri: &axum::http::Uri) -> Option<String> {
    if let Some(name) = parse_site_name_from_host(host) {
        return Some(name);
    }
    uri.host().and_then(parse_site_name_from_host)
}

fn parse_site_name_from_host(raw_host: &str) -> Option<String> {
    let host = raw_host
        .trim()
        .trim_end_matches('.')
        .split(':')
        .next()
        .unwrap_or(raw_host)
        .to_ascii_lowercase();
    match host.strip_suffix(".lat") {
        Some(name) if !name.is_empty() => Some(name.to_string()),
        _ => None,
    }
}

fn infer_mime_type(path: &str) -> &'static str {
    let ext = std::path::Path::new(path)
        .extension()
        .and_then(|e| e.to_str())
        .map(|e| e.to_ascii_lowercase());

    match ext.as_deref() {
        Some("html") => "text/html",
        Some("css") => "text/css",
        Some("js") => "application/javascript",
        Some("png") => "image/png",
        Some("jpg") | Some("jpeg") => "image/jpeg",
        Some("gif") => "image/gif",
        Some("svg") => "image/svg+xml",
        Some("ico") => "image/x-icon",
        Some("mp4") => "video/mp4",
        Some("webm") => "video/webm",
        Some("mp3") => "audio/mpeg",
        Some("wav") => "audio/wav",
        _ => "application/octet-stream",
    }
}

fn file_response(
    mime_type: &str,
    body: Body,
    content_length: u64,
    total_size: u64,
    range: Option<ByteRange>,
) -> Response {
    let mut headers = HeaderMap::new();
    let content_type = HeaderValue::from_str(mime_type)
        .unwrap_or_else(|_| HeaderValue::from_static("application/octet-stream"));
    headers.insert(header::CONTENT_TYPE, content_type);
    headers.insert(
        header::ACCESS_CONTROL_ALLOW_ORIGIN,
        HeaderValue::from_static("*"),
    );
    headers.insert(header::ACCEPT_RANGES, HeaderValue::from_static("bytes"));

    let len_header = HeaderValue::from_str(&content_length.to_string())
        .unwrap_or_else(|_| HeaderValue::from_static("0"));
    headers.insert(header::CONTENT_LENGTH, len_header);

    if let Some(range) = range {
        let content_range = format!("bytes {}-{}/{}", range.start, range.end, total_size);
        if let Ok(value) = HeaderValue::from_str(&content_range) {
            headers.insert(header::CONTENT_RANGE, value);
        }
        (StatusCode::PARTIAL_CONTENT, headers, body).into_response()
    } else {
        (StatusCode::OK, headers, body).into_response()
    }
}

fn range_not_satisfiable(total_size: u64) -> Response {
    let mut headers = HeaderMap::new();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("text/plain; charset=utf-8"),
    );
    headers.insert(
        header::ACCESS_CONTROL_ALLOW_ORIGIN,
        HeaderValue::from_static("*"),
    );
    if let Ok(value) = HeaderValue::from_str(&format!("bytes */{total_size}")) {
        headers.insert(header::CONTENT_RANGE, value);
    }
    (
        StatusCode::RANGE_NOT_SATISFIABLE,
        headers,
        "invalid byte range".to_string(),
    )
        .into_response()
}

async fn metrics_endpoint() -> Response {
    let body = format!(
        concat!(
            "# HELP lattice_http_requests_total Total HTTP requests.\n",
            "# TYPE lattice_http_requests_total counter\n",
            "lattice_http_requests_total {}\n",
            "# HELP lattice_http_errors_total Total HTTP errors.\n",
            "# TYPE lattice_http_errors_total counter\n",
            "lattice_http_errors_total {}\n",
            "# HELP lattice_http_range_requests_total Total requests with Range header.\n",
            "# TYPE lattice_http_range_requests_total counter\n",
            "lattice_http_range_requests_total {}\n",
            "# HELP lattice_http_range_416_total Total unsatisfiable range responses.\n",
            "# TYPE lattice_http_range_416_total counter\n",
            "lattice_http_range_416_total {}\n",
            "# HELP lattice_http_block_fetch_total Total block fetches.\n",
            "# TYPE lattice_http_block_fetch_total counter\n",
            "lattice_http_block_fetch_total {}\n",
            "# HELP lattice_http_block_fetch_errors_total Total block fetch failures.\n",
            "# TYPE lattice_http_block_fetch_errors_total counter\n",
            "lattice_http_block_fetch_errors_total {}\n",
            "# HELP lattice_http_owner_mismatch_total Manifest/name-owner mismatches.\n",
            "# TYPE lattice_http_owner_mismatch_total counter\n",
            "lattice_http_owner_mismatch_total {}\n"
        ),
        HTTP_REQUESTS_TOTAL.load(Ordering::Relaxed),
        HTTP_ERRORS_TOTAL.load(Ordering::Relaxed),
        HTTP_RANGE_REQUESTS_TOTAL.load(Ordering::Relaxed),
        HTTP_RANGE_416_TOTAL.load(Ordering::Relaxed),
        HTTP_BLOCK_FETCH_TOTAL.load(Ordering::Relaxed),
        HTTP_BLOCK_FETCH_ERRORS_TOTAL.load(Ordering::Relaxed),
        HTTP_OWNER_MISMATCH_TOTAL.load(Ordering::Relaxed),
    );

    let mut headers = HeaderMap::new();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("text/plain; version=0.0.4"),
    );
    headers.insert(
        header::ACCESS_CONTROL_ALLOW_ORIGIN,
        HeaderValue::from_static("*"),
    );
    (StatusCode::OK, headers, body).into_response()
}

fn record_http_error(status: StatusCode) {
    if status.is_client_error() || status.is_server_error() {
        HTTP_ERRORS_TOTAL.fetch_add(1, Ordering::Relaxed);
    }
    if status == StatusCode::RANGE_NOT_SATISFIABLE {
        HTTP_RANGE_416_TOTAL.fetch_add(1, Ordering::Relaxed);
    }
}

fn is_safe_manifest_path(path: &str) -> bool {
    if path.is_empty() {
        return false;
    }
    let path = std::path::Path::new(path);
    if path.is_absolute() {
        return false;
    }
    for component in path.components() {
        if matches!(
            component,
            std::path::Component::ParentDir
                | std::path::Component::CurDir
                | std::path::Component::RootDir
                | std::path::Component::Prefix(_)
        ) {
            return false;
        }
    }
    true
}

fn plain(status: StatusCode, msg: &'static str) -> Response {
    plain_owned(status, msg.to_string())
}

fn plain_owned(status: StatusCode, msg: String) -> Response {
    let mut headers = HeaderMap::new();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("text/plain; charset=utf-8"),
    );
    headers.insert(
        header::ACCESS_CONTROL_ALLOW_ORIGIN,
        HeaderValue::from_static("*"),
    );
    (status, headers, msg).into_response()
}
