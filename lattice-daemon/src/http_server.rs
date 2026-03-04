use anyhow::{Context, Result};
use axum::extract::{Host, OriginalUri, State};
use axum::http::{header, HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::Router;
use sha2::{Digest, Sha256};
use tokio::sync::{mpsc, oneshot};
use tokio::time::{timeout, Duration};
use tower_http::cors::{Any, CorsLayer};
use tracing::warn;

use lattice_site::manifest::{
    hash_bytes, verify_manifest, FileEntry, SiteManifest, DEFAULT_CHUNK_SIZE_BYTES,
};

use crate::rpc::RpcCommand;

const RPC_SEND_TIMEOUT: Duration = Duration::from_secs(10);
const RPC_RESPONSE_TIMEOUT: Duration = Duration::from_secs(20);

pub async fn start_http_server(port: u16, rpc_tx: mpsc::Sender<RpcCommand>) -> Result<()> {
    let app = Router::new()
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
    let site_name = match extract_site_name(&host, &uri) {
        Some(name) => name,
        None => {
            return plain(
                StatusCode::NOT_FOUND,
                "site not found on Lattice (expected *.lat host)",
            );
        }
    };

    let request_path = normalize_path(uri.path());

    let manifest = match fetch_manifest(&rpc_tx, &site_name).await {
        Ok(manifest) => manifest,
        Err(response) => return response,
    };

    let file = match manifest.files.iter().find(|f| f.path == request_path) {
        Some(file) => file,
        None => return plain(StatusCode::NOT_FOUND, "site not found on Lattice"),
    };

    let requested_range = match parse_requested_range(&headers, file.size) {
        Ok(range) => range,
        Err(response) => return response,
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

    let bytes = match fetch_file_range(&rpc_tx, file, effective_range).await {
        Ok(bytes) => bytes,
        Err(response) => return response,
    };

    let mime_type = infer_mime_type(&file.path);
    if let Some(range) = requested_range {
        file_response(mime_type, bytes, file.size, Some(range))
    } else {
        file_response(mime_type, bytes, file.size, None)
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

    let manifest: SiteManifest = match serde_json::from_str(&manifest_json) {
        Ok(manifest) => manifest,
        Err(_) => return Err(plain(StatusCode::BAD_GATEWAY, "invalid site manifest")),
    };

    if manifest.name != site_name {
        return Err(plain(StatusCode::BAD_GATEWAY, "manifest name mismatch"));
    }

    if let Err(err) = verify_manifest(&manifest) {
        warn!(name = %site_name, error = %err, "manifest signature invalid");
        return Err(plain(StatusCode::BAD_GATEWAY, "manifest signature invalid"));
    }

    Ok(manifest)
}

#[derive(Clone, Copy)]
struct ByteRange {
    start: u64,
    end: u64,
}

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

async fn fetch_file_range(
    rpc_tx: &mpsc::Sender<RpcCommand>,
    file: &FileEntry,
    range: ByteRange,
) -> std::result::Result<Vec<u8>, Response> {
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

    let mut out = Vec::with_capacity((range.end - range.start + 1) as usize);

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

        let chunk_bytes = fetch_block_bytes(rpc_tx, block_hash).await?;
        let actual_hash = hex::encode(Sha256::digest(&chunk_bytes));
        if actual_hash != *block_hash {
            return Err(plain_owned(
                StatusCode::BAD_GATEWAY,
                format!(
                    "block hash mismatch for chunk {}: expected {} got {}",
                    block_hash, block_hash, actual_hash
                ),
            ));
        }

        let within_start = range.start.saturating_sub(chunk_start) as usize;
        let within_end = if range.end < chunk_end_from_manifest {
            range.end.saturating_sub(chunk_start) as usize
        } else {
            chunk_bytes.len().saturating_sub(1)
        };

        if within_start >= chunk_bytes.len()
            || within_end >= chunk_bytes.len()
            || within_start > within_end
        {
            return Err(plain(StatusCode::BAD_GATEWAY, "chunk bounds mismatch"));
        }

        out.extend_from_slice(&chunk_bytes[within_start..=within_end]);
    }

    let expected_len = (range.end - range.start + 1) as usize;
    if out.len() != expected_len {
        return Err(plain(StatusCode::BAD_GATEWAY, "incomplete file range"));
    }

    if range.start == 0 && range.end.saturating_add(1) == file.size {
        let full_hash = hash_bytes(&out);
        if full_hash != file.hash {
            return Err(plain_owned(
                StatusCode::BAD_GATEWAY,
                format!(
                    "file hash mismatch for {}: expected {} got {}",
                    file.path, file.hash, full_hash
                ),
            ));
        }
    }

    Ok(out)
}

async fn fetch_block_bytes(
    rpc_tx: &mpsc::Sender<RpcCommand>,
    block_hash: &str,
) -> std::result::Result<Vec<u8>, Response> {
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
        Ok(Err(_)) => return Err(plain(StatusCode::BAD_GATEWAY, "lattice daemon error")),
        Err(_) => return Err(plain(StatusCode::GATEWAY_TIMEOUT, "lattice daemon timeout")),
    }

    let encoded = match timeout(RPC_RESPONSE_TIMEOUT, resp_rx).await {
        Err(_) => return Err(plain(StatusCode::GATEWAY_TIMEOUT, "lattice daemon timeout")),
        Ok(Ok(Some(encoded))) => encoded,
        Ok(Ok(None)) => {
            return Err(plain_owned(
                StatusCode::NOT_FOUND,
                format!("block missing: {block_hash}"),
            ))
        }
        Ok(Err(_)) => return Err(plain(StatusCode::BAD_GATEWAY, "lattice daemon error")),
    };

    let stored_bytes = match hex::decode(encoded.trim()) {
        Ok(bytes) => bytes,
        Err(_) => return Err(plain(StatusCode::BAD_GATEWAY, "invalid block encoding")),
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

fn normalize_path(path: &str) -> String {
    if path.is_empty() || path == "/" {
        return "index.html".to_string();
    }

    let trimmed = path.trim_start_matches('/');
    if trimmed.is_empty() {
        return "index.html".to_string();
    }

    if trimmed.ends_with('/') {
        format!("{trimmed}index.html")
    } else {
        trimmed.to_string()
    }
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
    body: Vec<u8>,
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

    let len_header = HeaderValue::from_str(&body.len().to_string())
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
