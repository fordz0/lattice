use anyhow::{Context, Result};
use axum::body::{to_bytes, Body, Bytes};
use axum::extract::{Host, Request, State};
use axum::http::{header, HeaderMap, HeaderName, HeaderValue, Method, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{any, get};
use axum::Router;
use axum_server::tls_rustls::RustlsConfig;
use reqwest::redirect::Policy;
use sha2::{Digest, Sha256};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::{mpsc, oneshot};
use tokio::time::{timeout, Duration};
use tower_http::cors::{Any, CorsLayer};
use tracing::warn;

use lattice_site::manifest::{verify_manifest, FileEntry, SiteManifest};

use crate::app_registry::{pid_is_alive, AppRegistry, LocalAppRegistration};
use crate::mime;
use crate::rpc::{GetSiteManifestResponse, RpcCommand};

const RPC_SEND_TIMEOUT: Duration = Duration::from_secs(10);
const RPC_RESPONSE_TIMEOUT: Duration = Duration::from_secs(20);
const MAX_MANIFEST_BYTES: usize = 1024 * 1024;
const MAX_MANIFEST_FILES: usize = 1000;
const MAX_MANIFEST_TOTAL_BYTES: u64 = 100 * 1024 * 1024;
const MAX_HTTP_RESPONSE_BYTES: u64 = 100 * 1024 * 1024;
const MAX_PROXY_REQUEST_BODY_BYTES: usize = 1024 * 1024;
const PROXY_CONNECT_TIMEOUT: Duration = Duration::from_secs(2);
const PROXY_RESPONSE_TIMEOUT: Duration = Duration::from_secs(30);
const LOOM_SUFFIX: &str = ".loom";
const LOCAL_HTTPS_SUFFIX: &str = ".loom.lattice.localhost";

static HTTP_REQUESTS_TOTAL: AtomicU64 = AtomicU64::new(0);
static HTTP_ERRORS_TOTAL: AtomicU64 = AtomicU64::new(0);
static HTTP_RANGE_REQUESTS_TOTAL: AtomicU64 = AtomicU64::new(0);
static HTTP_RANGE_416_TOTAL: AtomicU64 = AtomicU64::new(0);
static HTTP_BLOCK_FETCH_TOTAL: AtomicU64 = AtomicU64::new(0);
static HTTP_BLOCK_FETCH_ERRORS_TOTAL: AtomicU64 = AtomicU64::new(0);
static HTTP_OWNER_MISMATCH_TOTAL: AtomicU64 = AtomicU64::new(0);

#[derive(Clone)]
struct AppState {
    rpc_tx: mpsc::Sender<RpcCommand>,
    ca_cert_pem: Option<String>,
    mime_policy_strict: bool,
    app_registry: AppRegistry,
    proxy_client: reqwest::Client,
}

fn build_app(state: AppState) -> Router {
    Router::new()
        .route("/__lattice_metrics", get(metrics_endpoint))
        .route("/__lattice_ca.pem", get(ca_cert_endpoint))
        .route("/", any(serve_site))
        .route("/*path", any(serve_site))
        .with_state(state)
        .layer(CorsLayer::new().allow_origin(Any))
}

fn build_proxy_client() -> Result<reqwest::Client> {
    reqwest::Client::builder()
        .redirect(Policy::none())
        .connect_timeout(PROXY_CONNECT_TIMEOUT)
        .timeout(PROXY_RESPONSE_TIMEOUT)
        .build()
        .context("failed to build local app proxy client")
}

pub async fn start_http_server(
    port: u16,
    rpc_tx: mpsc::Sender<RpcCommand>,
    ca_cert_pem: Option<String>,
    mime_policy_strict: bool,
    app_registry: AppRegistry,
) -> Result<()> {
    let app = build_app(AppState {
        rpc_tx,
        ca_cert_pem,
        mime_policy_strict,
        app_registry,
        proxy_client: build_proxy_client()?,
    });

    let listen_addr = format!("127.0.0.1:{port}");
    let listener = tokio::net::TcpListener::bind(&listen_addr)
        .await
        .with_context(|| format!("failed to bind HTTP server on {listen_addr}"))?;

    axum::serve(listener, app)
        .await
        .context("HTTP server stopped unexpectedly")?;

    Ok(())
}

pub async fn start_https_server(
    port: u16,
    rpc_tx: mpsc::Sender<RpcCommand>,
    ca_cert_pem: String,
    cert_path: PathBuf,
    key_path: PathBuf,
    mime_policy_strict: bool,
    app_registry: AppRegistry,
) -> Result<()> {
    let app = build_app(AppState {
        rpc_tx,
        ca_cert_pem: Some(ca_cert_pem),
        mime_policy_strict,
        app_registry,
        proxy_client: build_proxy_client()?,
    });
    let listen_addr = format!("127.0.0.1:{port}");
    let addr: std::net::SocketAddr = listen_addr
        .parse()
        .with_context(|| format!("failed to parse HTTPS listen address {listen_addr}"))?;
    let tls_config = RustlsConfig::from_pem_file(cert_path, key_path)
        .await
        .context("failed to load HTTPS cert/key")?;

    axum_server::bind_rustls(addr, tls_config)
        .serve(app.into_make_service())
        .await
        .context("HTTPS server stopped unexpectedly")?;
    Ok(())
}

async fn serve_site(
    Host(host): Host,
    State(state): State<AppState>,
    request: Request,
) -> impl IntoResponse {
    let (parts, body) = request.into_parts();
    let method = parts.method.clone();
    let uri = parts.uri.clone();
    let headers = parts.headers.clone();

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
                "site not found on Lattice (expected *.loom host)",
            ));
        }
    };

    let request_path = match normalize_path(uri.path()) {
        Some(path) => path,
        None => return fail(plain(StatusCode::BAD_REQUEST, "invalid path")),
    };

    let manifest = match fetch_manifest(&state.rpc_tx, &site_name).await {
        Ok(manifest) => manifest,
        Err(response) => return fail(response),
    };

    if let Some(registration) = state.app_registry.get(&site_name) {
        if path_matches_proxy_prefix(uri.path(), &registration.proxy_paths) {
            let request_body = match to_bytes(body, MAX_PROXY_REQUEST_BODY_BYTES).await {
                Ok(bytes) => bytes,
                Err(_) => return fail(plain(StatusCode::PAYLOAD_TOO_LARGE, "payload too large")),
            };
            let response = match proxy_local_app_request(
                &state,
                &manifest,
                &site_name,
                &registration,
                method,
                &uri,
                &headers,
                request_body,
            )
            .await
            {
                Ok(response) => response,
                Err(response) => response,
            };
            return fail(response);
        }
    }

    if method != Method::GET && method != Method::HEAD {
        return fail(plain(StatusCode::METHOD_NOT_ALLOWED, "method not allowed"));
    }

    let file = match find_manifest_file_or_index(&manifest.files, &request_path) {
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
    let file_bytes = match load_file_bytes(state.rpc_tx.clone(), &site_name, &file).await {
        Ok(bytes) => bytes,
        Err(response) => return fail(response),
    };
    let detected_mime = mime::detect_mime(&file.path, &file_bytes);
    let violation = mime::violation_reason(&detected_mime, file_bytes.len());
    if let Some(reason) = violation.as_deref() {
        warn!(
            filename = %file.path,
            detected_mime = %detected_mime,
            file_size = file_bytes.len(),
            reason = %reason,
            "MIME policy violation while serving site file"
        );
        if state.mime_policy_strict {
            return fail(plain(StatusCode::FORBIDDEN, "file rejected by MIME policy"));
        }
    }
    let body = match slice_file_range(&file_bytes, effective_range) {
        Ok(body) => body,
        Err(response) => return fail(response),
    };
    if let Some(range) = requested_range {
        file_response(&detected_mime, body, content_length, file.size, Some(range))
    } else {
        file_response(&detected_mime, body, content_length, file.size, None)
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

    let manifest_response = match timeout(RPC_RESPONSE_TIMEOUT, resp_rx).await {
        Err(_) => return Err(plain(StatusCode::GATEWAY_TIMEOUT, "lattice daemon timeout")),
        Ok(Ok(Some(manifest_response))) => manifest_response,
        Ok(Ok(None)) => return Err(plain(StatusCode::NOT_FOUND, "site not found on Lattice")),
        Ok(Err(_)) => return Err(plain(StatusCode::BAD_GATEWAY, "lattice daemon error")),
    };
    let GetSiteManifestResponse { manifest_json, .. } = manifest_response;
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

async fn proxy_local_app_request(
    state: &AppState,
    manifest: &SiteManifest,
    site_name: &str,
    registration: &LocalAppRegistration,
    method: Method,
    uri: &axum::http::Uri,
    headers: &HeaderMap,
    body: Bytes,
) -> std::result::Result<Response, Response> {
    let Some(app) = manifest.app.as_ref() else {
        return Err(plain(StatusCode::BAD_GATEWAY, "app proxy port mismatch"));
    };
    if registration.proxy_port != app.proxy_port {
        return Err(plain(StatusCode::BAD_GATEWAY, "app proxy port mismatch"));
    }
    if registration.proxy_port < 1024 {
        return Err(plain(StatusCode::BAD_GATEWAY, "app proxy port mismatch"));
    }
    if !pid_is_alive(registration.pid) {
        let _ = state
            .app_registry
            .unregister(site_name, registration.pid);
        return Err(plain(
            StatusCode::SERVICE_UNAVAILABLE,
            "local app is not running",
        ));
    }

    let path_and_query = uri
        .path_and_query()
        .map(|value| value.as_str())
        .unwrap_or(uri.path());
    let upstream_url = format!("http://127.0.0.1:{}{}", registration.proxy_port, path_and_query);
    let mut upstream = state
        .proxy_client
        .request(method, &upstream_url)
        .header(header::HOST, format!("127.0.0.1:{}", registration.proxy_port))
        .header("X-Lattice-Site", site_name);

    for (name, value) in headers {
        if should_strip_proxy_header(name) || name == header::HOST {
            continue;
        }
        upstream = upstream.header(name, value);
    }

    let upstream_response = match upstream.body(body).send().await {
        Ok(response) => response,
        Err(err) if err.is_connect() || err.is_timeout() => {
            return Err(plain(
                StatusCode::SERVICE_UNAVAILABLE,
                "local app is not running",
            ));
        }
        Err(_) => return Err(plain(StatusCode::BAD_GATEWAY, "local app proxy failed")),
    };

    let status = upstream_response.status();
    let response_headers = upstream_response.headers().clone();
    let response_body = match upstream_response.bytes().await {
        Ok(bytes) => bytes,
        Err(_) => return Err(plain(StatusCode::BAD_GATEWAY, "local app proxy failed")),
    };

    let mut final_response = Response::new(Body::from(response_body));
    *final_response.status_mut() = status;
    let final_headers = final_response.headers_mut();
    for (name, value) in response_headers.iter() {
        final_headers.append(name, value.clone());
    }
    final_headers.insert(
        header::ACCESS_CONTROL_ALLOW_ORIGIN,
        HeaderValue::from_static("*"),
    );
    Ok(final_response)
}

fn should_strip_proxy_header(name: &HeaderName) -> bool {
    name.as_str().eq_ignore_ascii_case("x-forwarded-for")
        || name.as_str().eq_ignore_ascii_case("x-real-ip")
        || name.as_str().eq_ignore_ascii_case("x-forwarded-host")
        || name.as_str().eq_ignore_ascii_case("x-forwarded-proto")
        || name.as_str().eq_ignore_ascii_case("connection")
        || name.as_str().eq_ignore_ascii_case("content-length")
        || name.as_str().eq_ignore_ascii_case("transfer-encoding")
}

fn path_matches_proxy_prefix(path: &str, prefixes: &[String]) -> bool {
    prefixes.iter().any(|prefix| {
        path == prefix
            || path
                .strip_prefix(prefix.as_str())
                .map(|rest| rest.is_empty() || rest.starts_with('/'))
                .unwrap_or(false)
    })
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

async fn load_file_bytes(
    rpc_tx: mpsc::Sender<RpcCommand>,
    site_name: &str,
    file: &FileEntry,
) -> std::result::Result<Vec<u8>, Response> {
    if file.size == 0 {
        return Ok(Vec::new());
    }

    let block_hashes = file_block_hashes(file);
    if block_hashes.is_empty() {
        return Err(plain(StatusCode::BAD_GATEWAY, "invalid site manifest"));
    }

    let mut bytes = Vec::with_capacity(file.size.min(MAX_HTTP_RESPONSE_BYTES) as usize);
    for block_hash in block_hashes {
        let chunk_bytes = fetch_block_bytes(&rpc_tx, site_name, &block_hash).await?;
        let actual_hash = hex::encode(Sha256::digest(&chunk_bytes));
        if actual_hash != block_hash {
            return Err(plain(StatusCode::BAD_GATEWAY, "block hash mismatch"));
        }
        bytes.extend_from_slice(&chunk_bytes);
        if bytes.len() as u64 > MAX_HTTP_RESPONSE_BYTES {
            return Err(plain(
                StatusCode::PAYLOAD_TOO_LARGE,
                "requested file too large",
            ));
        }
    }

    if bytes.len() as u64 != file.size {
        return Err(plain(StatusCode::BAD_GATEWAY, "file size mismatch"));
    }
    let actual_file_hash = hex::encode(Sha256::digest(&bytes));
    if actual_file_hash != file.hash {
        return Err(plain(StatusCode::BAD_GATEWAY, "file hash mismatch"));
    }

    Ok(bytes)
}

#[allow(clippy::result_large_err)]
fn slice_file_range(bytes: &[u8], range: ByteRange) -> std::result::Result<Body, Response> {
    if bytes.is_empty() {
        return Ok(Body::empty());
    }
    let start = usize::try_from(range.start)
        .map_err(|_| plain(StatusCode::BAD_GATEWAY, "invalid range start"))?;
    let end = usize::try_from(range.end)
        .map_err(|_| plain(StatusCode::BAD_GATEWAY, "invalid range end"))?;
    if start >= bytes.len() || end >= bytes.len() || start > end {
        return Err(plain(StatusCode::BAD_GATEWAY, "invalid range bounds"));
    }
    Ok(Body::from(Bytes::copy_from_slice(&bytes[start..=end])))
}

async fn fetch_block_bytes(
    rpc_tx: &mpsc::Sender<RpcCommand>,
    site_name: &str,
    block_hash: &str,
) -> std::result::Result<Vec<u8>, Response> {
    HTTP_BLOCK_FETCH_TOTAL.fetch_add(1, Ordering::Relaxed);
    let (resp_tx, resp_rx) = oneshot::channel();
    let send_result = timeout(
        RPC_SEND_TIMEOUT,
        rpc_tx.send(RpcCommand::GetBlock {
            hash: block_hash.to_string(),
            site_key: Some(format!("site:{site_name}")),
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

    Ok(stored_bytes)
}

fn file_block_hashes(file: &FileEntry) -> Vec<String> {
    if !file.chunks.is_empty() {
        return file.chunks.clone();
    }
    vec![file.hash.clone()]
}

fn normalize_path(path: &str) -> Option<String> {
    if path.is_empty() || path == "/" {
        return Some("index.html".to_string());
    }

    if path.contains('\0') || path.contains('\\') {
        return None;
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

fn find_manifest_file<'a>(files: &'a [FileEntry], request_path: &str) -> Option<&'a FileEntry> {
    if let Some(file) = files.iter().find(|f| f.path == request_path) {
        return Some(file);
    }

    for candidate in manifest_path_fallbacks(request_path) {
        if let Some(file) = files.iter().find(|f| f.path == candidate) {
            return Some(file);
        }
    }

    None
}

fn find_manifest_file_or_index<'a>(
    files: &'a [FileEntry],
    request_path: &str,
) -> Option<&'a FileEntry> {
    find_manifest_file(files, request_path)
        .or_else(|| files.iter().find(|f| f.path == "index.html"))
}

fn manifest_path_fallbacks(request_path: &str) -> Vec<String> {
    if request_path == "index.html" {
        return Vec::new();
    }

    let path = std::path::Path::new(request_path);
    if path.extension().is_some() {
        return Vec::new();
    }

    vec![
        format!("{request_path}/index.html"),
        format!("{request_path}.html"),
    ]
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
    if let Some(name) = host.strip_suffix(LOCAL_HTTPS_SUFFIX) {
        if is_valid_site_label(name) {
            return Some(name.to_string());
        }
    }
    if let Some(name) = host.strip_suffix(LOOM_SUFFIX) {
        if is_valid_site_label(name) {
            return Some(name.to_string());
        }
    }
    None
}

fn is_valid_site_label(name: &str) -> bool {
    if name.is_empty() || name.len() > 63 {
        return false;
    }
    if name.starts_with('-') || name.ends_with('-') || name.contains('.') {
        return false;
    }
    name.chars()
        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
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

async fn ca_cert_endpoint(State(state): State<AppState>) -> Response {
    let Some(ca_cert_pem) = state.ca_cert_pem else {
        return plain(StatusCode::NOT_FOUND, "lattice local CA unavailable");
    };

    let mut headers = HeaderMap::new();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/x-pem-file"),
    );
    headers.insert(
        header::CONTENT_DISPOSITION,
        HeaderValue::from_static("attachment; filename=\"lattice-local-ca.pem\""),
    );
    headers.insert(
        header::ACCESS_CONTROL_ALLOW_ORIGIN,
        HeaderValue::from_static("*"),
    );
    (StatusCode::OK, headers, ca_cert_pem).into_response()
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
    if path.contains('\0') || path.contains('\\') {
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

#[cfg(test)]
mod tests {
    use super::*;

    fn file(path: &str) -> FileEntry {
        FileEntry {
            path: path.to_string(),
            hash: String::new(),
            size: 0,
            chunks: Vec::new(),
            chunk_size: None,
        }
    }

    #[test]
    fn matches_exact_manifest_path_first() {
        let files = vec![file("projects"), file("projects/index.html")];
        let matched = find_manifest_file(&files, "projects").expect("match");
        assert_eq!(matched.path, "projects");
    }

    #[test]
    fn falls_back_to_directory_index_html() {
        let files = vec![file("index.html"), file("projects/index.html")];
        let matched = find_manifest_file(&files, "projects").expect("match");
        assert_eq!(matched.path, "projects/index.html");
    }

    #[test]
    fn falls_back_to_flat_html_file() {
        let files = vec![file("index.html"), file("projects.html")];
        let matched = find_manifest_file(&files, "projects").expect("match");
        assert_eq!(matched.path, "projects.html");
    }

    #[test]
    fn falls_back_to_site_index_for_spa_routes() {
        let files = vec![file("index.html"), file("assets/app.js")];
        let matched = find_manifest_file_or_index(&files, "f/lattice/thread-1").expect("match");
        assert_eq!(matched.path, "index.html");
    }

    #[test]
    fn does_not_apply_html_fallback_for_extension_paths() {
        let files = vec![file("assets/app.js"), file("assets/app.js/index.html")];
        let matched = find_manifest_file(&files, "assets/app.js").expect("match");
        assert_eq!(matched.path, "assets/app.js");
    }

    #[test]
    fn normalize_path_rejects_backslashes_and_nul() {
        assert!(normalize_path("/a\\b").is_none());
        assert!(normalize_path("/a\0b").is_none());
    }

    #[test]
    fn safe_manifest_path_rejects_backslashes_and_nul() {
        assert!(!is_safe_manifest_path("a\\b"));
        assert!(!is_safe_manifest_path("a\0b"));
    }

    #[test]
    fn parses_loom_host_suffixes() {
        assert_eq!(
            parse_site_name_from_host("benjf.loom"),
            Some("benjf".to_string())
        );
        assert_eq!(
            parse_site_name_from_host("benjf.loom.lattice.localhost:7443"),
            Some("benjf".to_string())
        );
    }

    #[test]
    fn rejects_invalid_loom_hosts() {
        assert_eq!(parse_site_name_from_host("bad.name.loom"), None);
        assert_eq!(parse_site_name_from_host("_bad.loom"), None);
        assert_eq!(parse_site_name_from_host("-bad.loom"), None);
        assert_eq!(parse_site_name_from_host("bad-.loom"), None);
        assert_eq!(parse_site_name_from_host(".loom"), None);
    }

    #[test]
    fn proxy_prefix_matches_exact_path() {
        assert!(path_matches_proxy_prefix("/api", &["/api".to_string()]));
    }

    #[test]
    fn proxy_prefix_matches_trailing_slash_children() {
        assert!(path_matches_proxy_prefix("/api/v1/posts", &["/api".to_string()]));
        assert!(path_matches_proxy_prefix("/api/", &["/api".to_string()]));
    }

    #[test]
    fn proxy_prefix_rejects_non_matching_prefixes() {
        assert!(!path_matches_proxy_prefix("/apix/test", &["/api".to_string()]));
        assert!(!path_matches_proxy_prefix("/other", &["/api".to_string()]));
    }
}
