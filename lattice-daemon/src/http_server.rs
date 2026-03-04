use anyhow::{Context, Result};
use axum::extract::{Host, OriginalUri, State};
use axum::http::{header, HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::Router;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use base64::Engine as _;
use tokio::sync::{mpsc, oneshot};
use tokio::time::{timeout, Duration};
use tower_http::cors::{Any, CorsLayer};
use tracing::warn;

use crate::rpc::RpcCommand;

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

    let (resp_tx, resp_rx) = oneshot::channel();
    let send_result = timeout(
        Duration::from_secs(10),
        rpc_tx.send(RpcCommand::GetSite {
            name: site_name,
            respond_to: resp_tx,
        }),
    )
    .await;
    match send_result {
        Ok(Ok(())) => {}
        Ok(Err(_)) => return plain(StatusCode::BAD_GATEWAY, "lattice daemon error"),
        Err(_) => return plain(StatusCode::GATEWAY_TIMEOUT, "lattice daemon timeout"),
    }

    let site = match timeout(Duration::from_secs(20), resp_rx).await {
        Err(_) => return plain(StatusCode::GATEWAY_TIMEOUT, "lattice daemon timeout"),
        Ok(Ok(Ok(site))) => site,
        Ok(Ok(Err(err))) => {
            if err == "site not found" {
                return plain(StatusCode::NOT_FOUND, "site not found on Lattice");
            }
            if err == "too many concurrent requests" {
                return plain(StatusCode::SERVICE_UNAVAILABLE, "lattice daemon busy");
            }
            if err.starts_with("block missing:") {
                return plain_owned(StatusCode::NOT_FOUND, err);
            }
            warn!(error = %err, "get_site failed");
            return plain_owned(
                StatusCode::BAD_GATEWAY,
                format!("lattice daemon error: {err}"),
            );
        }
        Ok(Err(_)) => return plain(StatusCode::BAD_GATEWAY, "lattice daemon error"),
    };

    let file = match site.files.iter().find(|f| f.path == request_path) {
        Some(file) => file,
        None => return plain(StatusCode::NOT_FOUND, "site not found on Lattice"),
    };

    let bytes = match BASE64_STANDARD.decode(file.contents.as_bytes()) {
        Ok(bytes) => bytes,
        Err(_) => return plain(StatusCode::BAD_GATEWAY, "lattice daemon error"),
    };

    file_response(&file.mime_type, bytes)
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

fn file_response(mime_type: &str, body: Vec<u8>) -> Response {
    let mut headers = HeaderMap::new();
    let content_type = HeaderValue::from_str(mime_type)
        .unwrap_or_else(|_| HeaderValue::from_static("application/octet-stream"));
    headers.insert(header::CONTENT_TYPE, content_type);
    headers.insert(
        header::ACCESS_CONTROL_ALLOW_ORIGIN,
        HeaderValue::from_static("*"),
    );

    (StatusCode::OK, headers, body).into_response()
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
