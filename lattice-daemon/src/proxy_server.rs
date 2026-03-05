use anyhow::{Context, Result};
use rcgen::{BasicConstraints, Certificate, CertificateParams, DistinguishedName, DnType, IsCa};
use std::fmt::Write as _;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio_rustls::TlsAcceptor;

const MAX_PROXY_HEADER_BYTES: usize = 64 * 1024;

pub async fn start_proxy_server(port: u16, http_port: u16, ca_key_pem: String) -> Result<()> {
    let listen_addr = format!("127.0.0.1:{port}");
    let listener = tokio::net::TcpListener::bind(&listen_addr)
        .await
        .with_context(|| format!("failed to bind proxy server on {listen_addr}"))?;

    loop {
        let (stream, _addr) = listener
            .accept()
            .await
            .context("proxy server accept failed")?;
        let ca_key = ca_key_pem.clone();
        tokio::spawn(async move {
            if let Err(err) = handle_connection(stream, http_port, &ca_key).await {
                tracing::warn!(error = %err, "proxy connection failed");
            }
        });
    }
}

async fn handle_connection(
    mut client: tokio::net::TcpStream,
    http_port: u16,
    ca_key_pem: &str,
) -> Result<()> {
    let mut buffer = Vec::with_capacity(4096);
    let header_end = read_until_headers_complete(&mut client, &mut buffer).await?;
    let head = std::str::from_utf8(&buffer[..header_end]).context("proxy request not utf8")?;

    let mut lines = head.split("\r\n");
    let request_line = lines.next().unwrap_or_default();
    let mut parts = request_line.split_whitespace();
    let method = parts.next().unwrap_or_default();
    let target = parts.next().unwrap_or_default();
    let version = parts.next().unwrap_or("HTTP/1.1");

    if method.eq_ignore_ascii_case("CONNECT") {
        handle_connect(client, target, http_port, ca_key_pem).await?;
        return Ok(());
    }

    forward_http_request(
        &mut client,
        method,
        target,
        version,
        lines,
        &buffer[header_end..],
        http_port,
        None,
    )
    .await
}

async fn handle_connect(
    mut client: tokio::net::TcpStream,
    target: &str,
    http_port: u16,
    ca_key_pem: &str,
) -> Result<()> {
    let (host, port) = parse_connect_target(target)?;
    if port != 443 || !is_loom_host(&host) {
        write_response(
            &mut client,
            403,
            "Forbidden",
            b"proxy only allows CONNECT to *.loom:443",
        )
        .await?;
        return Ok(());
    }

    client
        .write_all(b"HTTP/1.1 200 Connection Established\r\n\r\n")
        .await
        .context("failed to write CONNECT response")?;

    let tls_config = build_tls_server_config(&host, ca_key_pem)?;
    let acceptor = TlsAcceptor::from(Arc::new(tls_config));
    let mut tls_stream = acceptor
        .accept(client)
        .await
        .context("failed to accept tunneled TLS connection")?;

    let mut buffer = Vec::with_capacity(4096);
    let header_end = read_until_headers_complete(&mut tls_stream, &mut buffer).await?;
    let head = std::str::from_utf8(&buffer[..header_end]).context("tunneled request not utf8")?;

    let mut lines = head.split("\r\n");
    let request_line = lines.next().unwrap_or_default();
    let mut parts = request_line.split_whitespace();
    let method = parts.next().unwrap_or_default();
    let target = parts.next().unwrap_or_default();
    let version = parts.next().unwrap_or("HTTP/1.1");

    forward_http_request(
        &mut tls_stream,
        method,
        target,
        version,
        lines,
        &buffer[header_end..],
        http_port,
        Some(&host),
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn forward_http_request<S>(
    client: &mut S,
    method: &str,
    target: &str,
    version: &str,
    header_lines: std::str::Split<'_, &str>,
    tail: &[u8],
    http_port: u16,
    expected_host: Option<&str>,
) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    if !matches!(method, "GET" | "HEAD" | "OPTIONS") {
        write_response(client, 405, "Method Not Allowed", b"method not allowed").await?;
        return Ok(());
    }

    let mut host_header: Option<String> = None;
    let mut passthrough_headers = Vec::new();
    for raw in header_lines {
        if raw.is_empty() {
            continue;
        }
        let Some((name, value)) = raw.split_once(':') else {
            continue;
        };
        let name_trimmed = name.trim();
        let value_trimmed = value.trim();
        if name_trimmed.eq_ignore_ascii_case("Host") {
            host_header = Some(value_trimmed.to_string());
            continue;
        }
        if name_trimmed.eq_ignore_ascii_case("Proxy-Connection")
            || name_trimmed.eq_ignore_ascii_case("Connection")
        {
            continue;
        }
        passthrough_headers.push((name_trimmed.to_string(), value_trimmed.to_string()));
    }

    let (host, path_and_query) = parse_http_target(target, host_header.as_deref())?;
    if !is_loom_host(&host) {
        write_response(client, 403, "Forbidden", b"proxy only allows *.loom").await?;
        return Ok(());
    }
    if let Some(expected) = expected_host {
        if host != expected {
            write_response(
                client,
                400,
                "Bad Request",
                b"host mismatch in CONNECT tunnel",
            )
            .await?;
            return Ok(());
        }
    }

    let mut forward = String::new();
    let _ = write!(forward, "{method} {path_and_query} {version}\r\n");
    let _ = write!(forward, "Host: {host}\r\n");
    for (name, value) in passthrough_headers {
        let _ = write!(forward, "{name}: {value}\r\n");
    }
    forward.push_str("Connection: close\r\n\r\n");

    let mut upstream = tokio::net::TcpStream::connect(("127.0.0.1", http_port))
        .await
        .with_context(|| format!("failed to connect to local HTTP server on {http_port}"))?;
    upstream
        .write_all(forward.as_bytes())
        .await
        .context("failed to forward request headers")?;
    if !tail.is_empty() {
        upstream
            .write_all(tail)
            .await
            .context("failed to forward request tail bytes")?;
    }

    tokio::io::copy(&mut upstream, client)
        .await
        .context("failed to stream upstream response")?;
    Ok(())
}

fn build_tls_server_config(host: &str, ca_key_pem: &str) -> Result<rustls::ServerConfig> {
    let mut ca_params = CertificateParams::default();
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca_params.distinguished_name = DistinguishedName::new();
    ca_params
        .distinguished_name
        .push(DnType::CommonName, "Lattice Local Root CA");
    ca_params.key_pair =
        Some(rcgen::KeyPair::from_pem(ca_key_pem).context("failed to parse CA private key")?);
    let ca_cert =
        Certificate::from_params(ca_params).context("failed to reconstruct CA signer cert")?;

    let mut leaf_params = CertificateParams::new(vec![host.to_string()]);
    leaf_params.distinguished_name = DistinguishedName::new();
    leaf_params
        .distinguished_name
        .push(DnType::CommonName, host);
    let leaf_cert = Certificate::from_params(leaf_params).context("failed to build leaf cert")?;

    let cert_der = leaf_cert
        .serialize_der_with_signer(&ca_cert)
        .context("failed to sign leaf cert")?;
    let key_der = leaf_cert.serialize_private_key_der();

    let cert_chain = vec![rustls::pki_types::CertificateDer::from(cert_der)];
    let key_der = rustls::pki_types::PrivatePkcs8KeyDer::from(key_der);
    let key = rustls::pki_types::PrivateKeyDer::from(key_der);
    let config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(cert_chain, key)
        .context("failed to build rustls server config")?;
    Ok(config)
}

async fn read_until_headers_complete<S>(stream: &mut S, buffer: &mut Vec<u8>) -> Result<usize>
where
    S: AsyncRead + Unpin,
{
    let mut temp = [0_u8; 4096];
    loop {
        if let Some(end) = find_header_end(buffer) {
            return Ok(end);
        }
        if buffer.len() >= MAX_PROXY_HEADER_BYTES {
            anyhow::bail!("proxy request headers too large");
        }
        let read = stream
            .read(&mut temp)
            .await
            .context("failed reading proxy request")?;
        if read == 0 {
            anyhow::bail!("proxy client closed before sending headers");
        }
        buffer.extend_from_slice(&temp[..read]);
    }
}

fn find_header_end(buffer: &[u8]) -> Option<usize> {
    buffer
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .map(|idx| idx + 4)
}

fn parse_connect_target(target: &str) -> Result<(String, u16)> {
    let (host_raw, port_raw) = target.rsplit_once(':').unwrap_or((target, "443"));
    let host = host_raw
        .trim_matches('[')
        .trim_matches(']')
        .to_ascii_lowercase();
    let port = port_raw.parse::<u16>().context("invalid CONNECT port")?;
    Ok((host, port))
}

fn parse_http_target(target: &str, host_header: Option<&str>) -> Result<(String, String)> {
    if target.starts_with("http://") {
        let uri: axum::http::Uri = target.parse().context("invalid absolute proxy URI")?;
        let host = uri
            .host()
            .map(str::to_ascii_lowercase)
            .context("proxy URI missing host")?;
        let path = uri
            .path_and_query()
            .map(|pq| pq.as_str().to_string())
            .unwrap_or_else(|| "/".to_string());
        return Ok((host, path));
    }

    if target.starts_with('/') {
        let host = host_header
            .map(|h| h.split(':').next().unwrap_or(h).to_ascii_lowercase())
            .context("missing Host header in proxy request")?;
        return Ok((host, target.to_string()));
    }

    anyhow::bail!("unsupported proxy target format");
}

fn is_loom_host(host: &str) -> bool {
    host.strip_suffix(".loom")
        .map(is_valid_site_label)
        .unwrap_or(false)
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

async fn write_response<S>(stream: &mut S, status: u16, reason: &str, body: &[u8]) -> Result<()>
where
    S: AsyncWrite + Unpin,
{
    let mut response = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    )
    .into_bytes();
    response.extend_from_slice(body);
    stream
        .write_all(&response)
        .await
        .context("failed writing proxy response")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::is_loom_host;

    #[test]
    fn validates_loom_hosts() {
        assert!(is_loom_host("benjf.loom"));
        assert!(is_loom_host("a1-2.loom"));
        assert!(!is_loom_host("bad.name.loom"));
        assert!(!is_loom_host("_bad.loom"));
        assert!(!is_loom_host("-bad.loom"));
        assert!(!is_loom_host("bad-.loom"));
    }
}
