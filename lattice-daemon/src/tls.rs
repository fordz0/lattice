use anyhow::{Context, Result};
use rcgen::{
    BasicConstraints, Certificate, CertificateParams, DistinguishedName, DnType, IsCa, SanType,
};
use std::fs;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::path::{Path, PathBuf};

const TLS_DIR: &str = "tls";
const CA_CERT_FILE: &str = "lattice-local-ca.pem";
const CA_KEY_FILE: &str = "lattice-local-ca.key";
const SERVER_CERT_FILE: &str = "lattice-local-server.pem";
const SERVER_KEY_FILE: &str = "lattice-local-server.key";
const TLS_VERSION_FILE: &str = "version";
const TLS_MATERIAL_VERSION: &str = "2";

#[derive(Debug, Clone)]
pub struct LocalTlsMaterial {
    pub ca_cert_pem: String,
    pub ca_key_pem: String,
    pub ca_cert_path: PathBuf,
    pub server_cert_path: PathBuf,
    pub server_key_path: PathBuf,
}

pub fn load_or_create_local_tls(data_dir: &Path) -> Result<LocalTlsMaterial> {
    let tls_dir = data_dir.join(TLS_DIR);
    fs::create_dir_all(&tls_dir)
        .with_context(|| format!("failed to create TLS dir {}", tls_dir.display()))?;

    let ca_cert_path = tls_dir.join(CA_CERT_FILE);
    let ca_key_path = tls_dir.join(CA_KEY_FILE);
    let server_cert_path = tls_dir.join(SERVER_CERT_FILE);
    let server_key_path = tls_dir.join(SERVER_KEY_FILE);
    let version_path = tls_dir.join(TLS_VERSION_FILE);
    let material_is_current = version_path.exists()
        && fs::read_to_string(&version_path)
            .map(|v| v.trim() == TLS_MATERIAL_VERSION)
            .unwrap_or(false)
        && ca_cert_path.exists()
        && ca_key_path.exists()
        && server_cert_path.exists()
        && server_key_path.exists();
    if material_is_current {
        let ca_cert_pem = fs::read_to_string(&ca_cert_path)
            .with_context(|| format!("failed to read {}", ca_cert_path.display()))?;
        let ca_key_pem = fs::read_to_string(&ca_key_path)
            .with_context(|| format!("failed to read {}", ca_key_path.display()))?;
        return Ok(LocalTlsMaterial {
            ca_cert_pem,
            ca_key_pem,
            ca_cert_path,
            server_cert_path,
            server_key_path,
        });
    }

    let mut ca_params = CertificateParams::default();
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca_params.distinguished_name = DistinguishedName::new();
    ca_params
        .distinguished_name
        .push(DnType::CommonName, "Lattice Local Root CA");
    let ca_cert = Certificate::from_params(ca_params).context("failed to build CA cert params")?;
    let ca_cert_pem = ca_cert
        .serialize_pem()
        .context("failed to encode CA cert PEM")?;
    let ca_key_pem = ca_cert.serialize_private_key_pem();

    let mut server_params = CertificateParams::new(vec![
        "localhost".to_string(),
        "loom".to_string(),
        "*.loom".to_string(),
        "loom.lattice.localhost".to_string(),
        "*.loom.lattice.localhost".to_string(),
    ]);
    server_params
        .subject_alt_names
        .push(SanType::IpAddress(IpAddr::V4(Ipv4Addr::LOCALHOST)));
    server_params
        .subject_alt_names
        .push(SanType::IpAddress(IpAddr::V6(Ipv6Addr::LOCALHOST)));
    server_params.distinguished_name = DistinguishedName::new();
    server_params
        .distinguished_name
        .push(DnType::CommonName, "Lattice Local HTTPS");
    let server_cert =
        Certificate::from_params(server_params).context("failed to build server cert params")?;

    let server_cert_pem = server_cert
        .serialize_pem_with_signer(&ca_cert)
        .context("failed to sign server cert")?;
    let server_key_pem = server_cert.serialize_private_key_pem();

    fs::write(&ca_cert_path, &ca_cert_pem)
        .with_context(|| format!("failed to write {}", ca_cert_path.display()))?;
    fs::write(&ca_key_path, &ca_key_pem)
        .with_context(|| format!("failed to write {}", ca_key_path.display()))?;
    fs::write(&server_cert_path, &server_cert_pem)
        .with_context(|| format!("failed to write {}", server_cert_path.display()))?;
    fs::write(&server_key_path, &server_key_pem)
        .with_context(|| format!("failed to write {}", server_key_path.display()))?;
    fs::write(&version_path, TLS_MATERIAL_VERSION)
        .with_context(|| format!("failed to write {}", version_path.display()))?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&ca_key_path, fs::Permissions::from_mode(0o600))
            .with_context(|| format!("failed to chmod {}", ca_key_path.display()))?;
        fs::set_permissions(&server_key_path, fs::Permissions::from_mode(0o600))
            .with_context(|| format!("failed to chmod {}", server_key_path.display()))?;
    }

    Ok(LocalTlsMaterial {
        ca_cert_pem,
        ca_key_pem,
        ca_cert_path,
        server_cert_path,
        server_key_path,
    })
}
