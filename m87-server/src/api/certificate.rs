use axum::{Extension, Json, extract::State};
use pem::parse_many;
use rustls::pki_types::{PrivateKeyDer, PrivatePkcs1KeyDer, PrivatePkcs8KeyDer};
use rustls::{
    ServerConfig as RustlsServerConfig, crypto::ring::default_provider, pki_types::CertificateDer,
};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::{fs, sync::watch};
use tracing::warn;

use quinn::{ServerConfig as QuicServerConfig, TransportConfig};
use quinn_proto::crypto::rustls::QuicServerConfig as QuinnQuicServerCrypto;

use crate::{
    auth::claims::Claims,
    config::AppConfig,
    response::{ServerAppResult, ServerError, ServerResponse, ServerResult},
    util::app_state::AppState,
};

fn split_pem_blocks(input: &[u8]) -> ServerResult<Vec<(String, Vec<u8>)>> {
    let text =
        std::str::from_utf8(input).map_err(|_| ServerError::internal_error("invalid UTF-8"))?;

    let pems =
        parse_many(text).map_err(|e| ServerError::internal_error(&format!("PEM parse: {e}")))?;

    let blocks = pems
        .into_iter()
        .map(|p| (p.tag().to_string(), p.contents().to_vec()))
        .collect();

    Ok(blocks)
}

fn parse_cert_chain(bytes: &[u8]) -> ServerResult<Vec<CertificateDer<'static>>> {
    let blocks = split_pem_blocks(bytes)?;

    let certs = blocks
        .into_iter()
        .filter(|(label, _)| label.to_uppercase() == "CERTIFICATE")
        .map(|(_, der)| CertificateDer::from(der))
        .collect::<Vec<_>>();

    if certs.is_empty() {
        return Err(ServerError::internal_error("no certificates found"));
    }

    Ok(certs)
}

fn parse_private_key(bytes: &[u8]) -> ServerResult<PrivateKeyDer<'static>> {
    let blocks = split_pem_blocks(bytes)?;

    for (label, der) in blocks {
        match label.as_str() {
            "PRIVATE KEY" => {
                return Ok(PrivateKeyDer::from(PrivatePkcs8KeyDer::from(der)));
            }
            "RSA PRIVATE KEY" => {
                return Ok(PrivateKeyDer::from(PrivatePkcs1KeyDer::from(der)));
            }
            _ => continue,
        }
    }

    Err(ServerError::internal_error("no valid private key found"))
}

/// Load certificate + key from disk or generate self-signed if missing.
pub async fn load_cert_and_key(
    cfg: &AppConfig,
) -> ServerResult<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>)> {
    // Staging mode → always self-signed
    if cfg.is_staging {
        return create_selfsigned_pair(cfg);
    }

    let dir = PathBuf::from(&cfg.certificate_path);
    let key_path = dir.join("private.key");
    let cert_path = dir.join("fullchain.pem");

    if !key_path.exists() || !cert_path.exists() {
        warn!("No TLS certs found, generating temporary self-signed certificate");
        return create_selfsigned_pair(cfg);
    }

    // Load certificate chain
    let cert_bytes = fs::read(&cert_path)
        .await
        .map_err(|e| ServerError::internal_error(&format!("read cert: {e:?}")))?;

    let certs = parse_cert_chain(&cert_bytes)?;
    if certs.is_empty() {
        return Err(ServerError::internal_error("no certs in fullchain.pem"));
    }

    // Load private key
    let key_bytes = fs::read(&key_path)
        .await
        .map_err(|e| ServerError::internal_error(&format!("read key: {e:?}")))?;

    let key = parse_private_key(&key_bytes)?;

    Ok((certs, key))
}

/// Self-signed certificate pair
pub fn create_selfsigned_pair(
    cfg: &AppConfig,
) -> ServerResult<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>)> {
    let ck = rcgen::generate_simple_self_signed(vec![cfg.public_address.clone()])
        .map_err(|e| ServerError::internal_error(&format!("rcgen: {e}")))?;

    let cert = CertificateDer::from(ck.cert.der().to_vec());
    let key = PrivateKeyDer::from(PrivatePkcs8KeyDer::from(ck.signing_key.serialize_der()));

    Ok((vec![cert], key))
}

/// REST TLS config → Axum will use this directly.
pub async fn create_tls_config(cfg: &AppConfig) -> ServerResult<RustlsServerConfig> {
    let (certs, key) = load_cert_and_key(cfg).await?;

    let provider = Arc::new(default_provider());
    let tls = RustlsServerConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&rustls::version::TLS13])
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|e| ServerError::internal_error(&format!("TLS build: {e}")))?;

    Ok(tls)
}

/// QUIC TLS config → Quinn endpoint
pub async fn create_quic_server_config(cfg: &AppConfig) -> ServerResult<QuicServerConfig> {
    let (certs, key) = load_cert_and_key(cfg).await?;

    let provider = Arc::new(default_provider());
    let mut tls = RustlsServerConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&rustls::version::TLS13])
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|e| ServerError::internal_error(&format!("TLS build: {e}")))?;

    tls.alpn_protocols = vec![
        b"m87-quic".to_vec(), // CLI & m87 agent QUIC
        b"h3".to_vec(),       // HTTP/3 for WebTransport
        b"h3-29".to_vec(),    // optional, but Chrome still sends these
    ];

    let crypto = QuinnQuicServerCrypto::try_from(tls)
        .map_err(|e| ServerError::internal_error(&format!("quic rustls: {e}")))?;

    let mut cfg = QuicServerConfig::with_crypto(Arc::new(crypto));

    let mut t = TransportConfig::default();
    t.max_concurrent_bidi_streams(1024u32.into());
    t.keep_alive_interval(Some(std::time::Duration::from_secs(10)));

    cfg.transport = Arc::new(t);
    Ok(cfg)
}

/// Admin API → Updates certs on disk and signals reload
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateCertBody {
    pub pvtkey: String,
    pub fullchain: String,
}

pub async fn update_cert(
    claims: Claims,
    State(state): State<AppState>,
    Extension(reload_tx): Extension<watch::Sender<()>>,
    Json(payload): Json<UpdateCertBody>,
) -> ServerAppResult<()> {
    if !claims.is_admin {
        return Err(ServerError::unauthorized(""));
    }

    // Validate certificate chain
    let certs = parse_cert_chain(payload.fullchain.as_bytes())
        .map_err(|e| ServerError::internal_error(&format!("invalid certs: {e}")))?;

    if certs.is_empty() {
        return Err(ServerError::internal_error(
            "no certificates in fullchain.pem",
        ));
    }

    // Validate private key
    parse_private_key(payload.pvtkey.as_bytes())
        .map_err(|e| ServerError::internal_error(&format!("invalid private key: {e}")))?;

    // Save to disk
    let dir = PathBuf::from(&state.config.certificate_path);
    fs::write(dir.join("private.key"), payload.pvtkey.as_bytes()).await?;
    fs::write(dir.join("fullchain.pem"), payload.fullchain.as_bytes()).await?;

    // Notify QUIC + HTTPS loops
    let _ = reload_tx.send(());

    Ok(ServerResponse::builder().ok().build())
}
