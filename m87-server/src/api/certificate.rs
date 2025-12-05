use axum::{Extension, Json, extract::State};
use rustls::{
    ServerConfig as RustlsServerConfig,
    crypto::ring::default_provider,
    pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer},
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::{
    io::Cursor,
    path::{Path, PathBuf},
};
use tokio::{fs, sync::watch};
use tracing::{info, warn};

use quinn::{ServerConfig as QuicServerConfig, TransportConfig};
use quinn_proto::crypto::rustls::QuicServerConfig as QuinnQuicServerCrypto;

use crate::{
    auth::claims::Claims,
    config::AppConfig,
    response::{ServerAppResult, ServerError, ServerResponse, ServerResult},
    util::app_state::AppState,
};

/// Load certificate + key from disk or generate self-signed if missing.
pub async fn load_cert_and_key(
    cfg: &AppConfig,
) -> ServerResult<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>)> {
    // Staging mode uses self-signed always
    if cfg.is_staging {
        return create_selfsigned_pair(cfg);
    }

    let dir = PathBuf::from(&cfg.certificate_path);
    let key_path = dir.join("private.key");
    let cert_path = dir.join("fullchain.pem");

    if !Path::new(&key_path).exists() || !Path::new(&cert_path).exists() {
        warn!("No TLS certs found, generating temporary self-signed certificate");
        return create_selfsigned_pair(cfg);
    }

    // Load certificate chain
    let cert_bytes = fs::read(&cert_path)
        .await
        .map_err(|e| ServerError::internal_error(&format!("read cert: {e:?}")))?;

    let mut cur = Cursor::new(cert_bytes);
    let certs = rustls_pemfile::certs(&mut cur)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| ServerError::internal_error(&format!("parse certs: {e:?}")))?;

    if certs.is_empty() {
        return Err(ServerError::internal_error("no certs in fullchain.pem"));
    }

    // Load private key
    let key_bytes = fs::read(&key_path)
        .await
        .map_err(|e| ServerError::internal_error(&format!("read key: {e:?}")))?;

    let mut cur = Cursor::new(key_bytes);
    let key = rustls_pemfile::pkcs8_private_keys(&mut cur)
        .next()
        .ok_or_else(|| ServerError::internal_error("missing private key"))?
        .map_err(|e| ServerError::internal_error(&format!("parse key: {e:?}")))?;

    Ok((certs, key.into()))
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

    tls.alpn_protocols = vec![b"m87-quic".to_vec()];

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

    // Validate inputs
    let mut cert_cursor = Cursor::new(payload.fullchain.clone());
    let certs = rustls_pemfile::certs(&mut cert_cursor)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| ServerError::internal_error(&format!("invalid certs: {e}")))?;

    if certs.is_empty() {
        return Err(ServerError::internal_error(
            "no certificates in fullchain.pem",
        ));
    }

    let mut key_cursor = Cursor::new(payload.pvtkey.clone());
    rustls_pemfile::pkcs8_private_keys(&mut key_cursor)
        .next()
        .ok_or_else(|| ServerError::internal_error("invalid private key"))?
        .map_err(|e| ServerError::internal_error(&format!("parse key: {e}")))?;

    // Save to disk
    let dir = PathBuf::from(&state.config.certificate_path);
    fs::write(dir.join("private.key"), payload.pvtkey).await?;
    fs::write(dir.join("fullchain.pem"), payload.fullchain).await?;

    // Notify QUIC + HTTPS loops
    let _ = reload_tx.send(());

    Ok(ServerResponse::builder().ok().build())
}
