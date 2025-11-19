use arc_swap::ArcSwap;
use axum::{extract::State, Extension, Json};
use rustls::{
    crypto::ring::default_provider,
    pki_types::{CertificateDer, PrivateKeyDer},
};
use rustls::{pki_types::PrivatePkcs8KeyDer, ServerConfig};
use serde::{Deserialize, Serialize};
use std::{
    io::Cursor,
    path::{Path, PathBuf},
    sync::Arc,
};

use tokio::fs;
use tracing::{info, warn};

use crate::{
    auth::claims::Claims,
    config::AppConfig,
    response::{ServerAppResult, ServerError, ServerResponse, ServerResult},
    util::app_state::AppState,
};
use rcgen::generate_simple_self_signed;

pub async fn create_tls_config(cfg: &AppConfig) -> ServerResult<ServerConfig> {
    // === Local / staging ===
    if cfg.is_staging {
        let ck = generate_simple_self_signed(vec![
            "localhost".into(),
            "127.0.0.1".into(),
            cfg.public_address.clone(),
        ])
        .map_err(|e| ServerError::internal_error(&format!("selfsigned: {e}")))?;

        let cert_der: CertificateDer<'static> = ck.cert.der().clone().into();
        let key_bytes = ck.signing_key.serialize_der();
        let key_der: PrivateKeyDer<'static> =
            PrivateKeyDer::from(PrivatePkcs8KeyDer::from(key_bytes));

        let provider = Arc::new(default_provider());
        let config = ServerConfig::builder_with_provider(provider)
            .with_protocol_versions(&[&rustls::version::TLS13])
            .unwrap()
            .with_no_client_auth()
            .with_single_cert(vec![cert_der], key_der)
            .map_err(|e| ServerError::internal_error(&format!("{e}")))?;
        return Ok(config);
    }

    let certs_dir = PathBuf::from(&cfg.certificate_path);
    let privkey = certs_dir.join("private.key");
    let fullchain = certs_dir.join("fullchain.pem");

    // fallback if missing
    if !Path::new(&fullchain).exists() || !Path::new(&privkey).exists() {
        warn!("No TLS certs found, generating temporary self-signed certificate");
        return create_selfsigned_config(cfg);
    }

    let cert_bytes = fs::read(&fullchain)
        .await
        .map_err(|e| ServerError::internal_error(&format!("read cert: {e:?}")))?;

    let mut cursor = Cursor::new(cert_bytes);
    let certs = rustls_pemfile::certs(&mut cursor)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| ServerError::internal_error(&format!("parse certs: {e:?}")))?;

    let key_bytes = fs::read(&privkey)
        .await
        .map_err(|e| ServerError::internal_error(&format!("read key: {e:?}")))?;

    let mut cursor = Cursor::new(key_bytes);
    let key = rustls_pemfile::pkcs8_private_keys(&mut cursor)
        .next()
        .ok_or_else(|| ServerError::internal_error("missing private key"))?
        .map_err(|e| ServerError::internal_error(&format!("parse key: {e:?}")))?;

    let key: PrivateKeyDer<'static> = key.into();

    let provider = Arc::new(default_provider());
    let config = ServerConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&rustls::version::TLS13])
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(certs, key.into())
        .map_err(|e| ServerError::internal_error(&format!("{e}")))?;

    Ok(config)
}

pub fn create_selfsigned_config(cfg: &AppConfig) -> ServerResult<ServerConfig> {
    use rcgen::generate_simple_self_signed;
    let ck = generate_simple_self_signed(vec![cfg.public_address.clone()])
        .map_err(|e| ServerError::internal_error(&format!("selfsigned: {e}")))?;
    let cert_der = rustls::pki_types::CertificateDer::from(ck.cert.der().to_vec());
    let key_bytes = ck.signing_key.serialize_der();
    let key_der = rustls::pki_types::PrivateKeyDer::from(
        rustls::pki_types::PrivatePkcs8KeyDer::from(key_bytes),
    );
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    Ok(ServerConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&rustls::version::TLS13])
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(vec![cert_der], key_der)
        .map_err(|e| ServerError::internal_error(&format!("{e}")))?)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateCertBody {
    pub pvtkey: String,
    pub fullchain: String,
}

pub async fn update_cert(
    claims: Claims,
    State(state): State<AppState>,
    Extension(current): Extension<Arc<ArcSwap<ServerConfig>>>,
    Json(payload): Json<UpdateCertBody>,
) -> ServerAppResult<()> {
    // optionally restrict to admin
    if !claims.is_admin {
        return Err(ServerError::unauthorized(""));
    }

    let mut cert_cursor = Cursor::new(payload.fullchain.clone());
    let certs = rustls_pemfile::certs(&mut cert_cursor)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| ServerError::internal_error(&format!("parse certs: {e:?}")))?;

    if certs.is_empty() {
        return Err(ServerError::internal_error(
            "no certificates found in fullchain.pem",
        ));
    }

    let mut key_cursor = Cursor::new(payload.pvtkey.clone());
    let key = rustls_pemfile::pkcs8_private_keys(&mut key_cursor)
        .next()
        .ok_or_else(|| ServerError::internal_error("missing private key"))?
        .map_err(|e| ServerError::internal_error(&format!("parse key: {e:?}")))?;

    let key: PrivateKeyDer<'static> = key.into();

    let provider = Arc::new(default_provider());
    let config = ServerConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&rustls::version::TLS13])
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|e| ServerError::internal_error(&format!("TLS config: {e}")))?;

    current.store(Arc::new(config));

    let cert_folder = PathBuf::from(&state.config.certificate_path);
    let pvtkey_path = cert_folder.join("private.key");
    let fullchain_path = cert_folder.join("fullchain.pem");

    fs::write(&pvtkey_path, payload.pvtkey)
        .await
        .map_err(|e| ServerError::internal_error(&format!("write pvtkey: {e:?}")))?;

    fs::write(&fullchain_path, payload.fullchain)
        .await
        .map_err(|e| ServerError::internal_error(&format!("write fullchain: {e:?}")))?;

    Ok(ServerResponse::builder().ok().build())
}
