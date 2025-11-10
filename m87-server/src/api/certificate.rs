use acme2::{
    gen_rsa_private_key,
    openssl::{
        pkey::{PKey, Private},
        rsa::Rsa,
    },
    AccountBuilder, AuthorizationStatus, ChallengeStatus, DirectoryBuilder, OrderBuilder,
    OrderStatus,
};
use hickory_resolver::{proto::rr::RecordType, Resolver};
use rustls::{
    crypto::ring::default_provider,
    pki_types::{CertificateDer, PrivateKeyDer},
};
use rustls::{pki_types::PrivatePkcs8KeyDer, ServerConfig};
use std::{io::Cursor, path::Path, sync::Arc, time::Duration};

use chrono::Utc;
use tokio::fs;
use tracing::{info, warn};
use x509_parser::prelude::*;

use crate::{
    config::AppConfig,
    response::{ServerError, ServerResult},
};
use rcgen::generate_simple_self_signed;

const COOLDOWN_HOURS: i64 = 12;

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

    // === Production: load wildcard cert ===
    let certs_dir = cfg.certificate_path.clone();
    let fullchain = format!("{}/fullchain.pem", certs_dir);
    let privkey = format!("{}/privkey.pem", certs_dir);

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

pub async fn maybe_renew_wildcard(cfg: &AppConfig) -> ServerResult<()> {
    // --- sanity & cooldown checks ---
    if let Err(e) = check_can_attempt_renewal(cfg).await {
        return Ok(()); // safe skip, no renewal attempted
    }

    // === ACME issuance ===
    let result = inner_acme_renew(cfg).await;

    match result {
        Ok(_) => {
            clear_acme_failure(cfg).await;
            Ok(())
        }
        Err(e) => {
            mark_acme_failure(cfg).await;
            Err(e)
        }
    }
}

/// Checks expiry and renews wildcard cert via ACME DNS-01 if needed.
pub async fn inner_acme_renew(cfg: &AppConfig) -> ServerResult<()> {
    let cache_dir = cfg.certificate_path.clone();
    let cert_path = format!("{}/fullchain.pem", cache_dir);

    // --- Skip if cert still valid (>10 days left) ---
    if let Ok(data) = fs::read(&cert_path).await {
        if let Ok((_, cert)) = X509Certificate::from_der(&data) {
            let not_after = cert.validity().not_after.timestamp();
            let now = Utc::now().timestamp();
            if not_after - now > 86400 * 10 {
                return Ok(());
            }
        }
    }

    // === ACME DNS-01 issuance ===
    const LETS_ENCRYPT_URL: &str = "https://acme-v02.api.letsencrypt.org/directory";

    let dir = DirectoryBuilder::new(LETS_ENCRYPT_URL.to_string())
        .build()
        .await
        .map_err(|e| ServerError::internal_error(&format!("ACME dir: {e:?}")))?;

    let pkey = pem_string_to_private_key(
        &cfg.acme_acc_prv_pem_key
            .clone()
            .ok_or_else(|| ServerError::internal_error("Missing private key"))?,
    )?;
    let mut builder = AccountBuilder::new(dir);
    builder
        .private_key(pkey.clone())
        .contact(vec![format!("mailto:{}", cfg.cert_contact)])
        .terms_of_service_agreed(true);
    let account = builder
        .build()
        .await
        .map_err(|e| ServerError::internal_error(&format!("ACME account: {e:?}")))?;

    let mut ob = OrderBuilder::new(account);
    ob.add_dns_identifier(cfg.public_address.clone());
    ob.add_dns_identifier(format!("*.{}", cfg.public_address));
    let order = ob
        .build()
        .await
        .map_err(|e| ServerError::internal_error(&format!("ACME order: {e:?}")))?;

    // --- DNS TXT record must already be present ---
    let authorizations = order
        .authorizations()
        .await
        .map_err(|e| ServerError::internal_error(&format!("ACME authz: {e:?}")))?;

    for auth in authorizations {
        if auth.status != AuthorizationStatus::Valid {
            let challenge = auth
                .get_challenge("dns-01")
                .ok_or_else(|| ServerError::internal_error("no dns-01 challenge"))?;

            // Start validation; expect TXT already resolvable
            let challenge = challenge
                .validate()
                .await
                .map_err(|e| ServerError::internal_error(&format!("challenge validate: {e:?}")))?;

            let challenge = challenge
                .wait_done(Duration::from_secs(5), 24)
                .await
                .map_err(|e| ServerError::internal_error(&format!("challenge wait: {e:?}")))?;

            if challenge.status != ChallengeStatus::Valid {
                return Err(ServerError::internal_error(
                    "ACME challenge not valid after polling",
                ));
            }

            let authorization = auth
                .wait_done(Duration::from_secs(5), 24)
                .await
                .map_err(|e| ServerError::internal_error(&format!("auth wait: {e:?}")))?;

            if authorization.status != AuthorizationStatus::Valid {
                return Err(ServerError::internal_error("ACME authorization not valid"));
            }
        }
    }

    // --- Wait for order to be ready ---
    let order = order
        .wait_ready(Duration::from_secs(5), 12)
        .await
        .map_err(|e| ServerError::internal_error(&format!("wait_ready: {e:?}")))?;

    if order.status != OrderStatus::Ready {
        return Err(ServerError::internal_error("ACME order not ready"));
    }

    // --- Generate key + finalize ---
    let pkey = gen_rsa_private_key(4096)
        .map_err(|e| ServerError::internal_error(&format!("gen_rsa_private_key: {e:?}")))?;

    let order = order
        .finalize(acme2::Csr::Automatic(pkey.clone()))
        .await
        .map_err(|e| ServerError::internal_error(&format!("finalize: {e:?}")))?;

    let order = order
        .wait_done(Duration::from_secs(5), 12)
        .await
        .map_err(|e| ServerError::internal_error(&format!("wait_done: {e:?}")))?;

    if order.status != OrderStatus::Valid {
        return Err(ServerError::internal_error("ACME order did not finalize"));
    }

    // --- Download certificate ---
    let cert = order
        .certificate()
        .await
        .map_err(|e| ServerError::internal_error(&format!("download cert: {e:?}")))?
        .ok_or_else(|| ServerError::internal_error("missing certificate payload"))?;

    let pem_data: Vec<u8> = cert
        .iter()
        .map(|c| {
            c.to_pem()
                .map_err(|e| ServerError::internal_error(&format!("pem encode: {e:?}")))
        })
        .collect::<Result<Vec<_>, _>>()? // Vec<Vec<u8>>
        .concat(); // flatten into Vec<u8>

    fs::write(format!("{}/fullchain.pem", cache_dir), pem_data)
        .await
        .map_err(|e| ServerError::internal_error(&format!("write cert: {e:?}")))?;

    let key_pem = pkey
        .private_key_to_pem_pkcs8()
        .map_err(|e| ServerError::internal_error(&format!("pem encode: {e:?}")))?;

    // Write PEM directly
    fs::write(format!("{}/privkey.pem", cache_dir), key_pem)
        .await
        .map_err(|e| ServerError::internal_error(&format!("write key: {e:?}")))?;

    info!("renewed wildcard cert for {}", cfg.public_address);
    Ok(())
}

/// Performs all safety checks before attempting ACME renewal:
/// 1. Skip if cert valid >10 days
/// 2. Skip if last failure <12h ago
/// 3. Skip if DNS TXT not found for `_acme-challenge.<domain>`
pub async fn check_can_attempt_renewal(cfg: &AppConfig) -> ServerResult<()> {
    let cert_path = format!("{}/fullchain.pem", cfg.certificate_path);
    let failure_record = format!("{}/last_failure", cfg.certificate_path);

    // --- 1. Check existing certificate validity ---
    if let Ok(data) = fs::read(&cert_path).await {
        if let Ok((_, cert)) = x509_parser::prelude::X509Certificate::from_der(&data) {
            let not_after = cert.validity().not_after.timestamp();
            let now = Utc::now().timestamp();
            if not_after - now > 86400 * 10 {
                info!("certificate still valid >10 days; skipping renewal");
                return Err(ServerError::internal_error("skip-renewal-valid"));
            }
        }
    }

    // --- 2. Check cooldown after last failure ---
    if Path::new(&failure_record).exists() {
        if let Ok(ts_str) = fs::read_to_string(failure_record).await {
            if let Ok(ts) = ts_str.trim().parse::<i64>() {
                let now = Utc::now().timestamp();
                if now - ts < COOLDOWN_HOURS * 3600 {
                    warn!(
                        "last ACME failure was less than {COOLDOWN_HOURS}h ago; skipping renewal"
                    );
                    return Err(ServerError::internal_error("skip-renewal-recent-failure"));
                }
            }
        }
    }

    if !has_dns_records(cfg).await {
        return Err(ServerError::internal_error("skip-renewal-no-dns"));
    }

    Ok(())
}

/// Marks a failed renewal timestamp.
pub async fn mark_acme_failure(cfg: &AppConfig) {
    let failure_record = format!("{}/last_failure", cfg.certificate_path);
    let _ = fs::write(failure_record, Utc::now().timestamp().to_string()).await;
}

/// Clears the failure flag after successful renewal.
pub async fn clear_acme_failure(cfg: &AppConfig) {
    let failure_record = format!("{}/last_failure", cfg.certificate_path);
    let _ = fs::remove_file(failure_record).await;
}

async fn has_dns_records(cfg: &AppConfig) -> bool {
    // Use system config if available (reads /etc/resolv.conf)
    let domain = &cfg.public_address;
    if !has_dns_record(domain, RecordType::A).await {
        warn!("A record missing for {domain}, skipping renewal");
        return false;
    }

    let txt_name = format!("_acme-challenge.{domain}");
    if !has_dns_record(&txt_name, RecordType::TXT).await {
        warn!("TXT record missing for {txt_name}, skipping renewal");
        return false;
    }
    true
}

async fn has_dns_record(name: &str, record_type: RecordType) -> bool {
    let resolver = Resolver::builder_tokio().unwrap().build();
    resolver.lookup(name, record_type).await.is_ok()
}

pub fn pem_string_to_private_key(pem_str: &str) -> ServerResult<PKey<Private>> {
    let rsa = Rsa::private_key_from_pem(pem_str.as_bytes())
        .map_err(|_| ServerError::internal_error("Failed to create private key from PEM string"))?;
    PKey::from_rsa(rsa)
        .map_err(|_| ServerError::internal_error("Failed to create private key from PEM string"))
}
