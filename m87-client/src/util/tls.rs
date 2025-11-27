use std::sync::{Arc, Once};

use anyhow::{anyhow, Context, Result};
use rustls::{
    client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier},
    pki_types::{CertificateDer, ServerName, UnixTime},
    ClientConfig, RootCertStore, SignatureScheme,
};
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;
use tracing::{info, warn};
use webpki_roots::TLS_SERVER_ROOTS;

pub async fn connect_host(host: &str, port: u16) -> anyhow::Result<TcpStream> {
    for i in 0..10 {
        match tokio::net::lookup_host((host, port)).await {
            Ok(addrs) => {
                for addr in addrs {
                    if addr.is_ipv4() {
                        if let Ok(stream) = TcpStream::connect(addr).await {
                            return Ok(stream);
                        }
                    }
                }
            }
            Err(_) => {}
        }

        let backoff = 200 + (i * 150);
        tokio::time::sleep(std::time::Duration::from_millis(backoff)).await;
    }
    Err(anyhow!("DNS resolution failed after retries"))
}

pub async fn get_tls_connection(
    host_name: String,
    trust_invalid_server_cert: bool,
) -> Result<tokio_rustls::client::TlsStream<tokio::net::TcpStream>> {
    let tcp = connect_host(&host_name, 443).await?;

    // 2. Root store (use system roots or webpki)
    let mut root_store = RootCertStore::empty();
    root_store.roots.extend(TLS_SERVER_ROOTS.iter().cloned());

    // 3. TLS client config
    info!(
        "Creating TLS client config with trust_invalid_server_cert: {}",
        trust_invalid_server_cert
    );
    let tls_config = if trust_invalid_server_cert {
        warn!("Trusting invalid server certificate");
        Arc::new(
            ClientConfig::builder()
                .dangerous()
                .with_custom_certificate_verifier(Arc::new(NoVerify))
                .with_no_client_auth(),
        )
    } else {
        Arc::new(
            ClientConfig::builder()
                .with_root_certificates(root_store)
                .with_no_client_auth(),
        )
    };

    // 4. TLS handshake (SNI)
    let connector = TlsConnector::from(tls_config);
    let server_name = ServerName::try_from(host_name.clone()).context("invalid SNI name")?;
    let tls = connector.connect(server_name, tcp).await?;
    Ok(tls)
}

#[derive(Debug)]
struct NoVerify;

impl ServerCertVerifier for NoVerify {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        vec![
            SignatureScheme::ECDSA_NISTP256_SHA256,
            SignatureScheme::ECDSA_NISTP384_SHA384,
            SignatureScheme::ED25519,
            SignatureScheme::RSA_PSS_SHA256,
            SignatureScheme::RSA_PKCS1_SHA256,
        ]
    }
}

static INIT: Once = Once::new();

pub fn set_tls_provider() {
    INIT.call_once(|| {
        rustls::crypto::CryptoProvider::install_default(rustls::crypto::ring::default_provider())
            .expect("failed to install ring crypto provider");
    });
}
