use std::sync::Once;

use anyhow::Result;
use rustls::{
    SignatureScheme,
    client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier},
    pki_types::{CertificateDer, ServerName, UnixTime},
};

#[derive(Debug)]
pub struct NoVerify;

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

#[cfg(test)]
mod tests {
    use super::*;
    use rustls::client::danger::ServerCertVerifier;

    #[test]
    fn test_no_verify_server_cert() {
        let verifier = NoVerify;
        let cert = CertificateDer::from(vec![0u8; 32]);
        let server_name = ServerName::try_from("example.com").unwrap();
        let now = UnixTime::now();

        let result = verifier.verify_server_cert(&cert, &[], &server_name, &[], now);
        assert!(result.is_ok());
    }

    // Note: verify_tls12_signature and verify_tls13_signature tests are omitted
    // because DigitallySignedStruct::new is private in rustls

    #[test]
    fn test_no_verify_supported_schemes() {
        let verifier = NoVerify;
        let schemes = verifier.supported_verify_schemes();
        assert_eq!(schemes.len(), 5);
    }

    #[test]
    fn test_no_verify_supported_schemes_contains_ed25519() {
        let verifier = NoVerify;
        let schemes = verifier.supported_verify_schemes();
        assert!(schemes.contains(&SignatureScheme::ED25519));
        assert!(schemes.contains(&SignatureScheme::ECDSA_NISTP256_SHA256));
    }
}
