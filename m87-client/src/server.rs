use anyhow::{anyhow, Context, Result};
use futures::StreamExt;
use reqwest::Client;
use rustls::{
    client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier},
    pki_types::{CertificateDer, ServerName, UnixTime},
    ClientConfig, RootCertStore, SignatureScheme,
};
use std::sync::Arc;
use tokio::{io, net::TcpStream};
use tokio_rustls::{rustls, TlsConnector};
use tokio_yamux::{Config as YamuxConfig, Session};
use tracing::{error, info, warn};
use webpki_roots::TLS_SERVER_ROOTS;

use crate::device::services::service_info::ServiceInfo;
use crate::device::system_metrics::SystemMetrics;
use crate::{auth::AuthManager, config::Config, retry_async};

// Import shared types
pub use m87_shared::auth::{
    AuthRequestAction, CheckAuthRequest, DeviceAuthRequest, DeviceAuthRequestBody,
    DeviceAuthRequestCheckResponse,
};
pub use m87_shared::device::{DeviceSystemInfo, PublicDevice, UpdateDeviceBody};
pub use m87_shared::heartbeat::{Digests, HeartbeatRequest, HeartbeatResponse};

pub async fn set_auth_request(
    api_url: &str,
    body: DeviceAuthRequestBody,
    trust_invalid_server_cert: bool,
) -> Result<String> {
    let url = format!("{}/auth/request", api_url);
    let client = get_client(trust_invalid_server_cert)?;

    let res = retry_async!(3, 3, client.post(&url).json(&body).send())?;
    match res.error_for_status() {
        Ok(r) => {
            // returns a string with device id on success
            let device_id: String = r.json().await?;
            Ok(device_id)
        }
        Err(e) => Err(anyhow!(e)),
    }
}

pub async fn check_auth_request(
    api_url: &str,
    request_id: &str,
    trust_invalid_server_cert: bool,
) -> Result<DeviceAuthRequestCheckResponse> {
    let url = format!("{}/auth/request/check", api_url);
    let client = get_client(trust_invalid_server_cert)?;

    let res = retry_async!(
        3,
        3,
        client
            .post(&url)
            .json(&CheckAuthRequest {
                request_id: request_id.to_string()
            })
            .send()
    )?;
    match res.error_for_status() {
        Ok(r) => {
            // returns a string with device id on success
            let response: DeviceAuthRequestCheckResponse = r.json().await?;
            Ok(response)
        }
        Err(e) => Err(anyhow!(e)),
    }
}

pub async fn list_auth_requests(
    api_url: &str,
    token: &str,
    trust_invalid_server_cert: bool,
) -> Result<Vec<DeviceAuthRequest>, anyhow::Error> {
    let url = format!("{}/auth/request", api_url);
    let client = get_client(trust_invalid_server_cert)?;

    let res = retry_async!(3, 3, client.get(&url).bearer_auth(token).send())?;
    match res.error_for_status() {
        Ok(r) => {
            let response: Vec<DeviceAuthRequest> = r.json().await?;
            Ok(response)
        }
        Err(e) => Err(anyhow!(e)),
    }
}

pub async fn handle_auth_request(
    api_url: &str,
    token: &str,
    request_id: &str,
    accept: bool,
    trust_invalid_server_cert: bool,
) -> Result<(), anyhow::Error> {
    let url = format!("{}/auth/request/{}", api_url, request_id);
    let client = get_client(trust_invalid_server_cert)?;

    let res = retry_async!(
        3,
        3,
        client
            .post(&url)
            .bearer_auth(token)
            .json(&AuthRequestAction {
                accept,
                request_id: request_id.to_string()
            })
            .send()
    )?;
    match res.error_for_status() {
        Ok(_) => Ok(()),
        Err(e) => Err(anyhow!(e)),
    }
}

pub async fn list_devices(
    api_url: &str,
    token: &str,
    trust_invalid_server_cert: bool,
) -> Result<Vec<PublicDevice>> {
    let client = get_client(trust_invalid_server_cert)?;

    let res = retry_async!(
        3,
        3,
        client
            .get(&format!("{}/device", api_url))
            .bearer_auth(token)
            .send()
    )?;
    match res.error_for_status() {
        Ok(res) => Ok(res.json().await?),
        Err(e) => Err(anyhow!(e)),
    }
}

pub async fn report_device_details(
    api_url: &str,
    token: &str,
    device_id: &str,
    body: UpdateDeviceBody,
    trust_invalid_server_cert: bool,
) -> Result<()> {
    let client = get_client(trust_invalid_server_cert)?;
    let url = format!("{}/device/{}", api_url.trim_end_matches('/'), device_id);

    let res = retry_async!(
        3,
        3,
        client.post(&url).bearer_auth(token).json(&body).send()
    );
    if let Err(e) = res {
        eprintln!("[Device] Error reporting device details: {}", e);
        return Err(anyhow!(e));
    }
    match res.unwrap().error_for_status() {
        Ok(_) => Ok(()),
        Err(e) => {
            eprintln!("[Device] Error reporting device details: {}", e);
            Err(anyhow!(e))
        }
    }
}

pub async fn request_control_tunnel_token(
    api_url: &str,
    token: &str,
    device_id: &str,
    trust_invalid_server_cert: bool,
) -> Result<String> {
    let client = get_client(trust_invalid_server_cert)?;
    let url = format!(
        "{}/device/{}/token",
        api_url.trim_end_matches('/'),
        device_id
    );

    let res = retry_async!(3, 3, client.get(&url).bearer_auth(token).send());
    if let Err(e) = res {
        eprintln!("[Device] Error reporting device details: {}", e);
        return Err(anyhow!(e));
    }
    match res.unwrap().error_for_status() {
        Ok(r) => {
            let control_token = r.text().await?;
            Ok(control_token)
        }
        Err(e) => {
            eprintln!("[Device] Error reporting device details: {}", e);
            Err(anyhow!(e))
        }
    }
}

pub async fn connect_control_tunnel() -> anyhow::Result<()> {
    let config = Config::load().context("Failed to load configuration")?;
    let token = AuthManager::get_device_token()?;

    let device_id = config.device_id.clone();
    let control_tunnel_token = request_control_tunnel_token(
        &config.api_url,
        &token,
        &device_id,
        config.trust_invalid_server_cert,
    )
    .await?;

    // 1. TCP connect
    // prepend control to the api hsot name e.g. https://server.make87.com to https://control.server.make87.com
    let api_url = format!(
        "https://control.{}",
        config
            .api_url
            .trim_start_matches("https://")
            .trim_start_matches("http://")
    );
    let tcp = TcpStream::connect((api_url.clone(), 443)).await?;

    // 2. Root store (use system roots or webpki)
    let mut root_store = RootCertStore::empty();
    root_store.roots.extend(TLS_SERVER_ROOTS.iter().cloned());

    // 3. TLS client config
    info!(
        "Creating TLS client config with trust_invalid_server_cert: {}",
        config.trust_invalid_server_cert
    );
    let tls_config = if config.trust_invalid_server_cert {
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
    let domain = config
        .api_url
        .clone()
        .replace("https://", "")
        .replace("http://", "");
    let server_name = ServerName::try_from(domain).context("invalid SNI name")?;
    let mut tls = connector.connect(server_name, tcp).await?;

    // 4. Send handshake line
    use tokio::io::AsyncWriteExt;
    let line = format!(
        "M87 device_id={} token={}\n",
        device_id, control_tunnel_token
    );
    tls.write_all(line.as_bytes()).await?;
    tls.flush().await?;

    // create client session
    let mut sess = Session::new_client(tls, YamuxConfig::default());
    // continuously poll session to handle keep-alives, frame exchange
    tokio::spawn(async move {
        while let Some(Ok(mut stream)) = sess.next().await {
            tokio::spawn(async move {
                // header with port number
                let mut buf = [0u8; 16];
                if let Ok(n) = stream.peek(&mut buf).await {
                    let port: u16 = String::from_utf8_lossy(&buf[..n])
                        .trim()
                        .parse()
                        .unwrap_or(0);
                    if port > 0 {
                        if let Ok(mut local) = TcpStream::connect(("127.0.0.1", port)).await {
                            let _ = match io::copy_bidirectional(&mut stream, &mut local).await {
                                Ok((_a, _b)) => {
                                    info!("proxy session closed cleanly ");
                                    let _ = stream.shutdown().await;
                                    let _ = local.shutdown().await;
                                    Ok(())
                                }
                                Err(e) => {
                                    info!("proxy session closed with error ");
                                    let _ = stream.shutdown().await;
                                    let _ = local.shutdown().await;
                                    Err(e)
                                }
                            };
                        }
                    }
                }
            });
        }
    });
    // register / run streams as needed
    Ok(())
}

pub async fn send_heartbeat(
    last_instruction_hash: &str,
    device_id: &str,
    api_url: &str,
    token: &str,
    metrics: SystemMetrics,
    services: Vec<ServiceInfo>,
) -> Result<HeartbeatResponse> {
    let req = HeartbeatRequest {
        last_instruction_hash: last_instruction_hash.to_string(),
        system: metrics,
        services,
    };

    let client = reqwest::Client::new();
    let url = format!("{}/device/{}/heartbeat", api_url, device_id);

    let resp = client
        .post(&url)
        .bearer_auth(token)
        .json(&req)
        .send()
        .await
        .context("Failed to send heartbeat")?;

    let status = resp.status();
    let text = resp
        .text()
        .await
        .context("Failed to read heartbeat response body")?;

    if !status.is_success() {
        error!("Heartbeat request failed with status {}: {}", status, text);
        return Err(anyhow::anyhow!(
            "Heartbeat failed with status {}: {}",
            status,
            text
        ));
    }

    // Try to decode JSON, log the body in case it fails
    match serde_json::from_str::<HeartbeatResponse>(&text) {
        Ok(decoded) => {
            info!("Heartbeat sent successfully: {:?}", decoded);
            Ok(decoded)
        }
        Err(err) => {
            error!(
                "Failed to decode heartbeat response: {}\nRaw response: {}",
                err, text
            );
            Err(anyhow::anyhow!("Invalid heartbeat response: {}", err))
        }
    }
}

fn get_client(trust_invalid_server_cert: bool) -> Result<Client> {
    // if its localhost we accept invalid certificates
    if trust_invalid_server_cert {
        let client = reqwest::Client::builder()
            .danger_accept_invalid_certs(true)
            .build()?;
        Ok(client)
    } else {
        // otherwise we verify the certificate
        let client = Client::new();
        Ok(client)
    }
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
        vec![SignatureScheme::RSA_PKCS1_SHA256]
    }
}
