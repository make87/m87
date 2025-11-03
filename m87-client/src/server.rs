use anyhow::{anyhow, Context, Result};
use futures::StreamExt;
use reqwest::Client;
use rustls::{
    client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier},
    pki_types::{CertificateDer, ServerName, UnixTime},
    ClientConfig, RootCertStore, SignatureScheme,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::{
    io::{self, AsyncWriteExt},
    net::TcpStream,
};
use tokio_rustls::{rustls, TlsConnector};
use tokio_yamux::{Config as YamuxConfig, Session};
use tracing::{info, warn};
use webpki_roots::TLS_SERVER_ROOTS;

use crate::{auth::AuthManager, config::Config, retry_async};

#[derive(Serialize, Deserialize)]
pub struct NodeAuthRequestBody {
    pub node_info: String,
    pub hostname: String,
    pub owner_scope: String,
    pub node_id: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct NodeAuthRequestCheckResponse {
    pub state: String,
    pub api_key: Option<String>,
}

pub async fn set_auth_request(
    api_url: &str,
    body: NodeAuthRequestBody,
    trust_invalid_server_cert: bool,
) -> Result<String> {
    let url = format!("{}/auth/request", api_url);
    let client = get_client(trust_invalid_server_cert)?;

    let res = retry_async!(3, 3, client.post(&url).json(&body).send())?;
    match res.error_for_status() {
        Ok(r) => {
            // returns a string with node id on success
            let node_id: String = r.json().await?;
            Ok(node_id)
        }
        Err(e) => Err(anyhow!(e)),
    }
}

#[derive(Serialize)]
pub struct CheckAuthRequest {
    pub request_id: String,
}

pub async fn check_auth_request(
    api_url: &str,
    request_id: &str,
    trust_invalid_server_cert: bool,
) -> Result<NodeAuthRequestCheckResponse> {
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
            // returns a string with node id on success
            let response: NodeAuthRequestCheckResponse = r.json().await?;
            Ok(response)
        }
        Err(e) => Err(anyhow!(e)),
    }
}

#[derive(Debug, Deserialize, Clone)]
pub struct NodeAuthRequest {
    pub request_id: String,
    pub node_info: String,
    pub created_at: String,
}

pub async fn list_auth_requests(
    api_url: &str,
    token: &str,
    trust_invalid_server_cert: bool,
) -> Result<Vec<NodeAuthRequest>, anyhow::Error> {
    let url = format!("{}/auth/request", api_url);
    let client = get_client(trust_invalid_server_cert)?;

    let res = retry_async!(3, 3, client.get(&url).bearer_auth(token).send())?;
    match res.error_for_status() {
        Ok(r) => {
            let response: Vec<NodeAuthRequest> = r.json().await?;
            Ok(response)
        }
        Err(e) => Err(anyhow!(e)),
    }
}

#[derive(Serialize)]
pub struct AuthRequestAction {
    pub accept: bool,
    pub request_id: String,
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

#[derive(Debug, Deserialize, Clone)]
pub struct Node {
    pub id: String,
    pub name: String,
    pub updated_at: String,
    pub created_at: String,
    pub last_connection: String,
    pub online: bool,
    pub client_version: String,
    pub target_client_version: String,
    #[serde(default)]
    pub system_info: NodeSystemInfo,
}

pub async fn list_nodes(
    api_url: &str,
    token: &str,
    trust_invalid_server_cert: bool,
) -> Result<Vec<Node>> {
    let client = get_client(trust_invalid_server_cert)?;

    let res = retry_async!(
        3,
        3,
        client
            .get(&format!("{}/node", api_url))
            .bearer_auth(token)
            .send()
    )?;
    match res.error_for_status() {
        Ok(res) => Ok(res.json().await?),
        Err(e) => Err(anyhow!(e)),
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct NodeSystemInfo {
    pub hostname: String,
    pub public_ip_address: Option<String>,
    pub operating_system: String,
    pub architecture: String,
    #[serde(default)]
    pub cores: Option<u32>,
    pub cpu_name: String,
    #[serde(default)]
    /// Memory in GB
    pub memory: Option<f64>,
    #[serde(default)]
    pub gpus: Vec<String>,
    #[serde(default)]
    pub latitude: Option<f64>,
    #[serde(default)]
    pub longitude: Option<f64>,
    #[serde(default)]
    pub country_code: Option<String>,
}

#[derive(Deserialize, Serialize, Default)]
pub struct UpdateNodeBody {
    pub system_info: Option<NodeSystemInfo>,
    pub client_version: Option<String>,
}

pub async fn report_node_details(
    api_url: &str,
    token: &str,
    node_id: &str,
    body: UpdateNodeBody,
    trust_invalid_server_cert: bool,
) -> Result<()> {
    let client = get_client(trust_invalid_server_cert)?;
    let url = format!("{}/node/{}", api_url.trim_end_matches('/'), node_id);

    let res = retry_async!(
        3,
        3,
        client.post(&url).bearer_auth(token).json(&body).send()
    );
    if let Err(e) = res {
        eprintln!("[Node] Error reporting node details: {}", e);
        return Err(anyhow!(e));
    }
    match res.unwrap().error_for_status() {
        Ok(_) => Ok(()),
        Err(e) => {
            eprintln!("[Node] Error reporting node details: {}", e);
            Err(anyhow!(e))
        }
    }
}

pub async fn request_control_tunnel_token(
    api_url: &str,
    token: &str,
    node_id: &str,
    trust_invalid_server_cert: bool,
) -> Result<String> {
    let client = get_client(trust_invalid_server_cert)?;
    let url = format!("{}/node/{}/token", api_url.trim_end_matches('/'), node_id);

    let res = retry_async!(3, 3, client.get(&url).bearer_auth(token).send());
    if let Err(e) = res {
        eprintln!("[Node] Error reporting node details: {}", e);
        return Err(anyhow!(e));
    }
    match res.unwrap().error_for_status() {
        Ok(r) => {
            let control_token = r.text().await?;
            Ok(control_token)
        }
        Err(e) => {
            eprintln!("[Node] Error reporting node details: {}", e);
            Err(anyhow!(e))
        }
    }
}

pub async fn connect_control_tunnel() -> anyhow::Result<()> {
    let config = Config::load().context("Failed to load configuration")?;
    let token = AuthManager::get_agent_token()?;

    let node_id = config.node_id.clone();
    let control_tunnel_token = request_control_tunnel_token(
        &config.api_url,
        &token,
        &node_id,
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
    let line = format!("M87 node_id={} token={}\n", node_id, control_tunnel_token);
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
