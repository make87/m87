#[cfg(feature = "agent")]
use anyhow::Context;
use anyhow::{Result, anyhow};
#[cfg(feature = "agent")]
use m87_shared::metrics::SystemMetrics;
use reqwest::Client;

#[cfg(feature = "agent")]
use tracing::warn;
use tracing::{error, info};

#[cfg(feature = "agent")]
use crate::{auth::AuthManager, config::Config, device::services::service_info::ServiceInfo};

use crate::retry_async;
// Import shared types
pub use m87_shared::auth::{
    AuthRequestAction, CheckAuthRequest, DeviceAuthRequest, DeviceAuthRequestBody,
    DeviceAuthRequestCheckResponse,
};
#[cfg(feature = "agent")]
pub use m87_shared::device::UpdateDeviceBody;
pub use m87_shared::device::{DeviceSystemInfo, PublicDevice};
pub use m87_shared::heartbeat::{Digests, HeartbeatRequest, HeartbeatResponse};

pub async fn get_server_url_and_owner_reference(
    make87_api_url: &str,
    make87_app_url: &str,
    owner_reference: Option<String>,
    server_url: Option<String>,
) -> Result<(String, String)> {
    // if owner ref and server url are some return them right away
    if let Some(owner_ref) = &owner_reference {
        if let Some(server) = &server_url {
            return Ok((server.clone(), owner_ref.clone()));
        }
    }

    let client = reqwest::Client::new();

    let post_url = format!("{}/v1/device/login", make87_api_url);

    #[derive(serde::Serialize)]
    struct EmptyBody {
        owner_reference: Option<String>,
        server_url: Option<String>,
    }

    let id: String = client
        .post(&post_url)
        .json(&EmptyBody {
            owner_reference: owner_reference.clone(),
            server_url,
        })
        .send()
        .await?
        .error_for_status()
        .map_err(|e| {
            error!("{:?}", e);
            e
        })?
        .json()
        .await?;

    if owner_reference.is_none() {
        // we only need the user to interact if we are missing a assigned owner. If we know the owner server can be aut oassigned
        let browser_url = format!("{}/devices/login/{}", make87_app_url, id);
        eprintln!("No server configured.");
        eprintln!("Open this link in your browser to log in:");
        eprintln!("{}", browser_url);
        eprintln!("Waiting for authentication...");
    }

    let get_url = format!("{}/v1/device/login/{}", make87_api_url, id);

    #[derive(serde::Deserialize)]
    struct LoginUrlResponse {
        url: Option<String>,
        owner_reference: Option<String>,
    }

    let mut wait_time = 0;

    loop {
        let resp = client
            .get(&get_url)
            .send()
            .await?
            .error_for_status()?
            .json::<LoginUrlResponse>()
            .await?;

        match (resp.url, resp.owner_reference) {
            (Some(url), Some(owner_reference)) => {
                return Ok((url, owner_reference));
            }
            _ => {}
        }

        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        wait_time += 2;
        if wait_time >= 120 {
            eprintln!("Timeout waiting 120s for authentication");
            return Err(anyhow::anyhow!("Timeout waiting for authentication"));
        }
    }
}

// Agent-specific: Used by device registration
#[cfg(feature = "agent")]
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

// Agent-specific: Used by device registration
#[cfg(feature = "agent")]
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

// Manager-specific: List pending device auth requests
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

// Manager-specific: Approve or reject device registration
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

// Manager-specific: List all accessible devices
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

// Agent-specific: Report device details to backend
#[cfg(feature = "agent")]
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

// Agent-specific: Request token for control tunnel
#[cfg(feature = "agent")]
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
            let control_token = r.text().await?.trim_matches('"').to_string();
            Ok(control_token)
        }
        Err(e) => {
            eprintln!("[Device] Error reporting device details: {}", e);
            Err(anyhow!(e))
        }
    }
}

// Agent-specific: Maintain persistent control tunnel connection
#[cfg(feature = "agent")]
pub async fn connect_control_tunnel() -> Result<()> {
    use quinn::Connection;

    use crate::streams::quic::get_quic_connection;

    let config = Config::load().context("Failed to load configuration")?;
    let token = AuthManager::get_device_token()?;
    let device_id = config.device_id.clone();

    // 1) Request tunnel token
    let control_tunnel_token = request_control_tunnel_token(
        &config.get_server_url(),
        &token,
        &device_id,
        config.trust_invalid_server_cert,
    )
    .await?;

    // 2) Build hostname control.<domain>
    let control_host = format!("control.{}", config.get_server_hostname());
    info!("Connecting QUIC control tunnel to {}", control_host);

    // 3) Establish QUIC connection
    let (_endpoint, quic_conn): (_, Connection) =
        get_quic_connection(control_host.clone(), config.trust_invalid_server_cert)
            .await
            .context("QUIC connect failed")?;

    info!("QUIC connection established. Sending handshake.");

    // ------------------------------------------------------------
    // 4) Send handshake over a dedicated control stream (bidi)
    // ------------------------------------------------------------
    let mut send = quic_conn
        .open_bi()
        .await
        .context("failed to open QUIC control handshake stream")?
        .0;

    let handshake = format!(
        "M87 device_id={} token={}\n",
        device_id, control_tunnel_token
    );
    send.write_all(handshake.as_bytes())
        .await
        .context("failed to send QUIC handshake")?;
    send.finish().ok();

    info!("Handshake sent. Control tunnel active.");

    loop {
        use crate::streams::{self, quic::QuicIo};

        tokio::select! {
            incoming = quic_conn.accept_bi() => {
                let Ok((quic_send, quic_recv)) = incoming else {
                    warn!("Control QUIC stream closed — reconnect required");
                    break;
                };

                let io = QuicIo {
                    recv: quic_recv,
                    send: quic_send,
                };

                info!("QUIC: new control stream accepted");

                // Spawn proxy task
                tokio::spawn(async move {
                    if let Err(e) = streams::router::handle_incoming_stream(io).await {
                        warn!("control proxy closed with error: {:?}", e);
                    }
                });
            }

            _ = quic_conn.closed() => {
                warn!("QUIC control connection closed by server");
                break;
            }
        }
    }

    info!("control tunnel terminated");
    Ok(())
}

// Agent-specific: Send heartbeat with metrics and services
#[cfg(feature = "agent")]
pub async fn send_heartbeat(
    last_instruction_hash: &str,
    device_id: &str,
    api_url: &str,
    token: &str,
    metrics: SystemMetrics,
    services: Vec<ServiceInfo>,
    trust_invalid_server_cert: bool,
) -> Result<HeartbeatResponse> {
    let req = HeartbeatRequest {
        last_instruction_hash: last_instruction_hash.to_string(),
        system: metrics,
        services,
    };

    let client = get_client(trust_invalid_server_cert)?;
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
