use anyhow::{anyhow, Context, Result};
use futures::StreamExt;
use reqwest::Client;
use tokio::{
    io::{self},
    net::{TcpListener, TcpStream},
};
use tokio_yamux::{Config as YamuxConfig, Session};
use tracing::{debug, error, info, warn};

#[cfg(feature = "agent")]
use crate::device::services::service_info::ServiceInfo;
#[cfg(feature = "agent")]
use crate::device::system_metrics::SystemMetrics;
use crate::{
    auth::AuthManager,
    config::Config,
    retry_async,
    util::{raw_connection::open_raw_io, shutdown::SHUTDOWN},
};

use crate::util::tls::get_tls_connection;

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
) -> Result<(String, String)> {
    let client = reqwest::Client::new();

    // ------------------------------------------------------------
    // 1. POST /login → returns ID
    // ------------------------------------------------------------
    let post_url = format!("{}/v1/device/login", make87_api_url);

    #[derive(serde::Serialize)]
    struct EmptyBody {}

    let id: String = client
        .post(&post_url)
        .json(&EmptyBody {})
        .send()
        .await?
        .error_for_status()
        .map_err(|e| {
            error!("{:?}", e);
            e
        })?
        .json()
        .await?;

    // ------------------------------------------------------------
    // 2. Print browser login URL for the user
    // ------------------------------------------------------------

    let browser_url = format!("{}/app/devices/login/{}", make87_app_url, id);
    eprintln!("No server configured.");
    eprintln!("Open this link in your browser to log in:");
    eprintln!("{}", browser_url);
    eprintln!("Waiting for authentication...");

    // ------------------------------------------------------------
    // 3. Poll GET /login/{id} until url != None
    // ------------------------------------------------------------
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
pub async fn connect_control_tunnel() -> anyhow::Result<()> {
    let config = Config::load().context("Failed to load configuration")?;
    let token = AuthManager::get_device_token()?;

    let device_id = config.device_id.clone();
    let control_tunnel_token = request_control_tunnel_token(
        &config.get_server_url(),
        &token,
        &device_id,
        config.trust_invalid_server_cert,
    )
    .await?;

    // 1. TCP connect
    let control_host = format!("control.{}", config.get_server_hostname());
    let local_rest_port = config.server_port;
    let mut tls =
        get_tls_connection(control_host.to_string(), config.trust_invalid_server_cert).await?;

    info!("connected to {}. Starting handshake", &control_host);
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
    info!("control session created");
    // continuously poll session to handle keep-alives, frame exchange
    while let Some(Ok(mut yamux_stream)) = sess.next().await {
        info!("new yamux stream");
        tokio::spawn(async move {
            let mut local = match TcpStream::connect(("127.0.0.1", local_rest_port)).await {
                Ok(s) => s,
                Err(e) => {
                    warn!("failed to connect to local {}: {}", local_rest_port, e);
                    return;
                }
            };

            let _ = match io::copy_bidirectional(&mut yamux_stream, &mut local).await {
                Ok(_) => info!("proxy closed cleanly"),
                Err(e) => info!("proxy closed with error: {:?}", e),
            };
        });
    }
    info!("control session exited");
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

pub async fn tunnel_device_port(
    host_name: &str,
    token: &str,
    device_short_id: &str,
    remote_host: &str,
    remote_port: u16,
    local_port: u16,
) -> Result<()> {
    // Path is `/port/<remote_port>`
    let path = format!("/port/{remote_port}?host={remote_host}");

    let listener = TcpListener::bind(("127.0.0.1", local_port)).await?;
    info!("Listening on 127.0.0.1:{local_port} and forwarding to {device_short_id} -> {remote_host}:{remote_port}");

    loop {
        tokio::select! {
            accept_result = listener.accept() => {
                let (mut local_stream, addr) = accept_result?;
                info!("New local connection from {addr}");

                // Clone the raw tunnel uniquely for every connection:
                // We MUST NOT reuse the same raw IO for multiple forwards.
                //
                // Instead, reopen a raw IO for each TCP connection.
                //
                // This is identical to how SSH LocalForward works.
                let mut remote_io = open_raw_io(
                    host_name,
                    device_short_id,
                    &path,
                    token,
                    false,
                ).await?;

                tokio::spawn(async move {
                    let res = io::copy_bidirectional(&mut local_stream, &mut remote_io).await;
                    match res {
                        Ok(_) => info!("Port-forward connection {addr} closed cleanly"),
                        Err(e) => error!("Port-forward connection {addr} closed with error: {e:?}"),
                    }
                });
            }

            _ = SHUTDOWN.cancelled() => {
                info!("Shutdown requested — closing port forward");
                break;
            }
        }
    }

    Ok(())
}
