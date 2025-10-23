use anyhow::{anyhow, Context, Result};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::{
    pin, signal,
    time::{sleep, Duration},
};
use tracing::{error, info};

use std::path::Path;
use std::process::Command;

use crate::auth::{login, register};
use crate::{
    auth::AuthManager,
    config::Config,
    server::serve_server,
    util::macchina::{self, get_operating_system},
};

use crate::agent::heartbeat::send_heartbeat;
use crate::util::logging::init_tracing_with_log_layer;

const SERVICE_NAME: &str = "m87-agent";
const SERVICE_FILE: &str = "/etc/systemd/system/m87-agent.service";

pub async fn install_service() -> Result<()> {
    let exe_path = std::env::current_exe().context("Unable to resolve binary path")?;
    let service_content = format!(
        "[Unit]
Description=make87 Agent Service
After=network.target

[Service]
ExecStart={} agent run --headless
Restart=always
RestartSec=3
User=root
Environment=RUST_LOG=info

[Install]
WantedBy=multi-user.target
",
        exe_path.display()
    );

    std::fs::write(SERVICE_FILE, &service_content)
        .context("Failed to write systemd service file")?;

    Command::new("systemctl").args(["daemon-reload"]).status()?;
    Command::new("systemctl")
        .args(["enable", SERVICE_NAME])
        .status()?;
    Command::new("systemctl")
        .args(["start", SERVICE_NAME])
        .status()?;

    info!("Installed and started systemd service at {}", SERVICE_FILE);
    Ok(())
}

pub async fn uninstall_service() -> Result<()> {
    if Path::new(SERVICE_FILE).exists() {
        Command::new("systemctl")
            .args(["stop", SERVICE_NAME])
            .status()
            .ok();
        Command::new("systemctl")
            .args(["disable", SERVICE_NAME])
            .status()
            .ok();
        std::fs::remove_file(SERVICE_FILE).context("Failed to remove service file")?;
        Command::new("systemctl")
            .args(["daemon-reload"])
            .status()
            .ok();
        info!("Uninstalled make87 agent service");
    } else {
        info!("Service not found, nothing to uninstall");
    }

    Ok(())
}

pub async fn status_service() -> Result<()> {
    let output = Command::new("systemctl")
        .args(["status", SERVICE_NAME])
        .output()
        .context("Failed to query service status")?;

    let msg = match output.stdout.len() == 0 {
        true => String::from_utf8_lossy(&output.stderr),
        false => String::from_utf8_lossy(&output.stdout),
    };
    info!("{}", msg);
    Ok(())
}

pub async fn run(headless: bool) -> Result<()> {
    let _log_tx = init_tracing_with_log_layer();
    info!("Running agent");
    let shutdown = signal::ctrl_c();
    pin!(shutdown);
    tokio::select! {
        _ = login_and_run(headless) => {},
        _ = &mut shutdown => {
            info!("Received shutdown signal, stopping agent");
        }
    }

    Ok(())
}

async fn login_and_run(headless: bool) -> Result<()> {
    // retry login/register until wit works, then call agent_loop
    loop {
        let success = match headless {
            true => register(None).await,
            false => login().await,
        };
        if success.is_ok() {
            break;
        }
        sleep(Duration::from_secs(1)).await;
    }
    let config = Config::load().context("Failed to load configuration")?;
    let mut manager = AuthManager::from_default_path()?;
    let token = manager.get_token().await?;
    let res = report_node_details(&config.api_url, &config.node_id, &token).await;

    tokio::task::spawn_local(async move {
        loop {
            println!("Starting log server...");
            if let Err(e) = serve_server().await {
                eprintln!("Log server crashed with error: {e}. Restarting in 2 seconds...");
            } else {
                eprintln!("Log server exited normally. Restarting in 2 seconds...");
            }
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
    });

    if res.is_err() {
        error!("Failed to report node details: {:?}", res);
    }

    agent_loop().await?;
    Ok(())
}

async fn agent_loop() -> Result<()> {
    loop {
        if let Err(e) = sync_with_backend().await {
            error!("Sync failed: {:?}", e);
        }
        sleep(Duration::from_secs(60)).await; // 5 minutes
    }
}

async fn sync_with_backend() -> Result<()> {
    info!("Syncing with backend...");

    let config = Config::load().context("Failed to load configuration")?;
    let last_instruciotn_hash = "";

    let mut manager = AuthManager::from_default_path()?;
    let token = manager.get_token().await?;
    let _instruction = send_heartbeat(
        last_instruciotn_hash,
        &config.node_id,
        &config.api_url,
        &token,
    )
    .await?;
    // TODO: settings update, reverse proxy, compose secrets, ssh keys/
    // update::daemon_check_and_update().await?;
    info!("Sync complete");
    Ok(())
}

#[derive(Debug, Serialize, Deserialize, Default, Clone)]
pub struct UpdateNodeBody {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub operating_system: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_version: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub managed_node_reference: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub public_ip_address: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub connection_info: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub architecture: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub node_info: Option<String>, // or structured type if you have one

    #[serde(skip_serializing_if = "Option::is_none")]
    pub peripherals: Option<Vec<String>>, // or Vec<Peripheral> if defined
}

pub async fn report_node_details(api_url: &str, node_id: &str, token: &str) -> Result<()> {
    info!("Reporting node details");
    let client = Client::builder()
        .timeout(std::time::Duration::from_secs(3))
        .build()?;

    let node_info = macchina::get_detailed_printout();

    // Try to detect DigitalOcean managed node ID
    let managed_node_id = match client
        .get("http://169.254.169.254/metadata/v1/id")
        .send()
        .await
    {
        Ok(resp) => match resp.text().await {
            Ok(text) => Some(text),
            Err(_) => None,
        },
        Err(_) => None,
    };

    // We use this to find the geographically closest nexus to oyur node for faster tunneling.
    let (public_ip, connection_info) = match client.get("http://ip-api.com/json").send().await {
        Ok(resp) => {
            let text = resp.text().await?;
            let parsed: Value = serde_json::from_str(&text)?;
            let ip = parsed
                .get("query")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            if ip.is_some() {
                (ip, Some(text))
            } else {
                (None, None)
            }
        }
        Err(_) => (None, None),
    };

    // Determine architecture
    let arch = Command::new("uname")
        .arg("-m")
        .output()
        .ok()
        .and_then(|out| String::from_utf8(out.stdout).ok())
        .map(|s| match s.trim() {
            "aarch64" => "arm64".to_string(),
            "x86_64" => "amd64".to_string(),
            s => s.to_string(),
        })
        .unwrap_or_else(|| "unknown".to_string());

    // Build update body
    let body = UpdateNodeBody {
        operating_system: Some(get_operating_system()),
        client_version: Some(env!("CARGO_PKG_VERSION").to_string()),
        managed_node_reference: managed_node_id,
        public_ip_address: public_ip,
        connection_info,
        architecture: Some(arch),
        node_info: Some(node_info),
        ..Default::default()
    };

    // Send request
    let res = client
        .post(format!(
            "{}/api/v0/nodes/{}",
            api_url.trim_end_matches('/'),
            node_id
        ))
        .bearer_auth(token)
        .json(&body)
        .send()
        .await?;

    if !res.status().is_success() {
        let text = res.text().await.unwrap_or_default();
        return Err(anyhow!("Failed to update node: {}", text));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
}
