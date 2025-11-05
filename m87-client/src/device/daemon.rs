use anyhow::{Context, Result};
use reqwest::Client;
use serde_json::Value;
use tokio::{
    pin, signal,
    time::{sleep, Duration},
};
use tracing::{error, info};

use std::path::Path;
use std::process::Command;

use crate::{
    auth::register_device,
    device::{services::collect_all_services, system_metrics::collect_system_metrics},
    server,
};
use crate::{auth::AuthManager, config::Config, rest::serve_server, util::macchina};

use crate::server::send_heartbeat;
use crate::util::logging::init_tracing_with_log_layer;
use crate::util::system_info::get_system_info;

const SERVICE_NAME: &str = "m87-device";
const SERVICE_FILE: &str = "/etc/systemd/system/m87-device.service";

pub async fn install_service() -> Result<()> {
    let exe_path = std::env::current_exe().context("Unable to resolve binary path")?;
    let service_content = format!(
        "[Unit]
Description=Device Service for make87
After=network.target

[Service]
ExecStart={} device run --headless
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
        info!("Uninstalled m87 device service");
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

pub async fn run() -> Result<()> {
    let _log_tx = init_tracing_with_log_layer("info");
    info!("Running device");
    let shutdown = signal::ctrl_c();
    pin!(shutdown);
    tokio::select! {
        _ = login_and_run() => {},
        _ = &mut shutdown => {
            info!("Received shutdown signal, stopping device");
        }
    }

    Ok(())
}

async fn login_and_run() -> Result<()> {
    // retry login/register until wit works, then call device_loop
    let config = Config::load().context("Failed to load configuration")?;
    let system_info = get_system_info(config.enable_geo_lookup).await?;
    loop {
        let success = register_device(config.owner_reference.clone(), system_info.clone()).await;
        if success.is_ok() {
            break;
        }
        sleep(Duration::from_secs(1)).await;
    }
    let token = AuthManager::get_device_token()?;
    let res = report_device_details(
        &config.api_url,
        &config.device_id,
        &token,
        config.enable_geo_lookup,
        config.trust_invalid_server_cert,
    )
    .await;

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

    tokio::task::spawn_local(async move {
        loop {
            println!("Starting control tunnel...");
            if let Err(e) = server::connect_control_tunnel().await {
                eprintln!("Control tunnel crashed with error: {e}. Restarting in 2 seconds...");
            } else {
                eprintln!("Control tunnel exited normally. Restarting in 2 seconds...");
            }
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
    });

    if res.is_err() {
        error!("Failed to report device details: {:?}", res);
    }

    device_loop().await?;
    Ok(())
}

async fn device_loop() -> Result<()> {
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

    let token = AuthManager::get_device_token()?;
    let metrics = collect_system_metrics().await?;
    let services = collect_all_services().await?;
    let _instruction = send_heartbeat(
        last_instruciotn_hash,
        &config.device_id,
        &config.api_url,
        &token,
        metrics,
        services,
    )
    .await?;
    info!("Sync complete");
    Ok(())
}

pub async fn report_device_details(
    api_url: &str,
    device_id: &str,
    token: &str,
    enable_geo_lookup: bool,
    trust_invalid_server_cert: bool,
) -> Result<()> {
    info!("Reporting device details");

    // Build update body
    let body = server::UpdateDeviceBody {
        client_version: Some(env!("CARGO_PKG_VERSION").to_string()),
        system_info: Some(get_system_info(enable_geo_lookup).await?),
    };
    server::report_device_details(api_url, token, device_id, body, trust_invalid_server_cert).await
}
