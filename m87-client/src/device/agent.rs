use anyhow::{Context, Result};
use tokio::{
    net::TcpListener,
    pin, signal,
    time::{sleep, Duration},
};
use tracing::{error, info};

use std::process::Command;
use std::{net::SocketAddr, path::Path};

use crate::{
    auth::register_device,
    device::{services::collect_all_services, system_metrics::collect_system_metrics},
    rest::routes::build_router,
    server,
};
use crate::{auth::AuthManager, config::Config};

use crate::server::send_heartbeat;
use crate::util::logging::init_tracing_with_log_layer;
use crate::util::system_info::get_system_info;

const SERVICE_NAME: &str = "m87-agent";
const SERVICE_FILE: &str = "/etc/systemd/system/m87-agent.service";

/// Internal helper: Install the systemd service file and reload daemon
/// Not directly callable from CLI - used by other functions when service is missing
pub async fn install_service() -> Result<()> {
    let exe_path = std::env::current_exe()?;
    let username = std::env::var("USER")?;  // Get current user

    let service_content = format!(
        "[Unit]
Description=m87 Agent Service
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
ExecStart={} agent run
Restart=on-failure
RestartSec=3
User={}
StandardOutput=journal
StandardError=journal
TimeoutStopSec=30
StartLimitBurst=5
StartLimitIntervalSec=30
Environment=RUST_LOG=info
ProtectSystem=strict
ProtectHome=read-only
PrivateTmp=true
NoNewPrivileges=true

[Install]
WantedBy=multi-user.target
",
        exe_path.display(),
        username
    );

    std::fs::write(SERVICE_FILE, &service_content)
        .context("Failed to write systemd service file")?;

    Command::new("sudo")
        .args(["systemctl", "daemon-reload"])
        .status()
        .context("Failed to reload systemd daemon")?;

    info!("Installed systemd service at {}", SERVICE_FILE);
    Ok(())
}

/// Internal helper: Uninstall the systemd service file
/// Not directly callable from CLI - used by other functions
pub async fn uninstall_service() -> Result<()> {
    if Path::new(SERVICE_FILE).exists() {
        Command::new("sudo")
            .args(["systemctl", "stop", SERVICE_NAME])
            .status()
            .ok();
        Command::new("sudo")
            .args(["systemctl", "disable", SERVICE_NAME])
            .status()
            .ok();
        std::fs::remove_file(SERVICE_FILE).context("Failed to remove service file")?;
        Command::new("sudo")
            .args(["systemctl", "daemon-reload"])
            .status()
            .ok();
        info!("Uninstalled m87 device service");
    } else {
        info!("Service not found, nothing to uninstall");
    }

    Ok(())
}

/// Ensure service file exists, install if missing
async fn ensure_service_installed() -> Result<()> {
    if !Path::new(SERVICE_FILE).exists() {
        info!("Service file not found, installing...");
        install_service().await?;
    }
    Ok(())
}

/// CLI: m87 agent start
/// Starts the agent service (auto-installs if service file doesn't exist)
pub async fn start() -> Result<()> {
    ensure_service_installed().await?;

    Command::new("sudo")
        .args(["systemctl", "start", SERVICE_NAME])
        .status()
        .context("Failed to start service")?;

    info!("Started m87-agent service");
    Ok(())
}

/// CLI: m87 agent stop
/// Stops the agent service
pub async fn stop() -> Result<()> {
    Command::new("sudo")
        .args(["systemctl", "stop", SERVICE_NAME])
        .status()
        .context("Failed to stop service")?;

    info!("Stopped m87-agent service");
    Ok(())
}

/// CLI: m87 agent restart
/// Restarts the agent service (auto-installs if service file doesn't exist)
pub async fn restart() -> Result<()> {
    ensure_service_installed().await?;

    Command::new("sudo")
        .args(["systemctl", "restart", SERVICE_NAME])
        .status()
        .context("Failed to restart service")?;

    info!("Restarted m87-agent service");
    Ok(())
}

/// CLI: m87 agent enable [--now]
/// Enables auto-start on boot (auto-installs if service file doesn't exist)
pub async fn enable(now: bool) -> Result<()> {
    ensure_service_installed().await?;

    if now {
        Command::new("sudo")
            .args(["systemctl", "enable", "--now", SERVICE_NAME])
            .status()
            .context("Failed to enable service")?;
        info!("Enabled and started m87-agent service");
    } else {
        Command::new("sudo")
            .args(["systemctl", "enable", SERVICE_NAME])
            .status()
            .context("Failed to enable service")?;
        info!("Enabled m87-agent service");
    }

    Ok(())
}

/// CLI: m87 agent disable [--now]
/// Disables auto-start on boot
pub async fn disable(now: bool) -> Result<()> {
    if now {
        Command::new("sudo")
            .args(["systemctl", "disable", "--now", SERVICE_NAME])
            .status()
            .context("Failed to disable service")?;
        info!("Disabled and stopped m87-agent service");
    } else {
        Command::new("sudo")
            .args(["systemctl", "disable", SERVICE_NAME])
            .status()
            .context("Failed to disable service")?;
        info!("Disabled m87-agent service");
    }

    Ok(())
}

/// CLI: m87 agent status
/// Shows service status (auto-installs if service file doesn't exist)
pub async fn status() -> Result<()> {
    ensure_service_installed().await?;

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
    rustls::crypto::CryptoProvider::install_default(rustls::crypto::ring::default_provider())
        .expect("failed to install ring crypto provider");
    //
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

    let port = config.server_port.clone();
    tokio::task::spawn(async move {
        loop {
            info!("Starting log server...");
            let app = build_router();
            let addr = SocketAddr::from(([0, 0, 0, 0], port));
            let listener = TcpListener::bind(addr).await;
            if let Err(e) = listener {
                eprintln!("Failed to bind log server: {e}. Restarting in 2 seconds...");
                tokio::time::sleep(Duration::from_secs(2)).await;
                continue;
            }
            let listener = listener.unwrap();
            let res = axum::serve(listener, app.into_make_service()).await;
            if let Err(e) = res {
                eprintln!("Log server crashed with error: {e}. Restarting in 2 seconds...");
            }
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
    });

    tokio::task::spawn(async {
        loop {
            println!("Starting control tunnel...");
            if let Err(e) = server::connect_control_tunnel().await {
                eprintln!("Control tunnel crashed with error: {e}. Restarting in 10 seconds...");
            } else {
                eprintln!("Control tunnel exited normally. Restarting in 10 seconds...");
            }
            tokio::time::sleep(Duration::from_secs(10)).await;
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
        config.trust_invalid_server_cert,
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
