use anyhow::{Context, Result};
use tokio::{
    pin, signal,
    time::{Duration, sleep},
};
use tracing::{error, info, warn};

use std::process::Command;
use std::{path::Path, sync::Arc};

use crate::{auth::register_device, util::tls::set_tls_provider};
use crate::{config::Config, device::deployment_manager::UnitManager};

use crate::device::control_tunnel;
use crate::util::shutdown::SHUTDOWN;
use crate::util::system_info::get_system_info;

const SERVICE_NAME: &str = "m87-agent";
const SERVICE_FILE: &str = "/etc/systemd/system/m87-agent.service";

/// Helper to check if a command failed due to permission issues and provide helpful error message
fn check_permission_error(status: std::process::ExitStatus) -> Result<()> {
    // Exit code 1 is commonly used for permission denied by systemctl
    // Exit code 4 is used by systemctl for insufficient privileges
    if let Some(code) = status.code() {
        if code == 1 || code == 4 {
            anyhow::bail!(
                "Permission denied. This command requires root privileges.\n\
                Please run with: sudo m87 agent <command>"
            );
        }
    }
    anyhow::bail!("Command failed with exit code: {:?}", status.code());
}

/// Internal helper: Install the systemd service file and reload daemon
/// Not directly callable from CLI - used by other functions when service is missing
pub async fn install_service() -> Result<()> {
    let exe_path = std::env::current_exe()?;
    // Get the actual user (not root)
    let username = std::env::var("SUDO_USER")
        .or_else(|_| std::env::var("USER"))
        .context("Unable to determine username")?;

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

[Install]
WantedBy=multi-user.target
",
        exe_path.display(),
        username
    );

    std::fs::write(SERVICE_FILE, &service_content)
        .context("Failed to write systemd service file")?;

    let status = Command::new("systemctl")
        .args(["daemon-reload"])
        .stdin(std::process::Stdio::inherit())
        .stdout(std::process::Stdio::inherit())
        .stderr(std::process::Stdio::inherit())
        .status()
        .context("Failed to reload systemd daemon")?;

    if !status.success() {
        check_permission_error(status)?;
    }

    info!("Installed systemd service at {}", SERVICE_FILE);
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

    let status = Command::new("systemctl")
        .args(["start", SERVICE_NAME])
        .stdin(std::process::Stdio::inherit())
        .stdout(std::process::Stdio::inherit())
        .stderr(std::process::Stdio::inherit())
        .status()
        .context("Failed to start service")?;

    if !status.success() {
        check_permission_error(status)?;
    }

    info!("Started m87-agent service");
    Ok(())
}

/// CLI: m87 agent stop
/// Stops the agent service
pub async fn stop() -> Result<()> {
    let status = Command::new("systemctl")
        .args(["stop", SERVICE_NAME])
        .stdin(std::process::Stdio::inherit())
        .stdout(std::process::Stdio::inherit())
        .stderr(std::process::Stdio::inherit())
        .status()
        .context("Failed to stop service")?;

    if !status.success() {
        check_permission_error(status)?;
    }

    info!("Stopped m87-agent service");
    Ok(())
}

/// CLI: m87 agent restart
/// Restarts the agent service (auto-installs if service file doesn't exist)
pub async fn restart() -> Result<()> {
    ensure_service_installed().await?;

    let status = Command::new("systemctl")
        .args(["restart", SERVICE_NAME])
        .stdin(std::process::Stdio::inherit())
        .stdout(std::process::Stdio::inherit())
        .stderr(std::process::Stdio::inherit())
        .status()
        .context("Failed to restart service")?;

    if !status.success() {
        check_permission_error(status)?;
    }

    info!("Restarted m87-agent service");
    Ok(())
}

/// CLI: m87 agent enable [--now]
/// Enables auto-start on boot (auto-installs if service file doesn't exist)
pub async fn enable(now: bool) -> Result<()> {
    ensure_service_installed().await?;

    let status = if now {
        let s = Command::new("systemctl")
            .args(["enable", "--now", SERVICE_NAME])
            .stdin(std::process::Stdio::inherit())
            .stdout(std::process::Stdio::inherit())
            .stderr(std::process::Stdio::inherit())
            .status()
            .context("Failed to enable service")?;
        info!("Enabled and started m87-agent service");
        s
    } else {
        let s = Command::new("systemctl")
            .args(["enable", SERVICE_NAME])
            .stdin(std::process::Stdio::inherit())
            .stdout(std::process::Stdio::inherit())
            .stderr(std::process::Stdio::inherit())
            .status()
            .context("Failed to enable service")?;
        info!("Enabled m87-agent service");
        s
    };

    if !status.success() {
        check_permission_error(status)?;
    }

    Ok(())
}

/// CLI: m87 agent disable [--now]
/// Disables auto-start on boot
pub async fn disable(now: bool) -> Result<()> {
    let status = if now {
        let s = Command::new("systemctl")
            .args(["disable", "--now", SERVICE_NAME])
            .stdin(std::process::Stdio::inherit())
            .stdout(std::process::Stdio::inherit())
            .stderr(std::process::Stdio::inherit())
            .status()
            .context("Failed to disable service")?;
        info!("Disabled and stopped m87-agent service");
        s
    } else {
        let s = Command::new("systemctl")
            .args(["disable", SERVICE_NAME])
            .stdin(std::process::Stdio::inherit())
            .stdout(std::process::Stdio::inherit())
            .stderr(std::process::Stdio::inherit())
            .status()
            .context("Failed to disable service")?;
        info!("Disabled m87-agent service");
        s
    };

    if !status.success() {
        check_permission_error(status)?;
    }

    Ok(())
}

/// CLI: m87 agent status
/// Shows service status (auto-installs if service file doesn't exist)
pub async fn status() -> Result<()> {
    ensure_service_installed().await?;

    let status = Command::new("systemctl")
        .args(["status", "--lines=0", SERVICE_NAME])
        .stdin(std::process::Stdio::inherit())
        .stdout(std::process::Stdio::inherit())
        .stderr(std::process::Stdio::inherit())
        .status()
        .context("Failed to query service status")?;

    if !status.success() {
        check_permission_error(status)?;
    }

    Ok(())
}

pub async fn run() -> Result<()> {
    info!("Running device");
    let shutdown = signal::ctrl_c();
    pin!(shutdown);
    tokio::select! {
        _ = login_and_run() => {},
        _ = &mut shutdown => {
            info!("Received shutdown signal, stopping device");
            SHUTDOWN.cancel();
        }
    }

    Ok(())
}

async fn login_and_run() -> Result<()> {
    // retry login/register until wit works, then call device_loop
    set_tls_provider();

    let config = Config::load()?;
    let system_info = get_system_info().await?;
    loop {
        let success = register_device(config.owner_reference.clone(), system_info.clone()).await;
        if success.is_ok() {
            break;
        }
        sleep(Duration::from_secs(1)).await;
    }

    let unit_manager = UnitManager::new().await?;
    let manager = Arc::new(unit_manager);
    manager.clone().start();

    loop {
        if SHUTDOWN.is_cancelled() {
            break;
        }
        info!("Starting control tunnel...");
        tokio::select! {
            result = control_tunnel::connect_control_tunnel(manager.clone()) => {
                match result {
                    Err(e) => {
                        error!("Control tunnel crashed with error: {e}. Reconnecting in 5 seconds...");
                        tokio::time::sleep(Duration::from_secs(5)).await;
                    }
                    Ok(_) => {
                        warn!("Control tunnel exited normally. Reconnecting...");
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                }
            }
            _ = SHUTDOWN.cancelled() => {
                info!("Control tunnel shutting down");
                break;
            }
        }
    }

    Ok(())
}
