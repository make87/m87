use anyhow::{Context, Result, bail};
use std::fs::{self, Permissions};
use std::io::Write;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tempfile::NamedTempFile;
use tokio::{
    signal::unix::{SignalKind, signal},
    time::{Duration, sleep},
};
use tracing::{error, info, warn};

use crate::config::Config;
use crate::device::control_tunnel;
use crate::device::deployment_manager::DeploymentManager;
use crate::util::command::current_exe_path;
use crate::util::shutdown::SHUTDOWN;
use crate::util::system_info::get_system_info;
use crate::util::unix::{
    is_root, reexec_with_sudo, run_systemctl, run_systemctl_checked, validate_exec_path,
};
use crate::{auth::register_device, util::tls::set_tls_provider};

const SERVICE_NAME: &str = "m87-runtime";
const SERVICE_FILE: &str = "/etc/systemd/system/m87-runtime.service";
const SERVICE_FILE_MODE: u32 = 0o644;

/// Generate the systemd service file content with all XDG environment variables
fn generate_service_content(exe_path: &Path, username: &str, home: &Path) -> String {
    let home = home.display();

    format!(
        r#"[Unit]
Description=m87 Runtime Service
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
ExecStart={exe_path} runtime run
WorkingDirectory={home}
Restart=on-failure
RestartSec=3
User={username}

# Deterministic environment for user's config/data directories
Environment=HOME={home}
Environment=XDG_CONFIG_HOME={home}/.config
Environment=XDG_DATA_HOME={home}/.local/share
Environment=XDG_CACHE_HOME={home}/.cache
Environment=RUST_LOG=info

# Security hardening
UMask=0077

# Logging
StandardOutput=journal
StandardError=journal
SyslogIdentifier=m87-runtime

# Resource limits
TimeoutStopSec=30
StartLimitBurst=5
StartLimitIntervalSec=30

[Install]
WantedBy=multi-user.target
"#,
        exe_path = exe_path.display(),
        username = username,
        home = home,
    )
}

/// Write service file atomically using tempfile
/// Returns Ok(true) if file was changed, Ok(false) if no change needed
fn write_service_file_atomic(content: &str) -> Result<bool> {
    let service_path = Path::new(SERVICE_FILE);

    // Check if content differs from existing
    if service_path.exists() {
        let existing =
            fs::read_to_string(service_path).context("Failed to read existing service file")?;
        if existing == content {
            return Ok(false); // No change needed
        }
    }

    // Create temp file in same directory for atomic rename
    let dir = service_path
        .parent()
        .unwrap_or(Path::new("/etc/systemd/system"));
    let mut tmp = NamedTempFile::new_in(dir).context("Failed to create temporary service file")?;

    tmp.write_all(content.as_bytes())
        .context("Failed to write service content")?;

    tmp.as_file()
        .sync_all()
        .context("Failed to sync service file to disk")?;

    // Persist atomically (rename)
    let tmp_path = tmp.into_temp_path();
    tmp_path
        .persist(service_path)
        .context("Failed to persist service file")?;

    // Set permissions explicitly
    fs::set_permissions(service_path, Permissions::from_mode(SERVICE_FILE_MODE))
        .context("Failed to set service file permissions")?;

    Ok(true)
}

/// Internal function called by hidden subcommand after sudo re-exec
/// Must be run as root
pub async fn internal_setup_privileged(
    username: &str,
    home: &str,
    exe_path_str: &str,
    enable: bool,
    enable_now: bool,
    restart_if_running: bool,
) -> Result<()> {
    if !is_root() {
        bail!("internal_setup_privileged must be run as root");
    }

    let exe_path = PathBuf::from(exe_path_str);
    let home_dir = PathBuf::from(home);

    // Validate exe path
    validate_exec_path(&exe_path)?;

    // Generate service content
    let content = generate_service_content(&exe_path, username, &home_dir);

    // Write atomically
    let file_changed = write_service_file_atomic(&content)?;

    if file_changed {
        info!("Service file updated at {}", SERVICE_FILE);

        // Reload systemd daemon
        run_systemctl_checked(&["daemon-reload"])?;
    }

    // Handle enable/start based on flags
    if enable_now {
        // enable --now: enable at boot AND start immediately
        run_systemctl_checked(&["enable", "--now", SERVICE_NAME])?;
        info!("Enabled and started m87-runtime service");
    } else if enable {
        // enable without --now: just enable at boot
        run_systemctl_checked(&["enable", SERVICE_NAME])?;
        info!("Enabled m87-runtime service (starts on boot)");
    } else if restart_if_running && file_changed {
        // Check if service is active and restart if so
        let status = run_systemctl(&["is-active", "--quiet", SERVICE_NAME])?;
        if status.success() {
            run_systemctl_checked(&["restart", SERVICE_NAME])?;
            info!("Restarted m87-runtime service");
        }
    }

    Ok(())
}

/// Internal function to stop the service (must be run as root)
pub async fn internal_stop_privileged() -> Result<()> {
    if !is_root() {
        bail!("internal_stop_privileged must be run as root");
    }

    run_systemctl_checked(&["stop", SERVICE_NAME])?;
    info!("Stopped m87-runtime service");
    Ok(())
}

/// Internal function to disable the service (must be run as root)
pub async fn internal_disable_privileged(now: bool) -> Result<()> {
    if !is_root() {
        bail!("internal_disable_privileged must be run as root");
    }

    if now {
        run_systemctl_checked(&["disable", "--now", SERVICE_NAME])?;
        info!("Disabled and stopped m87-runtime service");
    } else {
        run_systemctl_checked(&["disable", SERVICE_NAME])?;
        info!("Disabled m87-runtime service");
    }
    Ok(())
}

/// Unified setup function that handles all installation scenarios
async fn setup_service(enable: bool, enable_now: bool, restart_if_running: bool) -> Result<()> {
    // Resolve user info from passwd database
    let user_info =
        crate::util::unix::resolve_invoking_user().context("Failed to determine user identity")?;

    let exe_path = current_exe_path()?;

    // Validate path early
    validate_exec_path(&exe_path)?;

    let exe_path_str = exe_path
        .to_str()
        .context("Executable path is not valid UTF-8")?;
    let home_str = user_info
        .home_dir
        .to_str()
        .context("Home directory path is not valid UTF-8")?;

    if is_root() {
        // Already root - run directly
        internal_setup_privileged(
            &user_info.username,
            home_str,
            exe_path_str,
            enable,
            enable_now,
            restart_if_running,
        )
        .await
    } else {
        // Re-exec with sudo using absolute path
        let mut args = vec![
            "internal",
            "runtime-setup-privileged",
            "--user",
            &user_info.username,
            "--home",
            home_str,
            "--exe-path",
            exe_path_str,
        ];

        if enable {
            args.push("--enable");
        }
        if enable_now {
            args.push("--enable-now");
        }
        if restart_if_running {
            args.push("--restart-if-running");
        }

        reexec_with_sudo(&args)
    }
}

/// CLI: m87 runtime enable [--now]
/// Enables auto-start on boot (auto-installs/updates service file)
pub async fn enable(now: bool) -> Result<()> {
    // enable: always enable at boot
    // enable --now: enable at boot AND start immediately
    setup_service(true, now, false).await
}

/// CLI: m87 runtime start
/// Starts the runtime service (auto-installs/updates service file)
pub async fn start() -> Result<()> {
    // start: enable at boot AND start immediately
    setup_service(true, true, false).await
}

/// CLI: m87 runtime restart
/// Restarts the runtime service (auto-installs/updates service file)
pub async fn restart() -> Result<()> {
    // restart: just restart if running (don't enable if not already enabled)
    setup_service(false, false, true).await
}

/// CLI: m87 runtime stop
/// Stops the runtime service
pub async fn stop() -> Result<()> {
    if is_root() {
        internal_stop_privileged().await
    } else {
        reexec_with_sudo(&["internal", "runtime-stop-privileged"])
    }
}

/// CLI: m87 runtime disable [--now]
/// Disables auto-start on boot
pub async fn disable(now: bool) -> Result<()> {
    if is_root() {
        internal_disable_privileged(now).await
    } else {
        let mut args = vec!["internal", "runtime-disable-privileged"];
        if now {
            args.push("--now");
        }
        reexec_with_sudo(&args)
    }
}

/// CLI: m87 runtime status
/// Shows service status (no sudo required for viewing status)
pub async fn status() -> Result<()> {
    let status = run_systemctl(&["status", "--lines=0", SERVICE_NAME])?;

    // Exit code 3 means service not running, which is valid for status
    // Exit code 4 means service unknown/not installed
    if let Some(code) = status.code() {
        if code == 4 {
            warn!("Service not installed. Run 'm87 runtime enable --now' to install and start.");
        }
    }

    Ok(())
}

/// CLI: m87 runtime run
/// Main runtime daemon entry point (used by systemd service)
pub async fn run() -> Result<()> {
    info!("Running device");

    // Handle both SIGTERM (systemd stop) and SIGINT (Ctrl+C)
    let mut sigterm =
        signal(SignalKind::terminate()).context("Failed to register SIGTERM handler")?;
    let mut sigint =
        signal(SignalKind::interrupt()).context("Failed to register SIGINT handler")?;

    tokio::select! {
        _ = login_and_run() => {},
        _ = sigterm.recv() => {
            info!("Received SIGTERM, stopping device");
            SHUTDOWN.cancel();
        }
        _ = sigint.recv() => {
            info!("Received SIGINT, stopping device");
            SHUTDOWN.cancel();
        }
    }

    Ok(())
}

async fn login_and_run() -> Result<()> {
    // retry login/register until it works, then call device_loop
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

    let unit_manager = DeploymentManager::new().await?;
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
