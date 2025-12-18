use anyhow::Result;
use self_update::cargo_crate_version;
use tracing::{error, info, warn};

fn arch_bin_name() -> &'static str {
    #[cfg(target_arch = "x86_64")]
    {
        "m87-x86_64-unknown-linux-gnu"
    }

    #[cfg(target_arch = "aarch64")]
    {
        "m87-aarch64-unknown-linux-gnu"
    }

    #[cfg(target_arch = "riscv64")]
    {
        "m87-riscv64gc-unknown-linux-gnu"
    }
}

pub async fn update(interactive: bool) -> Result<bool> {
    info!("Checking for updates...");
    let current_version = cargo_crate_version!();

    let asset_name = arch_bin_name();
    let maybe_status = self_update::backends::github::Update::configure()
        .repo_owner("make87")
        .repo_name("make87")
        .bin_name(asset_name)
        .identifier(asset_name)
        .current_version(current_version)
        .no_confirm(!interactive)
        .build()
        .and_then(|u| u.update());

    match maybe_status {
        Ok(status) => {
            let new_version = status.version();
            if new_version != current_version {
                info!("[done] Updated from {} → {}", current_version, new_version);
                return Ok(true);
            } else if interactive {
                info!(
                    "[done] You are running the latest version ({})",
                    current_version
                );
            }
        }
        Err(e) => {
            warn!("[done] Self-update failed: {}", e);
        }
    }

    Ok(false)
}

/// Helper for daemon use — silently apply and exit if updated.
pub async fn daemon_check_and_update() -> Result<()> {
    match update(false).await {
        Ok(true) => {
            info!("Device updated; exiting for restart via systemd");
            std::process::exit(1); // throw error code on exit so systemd restarts "on-failure"
        }
        Ok(false) => {}
        Err(e) => error!("Update check failed: {:?}", e),
    }
    Ok(())
}
