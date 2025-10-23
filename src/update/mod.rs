use anyhow::Result;
use self_update::cargo_crate_version;
use tracing::{error, info, warn};

/// Check for updates and apply them if available.
///
/// Used both by the CLI (`m87 update`) and the daemon’s self-check.
pub async fn update(interactive: bool) -> Result<bool> {
    info!("Checking for updates...");
    let current_version = cargo_crate_version!();

    // Replace this once you have tagged releases on GitHub
    let maybe_status = self_update::backends::github::Update::configure()
        .repo_owner("make87")
        .repo_name("make87-client")
        .bin_name("m87")
        .current_version(current_version)
        .no_confirm(!interactive)
        .build()
        .and_then(|u| u.update());

    match maybe_status {
        Ok(status) => {
            let new_version = status.version();
            if new_version != current_version {
                info!("Updated from {} → {}", current_version, new_version);
                if interactive {
                    info!("Updated from {} → {}", current_version, new_version);
                }
                return Ok(true);
            } else if interactive {
                info!("You are running the latest version ({})", current_version);
            }
        }
        Err(e) => {
            warn!("Self-update failed: {}", e);
            if interactive {
                warn!("Failed to check for updates: {}", e);
            }
        }
    }

    Ok(false)
}

/// Helper for daemon use — silently apply and exit if updated.
pub async fn daemon_check_and_update() -> Result<()> {
    match update(false).await {
        Ok(true) => {
            info!("Agent updated; exiting for restart via systemd");
            std::process::exit(0);
        }
        Ok(false) => {}
        Err(e) => error!("Update check failed: {:?}", e),
    }
    Ok(())
}
