mod daemon;
mod service;

use anyhow::Result;
use tracing::info;

pub async fn run(foreground: bool) -> Result<()> {
    info!("Starting agent daemon (foreground: {})", foreground);
    
    if foreground {
        daemon::run_foreground().await
    } else {
        daemon::run_background().await
    }
}

pub async fn install() -> Result<()> {
    info!("Installing agent service");
    service::install().await
}

pub async fn uninstall() -> Result<()> {
    info!("Uninstalling agent service");
    service::uninstall().await
}

pub async fn status() -> Result<()> {
    info!("Checking agent status");
    service::status().await
}
