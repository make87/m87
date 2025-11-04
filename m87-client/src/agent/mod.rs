mod daemon;
pub mod services;
pub mod system_metrics;

use anyhow::{anyhow, Result};
use tracing::info;

use crate::auth::AuthManager;
use crate::config::Config;

pub async fn run(owner_ref: Option<String>) -> Result<()> {
    info!("Running agent");
    match owner_ref {
        Some(owner) => {
            Config::add_owner_reference(owner)?;
        }
        None => {
            if !Config::has_owner_reference()? {
                return Err(anyhow!(
                    "No owner reference found. Pass a valid user email or organization id!"
                ));
            }
        }
    }
    daemon::run().await
}

pub async fn install(owner_ref: Option<String>) -> Result<()> {
    info!("Installing agent service");
    match owner_ref {
        Some(owner) => {
            Config::add_owner_reference(owner)?;
        }
        None => {
            if !Config::has_owner_reference()? {
                return Err(anyhow!(
                    "No owner reference found. Pass a valid user email or organization id!"
                ));
            }
        }
    }
    daemon::install_service().await
}

pub async fn uninstall() -> Result<()> {
    info!("Uninstalling agent service");
    daemon::uninstall_service().await
}

pub async fn status() -> Result<()> {
    daemon::status_service().await
}
