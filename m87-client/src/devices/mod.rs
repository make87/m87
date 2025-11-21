use anyhow::Result;
use m87_shared::device::PublicDevice;

use crate::{auth::AuthManager, config::Config, server};

pub async fn list_devices() -> Result<Vec<PublicDevice>> {
    let token = AuthManager::get_cli_token().await?;
    let config = Config::load()?;
    server::list_devices(
        &config.get_server_url(),
        &token,
        config.trust_invalid_server_cert,
    )
    .await
}

pub async fn metrics(_device_id: &str) -> Result<()> {
    Ok(())
}

pub async fn logs(_device_id: &str) -> Result<()> {
    Ok(())
}

pub async fn get_ssh_url(_device_id: &str) -> Result<String> {
    Ok(String::new())
}

pub async fn connect_ssh(_device_id: &str) -> Result<()> {
    Ok(())
}
