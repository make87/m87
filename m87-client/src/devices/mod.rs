use anyhow::Result;

use crate::{auth::AuthManager, config::Config, server};

pub async fn list_devices() -> Result<Vec<server::Device>> {
    let token = AuthManager::get_cli_token().await?;
    let config = Config::load()?;
    server::list_devices(&config.api_url, &token, config.trust_invalid_server_cert).await
}

pub async fn metrics(device_id: &str) -> Result<()> {
    Ok(())
}

pub async fn logs(device_id: &str) -> Result<()> {
    Ok(())
}

pub async fn get_ssh_url(device_id: &str) -> Result<String> {
    Ok(String::new())
}

pub async fn connect_ssh(device_id: &str) -> Result<()> {
    Ok(())
}
