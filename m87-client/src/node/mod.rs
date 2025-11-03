use anyhow::{Context, Result};

use crate::{auth::AuthManager, config::Config, server};

pub async fn list_nodes() -> Result<Vec<server::Node>> {
    let token = AuthManager::get_cli_token().await?;
    let config = Config::load()?;
    server::list_nodes(&config.api_url, &token, config.trust_invalid_server_cert).await
}

pub async fn metrics(node_id: &str) -> Result<()> {
    Ok(())
}

pub async fn logs(node_id: &str) -> Result<()> {
    Ok(())
}

pub async fn get_ssh_url(node_id: &str) -> Result<String> {
    Ok(String::new())
}

pub async fn connect_ssh(node_id: &str) -> Result<()> {
    Ok(())
}
