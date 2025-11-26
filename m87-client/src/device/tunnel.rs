use anyhow::Result;

use crate::devices;
use crate::server;
use crate::{auth::AuthManager, config::Config};

pub async fn open_local_tunnel(
    device_name: &str,
    remote_host: &str,
    remote_port: u16,
    local_port: u16,
) -> Result<()> {
    rustls::crypto::CryptoProvider::install_default(rustls::crypto::ring::default_provider())
        .expect("failed to install ring crypto provider");
    let config = Config::load()?;

    let dev = devices::list_devices()
        .await?
        .into_iter()
        .find(|d| d.name == device_name)
        .ok_or_else(|| anyhow::anyhow!("Device '{}' not found", device_name))?;

    let token = AuthManager::get_cli_token().await?;
    let device_short_id = dev.short_id;

    server::tunnel_device_port(
        &config.get_server_hostname(),
        &token,
        &device_short_id,
        remote_host,
        remote_port,
        local_port,
    )
    .await
}
