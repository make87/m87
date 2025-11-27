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

    let config = Config::load()?;

    let dev = devices::get_device_by_name(device_name).await?;

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
