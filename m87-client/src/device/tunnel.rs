use anyhow::Result;
use m87_shared::forward::ForwardAccess;
use m87_shared::forward::PublicForward;

use crate::devices;
use crate::server;
use crate::util::network::get_public_ip;
use crate::util::tls::forward_server_port;
use crate::{auth::AuthManager, config::Config};

pub async fn list_tunnels(device_name: &str) -> Result<Vec<PublicForward>> {
    let config = Config::load()?;
    let server_url = config.get_server_url();
    let token = AuthManager::get_cli_token().await?;

    let dev = devices::list_devices()
        .await?
        .into_iter()
        .find(|d| d.name == device_name)
        .ok_or_else(|| anyhow::anyhow!("Device '{}' not found", device_name))?;
    let fwds = server::list_forwards(
        &server_url,
        &token,
        &dev.id,
        config.trust_invalid_server_cert,
    )
    .await?;

    Ok(fwds)
}

pub async fn delete_tunnel(device_name: &str, port: u16) -> Result<()> {
    let config = Config::load()?;
    let server_url = config.get_server_url();
    let token = AuthManager::get_cli_token().await?;

    let dev = devices::list_devices()
        .await?
        .into_iter()
        .find(|d| d.name == device_name)
        .ok_or_else(|| anyhow::anyhow!("Device '{}' not found", device_name))?;

    server::delete_forward(
        &server_url,
        &token,
        &dev.id,
        config.trust_invalid_server_cert,
        port,
    )
    .await?;

    Ok(())
}

pub async fn create_tunnel(
    device_name: &str,
    target_port: u16,
    ip_addresses: Option<Vec<String>>,
    add_own_address: bool,
    open: bool,
    name: &str,
) -> Result<()> {
    let config = Config::load()?;
    let server_url = config.get_server_url();
    let token = AuthManager::get_cli_token().await?;

    let dev = devices::list_devices()
        .await?
        .into_iter()
        .find(|d| d.name == device_name)
        .ok_or_else(|| anyhow::anyhow!("Device '{}' not found", device_name))?;

    let access = match open {
        true => ForwardAccess::Open,
        false => {
            let mut ips = ip_addresses.unwrap_or(vec![]);
            if add_own_address {
                let own_ip = get_public_ip().await?;
                ips.push(own_ip);
            }
            ForwardAccess::IpWhitelist(ips)
        }
    };

    let _ = server::request_forward(
        &server_url,
        &token,
        &dev.id,
        &dev.short_id,
        target_port,
        access,
        name,
        config.trust_invalid_server_cert,
    )
    .await?;

    Ok(())
}

pub async fn open_local_tunnel(device_name: &str, name: &str, local_port: u16) -> Result<()> {
    let config = Config::load()?;

    let dev = devices::list_devices()
        .await?
        .into_iter()
        .find(|d| d.name == device_name)
        .ok_or_else(|| anyhow::anyhow!("Device '{}' not found", device_name))?;

    let host_name = format!("{}-{}.{}", name, dev.short_id, config.get_server_hostname());

    let _ = forward_server_port(&host_name, local_port, config.trust_invalid_server_cert).await?;

    Ok(())
}
