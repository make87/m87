use anyhow::Result;
use tracing::warn;

use crate::cli::{TunnelProtocol, TunnelSpec};
use crate::devices;
use crate::server;
use crate::{auth::AuthManager, config::Config};

/// Open multiple tunnels concurrently
pub async fn open_tunnels(device_name: &str, specs: Vec<TunnelSpec>) -> Result<()> {
    let config = Config::load()?;
    let dev = devices::get_device_by_name(device_name).await?;
    let token = AuthManager::get_cli_token().await?;
    let device_short_id = dev.short_id;
    let hostname = config.get_server_hostname();

    // Warn about UDP (not yet implemented)
    for spec in &specs {
        if spec.protocol == TunnelProtocol::Udp {
            warn!(
                "UDP forwarding not yet implemented for {}:{} - using TCP",
                spec.remote_host, spec.remote_port
            );
        }
    }

    // Spawn all tunnels concurrently
    let mut handles = Vec::new();
    for spec in specs {
        let hostname = hostname.clone();
        let token = token.clone();
        let device_short_id = device_short_id.clone();

        handles.push(tokio::spawn(async move {
            server::tunnel_device_port(
                &hostname,
                &token,
                &device_short_id,
                &spec.remote_host,
                spec.remote_port,
                spec.local_port,
            )
            .await
        }));
    }

    // Wait for all tunnels (they run until shutdown signal)
    for handle in handles {
        handle.await??;
    }

    Ok(())
}
