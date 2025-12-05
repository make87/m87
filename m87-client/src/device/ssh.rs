use anyhow::{Context, Result};
use tokio::{io, net::TcpListener};
use tracing::{error, info};

use crate::{
    auth::AuthManager,
    config::Config,
    devices,
    streams::{
        quic::{connect_quic_only, open_quic_stream},
        stream_type::StreamType,
    },
    util::shutdown::SHUTDOWN,
};

pub async fn tunnel_device_ssh(device_name: &str, local_port: u16) -> Result<()> {
    let config = Config::load()?;

    let dev = devices::get_device_by_name(device_name).await?;

    let token = AuthManager::get_cli_token().await?;
    let device_short_id = dev.short_id;
    let hostname = config.get_server_hostname();

    info!(
        "Connecting SSH tunnel to device {device_short_id}, \
         available locally on 127.0.0.1:{local_port}"
    );

    let listener = TcpListener::bind(("127.0.0.1", local_port)).await?;

    let (_, conn) = connect_quic_only(
        &hostname,
        &device_short_id,
        config.trust_invalid_server_cert,
    )
    .await
    .context("Failed to connect to device")?;

    loop {
        tokio::select! {
            Ok((mut local_stream, addr)) = listener.accept() => {
                info!("Local SSH connection from {addr}");
                let mut remote_io = open_quic_stream(&conn, StreamType::Ssh {
                    token: token.to_string(),
                }).await?;

                tokio::spawn(async move {
                    let res = io::copy_bidirectional(&mut local_stream, &mut remote_io).await;
                    match res {
                        Ok(_) => info!("SSH tunnel {addr} closed"),
                        Err(e) => error!("SSH tunnel {addr} error: {e:?}"),
                    }
                });
            }

            _ = SHUTDOWN.cancelled() => {
                info!("Shutdown requested — closing SSH tunnel");
                break;
            }
        }
    }

    Ok(())
}
