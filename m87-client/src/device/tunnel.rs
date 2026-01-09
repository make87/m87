use std::sync::Arc;

use crate::devices;
use crate::streams::quic::connect_quic_only;
use crate::streams::quic::open_quic_stream;
use crate::streams::stream_type::{SocketTarget, TcpTarget, TunnelTarget, UdpTarget};
use crate::util::shutdown::SHUTDOWN;
use crate::util::udp::decode_socket_addr;
use crate::util::udp::encode_socket_addr;
use crate::{auth::AuthManager, config::Config};
use anyhow::Result;
use bytes::BufMut;
use bytes::BytesMut;
use tokio::io;
use tokio::net::TcpListener;
use tokio::net::UdpSocket;
use tokio::net::UnixListener;
use tracing::{debug, error, info, warn};

pub async fn open_local_tunnel(device_name: &str, tunnel_specs: Vec<String>) -> Result<()> {
    let config = Config::load()?;
    let resolved = devices::resolve_device_short_id_cached(device_name).await?;
    let token = AuthManager::get_cli_token().await?;
    let trust = config.trust_invalid_server_cert;

    let tunnels: Vec<TunnelTarget> = TunnelTarget::from_list(tunnel_specs)?;

    // spawn each tunnel as a background task
    for t in tunnels {
        let token = token.clone();
        let resolved = resolved.clone();
        tokio::spawn(async move {
            if let Err(e) =
                tunnel_device_port(&resolved.host, &token, &resolved.short_id, t, trust).await
            {
                error!("Tunnel exited with error: {}", e);
                SHUTDOWN.cancel();
            }
        });
    }

    // Wait for Ctrl-C shutdown
    SHUTDOWN.cancelled().await;
    info!("SIGINT: shutting down all tunnels");
    Ok(())
}

pub async fn tunnel_device_port(
    host_name: &str,
    token: &str,
    device_short_id: &str,
    tunnel_target: TunnelTarget,
    trust_invalid_server_cert: bool,
) -> Result<()> {
    match &tunnel_target {
        TunnelTarget::Tcp(target) => {
            tunnel_device_port_tcp(
                host_name,
                token,
                device_short_id,
                target,
                trust_invalid_server_cert,
            )
            .await
        }
        TunnelTarget::Udp(target) => {
            tunnel_device_port_udp(
                host_name,
                token,
                device_short_id,
                target,
                trust_invalid_server_cert,
            )
            .await
        }
        TunnelTarget::Socket(target) => {
            tunnel_device_socket(
                host_name,
                token,
                device_short_id,
                target,
                trust_invalid_server_cert,
            )
            .await
        }
        TunnelTarget::Vpn(_target) => {
            info!("COMING SOON");
            Ok(())
        }
    }
}

async fn tunnel_device_port_tcp(
    host_name: &str,
    token: &str,
    device_short_id: &str,
    tunnel_spec: &TcpTarget,
    trust_invalid_server_cert: bool,
) -> Result<()> {
    debug!(
        "Binding TCP listener on 127.0.0.1:{}",
        tunnel_spec.local_port
    );
    let listener = TcpListener::bind(("127.0.0.1", tunnel_spec.local_port)).await?;
    let remote_host = tunnel_spec.remote_host.clone();

    debug!("Connecting to QUIC server...");
    let (_endpoint, conn) =
        connect_quic_only(host_name, token, device_short_id, trust_invalid_server_cert).await?;
    debug!("QUIC connection established, entering accept loop");

    info!(
        "[done] TCP forward: 127.0.0.1:{} → {}/{}:{}",
        &tunnel_spec.local_port, device_short_id, remote_host, &tunnel_spec.remote_port
    );
    loop {
        tokio::select! {
            accept_result = listener.accept() => {
                let (mut local_stream, addr) = accept_result?;
                info!("New local TCP connection from {addr}");
                let stream_type = tunnel_spec.to_stream_type(token);
                let mut quic_io = open_quic_stream(
                    &conn,
                    stream_type,
                ).await?;

                tokio::spawn(async move {
                    let res = io::copy_bidirectional(&mut local_stream, &mut quic_io).await;
                    match res {
                        Ok((a, b)) =>
                            debug!("TCP forward {addr} closed cleanly (rx={a}, tx={b})"),
                        Err(e) =>
                            error!("TCP forward {addr} closed with error: {e:?}"),
                    }
                });
            }
            reason = conn.closed() => {
                warn!("Connection closed: {:?}", reason);
                if let Some(close_reason) = conn.close_reason() {
                    warn!("Close reason: {:?}", close_reason);
                }
                break;
            }

            _ = SHUTDOWN.cancelled() => {
                info!("Shutdown requested — closing TCP port forward");
                break;
            }
        }
    }

    Ok(())
}

async fn tunnel_device_port_udp(
    host_name: &str,
    token: &str,
    device_short_id: &str,
    tunnel_spec: &UdpTarget,
    trust_invalid_server_cert: bool,
) -> Result<()> {
    let (_endpoint, conn) =
        connect_quic_only(host_name, token, device_short_id, trust_invalid_server_cert).await?;

    // Send StreamType::Tunnel over a QUIC stream
    let stream_type = tunnel_spec.to_stream_type(token);
    let mut quic_io = open_quic_stream(&conn, stream_type).await?;

    // === Read channel_id assigned by the runtime ===
    let mut id_buf = [0u8; 4];
    quic_io.recv.read_exact(&mut id_buf).await?;
    let channel_id = u32::from_be_bytes(id_buf);

    debug!("Opened UDP tunnel with channel ID {}", channel_id);

    // Close the control stream – we're done with it
    quic_io.send.finish()?;

    info!(
        "[done] UDP forward: 127.0.0.1:{} → {} {}:{}",
        &tunnel_spec.local_port,
        device_short_id,
        &tunnel_spec.remote_host,
        &tunnel_spec.remote_port
    );

    // Now switch to datagram forwarding
    udp_local_datagram_forward(tunnel_spec.local_port, channel_id, conn.clone()).await
}

pub async fn udp_local_datagram_forward(
    local_port: u16,
    channel_id: u32,
    conn: quinn::Connection,
) -> Result<()> {
    let sock = Arc::new(UdpSocket::bind(("0.0.0.0", local_port)).await?);

    let sock_rx = sock.clone();
    let sock_tx = sock.clone();
    let conn_rx = conn.clone();
    let conn_cl = conn.clone();

    // === UDP -> QUIC ===
    let udp_to_quic = tokio::spawn(async move {
        let mut buf = [0u8; 65535];

        loop {
            let (n, src) = match sock_rx.recv_from(&mut buf).await {
                Ok(r) => r,
                Err(e) => {
                    error!("CLI UDP recv_from failed: {:?}", e);
                    break;
                }
            };
            debug!("CLI UDP recv: {} bytes from {}", n, src);

            // [channel_id][src_addr_header][payload]
            let mut d = BytesMut::with_capacity(4 + 1 + 2 + 16 + n);
            d.put_u32(channel_id);
            encode_socket_addr(&mut d, src);
            d.extend_from_slice(&buf[..n]);

            if let Err(e) = conn_rx.send_datagram(d.freeze()) {
                error!("CLI send_datagram failed: {:?}", e);
                break;
            }
        }
    });

    // === QUIC -> UDP ===
    let quic_to_udp = tokio::spawn(async move {
        loop {
            let d = match conn_cl.read_datagram().await {
                Ok(d) => d,
                Err(e) => {
                    warn!("CLI read_datagram ended: {:?}", e);
                    break;
                }
            };

            if d.len() < 4 {
                continue;
            }

            let chan = u32::from_be_bytes([d[0], d[1], d[2], d[3]]);
            if chan != channel_id {
                // Other channel, ignore for this forwarder instance
                continue;
            }

            let body = &d[4..];
            let (src, hdr_len) = match decode_socket_addr(body) {
                Some(v) => v,
                None => {
                    warn!("CLI: invalid src header in datagram");
                    continue;
                }
            };

            let payload = &body[hdr_len..];
            if payload.is_empty() {
                continue;
            }

            if let Err(e) = sock_tx.send_to(payload, src).await {
                warn!("CLI UDP send_to failed: {:?}", e);
                break;
            }
        }
    });

    // === Wait for shutdown ===
    let conn_cl2 = conn.clone();
    tokio::select! {
        _ = conn_cl2.closed() => {
            warn!("CLI QUIC connection closed — stopping UDP forward");
        }

        _ = udp_to_quic => {}
        _ = quic_to_udp => {}

        _ = SHUTDOWN.cancelled() => {
            info!("CLI shutdown requested — closing UDP forward");
            let _ = conn.close(0u32.into(), b"shutdown");
        }
    }

    Ok(())
}

async fn tunnel_device_socket(
    host_name: &str,
    token: &str,
    device_short_id: &str,
    target: &SocketTarget,
    trust_invalid_server_cert: bool,
) -> Result<()> {
    let local_path = &target.local_path;

    // Remove stale socket file if it exists
    if tokio::fs::metadata(local_path).await.is_ok() {
        tokio::fs::remove_file(local_path).await?;
    }

    let listener = UnixListener::bind(local_path)?;
    info!(
        "[done] Socket forward: local {} → {} {}",
        local_path, device_short_id, target.remote_path
    );

    let (_endpoint, conn) =
        connect_quic_only(host_name, token, device_short_id, trust_invalid_server_cert).await?;

    loop {
        tokio::select! {
            Ok((mut local_stream, _addr)) = listener.accept() => {
                info!("New UNIX socket connection: {}", local_path);

                let stream_type = target.to_stream_type(token);
                let mut quic_io = open_quic_stream(&conn, stream_type).await?;

                tokio::spawn(async move {
                    let res = io::copy_bidirectional(&mut local_stream, &mut quic_io).await;
                    match res {
                        Ok((a, b)) =>
                            info!("UNIX forward closed cleanly (rx={a}, tx={b})"),
                        Err(e) =>
                            error!("UNIX forward closed with error: {e:?}"),
                    }
                });
            }

            reason = conn.closed() => {
                warn!("Connection closed: {:?}", reason);
                break;
            }

            _ = SHUTDOWN.cancelled() => {
                warn!("Shutdown requested — closing UNIX forward");
                break;
            }
        }
    }

    Ok(())
}
