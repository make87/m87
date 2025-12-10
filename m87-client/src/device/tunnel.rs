use std::sync::Arc;

use crate::devices;
use crate::streams::quic::QuicIo;
use crate::streams::quic::connect_quic_only;
use crate::streams::quic::open_quic_stream;
use crate::streams::stream_type::Protocols;
use crate::streams::stream_type::StreamType;
use crate::util::shutdown::SHUTDOWN;
use crate::{auth::AuthManager, config::Config};
use anyhow::Result;
use anyhow::anyhow;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use tokio::io;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tokio::net::UdpSocket;
use tracing::warn;
use tracing::{error, info};

#[derive(Debug, Clone)]
pub enum TunnelTarget {
    Tcp {
        remote_host: String,
        remote_port: u16,
        local_port: u16,
    },
    Udp {
        remote_host: String,
        remote_port: u16,
        local_port: u16,
    },
    Unix {
        local_path: String,
        remote_path: String,
    },
}

// Examples accepted:
// "8080"
// "8080:1337"
// "192.168.1.2:8080:1337"
// "8080/tcp"
// "8080:1337/udp"
// "192.168.1.2:8080:1337/udp"
impl TunnelTarget {
    fn from_list(tunnel_specs: Vec<String>) -> Result<Vec<Self>> {
        let mut specs = Vec::new();

        for token in tunnel_specs {
            if token.starts_with('/') {
                // "/local.sock"
                // "/local.sock:/remote.sock"
                let mut parts = token.split('/');
                // split_once is better
                let (local, remote) = if let Some((l, r)) = token.split_once(':') {
                    (l.to_string(), r.to_string())
                } else {
                    let p = token.to_string();
                    (p.clone(), p)
                };

                specs.push(TunnelSpec {
                    remote_host: Some(remote),
                    remote_port: 0,
                    local_port: 0,
                    protocol: Protocols::Unix,
                });
                continue;
            }

            // split protocol tail
            let mut parts = token.split('/');
            let ports = parts.next().unwrap();
            let tail = parts.next(); // e.g. tcp, udp, udp+mcast, etc.

            let mut protocol: Option<Protocols> = None;

            if let Some(t) = tail {
                for item in t.split('+') {
                    match item.to_lowercase().as_str() {
                        "tcp" => protocol = Some(Protocols::Tcp),
                        "udp" => protocol = Some(Protocols::Udp),
                        _ => return Err(anyhow!("invalid protocol/addition '{}'", item)),
                    }
                }
            }

            // parse host:port:port tuple
            let nums: Vec<&str> = ports.split(':').collect();
            let (remote_host, remote_port, local_port) = match nums.as_slice() {
                // "8080"
                [rp] => (None, rp.parse()?, rp.parse()?),

                // "8080:1337"
                [rp, lp] => (None, rp.parse()?, lp.parse()?),

                // "192.168.2.2:8080:1337"
                [host, rp, lp] => (Some(host.to_string()), rp.parse()?, lp.parse()?),

                _ => return Err(anyhow!("invalid tunnel spec '{}'", token)),
            };

            // expand protocols
            match protocol {
                Some(p) => {
                    specs.push(TunnelSpec {
                        remote_host,
                        remote_port,
                        local_port,
                        protocol: p,
                    });
                }
                None => {
                    // expand into TCP + UDP
                    specs.push(TunnelSpec {
                        remote_host: remote_host.clone(),
                        remote_port,
                        local_port,
                        protocol: Protocols::Tcp,
                    });
                }
            }
        }

        Ok(specs)
    }
}

pub async fn open_local_tunnel(device_name: &str, tunnel_specs: Vec<String>) -> Result<()> {
    let config = Config::load()?;
    let dev = devices::get_device_by_name(device_name).await?;
    let token = AuthManager::get_cli_token().await?;
    let device_short_id = dev.short_id;
    let trust = config.trust_invalid_server_cert;

    let tunnels: Vec<TunnelSpec> = TunnelSpec::from_list(tunnel_specs)?;

    // spawn each tunnel as a background task
    for t in tunnels {
        let host = config.get_server_hostname();
        let token = token.clone();
        let device_short_id = device_short_id.clone();

        tokio::spawn(async move {
            if let Err(e) = tunnel_device_port(&host, &token, &device_short_id, t, trust).await {
                error!("Tunnel exited with error: {}", e);
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
    tunnel_spec: TunnelSpec,
    trust_invalid_server_cert: bool,
) -> Result<()> {
    match &tunnel_spec.protocol {
        Protocols::Tcp => {
            tunnel_device_port_tcp(
                host_name,
                token,
                device_short_id,
                tunnel_spec,
                trust_invalid_server_cert,
            )
            .await
        }
        Protocols::Udp => {
            tunnel_device_port_udp(
                host_name,
                token,
                device_short_id,
                tunnel_spec,
                trust_invalid_server_cert,
            )
            .await
        }
    }
}

async fn tunnel_device_port_tcp(
    host_name: &str,
    token: &str,
    device_short_id: &str,
    tunnel_spec: TunnelSpec,
    trust_invalid_server_cert: bool,
) -> Result<()> {
    let listener = TcpListener::bind(("127.0.0.1", tunnel_spec.local_port)).await?;
    let remote_host = tunnel_spec
        .remote_host
        .clone()
        .unwrap_or("127.0.0.1".to_string());
    info!(
        "TCP forward: 127.0.0.1:{} → {}/{}:{}",
        &tunnel_spec.local_port, device_short_id, remote_host, &tunnel_spec.remote_port
    );

    let (_endpoint, conn) =
        connect_quic_only(host_name, token, device_short_id, trust_invalid_server_cert).await?;

    loop {
        tokio::select! {
            accept_result = listener.accept() => {
                let (mut local_stream, addr) = accept_result?;
                info!("New local TCP connection from {addr}");
                let stream_type = StreamType::Port {
                    token: token.to_string(),
                    port: tunnel_spec.remote_port,
                    protocol: Protocols::Tcp,
                    host: tunnel_spec.remote_host.clone(),
                };
                let mut quic_io = open_quic_stream(
                    &conn,
                    stream_type,
                ).await?;

                tokio::spawn(async move {
                    let res = io::copy_bidirectional(&mut local_stream, &mut quic_io).await;
                    match res {
                        Ok((a, b)) =>
                            info!("TCP forward {addr} closed cleanly (rx={a}, tx={b})"),
                        Err(e) =>
                            error!("TCP forward {addr} closed with error: {e:?}"),
                    }
                });
            }
            _ = conn.closed() => {
                info!("Connection closed");
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
    tunnel_spec: TunnelSpec,
    trust_invalid_server_cert: bool,
) -> Result<()> {
    let remote_host = tunnel_spec
        .remote_host
        .clone()
        .unwrap_or("127.0.0.1".to_string());

    let (_endpoint, conn) =
        connect_quic_only(host_name, token, device_short_id, trust_invalid_server_cert).await?;

    let stream_type = StreamType::Port {
        token: token.to_string(),
        port: tunnel_spec.remote_port,
        protocol: Protocols::Udp,
        host: tunnel_spec.remote_host.clone(),
    };

    let quic_io = open_quic_stream(&conn, stream_type).await?;

    udp_local_unicast_forward(
        tunnel_spec.local_port,
        remote_host,
        tunnel_spec.remote_port,
        quic_io,
        conn,
    )
    .await
}

async fn udp_local_unicast_forward(
    local_port: u16,
    remote_host: String,
    remote_port: u16,
    mut quic_io: QuicIo,
    conn: quinn::Connection,
) -> Result<()> {
    let local = UdpSocket::bind(("127.0.0.1", local_port)).await?;
    info!(
        "UDP forward unicast: 127.0.0.1:{} → {}/{}:{}",
        local_port,
        conn.remote_address(),
        remote_host,
        remote_port
    );

    // Connect the local UDP socket to simplify send()
    local.connect((remote_host.as_str(), remote_port)).await?;

    let local = Arc::new(local);
    let sock_rx = local.clone();
    let sock_tx = local.clone();

    let (mut quic_r, mut quic_w) = tokio::io::split(quic_io);

    // QUIC → Local UDP
    let mut to_udp = tokio::spawn(async move {
        let mut buf = [0u8; 65535];
        loop {
            let n = match quic_r.read(&mut buf).await {
                Ok(0) => break,
                Ok(n) => n,
                Err(e) => {
                    error!("udp unicast read from quic failed: {e}");
                    break;
                }
            };
            if let Err(e) = sock_tx.send(&buf[..n]).await {
                error!("udp unicast send to socket failed: {e}");
                break;
            }
        }
    });

    // Local UDP → QUIC
    let mut to_quic = tokio::spawn(async move {
        let mut buf = [0u8; 65535];
        loop {
            let n = match sock_rx.recv(&mut buf).await {
                Ok(n) => n,
                Err(e) => {
                    error!("udp unicast recv failed: {e}");
                    break;
                }
            };
            if let Err(e) = quic_w.write_all(&buf[..n]).await {
                error!("udp unicast write to quic failed: {e}");
                break;
            }
        }
    });

    tokio::select! {
        _ = conn.closed() => warn!("QUIC connection closed — stopping UDP unicast forward"),
        _ = &mut to_udp => {},
        _ = &mut to_quic => {},
        _ = SHUTDOWN.cancelled() => warn!("Shutdown requested — stopping UDP unicast"),
    };

    Ok(())
}
