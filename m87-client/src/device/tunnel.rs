use std::net::IpAddr;
use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::Result;
use anyhow::anyhow;
use tokio::io;
use tokio::net::TcpListener;
use tokio::net::UdpSocket;
use tracing::warn;

use crate::device::udp::UdpFlowManager;
use crate::devices;
use crate::streams::quic::connect_quic_only;
use crate::streams::quic::open_quic_stream;
use crate::streams::stream_type::Additions;
use crate::streams::stream_type::Protocols;
use crate::streams::stream_type::StreamType;
use crate::util::shutdown::SHUTDOWN;
use crate::{auth::AuthManager, config::Config};
use tracing::{error, info};

#[derive(Debug, Clone)]
pub struct TunnelSpec {
    pub remote_host: Option<String>,
    pub remote_port: u16,
    pub local_port: u16,
    pub protocol: Protocols,
    pub addition: Option<Additions>,
}

// Examples accepted:
// "8080"
// "8080:1337"
// "192.168.1.2:8080:1337"
// "8080/tcp"
// "8080:1337/udp"
// "192.168.1.2:8080:1337/udp+mcast"
impl TunnelSpec {
    fn from_list(tunnel_specs: Vec<String>) -> Result<Vec<Self>> {
        let mut specs = Vec::new();

        for token in tunnel_specs {
            // split protocol tail
            let mut parts = token.split('/');
            let ports = parts.next().unwrap();
            let tail = parts.next(); // e.g. tcp, udp, udp+mcast, etc.

            let mut protocol: Option<Protocols> = None;
            let mut addition: Option<Additions> = None;

            if let Some(t) = tail {
                for item in t.split('+') {
                    match item.to_lowercase().as_str() {
                        "tcp" => protocol = Some(Protocols::Tcp),
                        "udp" => protocol = Some(Protocols::Udp),
                        "mcast" => addition = Some(Additions::MCAST),
                        "udp+mcast" => {
                            protocol = Some(Protocols::Udp);
                            addition = Some(Additions::MCAST);
                        }
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
                        addition: addition.clone(),
                    });
                }
                None => {
                    // expand into TCP + UDP
                    specs.push(TunnelSpec {
                        remote_host: remote_host.clone(),
                        remote_port,
                        local_port,
                        protocol: Protocols::Tcp,
                        addition: addition.clone(),
                    });

                    specs.push(TunnelSpec {
                        remote_host,
                        remote_port,
                        local_port,
                        protocol: Protocols::Udp,
                        addition: addition.clone(),
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

    let (_, conn) = connect_quic_only(host_name, token, trust_invalid_server_cert).await?;

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
                    addition: None,
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

            _ = SHUTDOWN.cancelled() => {
                info!("Shutdown requested — closing TCP port forward");
                break;
            }
        }
    }

    Ok(())
}

pub async fn tunnel_device_port_udp(
    host_name: &str,
    token: &str,
    device_short_id: &str,
    tunnel_spec: TunnelSpec,
    trust_invalid: bool,
) -> Result<()> {
    use crate::streams::quic::connect_quic_only;
    let remote_host = tunnel_spec
        .remote_host
        .clone()
        .unwrap_or("127.0.0.1".to_string());
    info!(
        "UDP forward: 127.0.0.1:{} → {}/{}:{}",
        &tunnel_spec.local_port, device_short_id, remote_host, &tunnel_spec.remote_port
    );

    // --- 1. Establish QUIC connection once (reused for all flows) ---
    let (_endpoint, conn) = connect_quic_only(host_name, device_short_id, trust_invalid).await?;

    // --- 2. Bind local UDP socket ---
    let local_addr = SocketAddr::from(([0, 0, 0, 0], tunnel_spec.local_port));
    let udp = Arc::new(UdpSocket::bind(local_addr).await?);

    let multicast = match &tunnel_spec.addition {
        Some(Additions::MCAST) => true,
        _ => false,
    };

    // --- 3. Optionally join multicast group ---
    if multicast {
        if let Some(group) = tunnel_spec.remote_host.clone() {
            let group_ip: IpAddr = group
                .parse()
                .map_err(|_| anyhow!("Invalid multicast group IP"))?;
            match group_ip {
                IpAddr::V4(g) => {
                    udp.join_multicast_v4(g, std::net::Ipv4Addr::UNSPECIFIED)?;
                }
                IpAddr::V6(g) => {
                    udp.join_multicast_v6(&g, 0)?;
                }
            }
            info!("Joined multicast group {}", group);
        }
    }

    // --- 4. Create the manager (with correct arg order!) ---
    let mgr = Arc::new(UdpFlowManager::new(conn.clone(), udp.clone()));
    tokio::spawn(mgr.clone().cleanup_task());

    let mut buf = vec![0u8; 65535];

    // --- 5. Main UDP receive loop ---
    loop {
        let (n, addr) = udp.recv_from(&mut buf).await?;
        let src_ip = addr.ip();
        let src_port = addr.port();
        let payload = buf[..n].to_vec();

        let mgr_cl = mgr.clone();
        let token_cl = token.to_string();
        let host_cl = tunnel_spec.remote_host.clone();
        let addition_cl = tunnel_spec.addition.clone();
        let remote_port_cl = tunnel_spec.remote_port.clone();

        tokio::spawn(async move {
            match mgr_cl
                .get_or_create_flow(
                    src_ip,
                    src_port,
                    token_cl,
                    remote_port_cl,
                    host_cl,
                    addition_cl,
                )
                .await
            {
                Ok(tx) => {
                    // Send the packet into the flow → QUIC task picks it up
                    if let Err(e) = tx.send(payload).await {
                        warn!("UDP→QUIC send failed: {}", e);
                    }
                }
                Err(e) => warn!("Failed to obtain flow for {src_ip}:{src_port}: {e}"),
            }
        });
    }
}
