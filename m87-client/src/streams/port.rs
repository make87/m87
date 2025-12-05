use std::net::SocketAddr;

use tokio::{io::AsyncWriteExt, net::TcpStream};

use crate::{
    streams::quic::QuicIo,
    streams::stream_type::{Additions, Protocols},
};

pub async fn handle_port_forward_io(
    port: u16,
    host: Option<String>,
    protocol: Protocols,
    addition: Option<Additions>,
    io: &mut QuicIo,
) {
    let host = host.unwrap_or_else(|| "127.0.0.1".to_string());

    match protocol {
        Protocols::Tcp => tcp_forward(host, port, io).await,

        Protocols::Udp => udp_forward(host, port, addition, io).await,
    }
}

pub async fn tcp_forward(host: String, port: u16, io: &mut QuicIo) {
    match TcpStream::connect((host.as_str(), port)).await {
        Ok(mut local) => match tokio::io::copy_bidirectional(io, &mut local).await {
            Ok((a, b)) => tracing::info!("tcp forward closed (rx={a}, tx={b})"),
            Err(e) => tracing::error!("tcp forward error: {e}"),
        },
        Err(e) => {
            let _ = io
                .send
                .write_all(format!("TCP connect failed: {e}\n").as_bytes())
                .await;
        }
    }
}

pub async fn udp_forward(host: String, port: u16, addition: Option<Additions>, io: &mut QuicIo) {
    use tokio::net::UdpSocket;

    // Bind to an ephemeral UDP socket
    let udp = match UdpSocket::bind("0.0.0.0:0").await {
        Ok(s) => s,
        Err(e) => {
            let _ = io
                .send
                .write_all(format!("UDP bind failed: {e}\n").as_bytes())
                .await;
            return;
        }
    };

    let target: SocketAddr = match format!("{host}:{port}").parse() {
        Ok(a) => a,
        Err(e) => {
            let _ = io
                .send
                .write_all(format!("invalid UDP target: {e}\n").as_bytes())
                .await;
            return;
        }
    };

    // Optional MULTICAST subscription
    if let Some(Additions::MCAST) = addition {
        if let Ok(maddr) = host.parse::<std::net::Ipv4Addr>() {
            // Join multicast group; use INADDR_ANY as interface
            let _ = udp.join_multicast_v4(maddr, std::net::Ipv4Addr::UNSPECIFIED);
        }
    }

    let mut udp_buf = vec![0u8; 65535];
    let mut quic_buf = vec![0u8; 65535];

    // QUIC → UDP
    let quic_to_udp = async {
        loop {
            // read length prefix
            let mut lenb = [0u8; 2];
            if io.recv.read_exact(&mut lenb).await.is_err() {
                break;
            }
            let size = u16::from_be_bytes(lenb) as usize;

            if io.recv.read_exact(&mut quic_buf[..size]).await.is_err() {
                break;
            }

            let _ = udp.send_to(&quic_buf[..size], target).await;
        }
    };

    // UDP → QUIC
    let udp_to_quic = async {
        loop {
            match udp.recv_from(&mut udp_buf).await {
                Ok((size, _peer)) => {
                    let lenb = (size as u16).to_be_bytes();
                    if io.send.write_all(&lenb).await.is_err() {
                        break;
                    }
                    if io.send.write_all(&udp_buf[..size]).await.is_err() {
                        break;
                    }
                    let _ = io.send.flush().await;
                }
                Err(_) => break,
            }
        }
    };

    tokio::select! {
        _ = quic_to_udp => {}
        _ = udp_to_quic => {}
    }
}
