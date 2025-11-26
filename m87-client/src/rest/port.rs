use axum::extract::ws::WebSocket;
use futures::StreamExt;
use tokio::{io, net::TcpStream};
use tokio_yamux::{Config as YamuxConfig, Session};
use tracing::{error, info};

use crate::util::websocket::ServerByteWebSocket;

pub async fn handle_port_forward_ws(port: String, host: Option<String>, socket: WebSocket) {
    let port: u16 = match port.parse() {
        Ok(p) => p,
        Err(e) => {
            error!("Invalid port '{port}': {e}");
            return;
        }
    };
    let host = host.unwrap_or_else(|| "127.0.0.1".to_string());

    // 1) Wrap Axum WebSocket as a byte stream
    let ws_bytes = ServerByteWebSocket::new(socket);

    // 2) Create Yamux server session
    let yamux_cfg = YamuxConfig::default();
    let mut session = Session::new_server(ws_bytes, yamux_cfg);

    info!("Yamux port-forward session started, forwarding to {host}:{port}");

    // 3) Accept streams from client; each stream = one TCP connection to local_port
    while let Some(stream_result) = session.next().await {
        let mut yamux_stream = match stream_result {
            Ok(s) => s,
            Err(e) => {
                error!("Yamux session error: {e}");
                break;
            }
        };

        let target_host = host.clone();
        let target_port = port;
        tokio::spawn(async move {
            match TcpStream::connect((target_host.as_str(), target_port)).await {
                Ok(mut target) => {
                    if let Err(e) = io::copy_bidirectional(&mut yamux_stream, &mut target).await {
                        info!("Yamux forward stream closed with error: {e:?}");
                    } else {
                        info!("Yamux forward stream closed cleanly");
                    }
                }
                Err(e) => {
                    error!("Failed to connect to {target_host}:{target_port}: {e}");
                }
            }
        });
    }

    info!("Yamux port-forward session ended for {host}:{port}");
}
