// tunnel.rs

use std::time::Duration;
use std::{net::SocketAddr, sync::Arc};

use quinn::{ConnectionError, Endpoint};
use tokio::io;
use tokio::sync::watch;
use tracing::{info, warn};

use crate::response::ServerError;
use crate::{
    auth::tunnel_token::verify_tunnel_token, config::AppConfig, relay::relay_state::RelayState,
    response::ServerResult,
};

pub async fn run_quic_endpoint(
    cfg: Arc<AppConfig>,
    relay: Arc<RelayState>,
    mut reload_rx: watch::Receiver<()>,
) -> ServerResult<()> {
    let addr = SocketAddr::from(([0, 0, 0, 0], cfg.unified_port));

    loop {
        // Build fresh TLS config
        let server_config = crate::api::certificate::create_quic_server_config(&cfg).await?;

        // Create fresh endpoint
        let endpoint = Endpoint::server(server_config, addr)
            .map_err(|e| ServerError::internal_error(&format!("bind QUIC: {e:?}")))?;

        info!("QUIC listening on udp://{}", addr);

        // Accept loop for this endpoint
        loop {
            tokio::select! {
                // === TLS reload request ===
                _ = reload_rx.changed() => {
                    info!("QUIC TLS reload requested — restarting QUIC endpoint");
                    endpoint.close(0u32.into(), b"tls-reload");
                    break;
                }

                // === Incoming QUIC connection ===
                incoming = endpoint.accept() => {
                    match incoming {
                        Some(incoming_conn) => {
                            let relay = relay.clone();
                            let cfg = cfg.clone();

                            tokio::spawn(async move {
                                match incoming_conn.await {
                                    Ok(conn) => {
                                        handle_quic_connection(conn, relay, cfg).await;
                                    }
                                    Err(e) => {
                                        warn!("Incoming QUIC handshake failed: {e:?}");
                                    }
                                }
                            });
                        }
                        None => {
                            warn!("Endpoint accept() returned None — endpoint driver lost?");
                            break;
                        }
                    }
                }
            }
        }

        // Drop endpoint -> closes UDP socket
        drop(endpoint);

        // Loop repeats → rebuild server with new certificate
    }
}

async fn handle_quic_connection(
    conn: quinn::Connection,
    relay: Arc<RelayState>,
    cfg: Arc<AppConfig>,
) {
    // Extract SNI from handshake_data
    let sni = conn
        .handshake_data()
        .and_then(|data| {
            data.downcast_ref::<quinn::crypto::rustls::HandshakeData>()
                .and_then(|hd| hd.server_name.clone())
        })
        .unwrap_or_default();

    let public = &cfg.public_address;
    let control_host = format!("control.{public}");

    if sni == control_host {
        let _ = handle_control_tunnel(conn, relay, &cfg.forward_secret).await;
        return;
    }

    if let Some(device_id) = extract_device_id_from_sni(&sni, public) {
        if let Some(device_conn) = relay.get_tunnel(&device_id).await {
            let _ = handle_forward(conn, device_conn).await;
        } else {
            conn.close(0u32.into(), b"No tunnel");
        }
        return;
    }

    conn.close(0u32.into(), b"Invalid SNI");
}

fn extract_device_id_from_sni(sni: &str, public_domain: &str) -> Option<String> {
    // Expected patterns:
    //   "<deviceid>.<public_domain>"
    //   "whatever-<deviceid>.<public_domain>" (you can refine this later)
    if let Some(stripped) = sni.strip_suffix(public_domain) {
        let stripped = stripped.trim_end_matches('.'); // remove trailing dot
        if stripped.is_empty() {
            return None;
        }
        // For now, assume the whole left label is the device id.
        // If you encode more data (like "app-deviceid"), adapt this.
        return Some(stripped.to_string());
    }
    None
}

async fn handle_control_tunnel(
    conn: quinn::Connection,
    relay: Arc<RelayState>,
    secret: &str,
) -> ServerResult<()> {
    // Accept first control handshake stream
    let (mut send, mut recv) = conn.accept_bi().await?;

    let mut buf = vec![0; 1024];
    let n = recv
        .read(&mut buf)
        .await?
        .ok_or_else(|| io::Error::new(io::ErrorKind::UnexpectedEof, "control: empty handshake"))?;
    if n == 0 {
        warn!("control: empty handshake");
        return Ok(());
    }

    let line = String::from_utf8_lossy(&buf[..n]);
    let device_id = extract_kv(&line, "device_id").unwrap_or_default();
    let token = extract_kv(&line, "token").unwrap_or_default();

    verify_tunnel_token(&token, secret)?;

    relay.remove_tunnel(&device_id).await;
    relay.register_tunnel(device_id.clone(), conn.clone()).await;

    tokio::spawn(async move {
        let reason = conn.closed().await;

        match reason {
            // --- GRACEFUL SHUTDOWN (no reconnect expected) ---
            ConnectionError::ApplicationClosed(_) => {
                relay.remove_tunnel(&device_id).await;
                info!(%device_id, "device gracefully closed");
            }
            ConnectionError::ConnectionClosed(_) => {
                relay.remove_tunnel(&device_id).await;
                warn!(%device_id, "device QUIC stack closed connection");
            }

            ConnectionError::LocallyClosed => {
                relay.remove_tunnel(&device_id).await;
                info!(%device_id, "connection closed locally");
            }

            ConnectionError::Reset => {
                // peer reset = intentional/terminal
                relay.remove_tunnel(&device_id).await;
                warn!(%device_id, "connection reset by peer");
            }

            // --- UNINTENTIONAL LOSS (reconnect expected) ---
            ConnectionError::TransportError(_) | ConnectionError::TimedOut => {
                warn!(%device_id, "device lost connection; waiting for reconnect");
                relay.mark_tunnel_lost(&device_id).await;

                let relay2 = relay.clone();
                let device_id2 = device_id.clone();
                tokio::spawn(async move {
                    tokio::time::sleep(Duration::from_secs(30)).await;

                    if relay2.is_still_lost(&device_id2).await {
                        relay2.remove_tunnel(&device_id2).await;
                        warn!(%device_id2, "device did not reconnect — marking offline");
                    }
                });
            }

            // --- EXTREMELY RARE / CONFIG ERRORS ---
            ConnectionError::VersionMismatch | ConnectionError::CidsExhausted => {
                relay.remove_tunnel(&device_id).await;
                warn!(%device_id, ?reason, "fatal QUIC error");
            }
        }
    });

    // optional ack
    let _ = send.write_all(b"OK").await;

    Ok(())
}

async fn handle_forward(
    client_conn: quinn::Connection, // forward client (CLI/browser)
    device_conn: quinn::Connection, // control-registered device conn
) -> io::Result<()> {
    // 1. Accept the client's first bidi stream
    let (mut client_send, mut client_recv) = match client_conn.accept_bi().await {
        Ok(s) => s,
        Err(e) => {
            // client closed before opening a stream
            return Err(io::Error::new(
                io::ErrorKind::Other,
                format!("client_bi: {e:?}"),
            ));
        }
    };

    // 2. Open a bidi stream to the device
    let (mut dev_send, mut dev_recv) = match device_conn.open_bi().await {
        Ok(s) => s,
        Err(e) => {
            // device lost / restarting / tunnel down
            let _ = client_send.write_all(b"NO_TUNNEL").await;
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                format!("device_bi: {e:?}"),
            ));
        }
    };

    // 3. Copy both directions simultaneously
    let uplink = tokio::io::copy(&mut client_recv, &mut dev_send); // client → device
    let downlink = tokio::io::copy(&mut dev_recv, &mut client_send); // device → client

    // 4. Whichever side closes first, we gracefully finish the opposite send stream
    tokio::select! {
        result = uplink => {
            // Client → Device finished
            let _ = dev_send.finish();
            result?;
        }
        result = downlink => {
            // Device → Client finished
            let _ = client_send.finish();
            result?;
        }
    }

    Ok(())
}

fn extract_kv(line: &str, key: &str) -> Option<String> {
    line.split_whitespace().find_map(|part| {
        part.strip_prefix(&(key.to_owned() + "="))
            .map(|s| s.to_string())
    })
}

fn map_quic_err(ctx: &'static str) -> impl Fn(quinn::ReadError) -> io::Error + '_ {
    move |e| io::Error::new(io::ErrorKind::Other, format!("{ctx}: {e:?}"))
}
