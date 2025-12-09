// tunnel.rs

use std::net::SocketAddr;
use std::pin::Pin;
use m87_shared::roles::Role;
use mongodb::bson::doc;
use quinn::crypto::rustls::HandshakeData;
use quinn::{ConnectionError, Endpoint};
use tokio::io::{self, AsyncRead, AsyncWrite, AsyncWriteExt};
use tokio::sync::watch;
use tracing::{debug, error, info, warn};

use crate::auth::claims::Claims;
use crate::models::device::DeviceDoc;
use crate::response::ServerError;
use crate::response::ServerResult;
use crate::util::app_state::AppState;
use std::task::{Context, Poll};

/// Combined stream that wraps separate read/write halves into a single AsyncRead + AsyncWrite
struct CombinedStream<R, W> {
    recv: R,
    send: W,
}

impl<R: AsyncRead + Unpin, W: Unpin> AsyncRead for CombinedStream<R, W> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().recv).poll_read(cx, buf)
    }
}

impl<R: Unpin, W: AsyncWrite + Unpin> AsyncWrite for CombinedStream<R, W> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().send).poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().send).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().send).poll_shutdown(cx)
    }
}

pub async fn run_quic_endpoint(
    state: AppState,
    mut reload_rx: watch::Receiver<()>,
) -> ServerResult<()> {
    let addr = SocketAddr::from(([0, 0, 0, 0], state.config.unified_port));

    loop {
        // Build fresh TLS config
        let server_config =
            crate::api::certificate::create_quic_server_config(&state.config).await?;

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
                            let state_cl = state.clone();
                            info!("Received incoming QUIC connection");

                            tokio::spawn(async move {
                                match incoming_conn.await {
                                    Ok(conn) => {
                                        let alpn = conn.handshake_data()
                                            .and_then(|d| {
                                                d.downcast_ref::<HandshakeData>()
                                                    .and_then(|h| h.protocol.clone())
                                            })
                                            .unwrap_or_else(|| Vec::new());
                                        if alpn == b"h3" {
                                            // WebTransport candidate
                                            if let Ok(req) = web_transport_quinn::Request::accept(conn.clone()).await {
                                                if let Ok(session) = req.ok().await {
                                                    let _ = handle_webtransport_forward(session, state_cl).await;
                                                    return;
                                                }
                                            }
                                            // If WT parsing fails → close quickly, do NOT fall back
                                            conn.close(0u32.into(), b"invalid-wt");
                                            return;
                                        }

                                        // Raw QUIC path (CLI, tunnels, forwards)
                                        let _ = handle_quic_connection(conn.clone(), state_cl).await;
                                        conn.close(0u32.into(), b"");
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

pub async fn extract_token(conn: &quinn::Connection) -> Option<String> {
    // Wait for Stream 0 (first client-initiated bi-stream)
    let (mut send, mut recv) = conn.accept_bi().await.ok()?;

    // Read token length (u16 BE)
    let mut len_buf = [0u8; 2];
    recv.read_exact(&mut len_buf).await.ok()?;
    let len = u16::from_be_bytes(len_buf) as usize;

    // Read token
    let mut buf = vec![0u8; len];
    recv.read_exact(&mut buf).await.ok()?;

    // Send ACK to client so it knows server received everything
    let ack = [1u8];
    send.write_all(&ack).await.ok()?;
    send.finish().ok()?; // Finish server send side

    // Convert to UTF-8 string
    String::from_utf8(buf).ok()
}

async fn handle_quic_connection(conn: quinn::Connection, state: AppState) -> ServerResult<()> {
    // Extract SNI from handshake_data
    let sni = conn
        .handshake_data()
        .and_then(|data| {
            data.downcast_ref::<quinn::crypto::rustls::HandshakeData>()
                .and_then(|hd| hd.server_name.clone())
        })
        .unwrap_or_default();

    let public = &state.config.public_address;
    info!("extracting token");
    let Some(token) = extract_token(&conn).await else {
        conn.close(0x100u32.into(), b"missing-token");
        return Err(ServerError::missing_token("missing api key or token"));
    };
    let claims = Claims::from_bearer_or_key(&token, &state.db, &state.config).await?;

    if let Some(device_id) = extract_device_id_from_control_sni(&sni, public) {
        info!(%sni, "control tunnel connection");
        if let Err(e) = handle_control_tunnel(conn, &device_id, claims, state).await {
            error!(%sni, %e, "error handling control tunnel");
        }
        return Ok(());
    }

    if let Some(device_id) = extract_device_id_from_sni(&sni, public) {
        let _ = claims
            .find_one_with_scope_and_role::<DeviceDoc>(
                &state.db.devices(),
                doc! { "short_id": &device_id },
                Role::Editor,
            )
            .await?
            .ok_or_else(|| ServerError::not_found("Device not found"))?;

        if let Some(device_conn) = state.relay.get_tunnel(&device_id).await {
            debug!(%device_id, "forwarding to device");
            let _ = handle_forward(ClientConn::Raw(conn), device_conn).await;
        } else {
            warn!(%device_id, "no tunnel registered for device");
            // print all tunnel ids
            conn.close(0u32.into(), b"No tunnel");
        }
        return Ok(());
    }

    warn!(%sni, "invalid SNI — no match");
    conn.close(0u32.into(), b"Invalid SNI");
    Ok(())
}

fn extract_device_id_from_control_sni(sni: &str, public_domain: &str) -> Option<String> {
    // Expected patters:
    //   "control-<deviceid>.<public_domain>"
    if sni.starts_with("control-") {
        if let Some(stripped) = sni
            .strip_prefix("control-")
            .and_then(|s| s.strip_suffix(&public_domain))
        {
            let short_id = stripped.trim_end_matches('.'); // remove trailing dot
            if short_id.is_empty() {
                return None;
            }
            return Some(short_id.to_string());
        }
    }

    None
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
    device_short_id: &str,
    claims: Claims,
    state: AppState,
) -> ServerResult<()> {
    // node has editor permissions to itself
    debug!(
        "handle_control_tunnel: device_short_id = {}",
        device_short_id
    );
    let device_id = device_short_id.to_string();
    let _ = claims
        .find_one_with_scope_and_role::<DeviceDoc>(
            &state.db.devices(),
            doc! { "short_id": &device_id },
            Role::Editor,
        )
        .await?
        .ok_or_else(|| ServerError::not_found("Device not found"))?;

    state.relay.remove_tunnel(&device_id).await;
    state.relay.register_tunnel(&device_id, conn.clone()).await;

    let reason = conn.closed().await;

    match reason {
        // --- GRACEFUL SHUTDOWN (no reconnect expected) ---
        ConnectionError::ApplicationClosed(_) => {
            // state.relay.remove_tunnel(&device_id).await;
            info!(%device_id, "device gracefully closed");
        }
        ConnectionError::ConnectionClosed(_) => {
            // state.relay.remove_tunnel(&device_id).await;
            warn!(%device_id, "device QUIC stack closed connection");
        }

        ConnectionError::LocallyClosed => {
            // state.relay.remove_tunnel(&device_id).await;
            info!(%device_id, "connection closed locally");
        }

        ConnectionError::Reset => {
            // peer reset = intentional/terminal
            // state.relay.remove_tunnel(&device_id).await;
            warn!(%device_id, "connection reset by peer");
        }

        ConnectionError::TransportError(err) => {
            warn!(%device_id, "device QUIC stack error: {}", err);
        }

        // --- UNINTENTIONAL LOSS (reconnect expected) ---
        ConnectionError::TimedOut => {
            warn!(%device_id, "device lost connection; waiting for reconnect");
            // state.relay.mark_tunnel_lost(&device_id).await;

            // let relay2 = state.relay.clone();
            // let device_id2 = device_id.clone();
            // tokio::spawn(async move {
            //     tokio::time::sleep(Duration::from_secs(30)).await;

            //     if relay2.is_still_lost(&device_id2).await {
            //         relay2.remove_tunnel(&device_id2).await;
            //         warn!(%device_id2, "device did not reconnect — marking offline");
            //     }
            // });
        }

        // --- EXTREMELY RARE / CONFIG ERRORS ---
        ConnectionError::VersionMismatch | ConnectionError::CidsExhausted => {
            // state.relay.remove_tunnel(&device_id).await;
            warn!(%device_id, ?reason, "fatal QUIC error");
        }
    }
    state.relay.remove_tunnel(&device_id).await;

    Ok(())
}

pub async fn handle_webtransport_forward(
    session: web_transport_quinn::Session,
    state: AppState,
) -> ServerResult<()> {
    // Extract device_id from URL: device_id.serverurl
    let url = session.url();
    let device_id = url
        .host()
        .unwrap()
        .to_string()
        .split('.')
        .next()
        .unwrap()
        .to_string();

    let (mut send, mut recv) = session
        .accept_bi()
        .await
        .map_err(|e| ServerError::bad_request(&format!("WT auth stream failed: {:?}", e)))?;

    let mut len_buf = [0u8; 2];
    recv.read_exact(&mut len_buf)
        .await
        .map_err(|_| ServerError::missing_token("missing token prefix"))?;
    let len = u16::from_be_bytes(len_buf) as usize;

    let mut token_buf = vec![0u8; len];
    recv.read_exact(&mut token_buf)
        .await
        .map_err(|_| ServerError::missing_token("token read failed"))?;

    let token = String::from_utf8(token_buf)
        .map_err(|_| ServerError::bad_request("token not valid UTF-8"))?;
    let claims = Claims::from_bearer_or_key(&token, &state.db, &state.config).await?;

    claims
        .find_one_with_scope_and_role::<DeviceDoc>(
            &state.db.devices(),
            doc! { "short_id": &device_id },
            Role::Editor,
        )
        .await?
        .ok_or_else(|| ServerError::not_found("device not found"))?;

    let Some(device_conn) = state.relay.get_tunnel(&device_id).await else {
        return Err(ServerError::not_found("device tunnel not connected"));
    };
    let fut_session = session.clone();
    tokio::spawn(async move {
        if let Err(e) = handle_forward(ClientConn::Web(fut_session), device_conn).await {
            warn!(%device_id, "WT forward error: {:?}", e);
        }
    });

    let _ = send.write_all(b"OK").await;

    Ok(())
}

#[derive(Clone)]
pub enum ClientConn {
    Raw(quinn::Connection),
    Web(web_transport_quinn::Session),
}

impl ClientConn {
    pub async fn accept_bi(
        &self,
    ) -> ServerResult<(
        Pin<Box<dyn AsyncWrite + Send>>,
        Pin<Box<dyn AsyncRead + Send>>,
    )> {
        match self {
            ClientConn::Raw(conn) => {
                let (send, recv) = conn.accept_bi().await?;
                Ok((Box::pin(send), Box::pin(recv)))
            }
            ClientConn::Web(session) => {
                let (send, recv) = session
                    .accept_bi()
                    .await
                    .map_err(|e| ServerError::internal_error(&format!("{:?}", e)))?;
                Ok((Box::pin(send), Box::pin(recv)))
            }
        }
    }
}

async fn handle_forward(
    client_conn: ClientConn,        // forward client (CLI/browser)
    device_conn: quinn::Connection, // control-registered device conn
) -> io::Result<()> {
    debug!("handle_forward: starting accept loop");

    loop {
        // 1. Accept the next client bidi stream
        let (mut client_send, client_recv) = match client_conn.accept_bi().await {
            Ok(s) => {
                debug!("handle_forward: accepted client bidi stream");
                s
            }
            Err(e) => {
                warn!("handle_forward: client accept_bi failed: {e:?}");
                // connection is probably closing; exit the loop and end handler
                return Err(io::Error::new(
                    io::ErrorKind::Other,
                    format!("client_bi: {e:?}"),
                ));
            }
        };

        let dev_conn = device_conn.clone();

        // 2. Spawn a task to pair this client stream with a device stream
        tokio::spawn(async move {
            debug!("handle_forward: opening stream to device");

            let (dev_send, dev_recv) = match dev_conn.open_bi().await {
                Ok(s) => {
                    debug!("handle_forward: opened device bidi stream");
                    s
                }
                Err(e) => {
                    warn!("handle_forward: device open_bi failed: {e:?}");
                    let _ = client_send.write_all(b"NO_TUNNEL").await;
                    return;
                }
            };

            debug!("handle_forward: starting bidirectional copy");

            // Wrap streams in a struct that implements AsyncRead + AsyncWrite
            let mut client_io = CombinedStream {
                recv: client_recv,
                send: client_send,
            };
            let mut dev_io = CombinedStream {
                recv: dev_recv,
                send: dev_send,
            };

            // Use copy_bidirectional which properly handles both directions
            match tokio::io::copy_bidirectional(&mut client_io, &mut dev_io).await {
                Ok((up, down)) => {
                    debug!(up, down, "handle_forward: bidirectional copy complete");
                }
                Err(e) => {
                    // Connection errors are expected when streams close
                    let msg = e.to_string();
                    if msg.contains("ConnectionLost")
                        || msg.contains("reset")
                        || msg.contains("closed")
                    {
                        debug!("handle_forward: connection closed: {e}");
                    } else {
                        warn!("handle_forward: copy error: {e:?}");
                    }
                }
            }

            debug!("handle_forward: stream bridge complete");
        });
    }
}
