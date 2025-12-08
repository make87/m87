// tunnel.rs

use std::net::SocketAddr;
use std::pin::Pin;
use std::time::Duration;

use m87_shared::roles::Role;
use mongodb::bson::doc;
use quinn::{ConnectionError, Endpoint};
use tokio::io::{self, AsyncRead, AsyncWrite, AsyncWriteExt};
use tokio::sync::watch;
use tracing::{debug, error, info, warn};

use crate::auth::claims::Claims;
use crate::models::device::DeviceDoc;
use crate::response::ServerError;
use crate::response::ServerResult;
use crate::util::app_state::AppState;

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

                            tokio::spawn(async move {
                                match incoming_conn.await {
                                    Ok(conn) => {

                                        if let Ok(req) = web_transport_quinn::Request::accept(conn.clone()).await {
                                            // browser path
                                            if let Ok(session) = req.ok().await {
                                                let _ = handle_webtransport_forward(session, state_cl).await;
                                            }
                                            conn.close(0u32.into(), b"");
                                            return;
                                        }

                                        // CLI / raw QUIC path
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

async fn extract_token(conn: &quinn::Connection) -> Option<String> {
    // Stream 0 is typically the first opened stream from the client
    let stream = conn.accept_bi().await.ok()?;
    let (_send, mut recv) = stream;

    // read token length (u16 BE)
    let mut len_buf = [0u8; 2];
    if recv.read_exact(&mut len_buf).await.is_err() {
        return None;
    }
    let len = u16::from_be_bytes(len_buf) as usize;

    // read token bytes
    let mut buf = vec![0u8; len];
    if recv.read_exact(&mut buf).await.is_err() {
        return None;
    }

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
    let control_host = format!("control.{public}");

    let Some(token) = extract_token(&conn).await else {
        conn.close(0x100u32.into(), b"missing-token");
        return Err(ServerError::missing_token("missing api key or token"));
    };
    let claims = Claims::from_bearer_or_key(&token, &state.db, &state.config).await?;

    if sni == control_host {
        info!(%sni, "control tunnel connection");
        if let Err(e) = handle_control_tunnel(conn, claims, state).await {
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
            conn.close(0u32.into(), b"No tunnel");
        }
        return Ok(());
    }

    warn!(%sni, "invalid SNI — no match");
    conn.close(0u32.into(), b"Invalid SNI");
    Ok(())
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
    claims: Claims,
    state: AppState,
) -> ServerResult<()> {
    // Accept first control handshake stream
    let (mut send, mut recv) = conn.accept_bi().await?;
    let mut len_buf = [0u8; 4];
    recv.read_exact(&mut len_buf)
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::UnexpectedEof, "control: empty handshake"))?;
    let len = u32::from_be_bytes(len_buf) as usize;

    // json body
    let mut buf = vec![0u8; len];
    recv.read_exact(&mut buf)
        .await
        .map_err(|e| io::Error::new(io::ErrorKind::UnexpectedEof, "control: empty handshake"))?;

    let device_id = String::from_utf8_lossy(&buf).to_string();

    // ndoe has editor permissions to itself
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

    tokio::spawn(async move {
        let reason = conn.closed().await;

        match reason {
            // --- GRACEFUL SHUTDOWN (no reconnect expected) ---
            ConnectionError::ApplicationClosed(_) => {
                state.relay.remove_tunnel(&device_id).await;
                info!(%device_id, "device gracefully closed");
            }
            ConnectionError::ConnectionClosed(_) => {
                state.relay.remove_tunnel(&device_id).await;
                warn!(%device_id, "device QUIC stack closed connection");
            }

            ConnectionError::LocallyClosed => {
                state.relay.remove_tunnel(&device_id).await;
                info!(%device_id, "connection closed locally");
            }

            ConnectionError::Reset => {
                // peer reset = intentional/terminal
                state.relay.remove_tunnel(&device_id).await;
                warn!(%device_id, "connection reset by peer");
            }

            ConnectionError::TransportError(err) => {
                warn!(%device_id, "device QUIC stack error: {}", err);
            }

            // --- UNINTENTIONAL LOSS (reconnect expected) ---
            ConnectionError::TimedOut => {
                warn!(%device_id, "device lost connection; waiting for reconnect");
                state.relay.mark_tunnel_lost(&device_id).await;

                let relay2 = state.relay.clone();
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
                state.relay.remove_tunnel(&device_id).await;
                warn!(%device_id, ?reason, "fatal QUIC error");
            }
        }
    });

    // optional ack
    let _ = send.write_all(b"OK").await;

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
    debug!("handle_forward: waiting for client stream");

    // 1. Accept the client's first bidi stream
    let (mut client_send, mut client_recv) = match client_conn.accept_bi().await {
        Ok(s) => {
            debug!("handle_forward: accepted client bidi stream");
            s
        }
        Err(e) => {
            warn!("handle_forward: client accept_bi failed: {e:?}");
            return Err(io::Error::new(
                io::ErrorKind::Other,
                format!("client_bi: {e:?}"),
            ));
        }
    };

    debug!("handle_forward: opening stream to device");

    // 2. Open a bidi stream to the device
    let (mut dev_send, mut dev_recv) = match device_conn.open_bi().await {
        Ok(s) => {
            debug!("handle_forward: opened device bidi stream");
            s
        }
        Err(e) => {
            warn!("handle_forward: device open_bi failed: {e:?}");
            let _ = client_send.write_all(b"NO_TUNNEL").await;
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                format!("device_bi: {e:?}"),
            ));
        }
    };

    debug!("handle_forward: starting bidirectional copy");

    // 3. Copy both directions simultaneously
    let uplink = tokio::io::copy(&mut client_recv, &mut dev_send); // client → device
    let downlink = tokio::io::copy(&mut dev_recv, &mut client_send); // device → client

    // 4. Whichever side closes first, we gracefully finish the opposite send stream
    tokio::select! {
        result = uplink => {
            let _ = dev_send.finish();
            match &result {
                Ok(bytes) => debug!(bytes, "handle_forward: uplink finished"),
                Err(e) => warn!("handle_forward: uplink error: {e:?}"),
            }
            result?;
        }
        result = downlink => {
            let _ = client_send.shutdown();
            match &result {
                Ok(bytes) => debug!(bytes, "handle_forward: downlink finished"),
                Err(e) => warn!("handle_forward: downlink error: {e:?}"),
            }
            result?;
        }
    }

    debug!("handle_forward: complete");
    Ok(())
}
