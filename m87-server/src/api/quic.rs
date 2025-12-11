use bytes::Bytes;
use futures::future::{AbortHandle, Abortable};
use governor::{Quota, RateLimiter};
use m87_shared::roles::Role;
use mongodb::bson::doc;
use quinn::{ConnectionError, Endpoint};
use std::net::SocketAddr;
use std::num::NonZeroU32;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{self, AsyncRead, AsyncWrite, AsyncWriteExt};
use tokio::sync::watch;
use tracing::{debug, error, info, warn};

use crate::api::client_connection::ClientConn;
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
                            info!("Received incoming QUIC connection");

                            tokio::spawn(async move {
                                match incoming_conn.await {
                                    Ok(conn) => {
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

        if state.relay.has_tunnel(&device_id).await {
            debug!(%device_id, "forwarding to device");
            let _ =
                handle_forward_supervised(ClientConn::Raw(conn), device_id.clone(), state.clone())
                    .await;
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
    let device_id = device_short_id.to_string();

    // Permission check
    claims
        .find_one_with_scope_and_role::<DeviceDoc>(
            &state.db.devices(),
            doc! { "short_id": &device_id },
            Role::Editor,
        )
        .await?
        .ok_or_else(|| ServerError::not_found("Device not found"))?;

    // Atomically replace any existing tunnel
    state.relay.replace_tunnel(&device_id, conn.clone()).await;

    // Wait until fully closed
    let reason = conn.closed().await;

    match reason {
        ConnectionError::ApplicationClosed(_) => {
            info!(%device_id, "device gracefully closed");
        }
        ConnectionError::ConnectionClosed(_) => {
            warn!(%device_id, "device closed connection");
        }
        ConnectionError::LocallyClosed => {
            info!(%device_id, "we closed connection locally");
        }
        ConnectionError::Reset => {
            warn!(%device_id, "connection reset by peer");
        }
        ConnectionError::TransportError(err) => {
            warn!(%device_id, "transport error: {}", err);
        }
        ConnectionError::TimedOut => {
            warn!(%device_id, "device timed out");
        }
        ConnectionError::VersionMismatch | ConnectionError::CidsExhausted => {
            warn!(%device_id, ?reason, "fatal QUIC error");
        }
    }

    // Clean up only if the closed conn is still the active tunnel
    state
        .relay
        .remove_if_match(&device_id, conn.stable_id())
        .await;

    Ok(())
}

enum ForwardEnd {
    ClientClosed,
    DeviceClosed,
}

async fn wait_for_device_conn(
    state: &AppState,
    device_id: &str,
    timeout: Duration,
) -> Option<quinn::Connection> {
    let start = Instant::now();
    loop {
        if let Some(conn) = state.relay.get_tunnel(device_id).await {
            return Some(conn);
        }
        if start.elapsed() >= timeout {
            return None;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

pub async fn handle_forward_supervised(
    client_conn: ClientConn,
    device_id: String,
    state: AppState,
) -> io::Result<()> {
    const RECONNECT_TIMEOUT: Duration = Duration::from_secs(45);

    loop {
        // wait (with timeout) for a device tunnel
        let Some(device_conn) = wait_for_device_conn(&state, &device_id, RECONNECT_TIMEOUT).await
        else {
            warn!(%device_id, "device did not reconnect within timeout, closing forward");
            return Err(io::Error::new(
                io::ErrorKind::NotConnected,
                "device did not reconnect in time",
            ));
        };

        debug!(%device_id, "starting forward session");
        match handle_forward_once(&client_conn, &device_conn, &device_id).await {
            ForwardEnd::ClientClosed => {
                debug!(%device_id, "client closed, ending supervised forward");
                return Ok(());
            }
            ForwardEnd::DeviceClosed => {
                warn!(%device_id, "device tunnel closed, waiting for reconnect");
                // loop again → wait_for_device_conn()
            }
        }
    }
}

const MAX_PARALLEL_STREAMS: usize = 128;

async fn handle_forward_once(
    client_conn: &ClientConn,
    device_conn: &quinn::Connection,
    device_id: &str,
) -> ForwardEnd {
    let active_streams = Arc::new(tokio::sync::Semaphore::new(MAX_PARALLEL_STREAMS));

    spawn_udp_bridge(
        client_conn.clone(),
        device_conn.clone(),
        device_id.to_string(),
    );

    let client_closed_fut = client_conn.closed();
    tokio::pin!(client_closed_fut);

    let device_closed_fut = device_conn.closed();
    tokio::pin!(device_closed_fut);

    debug!("handle_forward_once: starting");

    loop {
        tokio::select! {

            _ = &mut client_closed_fut => {
                debug!("handle_forward_once: client closed");
                return ForwardEnd::ClientClosed;
            }

            _ = &mut device_closed_fut => {
                warn!("handle_forward_once: device connection closed");
                return ForwardEnd::DeviceClosed;
            }

            client_bi = client_conn.accept_bi() => {
                // Acquire a stream slot
                let permit = match active_streams.clone().try_acquire_owned() {
                    Ok(p) => p,
                    Err(_) => {
                        warn!("too many parallel streams; rejecting new stream");
                        continue;
                    }
                };

                let (mut client_send, mut client_recv) = match client_bi {
                    Ok(s) => s,
                    Err(e) => {
                        warn!("client accept_bi failed: {e:?}");
                        return ForwardEnd::ClientClosed;
                    }
                };

                let dev_conn = device_conn.clone();
                let device_id = device_id.to_string();

                tokio::spawn(async move {
                    let _permit = permit;

                    debug!("forward: opening device stream");

                    let (mut dev_send, mut dev_recv) = match dev_conn.open_bi().await {
                        Ok(s) => s,
                        Err(e) => {
                            warn!("device open_bi failed: {e:?}");
                            let _ = client_send.write_all(b"NO_TUNNEL").await;
                            return;
                        }
                    };

                    let (abort_uplink, reg_up) = AbortHandle::new_pair();
                    let (abort_down, reg_dn) = AbortHandle::new_pair();

                    let uplink = tokio::spawn(Abortable::new(async move {
                        let r = tokio::io::copy(&mut client_recv, &mut dev_send).await;
                        let _ = dev_send.finish();
                        r
                    }, reg_up));

                    let downlink = tokio::spawn(Abortable::new(async move {
                        let r = tokio::io::copy(&mut dev_recv, &mut client_send).await;
                        let _ = client_send.shutdown().await;
                        r
                    }, reg_dn));

                    tokio::select! {
                        _ = uplink => {
                            abort_down.abort();
                        }
                        _ = downlink => {
                            abort_uplink.abort();
                        }
                    }

                    debug!(%device_id, "stream bridge complete");
                });
            }
        }
    }
}

const MAX_UDP_PAYLOAD: usize = 64 * 1024;
const UDP_SEND_BACKOFF: Duration = Duration::from_millis(1);

fn spawn_udp_bridge(client: ClientConn, device: quinn::Connection, device_id: String) {
    // CLIENT → DEVICE
    {
        let client = client.clone();
        let device = device.clone();
        let dev_id = device_id.clone();

        // REAL, ACTIVE RATE LIMITER
        let udp_limiter = Arc::new(RateLimiter::direct(
            Quota::per_second(NonZeroU32::new(50_000).unwrap()), // 50k packets/s
        ));

        tokio::spawn(async move {
            loop {
                let d = match client.read_datagram().await {
                    Ok(d) => d,
                    Err(e) => {
                        debug!(%dev_id, "udp client->device read_end: {e:?}");
                        break;
                    }
                };

                if d.len() > MAX_UDP_PAYLOAD {
                    warn!(%dev_id, len = d.len(), "udp datagram too large, dropping");
                    continue;
                }

                // ACTUAL RATE LIMITING
                if udp_limiter.check().is_err() {
                    // drop packet; do not queue → prevents memory blowup
                    warn!(%dev_id, "udp client->device rate limit exceeded, dropping");
                    continue;
                }

                if let Err(e) = device.send_datagram(d) {
                    warn!(%dev_id, "udp client->device send error: {e:?}");
                    tokio::time::sleep(UDP_SEND_BACKOFF).await;
                }
            }
        });
    }

    // DEVICE → CLIENT
    {
        let client = client.clone();
        let device = device.clone();
        let dev_id = device_id.clone();

        let udp_limiter = Arc::new(RateLimiter::direct(Quota::per_second(
            NonZeroU32::new(50_000).unwrap(),
        )));

        tokio::spawn(async move {
            loop {
                let d = match device.read_datagram().await {
                    Ok(d) => d,
                    Err(e) => {
                        debug!(%dev_id, "udp device->client read_end: {e:?}");
                        break;
                    }
                };

                if d.len() > MAX_UDP_PAYLOAD {
                    warn!(%dev_id, len = d.len(), "udp datagram too large, dropping");
                    continue;
                }

                if udp_limiter.check().is_err() {
                    warn!(%dev_id, "udp device->client rate limit exceeded, dropping");
                    continue;
                }

                if let Err(e) = client.send_datagram(d) {
                    warn!(%dev_id, "udp device->client send error: {e:?}");
                    tokio::time::sleep(UDP_SEND_BACKOFF).await;
                }
            }
        });
    }
}
