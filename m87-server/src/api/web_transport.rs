use std::{net::SocketAddr, sync::Arc, time::Duration};

use bytes::Bytes;
use h3::{ext::Protocol, quic::BidiStream, server::Connection as H3Connection};
use h3_quinn::quinn::{self, crypto::rustls::QuicServerConfig};
use h3_webtransport::server::{AcceptedBi, WebTransportSession};
use m87_shared::roles::Role;
use mongodb::bson::doc;
use reqwest::Method;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    sync::watch,
};
use tracing::{error, info, warn};

use crate::{
    api::{
        certificate::load_cert_and_key,
        client_connection::{ClientConn, WebConn},
    },
    auth::claims::Claims,
    models::device::DeviceDoc,
    response::{ServerError, ServerResult},
    util::app_state::AppState,
};

use super::quic::handle_forward_supervised;

pub async fn run_webtransport(
    state: AppState,
    mut reload_rx: watch::Receiver<()>,
) -> ServerResult<()> {
    let addr = SocketAddr::from(([0, 0, 0, 0], state.config.webtransport_port));

    loop {
        let (certs, key) = load_cert_and_key(&state.config).await?;

        let mut tls_config = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(certs, key)
            .map_err(|e| ServerError::internal_error(&format!("WT TLS build: {e}")))?;

        tls_config.max_early_data_size = u32::MAX;
        tls_config.alpn_protocols = vec![
            b"h3".to_vec(),
            b"h3-32".to_vec(),
            b"h3-31".to_vec(),
            b"h3-30".to_vec(),
            b"h3-29".to_vec(),
        ];

        let mut server_config = quinn::ServerConfig::with_crypto(Arc::new(
            QuicServerConfig::try_from(tls_config)
                .map_err(|e| ServerError::internal_error(&format!("WT quic cfg: {e}")))?,
        ));

        let mut transport_config = quinn::TransportConfig::default();
        transport_config.keep_alive_interval(Some(Duration::from_secs(5)));
        server_config.transport = Arc::new(transport_config);

        let endpoint = quinn::Endpoint::server(server_config, addr)
            .map_err(|e| ServerError::internal_error(&format!("WT bind: {e}")))?;

        info!("WebTransport listening (HTTP/3) on udp://{}", addr);

        loop {
            tokio::select! {
                incoming = endpoint.accept() => {
                    let Some(new_conn) = incoming else {
                        warn!("WT endpoint.accept() returned None");
                        break;
                    };

                    let state_cl = state.clone();

                    tokio::spawn(async move {
                        if let Err(e) = handle_h3_connection(new_conn, state_cl).await {
                            error!("WT conn error: {e:?}");
                        }
                    });
                }

                _ = reload_rx.changed() => {
                    warn!("WT TLS reload requested");
                    break;
                }
            }
        }

        // drop endpoint, then loop to rebuild with new cert
        drop(endpoint);
    }
}

async fn handle_h3_connection(new_conn: quinn::Incoming, state: AppState) -> ServerResult<()> {
    use h3_quinn::Connection as H3QuinnConn;

    let conn = new_conn
        .await
        .map_err(|e| ServerError::internal_error(&format!("WT quic accept: {e}")))?;

    let h3_conn: H3Connection<H3QuinnConn, Bytes> = h3::server::builder()
        .enable_webtransport(true)
        .enable_extended_connect(true)
        .enable_datagram(true)
        .max_webtransport_sessions(1)
        .send_grease(true)
        .build(H3QuinnConn::new(conn.clone()))
        .await
        .map_err(|e| ServerError::internal_error(&format!("WT h3 build: {e}")))?;

    handle_h3_requests(h3_conn, state, conn).await
}

async fn handle_h3_requests(
    mut conn: H3Connection<h3_quinn::Connection, Bytes>,
    state: AppState,
    inner_conn: quinn::Connection,
) -> ServerResult<()> {
    loop {
        match conn.accept().await {
            Ok(Some(resolver)) => {
                let (req, stream) = match resolver.resolve_request().await {
                    Ok(request) => request,
                    Err(err) => {
                        error!("WT resolve_request error: {err:?}");
                        continue;
                    }
                };

                let ext = req.extensions();

                match req.method() {
                    &Method::CONNECT if ext.get::<Protocol>() == Some(&Protocol::WEB_TRANSPORT) => {
                        info!("WT: CONNECT + WEB_TRANSPORT from {}", req.uri());
                        let url = req.uri();
                        let host = url.host().unwrap_or_default();
                        // device-short-id.yourdomain.tld
                        let device_id = host.split('.').next().unwrap_or_default().to_string();

                        let session: WebTransportSession<h3_quinn::Connection, Bytes> =
                            WebTransportSession::accept(req, stream, conn)
                                .await
                                .map_err(|e| {
                                    ServerError::bad_request(&format!("WT accept: {e:?}"))
                                })?;

                        // After this, conn is “owned” by the session, so we hand off and return.
                        handle_webtransport_forward(session, state, inner_conn, device_id).await?;
                        return Ok(());
                    }
                    _ => {
                        // You can optionally send a simple 404 for non-WT requests or just ignore.
                        warn!("WT: non-WebTransport request received, ignoring");
                    }
                }
            }
            Ok(None) => {
                // no more streams
                break;
            }
            Err(err) => {
                error!("WT h3 connection error: {err}");
                break;
            }
        }
    }

    Ok(())
}

async fn handle_webtransport_forward(
    session: WebTransportSession<h3_quinn::Connection, Bytes>,
    state: AppState,
    inner_conn: quinn::Connection,
    device_id: String,
) -> ServerResult<()> {
    let (mut send, mut recv) = match session.accept_bi().await {
        Ok(Some(AcceptedBi::BidiStream(_, stream))) => BidiStream::split(stream),
        _ => return Err(ServerError::bad_request("unexpected stream type")),
    };

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

    if !state.relay.has_tunnel(&device_id).await {
        return Err(ServerError::not_found("device tunnel not connected"));
    };
    let web = WebConn::new(Arc::new(session), inner_conn.clone());
    tokio::spawn(async move {
        if let Err(e) =
            handle_forward_supervised(ClientConn::Web(web), device_id.clone(), state.clone()).await
        {
            warn!(%device_id, "WT forward error: {:?}", e);
        }
    });

    let _ = send.write_all(b"OK").await;

    Ok(())
}
