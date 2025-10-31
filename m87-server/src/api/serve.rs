use axum::{
    http::{header, Method},
    response::IntoResponse,
    routing::get,
    Router,
};
use futures::StreamExt;
use std::{sync::Arc, time::Duration};
use tokio::io::{self, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;
use tokio_rustls::server::TlsStream;
use tokio_rustls_acme::{caches::DirCache, AcmeConfig};
use tokio_stream::wrappers::TcpListenerStream;
use tower_http::{
    compression::CompressionLayer,
    cors::{AllowOrigin, CorsLayer},
    sensitive_headers::SetSensitiveHeadersLayer,
    timeout::TimeoutLayer,
    trace::TraceLayer,
};
use tracing::{info, warn};

use crate::{
    api::{auth, node},
    config::AppConfig,
    db::Mongo,
    relay::relay_state::RelayState,
    response::{NexusError, NexusResult},
    util::{app_state::AppState, tcp_proxy::proxy_bidirectional},
};
use tokio_yamux::{Config as YamuxConfig, Session};

async fn get_status() -> impl IntoResponse {
    "ok".to_string()
}

pub async fn serve(db: Arc<Mongo>, relay: Arc<RelayState>, cfg: Arc<AppConfig>) -> NexusResult<()> {
    let state = AppState {
        db: db.clone(),
        config: cfg.clone(),
        relay: relay.clone(),
    };

    // ===== REST on loopback =====
    let cors = CorsLayer::new()
        .allow_origin(AllowOrigin::any())
        .allow_methods([Method::GET, Method::POST, Method::DELETE])
        .allow_headers([
            header::AUTHORIZATION,
            header::CONTENT_TYPE,
            header::HeaderName::from_static("sec-websocket-protocol"),
        ]);

    let app = Router::new()
        .nest("/auth", auth::create_route())
        .nest("/node", node::create_route())
        .route("/status", get(get_status))
        .with_state(state.clone())
        .layer(cors)
        .layer(SetSensitiveHeadersLayer::new(std::iter::once(
            header::AUTHORIZATION,
        )))
        .layer(TimeoutLayer::new(Duration::from_secs(30)))
        .layer(TraceLayer::new_for_http())
        .layer(CompressionLayer::new());

    let rest_listener = TcpListener::bind(("127.0.0.1", cfg.rest_port))
        .await
        .expect("bind REST");
    info!("REST listening on 127.0.0.1:{}", cfg.rest_port);

    let rest_task = tokio::spawn(async move {
        if let Err(e) = axum::serve(rest_listener, app).await {
            warn!("Axum server failed: {e:?}");
        }
    });

    // ===== TLS + ACME on public port =====
    let cache_dir = "/app/certs";
    let public = cfg.public_address.clone(); // e.g. "nexus.make87.com"
    let control = format!("control.{public}");

    let tcp = TcpListener::bind(("0.0.0.0", cfg.unified_port))
        .await
        .expect("bind TLS");
    let incoming = TcpListenerStream::new(tcp);

    // if staging exists and is 1 set to true

    let mut tls_incoming = AcmeConfig::new([public.as_str(), control.as_str()])
        .contact_push("mailto:admin@make87.com")
        .cache(DirCache::new(cache_dir))
        .directory_lets_encrypt(!state.config.is_staging)
        .incoming(incoming, Vec::new());

    info!("TLS listener (ACME) on :{}", cfg.unified_port);

    let tls_state = state.clone();
    let tls_task = tokio::spawn(async move {
        while let Some(conn) = tls_incoming.next().await {
            match conn {
                Ok(mut tls) => {
                    let state = tls_state.clone();
                    tokio::spawn(async move {
                        let sni = tls.get_ref().1.server_name().unwrap_or("").to_string();
                        if sni.is_empty() {
                            warn!("TLS no SNI; closing");
                            let _ = tls.shutdown().await;
                            return;
                        }

                        if sni == state.config.public_address {
                            if let Err(e) = proxy_to_rest(&mut tls, state.config.rest_port).await {
                                warn!("REST proxy failed: {e:?}");
                            }
                            return;
                        }

                        if sni == format!("control.{}", state.config.public_address) {
                            if let Err(e) = handle_control_tunnel(
                                state.relay.clone(),
                                tls,
                                &state.config.forward_secret,
                            )
                            .await
                            {
                                warn!("control tunnel failed: {e:?}");
                            }
                            return;
                        }

                        if let Err(e) =
                            handle_forward_connection(state.relay.clone(), sni, tls).await
                        {
                            warn!("forward failed: {e:?}");
                        }
                    });
                }
                Err(e) => warn!("ACME/TLS accept error: {e:?}"),
            }
        }
    });

    tokio::select! {
        _ = rest_task => warn!("REST task exited"),
        _ = tls_task => warn!("TLS task exited"),
    }
    Ok(())
}

// === Helpers ===

async fn proxy_to_rest(
    inbound: &mut TlsStream<tokio::net::TcpStream>,
    rest_port: u16,
) -> io::Result<()> {
    let mut outbound = tokio::net::TcpStream::connect(("127.0.0.1", rest_port)).await?;
    let _ = proxy_bidirectional(inbound, &mut outbound).await;
    Ok(())
}

pub async fn handle_control_tunnel(
    relay: Arc<RelayState>,
    tls: TlsStream<tokio::net::TcpStream>,
    secret: &str,
) -> io::Result<()> {
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt};
    let mut reader = BufReader::new(tls);

    // Expect: "M87 node_id=<id> token=<base64>\n"
    let mut line = String::new();
    if reader.read_line(&mut line).await? == 0 {
        warn!("control: empty handshake");
        return Ok(());
    }
    let node_id = extract_kv(&line, "node_id").unwrap_or_default();
    let token = extract_kv(&line, "token").unwrap_or_default();
    if node_id.is_empty() || token.is_empty() {
        warn!("control: missing node_id/token");
        return Ok(());
    }

    match crate::auth::tunnel_token::verify_tunnel_token(&token, secret) {
        Ok(id_ok) if id_ok == node_id => {}
        _ => {
            warn!("control: token invalid or mismatched");
            return Ok(());
        }
    }

    {
        let mut tunnels = relay.tunnels.write().await;
        tunnels.remove(&node_id);
    }

    // Upgrade to Yamux
    let base = reader.into_inner();
    let sess = Session::new_server(base, YamuxConfig::default());
    relay.register_tunnel(node_id.clone(), sess).await;
    info!(%node_id, "control tunnel active");
    Ok(())
}

async fn handle_forward_connection(
    relay: Arc<RelayState>,
    host: String,
    mut inbound: TlsStream<tokio::net::TcpStream>,
) -> NexusResult<()> {
    // ACL
    if let Ok(peer) = inbound.get_ref().0.peer_addr() {
        if let Some(meta) = relay.forwards.read().await.get(&host).cloned() {
            if let Some(ips) = meta.allowed_ips {
                let ip = peer.ip().to_string();
                if !ips.iter().any(|a| a == &ip) {
                    warn!(%host, %ip, "blocked by whitelist");
                    let _ = inbound.get_mut().0.shutdown().await;
                    return Ok(());
                }
            }
        }
    }

    let meta = match relay.forwards.read().await.get(&host).cloned() {
        Some(m) => m,
        None => {
            warn!(%host, "no forward mapping");
            let _ = inbound.shutdown().await;
            return Ok(());
        }
    };

    let Some(conn_arc) = relay.get_tunnel(&meta.node_id).await else {
        warn!(%host, node_id=%meta.node_id, "tunnel not active");
        let _ = inbound.shutdown().await;
        return Ok(());
    };

    let mut sess = conn_arc.lock().await;
    let mut sub = sess
        .open_stream()
        .map_err(|_| NexusError::internal_error("yamux open_stream failed"))?;
    let header = format!("{}\n", meta.target_port);
    sub.write_all(header.as_bytes())
        .await
        .map_err(|e| NexusError::internal_error(&format!("yamux header send failed: {e}")))?;

    tokio::spawn(async move {
        let _ = proxy_bidirectional(&mut inbound, &mut sub).await;
    });
    Ok(())
}

fn extract_kv(line: &str, key: &str) -> Option<String> {
    line.split_whitespace().find_map(|part| {
        part.strip_prefix(&(key.to_owned() + "="))
            .map(|s| s.to_string())
    })
}
