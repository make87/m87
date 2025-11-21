use futures::StreamExt;
use m87_shared::{forward::ForwardAccess, roles::Role};
use mongodb::bson::doc;
use std::sync::Arc;
use tokio::io::{self, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio_rustls::server::TlsStream;
use tracing::{info, warn};

use crate::{
    auth::claims::Claims,
    config::AppConfig,
    db::Mongo,
    models::device::DeviceDoc,
    relay::relay_state::RelayState,
    response::{ServerError, ServerResult},
    util::{app_state::AppState, tcp_proxy::proxy_bidirectional},
};

use tokio_yamux::{Config as YamuxConfig, Session};

pub async fn handle_sni(sni: &str, mut tls: TlsStream<tokio::net::TcpStream>, state: &AppState) {
    if sni.is_empty() {
        warn!("TLS no SNI; proxy to rest");
        if let Err(e) = proxy_to_rest(&mut tls, state.config.rest_port).await {
            warn!("REST proxy failed: {e:?}");
        }
        return;
    }

    let public = &state.config.public_address;
    let control_host = format!("control.{public}");

    if sni == *public {
        if let Err(e) = proxy_to_rest(&mut tls, state.config.rest_port).await {
            warn!("REST proxy failed: {e:?}");
        }
        return;
    }

    if sni == control_host {
        if let Err(e) =
            handle_control_tunnel(state.relay.clone(), tls, &state.config.forward_secret).await
        {
            warn!("control tunnel failed: {e:?}");
        }
        return;
    }

    if let Some(prefix) = sni.strip_suffix(public) {
        // e.g. "device123.", "myapp-device123."
        let prefix = prefix.trim_end_matches('.');

        let parts: Vec<&str> = prefix.split('-').collect();
        match parts.len() {
            1 => {
                let node_short_id = parts[0];
                if let Err(e) = proxy_to_device_rest(&mut tls, node_short_id, state).await {
                    warn!("device proxy failed: {e:?}");
                }
            }
            n if n == 2 => {
                let device_short_id = parts[1];
                let forward_name = parts[0];
                if let Err(e) = handle_forward_connection(
                    state.relay.clone(),
                    state.db.clone(),
                    state.config.clone(),
                    device_short_id.to_string(),
                    forward_name.to_string(),
                    tls,
                )
                .await
                {
                    warn!("forward failed: {e:?}");
                }
            }
            _ => {
                warn!("invalid SNI format: {}", sni);
                let _ = tls.shutdown().await;
            }
        }
        return;
    }

    warn!("unmatched SNI: {}", sni);
    let _ = tls.shutdown().await;
}

fn extract_bearer_token(request: &str) -> Option<String> {
    for line in request.lines() {
        let lower = line.to_ascii_lowercase();

        if lower.starts_with("authorization: bearer ") {
            return line
                .split_once("Bearer ")
                .map(|(_, v)| v.trim().to_string());
        }

        if lower.starts_with("sec-websocket-protocol:") {
            let original = line.splitn(2, ':').nth(1)?.trim();

            for proto in original.split(',') {
                let proto_trim = proto.trim();
                if proto_trim.to_ascii_lowercase().starts_with("bearer.") {
                    return Some(proto_trim["bearer.".len()..].to_string());
                }
            }
        }
    }

    None
}

pub async fn proxy_to_device_rest(
    inbound: &mut TlsStream<tokio::net::TcpStream>,
    short_id: &str,
    state: &AppState,
) -> ServerResult<()> {
    let (buffer, header_end) = read_full_http_request(inbound).await?;
    let header_bytes = &buffer[..header_end];
    let leftover_bytes = &buffer[header_end..];

    let request = String::from_utf8_lossy(header_bytes);

    let token = extract_bearer_token(&request);
    if token.is_none() {
        info!("Rejecting connection to {}. Missing token!", short_id);
        inbound
            .get_mut()
            .0
            .write_all(b"HTTP/1.1 401 Unauthorized\r\n\r\n")
            .await?;
        return Ok(());
    }
    let claims = Claims::from_bearer_or_key(&token.unwrap(), &state.db, &state.config).await;
    let device = match claims {
        Ok(claims) => claims
            .find_one_with_scope_and_role::<DeviceDoc>(
                &state.db.devices(),
                doc! { "short_id": short_id },
                Role::Editor,
            )
            .await?
            .ok_or_else(|| ServerError::not_found("Device not found"))?,
        Err(_) => {
            inbound
                .get_mut()
                .0
                .write_all(b"HTTP/1.1 401 Unauthorized\r\n\r\n")
                .await?;
            return Ok(());
        }
    };

    let device_id = device.id.clone().unwrap().to_string();

    let Some(conn_arc) = state.relay.get_tunnel(&device_id).await else {
        warn!("No active tunnel for {short_id}");
        inbound
            .get_mut()
            .0
            .write_all(b"HTTP/1.1 502 Bad Gateway\r\n\r\n")
            .await?;
        return Ok(());
    };

    let mut sess = conn_arc.lock().await;
    let mut sub = match sess.open_stream().await {
        Ok(s) => s,
        Err(_) => {
            inbound
                .get_mut()
                .0
                .write_all(b"HTTP/1.1 500 Internal Server Error\r\n\r\n")
                .await?;
            return Ok(());
        }
    };

    let rest_port = device.config.server_port;
    sub.write_all(format!("{rest_port}\n").as_bytes()).await?;

    sub.write_all(header_bytes).await?;
    if !leftover_bytes.is_empty() {
        sub.write_all(leftover_bytes).await?;
    }

    tokio::io::copy_bidirectional(inbound, &mut sub).await?;
    Ok(())
}

async fn read_full_http_request(
    inbound: &mut (impl AsyncReadExt + Unpin),
) -> io::Result<(Vec<u8>, usize)> {
    let mut buf = Vec::with_capacity(4096);

    let header_end = loop {
        // read chunk
        let mut tmp = [0u8; 1024];
        let n = inbound.read(&mut tmp).await?;

        if n == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "client closed before sending headers",
            ));
        }

        buf.extend_from_slice(&tmp[..n]);

        if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            break pos + 4;
        }

        if buf.len() > 32 * 1024 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "headers too large",
            ));
        }
    };

    Ok((buf, header_end))
}

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
    use tokio::io::AsyncBufReadExt;
    let mut reader = BufReader::new(tls);

    // Expect: "M87 device_id=<id> token=<base64>\n"
    let mut line = String::new();
    if reader.read_line(&mut line).await? == 0 {
        warn!("control: empty handshake");
        return Ok(());
    }
    let device_id = extract_kv(&line, "device_id").unwrap_or_default();
    let token = extract_kv(&line, "token").unwrap_or_default();
    if device_id.is_empty() || token.is_empty() {
        warn!("control: missing device_id/token");
        return Ok(());
    }

    match crate::auth::tunnel_token::verify_tunnel_token(&token, secret) {
        Ok(id_ok) if id_ok == device_id => {}
        Ok(id_ok) => {
            warn!(
                "control: token mismatch got {} but expected {}",
                device_id, id_ok
            );
            return Ok(());
        }
        Err(err) => {
            warn!("control: token invalid {}", err);
            return Ok(());
        }
    }

    {
        let mut tunnels = relay.tunnels.write().await;
        tunnels.remove(&device_id);
    }

    let base = reader.into_inner();
    let mut sess = Session::new_server(base, YamuxConfig::default());
    let control = sess.control();
    relay
        .register_tunnel(device_id.clone(), control.clone())
        .await;
    info!(%device_id, "control tunnel active");

    let relay_clone = relay.clone();
    tokio::spawn(async move {
        // keep Control alive for the duration of this task
        let _keep_alive = control;
        while let Some(item) = sess.next().await {
            match item {
                Ok(_stream) => {
                    // control sessions should not yield streams — usually ignore
                }
                Err(e) => {
                    warn!("yamux session error for {}: {:?}", device_id, e);
                    break;
                }
            }
        }

        info!("control session closed for {}", device_id);
        relay_clone.remove_tunnel(&device_id).await;
    });
    Ok(())
}

async fn handle_forward_connection(
    relay: Arc<RelayState>,
    db: Arc<Mongo>,
    config: Arc<AppConfig>,
    device_short_id: String,
    forward_name: String,
    mut inbound: TlsStream<tokio::net::TcpStream>,
) -> ServerResult<()> {
    let forward_doc = db
        .forwards()
        .find_one(doc! { "device_short_id": &device_short_id, "name": &forward_name })
        .await?
        .ok_or_else(|| ServerError::not_found("no matching forward"))?;

    match &forward_doc.access {
        ForwardAccess::Open => {
            // Nothing to check
        }

        ForwardAccess::IpWhitelist(whitelist) => {
            if let Ok(peer) = inbound.get_ref().0.peer_addr() {
                let ip = peer.ip().to_string();
                if !whitelist.iter().any(|a| a == &ip) {
                    warn!(
                        "{}-{} {}blocked by IP whitelist",
                        &forward_name, &device_short_id, &ip
                    );
                    let _ = inbound.get_mut().0.shutdown().await;
                    return Ok(());
                }
            }
        }
    }

    let Some(conn_arc) = relay.get_tunnel(&forward_doc.device_id.to_string()).await else {
        warn!(
            "{}-{} for device {} tunnel not active",
            &forward_name, &device_short_id, &forward_doc.device_id
        );
        let _ = inbound.shutdown().await;
        return Ok(());
    };

    let mut sess = conn_arc.lock().await;
    let mut sub = sess
        .open_stream()
        .await
        .map_err(|_| ServerError::internal_error("yamux open_stream failed"))?;

    sub.write_all(format!("{}\n", forward_doc.target_port).as_bytes())
        .await?;

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
