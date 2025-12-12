use anyhow::{Context, Result};
use quinn::{ClientConfig, Endpoint, IdleTimeout};
use quinn_proto::crypto::rustls::QuicClientConfig;
use rustls::{ClientConfig as RustlsClientConfig, RootCertStore};
use std::time::Duration;
use std::{net::SocketAddr, sync::Arc};
use std::{pin::Pin, task::Poll};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt, ReadBuf};
use tracing::{debug, warn};

use crate::streams::stream_type::StreamType;
use crate::util::tls::NoVerify; // reuse the same NoVerify struct

async fn resolve_host(host: &str, port: u16) -> Result<SocketAddr> {
    for i in 0..10 {
        match tokio::net::lookup_host((host, port)).await {
            Ok(addrs) => {
                for addr in addrs {
                    if addr.is_ipv4() {
                        return Ok(addr);
                    }
                }
            }
            Err(_) => { /* ignore and retry */ }
        }

        let backoff_ms = 200 + i * 150;
        tokio::time::sleep(std::time::Duration::from_millis(backoff_ms)).await;
    }

    Err(anyhow::anyhow!("DNS resolution failed after retries"))
}

pub async fn get_quic_connection(
    host_name: &str,
    token: &str,
    trust_invalid_server_cert: bool,
) -> Result<(Endpoint, quinn::Connection)> {
    // if hostname ends with :port extract port otherwise use 443
    let port = if let Some(Ok(port)) = host_name
        .rsplit_once(':')
        .map(|(_, port)| port.parse::<u16>())
    {
        port
    } else {
        443
    };
    let port_free_host_name = host_name
        .strip_suffix(&format!(":{}", port))
        .unwrap_or(host_name);
    let server_addr = resolve_host(port_free_host_name, port).await?;

    // 2. Root store (system roots)
    let mut root_store = RootCertStore::empty();
    root_store
        .roots
        .extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());

    // 3. Build rustls QUIC client config
    debug!(
        "Creating QUIC client config with trust_invalid_server_cert={}",
        trust_invalid_server_cert
    );

    let mut tls = if trust_invalid_server_cert {
        warn!("QUIC: trusting invalid server certificate");
        RustlsClientConfig::builder()
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(NoVerify))
            .with_no_client_auth()
    } else {
        RustlsClientConfig::builder()
            .with_root_certificates(root_store)
            .with_no_client_auth()
    };

    // 4. QUIC requires ALPN
    tls.alpn_protocols = vec![b"m87-quic".to_vec()];

    // 5. Convert rustls → QUIC crypto config
    let crypto =
        Arc::new(QuicClientConfig::try_from(tls).context("failed converting rustls→quic config")?);

    let mut client_cfg = ClientConfig::new(crypto);
    let mut transport = quinn::TransportConfig::default();
    transport.keep_alive_interval(Some(Duration::from_secs(5)));
    transport.max_idle_timeout(Some(
        IdleTimeout::try_from(Duration::from_secs(10)).unwrap(),
    )); // 10 seconds
    client_cfg.transport_config(Arc::new(transport));

    // 6. Create QUIC client endpoint (local ephemeral port)
    let mut endpoint =
        Endpoint::client("[::]:0".parse().unwrap()).context("failed creating QUIC endpoint")?;
    endpoint.set_default_client_config(client_cfg);

    let connecting = endpoint
        .connect(server_addr, port_free_host_name)
        .context("QUIC connect() failed")?;

    let conn = connecting.await.context("QUIC handshake failed")?;

    let mut send = conn.open_uni().await?;
    debug!("Connected to server");
    debug!("Sending token");
    let token_bytes = token.as_bytes();
    send.write_all(&(token_bytes.len() as u16).to_be_bytes())
        .await?;
    send.write_all(token_bytes).await?;
    send.finish()?;

    Ok((endpoint, conn))
}

pub struct QuicIo {
    pub recv: quinn::RecvStream,
    pub send: quinn::SendStream,
}

impl AsyncRead for QuicIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.recv).poll_read(cx, buf)
    }
}

impl AsyncWrite for QuicIo {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        data: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.send)
            .poll_write(cx, data)
            .map_err(|e| std::io::Error::from(e))
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.send).poll_flush(cx)
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.send).poll_shutdown(cx)
    }
}

pub async fn open_quic_io(
    host: &str,
    token: &str,
    device_short_id: &str,
    stream_type: StreamType,
    trust_invalid: bool,
) -> Result<(quinn::Connection, QuicIo)> {
    let (_endpoint, conn) = connect_quic_only(host, token, device_short_id, trust_invalid).await?;
    let io = open_quic_stream(&conn, stream_type).await?;
    Ok((conn, io))
}

pub async fn connect_quic_only(
    host: &str,
    token: &str,
    device_short_id: &str,
    trust_invalid: bool,
) -> Result<(Endpoint, quinn::Connection)> {
    let full_host = format!("{}.{}", device_short_id, host);
    get_quic_connection(&full_host, token, trust_invalid).await
}

pub async fn open_quic_stream(conn: &quinn::Connection, stream_type: StreamType) -> Result<QuicIo> {
    debug!("Opening QUIC stream");
    let (mut send, recv) = conn.open_bi().await?;

    let json = serde_json::to_vec(&stream_type)?;
    let len = (json.len() as u32).to_be_bytes();

    send.write_all(&len).await?;
    send.write_all(&json).await?;
    send.flush().await?;

    debug!("Stream opened");

    Ok(QuicIo { recv, send })
}
