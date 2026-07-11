//! iroh-based direct P2P connections.
//!
//! The CLI tries a direct iroh connection to the device before falling back to
//! the server relay path. The device advertises its iroh [`EndpointAddr`] via
//! the heartbeat; the server caches it and exposes it via
//! `GET /device/{id}/iroh-addr`.

use anyhow::{Context, Result};
use iroh::endpoint::Connection as IrohConnection;
use iroh::{Endpoint as IrohEndpoint, EndpointAddr};
use m87_shared::iroh_ticket::SignedIrohTicket;
use serde::Deserialize;
use std::time::Duration;
use tracing::{debug, warn};

use crate::streams::quic::QuicIo;
use crate::streams::stream_type::StreamType;

/// ALPN used for direct iroh P2P connections between CLI and device.
pub const IROH_ALPN: &[u8] = b"m87-iroh-p2p/1";

/// How long to wait for a direct iroh connection before falling back to the
/// server relay.
///
/// This only bites when iroh *cannot* connect (no direct path AND the iroh
/// relay is unreachable) — a working connection, even over a slow/high-latency
/// IoT uplink, establishes in a few seconds and never hits this. So the value
/// is a trade-off: low enough that a genuinely unreachable peer falls back to
/// the relay quickly, high enough that a slow-but-viable link (lossy cellular,
/// satellite RTTs) still completes the QUIC + holepunch handshake rather than
/// being cut off and losing the direct path for the whole session. 10s keeps
/// the fallback snappy while leaving comfortable headroom for slow IoT links.
const IROH_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// Response body from `GET /device/{id}/iroh-addr`.
#[derive(Deserialize)]
struct IrohAddrResponse {
    iroh_node_addr: String,
    ticket: SignedIrohTicket,
}

/// Fetch the iroh [`EndpointAddr`] and a server-signed connection ticket for a
/// device.
///
/// `server_url` is the HTTPS base URL, `token` is the user bearer token,
/// `device_id` is the MongoDB ObjectId string of the device. The ticket is
/// presented to the device to authorize the direct connection.
pub async fn fetch_device_iroh_addr(
    server_url: &str,
    token: &str,
    device_id: &str,
    trust_invalid_server_cert: bool,
) -> Result<(EndpointAddr, SignedIrohTicket)> {
    let url = format!(
        "{}/device/{}/iroh-addr",
        server_url.trim_end_matches('/'),
        device_id
    );

    // Honour the same cert-trust setting the rest of the CLI uses; otherwise
    // self-signed / staging servers reject this call and iroh silently never
    // engages (every connection falls back to the relay).
    let client = reqwest::Client::builder()
        .danger_accept_invalid_certs(trust_invalid_server_cert)
        .build()
        .context("building iroh-addr http client")?;

    let resp = client
        .get(&url)
        .bearer_auth(token)
        .timeout(Duration::from_secs(5))
        .send()
        .await
        .context("fetching iroh addr from server")?;

    if !resp.status().is_success() {
        anyhow::bail!("server returned {} for iroh-addr", resp.status());
    }

    let body: IrohAddrResponse = resp.json().await.context("parsing iroh addr response")?;

    let addr: EndpointAddr =
        serde_json::from_str(&body.iroh_node_addr).context("deserializing iroh EndpointAddr")?;

    Ok((addr, body.ticket))
}

/// Create an ephemeral iroh endpoint for the CLI (no ALPNs needed — we are
/// the dialling side only).
pub async fn create_cli_iroh_endpoint() -> Result<IrohEndpoint> {
    IrohEndpoint::bind(iroh::endpoint::presets::N0)
        .await
        .context("binding CLI iroh endpoint")
}

/// Try to establish a direct iroh connection to the device.
///
/// Returns the iroh `Connection` on success.
pub async fn try_iroh_connect(
    ep: &IrohEndpoint,
    device_addr: EndpointAddr,
) -> Result<IrohConnection> {
    debug!("iroh: attempting direct P2P connection");

    let conn = tokio::time::timeout(IROH_CONNECT_TIMEOUT, ep.connect(device_addr, IROH_ALPN))
        .await
        .map_err(|_| anyhow::anyhow!("iroh connection timed out after {:?}", IROH_CONNECT_TIMEOUT))?
        .context("iroh connect failed")?;

    debug!("iroh: direct connection established");
    Ok(conn)
}

/// Open an iroh bi-directional stream and send the stream-type header.
/// Analogous to `open_quic_stream` in `quic.rs`.
pub async fn open_iroh_stream(conn: &IrohConnection, stream_type: StreamType) -> Result<QuicIo> {
    use tokio::io::AsyncWriteExt;
    debug!("iroh: opening stream");
    let (mut send, recv) = conn.open_bi().await.context("iroh open_bi")?;

    let json = serde_json::to_vec(&stream_type)?;
    let len = (json.len() as u32).to_be_bytes();

    send.write_all(&len).await?;
    send.write_all(&json).await?;
    send.flush().await?;

    debug!("iroh: stream opened");
    Ok(QuicIo::from_iroh(recv, send))
}

/// Environment variable that disables the iroh direct-connection layer,
/// forcing all CLI traffic over the server relay. Doubles as an operational
/// kill switch and as the lever the e2e tests use to exercise relay fallback.
pub const DISABLE_IROH_ENV: &str = "M87_DISABLE_IROH";

/// Whether the iroh direct-connection layer has been disabled via
/// [`DISABLE_IROH_ENV`] (`1` / `true` / `yes`, case-insensitive).
pub fn iroh_disabled() -> bool {
    std::env::var(DISABLE_IROH_ENV)
        .map(|v| {
            let v = v.trim();
            v == "1" || v.eq_ignore_ascii_case("true") || v.eq_ignore_ascii_case("yes")
        })
        .unwrap_or(false)
}

/// High-level helper: fetch the device's iroh addr from the server and try to
/// establish a direct connection.
///
/// Returns the endpoint **and** the connection on success — the endpoint owns
/// the local socket and its driver, so it MUST be kept alive for the whole
/// lifetime of the connection (dropping it tears the connection down). Returns
/// `None` — so the caller transparently falls back to the server relay — when
/// iroh is disabled or any step (addr lookup, bind, connect) fails.
pub async fn try_iroh_connection(
    server_url: &str,
    token: &str,
    device_id: &str,
    trust_invalid_server_cert: bool,
) -> Option<(IrohEndpoint, IrohConnection)> {
    if iroh_disabled() {
        debug!("iroh: disabled via {DISABLE_IROH_ENV}, using server relay");
        return None;
    }

    let (addr, ticket) =
        match fetch_device_iroh_addr(server_url, token, device_id, trust_invalid_server_cert).await
        {
            Ok(v) => v,
            Err(e) => {
                debug!("iroh: could not get device addr (will use relay): {e}");
                return None;
            }
        };

    let ep = match create_cli_iroh_endpoint().await {
        Ok(e) => e,
        Err(e) => {
            warn!("iroh: could not create endpoint (will use relay): {e}");
            return None;
        }
    };

    let conn = match try_iroh_connect(&ep, addr).await {
        Ok(conn) => conn,
        Err(e) => {
            debug!("iroh: direct connect failed (will use relay): {e}");
            return None;
        }
    };

    // Authorize the connection: the device's accept loop reads this ticket from
    // a uni-stream before it will serve any data streams.
    if let Err(e) = send_iroh_ticket(&conn, &ticket).await {
        debug!("iroh: failed to send ticket (will use relay): {e}");
        return None;
    }

    Some((ep, conn))
}

/// Send the server-signed ticket over a uni-stream, matching the framing the
/// device's accept loop expects (`u16` big-endian length + JSON body).
async fn send_iroh_ticket(conn: &IrohConnection, ticket: &SignedIrohTicket) -> Result<()> {
    let json = serde_json::to_vec(ticket).context("serializing iroh ticket")?;
    if json.len() > u16::MAX as usize {
        anyhow::bail!("iroh ticket too large ({} bytes)", json.len());
    }

    let mut send = conn.open_uni().await.context("opening iroh ticket stream")?;
    send.write_all(&(json.len() as u16).to_be_bytes()).await?;
    send.write_all(&json).await?;
    send.finish().context("finishing iroh ticket stream")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    // ── constants ──────────────────────────────────────────────────

    #[test]
    fn test_iroh_alpn_is_correct() {
        assert_eq!(IROH_ALPN, b"m87-iroh-p2p/1");
        assert!(!IROH_ALPN.is_empty());
    }

    // ── QuicIo enum delegation ───────────────────────────────────────

    /// Verify that QuicIo::from_iroh + AsyncWrite/AsyncRead delegation works
    /// over a real in-process iroh connection between two local endpoints.
    /// This is the core wiring test for the new transport path.
    ///
    /// NOTE: requires a loopback network interface and a live internet
    /// connection is NOT needed — iroh discovers both endpoints on the same
    /// machine via direct UDP addresses before contacting any relay.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_quic_io_from_iroh_roundtrip() {
        use crate::streams::quic::QuicIo;
        use iroh::Endpoint;
        use iroh::endpoint::presets;

        // 1. Device endpoint — simulates the runtime side.
        let device_ep = Endpoint::builder(presets::N0)
            .alpns(vec![IROH_ALPN.to_vec()])
            .bind()
            .await
            .expect("device endpoint should bind");

        let device_addr = device_ep.addr();

        // 2. CLI endpoint — simulates the dialling side.
        let cli_ep = create_cli_iroh_endpoint()
            .await
            .expect("cli endpoint should bind");

        // 3. Device accept loop: accept one connection, one bi-stream.
        //    Echos whatever bytes it receives back to the sender.
        let device_task = tokio::spawn(async move {
            let incoming =
                tokio::time::timeout(std::time::Duration::from_secs(20), device_ep.accept())
                    .await
                    .expect("accept did not complete within timeout")
                    .expect("endpoint closed before accept");

            let conn = incoming.await.expect("iroh handshake failed");

            let (mut send, mut recv) = conn.accept_bi().await.expect("accept_bi failed");

            // Echo 5 bytes back.
            let mut buf = [0u8; 5];
            recv.read_exact(&mut buf)
                .await
                .expect("device: read failed");
            send.write_all(&buf).await.expect("device: write failed");
            send.finish().expect("device: finish failed");

            // Wait for the CLI to close the connection before tearing down the
            // endpoint. Closing immediately after `finish()` races the echoed
            // bytes on fast (localhost) paths — the CONNECTION_CLOSE can reach
            // the peer before the stream data, making the CLI's read_exact fail.
            conn.closed().await;
            device_ep.close().await;
        });

        // 4. CLI connects and wraps the stream in QuicIo::from_iroh.
        let conn = tokio::time::timeout(
            std::time::Duration::from_secs(20),
            try_iroh_connect(&cli_ep, device_addr),
        )
        .await
        .expect("connect timed out")
        .expect("iroh connect failed");

        let (send, recv) = conn.open_bi().await.expect("cli: open_bi failed");
        let mut io = QuicIo::from_iroh(recv, send);

        // Write 5 bytes through QuicIo::from_iroh and read the echo back.
        io.write_all(b"iroh!").await.expect("cli: write failed");
        io.flush().await.expect("cli: flush failed");

        let mut reply = [0u8; 5];
        io.read_exact(&mut reply)
            .await
            .expect("cli: read_exact failed");

        assert_eq!(&reply, b"iroh!", "echoed data must match what was sent");

        cli_ep.close().await;
        device_task.await.expect("device task panicked");
    }

    // ── full stream-type framing over iroh ────────────────────────────

    /// Verify that open_iroh_stream() sends the StreamType framing header and
    /// that from_incoming_stream() on the device side can parse it back.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_open_iroh_stream_framing() {
        use crate::streams::stream_type::StreamType;
        use iroh::Endpoint;
        use iroh::endpoint::presets;

        let device_ep = Endpoint::builder(presets::N0)
            .alpns(vec![IROH_ALPN.to_vec()])
            .bind()
            .await
            .expect("device ep");
        let device_addr = device_ep.addr();

        let cli_ep = create_cli_iroh_endpoint().await.expect("cli ep");

        // The StreamType that the CLI will send.
        let expected_stream_type = StreamType::Metrics {
            token: "tok".into(),
        };
        let expected_clone = serde_json::to_string(&expected_stream_type).unwrap();

        let device_task = tokio::spawn(async move {
            let incoming =
                tokio::time::timeout(std::time::Duration::from_secs(20), device_ep.accept())
                    .await
                    .unwrap()
                    .unwrap();
            let conn = incoming.await.unwrap();
            let (_send, mut recv) = conn.accept_bi().await.unwrap();

            // Parse the StreamType header exactly as the router does.
            let parsed = StreamType::from_incoming_stream(&mut recv)
                .await
                .expect("from_incoming_stream failed");

            let parsed_json = serde_json::to_string(&parsed).unwrap();
            assert_eq!(
                parsed_json, expected_clone,
                "parsed StreamType must match what the CLI sent"
            );

            device_ep.close().await;
        });

        let conn = tokio::time::timeout(
            std::time::Duration::from_secs(20),
            try_iroh_connect(&cli_ep, device_addr),
        )
        .await
        .unwrap()
        .unwrap();

        let _io = open_iroh_stream(&conn, expected_stream_type)
            .await
            .expect("open_iroh_stream failed");

        // Wait for the device to finish parsing before closing the endpoint.
        // Closing the endpoint first would terminate the connection and cause
        // accept_bi() on the device side to fail with ApplicationClosed.
        device_task.await.expect("device task panicked");
        cli_ep.close().await;
    }

    // ── datagram transport (used by UDP forwarding) ────────────────────

    /// Verify iroh negotiates datagram support and a datagram round-trips
    /// between two endpoints — the transport UDP forwarding relies on.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_iroh_datagram_roundtrip() {
        use iroh::Endpoint;
        use iroh::endpoint::presets;

        let device_ep = Endpoint::builder(presets::N0)
            .alpns(vec![IROH_ALPN.to_vec()])
            .bind()
            .await
            .expect("device ep");
        let device_addr = device_ep.addr();
        let cli_ep = create_cli_iroh_endpoint().await.expect("cli ep");

        let device_task = tokio::spawn(async move {
            let incoming =
                tokio::time::timeout(std::time::Duration::from_secs(20), device_ep.accept())
                    .await
                    .unwrap()
                    .unwrap();
            let conn = incoming.await.unwrap();
            // Datagrams are unreliable; the sender retries, we read the first.
            let d = tokio::time::timeout(std::time::Duration::from_secs(10), conn.read_datagram())
                .await
                .expect("datagram did not arrive")
                .expect("read_datagram failed");
            assert_eq!(&d[..], b"ping", "datagram payload must match");
            device_ep.close().await;
        });

        let conn = tokio::time::timeout(
            std::time::Duration::from_secs(20),
            try_iroh_connect(&cli_ep, device_addr),
        )
        .await
        .unwrap()
        .unwrap();

        assert!(
            conn.max_datagram_size().is_some(),
            "iroh must negotiate datagram support for UDP forwarding"
        );

        // Unreliable transport: send a few times until the device reads one.
        for _ in 0..20 {
            let _ = conn.send_datagram(bytes::Bytes::from_static(b"ping"));
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }

        device_task.await.expect("device task panicked");
        cli_ep.close().await;
    }
}
