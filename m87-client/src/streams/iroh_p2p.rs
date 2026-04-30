//! iroh-based direct P2P connections.
//!
//! The CLI tries a direct iroh connection to the device before falling back to
//! the server relay path. The device advertises its iroh [`EndpointAddr`] via
//! the heartbeat; the server caches it and exposes it via
//! `GET /device/{id}/iroh-addr`.

use anyhow::{Context, Result};
use iroh::endpoint::Connection as IrohConnection;
use iroh::{Endpoint as IrohEndpoint, EndpointAddr};
use serde::Deserialize;
use std::time::Duration;
use tracing::{debug, warn};

use crate::streams::quic::QuicIo;
use crate::streams::stream_type::StreamType;

/// ALPN used for direct iroh P2P connections between CLI and device.
pub const IROH_ALPN: &[u8] = b"m87-iroh-p2p/1";

/// Timeout when attempting an iroh P2P connection.
const IROH_CONNECT_TIMEOUT: Duration = Duration::from_secs(15);

/// Response body from `GET /device/{id}/iroh-addr`.
#[derive(Deserialize)]
struct IrohAddrResponse {
    iroh_node_addr: String,
}

/// Fetch the iroh [`EndpointAddr`] for a device from the m87 server.
///
/// `server_url` is the HTTPS base URL, `token` is the user bearer token,
/// `device_id` is the MongoDB ObjectId string of the device.
pub async fn fetch_device_iroh_addr(
    server_url: &str,
    token: &str,
    device_id: &str,
) -> Result<EndpointAddr> {
    let url = format!(
        "{}/device/{}/iroh-addr",
        server_url.trim_end_matches('/'),
        device_id
    );

    let resp = reqwest::Client::new()
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

    Ok(addr)
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

/// High-level helper: fetch iroh addr from server, try direct connection,
/// open stream. Returns `None` if anything fails (caller falls back to relay).
pub async fn try_open_iroh_stream(
    server_url: &str,
    token: &str,
    device_id: &str,
    stream_type: StreamType,
) -> Option<(IrohConnection, QuicIo)> {
    let addr = match fetch_device_iroh_addr(server_url, token, device_id).await {
        Ok(a) => a,
        Err(e) => {
            debug!("iroh: could not get device addr: {e}");
            return None;
        }
    };

    let ep = match create_cli_iroh_endpoint().await {
        Ok(e) => e,
        Err(e) => {
            warn!("iroh: could not create endpoint: {e}");
            return None;
        }
    };

    let conn = match try_iroh_connect(&ep, addr).await {
        Ok(c) => c,
        Err(e) => {
            debug!("iroh: direct connect failed (will use relay): {e}");
            return None;
        }
    };

    match open_iroh_stream(&conn, stream_type).await {
        Ok(io) => Some((conn, io)),
        Err(e) => {
            warn!("iroh: stream open failed: {e}");
            None
        }
    }
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
}
