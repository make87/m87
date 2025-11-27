use anyhow::{anyhow, Result};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_rustls::client::TlsStream;

use crate::util::tls::get_tls_connection;

/// Opens a raw upgraded byte stream.
/// Returns the TLS stream directly.
pub async fn open_raw_io(
    host: &str,
    device_short_id: &str,
    path: &str,
    token: &str,
    trust_invalid_server_cert: bool,
) -> Result<TlsStream<TcpStream>> {
    let full_host = format!("{}.{}", device_short_id, host);
    let full_path = path;

    let mut tls = get_tls_connection(full_host.clone(), trust_invalid_server_cert).await?;

    // --- Send upgrade request ---
    let req = format!(
        "GET {full_path} HTTP/1.1\r\n\
         Host: {full_host}\r\n\
         Connection: Upgrade\r\n\
         Upgrade: raw\r\n\
         Sec-WebSocket-Protocol: bearer.{token}\r\n\
         Content-Length: 0\r\n\
         \r\n"
    );

    tls.write_all(req.as_bytes()).await?;
    tls.flush().await?;

    // --- Read header ---
    let mut header = Vec::new();
    let mut byte = [0u8; 1];

    while !header.ends_with(b"\r\n\r\n") {
        let n = tls.read(&mut byte).await?;
        if n == 0 {
            return Err(anyhow!("server closed before upgrade"));
        }
        header.push(byte[0]);
        if header.len() > 8192 {
            return Err(anyhow!("response header too large"));
        }
    }

    let header_str = String::from_utf8_lossy(&header);
    if !header_str.starts_with("HTTP/1.1 101") {
        return Err(anyhow!("upgrade failed: {}", header_str));
    }

    // --- Now tls is a raw bidirectional byte stream ---
    Ok(tls)
}
