use crate::{auth::AuthManager, config::Config, devices};
use anyhow::{anyhow, Result};
use futures::{SinkExt, StreamExt};
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc;
use tokio_tungstenite::{connect_async, tungstenite::client::IntoClientRequest};

pub async fn run_logs(device: &str) -> Result<()> {
    rustls::crypto::CryptoProvider::install_default(rustls::crypto::ring::default_provider())
        .unwrap();

    let config = Config::load()?;
    let server_url = config.get_server_url();
    let base = server_url
        .trim_start_matches("https://")
        .trim_start_matches("http://");

    let dev = devices::list_devices()
        .await?
        .into_iter()
        .find(|d| d.name == device)
        .ok_or_else(|| anyhow!("Device '{}' not found", device))?;

    let url = format!("wss://{}.{}{}", dev.short_id, base, "/logs");

    println!("Connecting to logs of {} ...", device);

    let token = AuthManager::get_cli_token().await?;
    let mut req = url.into_client_request()?;
    req.headers_mut()
        .insert("Sec-WebSocket-Protocol", format!("bearer.{token}").parse()?);

    let (ws_stream, _) = connect_async(req).await?;
    let (mut ws_tx, mut ws_rx) = ws_stream.split();

    let mut stdout = tokio::io::stdout();

    println!("Connected. Press Ctrl+C to exit.\n");

    // Channel to catch Ctrl+C / stdin EOF
    let (stdin_tx, mut stdin_rx) = mpsc::unbounded_channel::<()>();

    // Thread watching stdin for Ctrl+C / EOF
    std::thread::spawn(move || {
        use std::io::Read;
        let mut buf = [0u8; 1];
        let mut stdin = std::io::stdin();

        loop {
            match stdin.read(&mut buf) {
                Ok(0) => {
                    let _ = stdin_tx.send(());
                    break;
                }
                Ok(_) => {}
                Err(_) => break,
            }
        }
    });

    // WS → stdout task
    let mut ws_task = tokio::spawn(async move {
        while let Some(msg) = ws_rx.next().await {
            let msg = match msg {
                Ok(m) => m,
                Err(_) => break,
            };

            match msg {
                tokio_tungstenite::tungstenite::Message::Text(t) => {
                    stdout.write_all(t.as_bytes()).await?;
                    stdout.write_all(b"\n").await?;
                    stdout.flush().await?;
                }
                tokio_tungstenite::tungstenite::Message::Binary(b) => {
                    stdout.write_all(&b).await?;
                    stdout.write_all(b"\n").await?;
                    stdout.flush().await?;
                }
                tokio_tungstenite::tungstenite::Message::Close(_) => break,
                _ => {}
            }
        }
        Ok::<_, anyhow::Error>(())
    });

    // Exit on Ctrl+C or WS
    tokio::select! {
        _ = &mut ws_task => {
            // logs stream ended
        }
        _ = stdin_rx.recv() => {
            // user pressed Ctrl+C or stdin closed
            let _ = ws_tx.send(
                tokio_tungstenite::tungstenite::Message::Close(None)
            ).await;
        }
    }

    println!("\nLogs stream closed.");
    Ok(())
}
