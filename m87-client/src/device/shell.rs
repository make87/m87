use crate::{auth::AuthManager, config::Config, devices};
use anyhow::{Context, Result};
use futures::{SinkExt, StreamExt};
use termion::raw::IntoRawMode;
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc;
use tokio_tungstenite::{connect_async, tungstenite::client::IntoClientRequest};

pub async fn run_shell(device: &str) -> Result<()> {
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
        .ok_or_else(|| anyhow::anyhow!("Device '{}' not found", device))?;
    let url = format!("wss://{}.{}{}", dev.short_id, base, "/terminal");

    println!("Connecting to shell on {} ...", device);

    let token = AuthManager::get_cli_token().await?;
    let mut req = url.into_client_request()?;
    req.headers_mut()
        .insert("Sec-WebSocket-Protocol", format!("bearer.{token}").parse()?);

    let (ws_stream, _) = connect_async(req).await?;
    let (ws_tx, ws_rx) = ws_stream.split();

    let ws_tx = std::sync::Arc::new(tokio::sync::Mutex::new(ws_tx));

    let raw_mode = std::io::stdout().into_raw_mode()?;
    let mut stdout = tokio::io::stdout();

    println!("Connected. Press Ctrl+C to exit.\n\r");

    let (stdin_tx, mut stdin_rx) = mpsc::unbounded_channel::<Vec<u8>>();

    std::thread::spawn(move || {
        use std::io::Read;
        let mut stdin = std::io::stdin();
        let mut buf = [0u8; 1024];
        loop {
            match stdin.read(&mut buf) {
                Ok(0) => {
                    let _ = stdin_tx.send(Vec::new());
                    break;
                }
                Ok(n) => {
                    let _ = stdin_tx.send(buf[..n].to_vec());
                }
                Err(_) => break,
            }
        }
    });

    let ws_tx_stdin = ws_tx.clone();
    let mut stdin_task = tokio::spawn(async move {
        while let Some(bytes) = stdin_rx.recv().await {
            if bytes.is_empty() {
                let mut tx = ws_tx_stdin.lock().await;
                let _ = tx
                    .send(tokio_tungstenite::tungstenite::Message::Close(None))
                    .await;
                break;
            }
            let mut tx = ws_tx_stdin.lock().await;
            tx.send(tokio_tungstenite::tungstenite::Message::Binary(bytes))
                .await?;
        }
        Ok::<_, anyhow::Error>(())
    });

    let ws_tx_close = ws_tx.clone();
    let mut ws_rx = ws_rx;
    let mut ws_task = tokio::spawn(async move {
        while let Some(m) = ws_rx.next().await {
            let m = m?;
            match m {
                tokio_tungstenite::tungstenite::Message::Binary(d) => {
                    stdout.write_all(&d).await?;
                    stdout.flush().await?;
                }
                tokio_tungstenite::tungstenite::Message::Text(t) => {
                    stdout.write_all(t.as_bytes()).await?;
                    stdout.flush().await?;
                }
                tokio_tungstenite::tungstenite::Message::Close(_) => break,
                _ => {}
            }
        }
        let mut tx = ws_tx_close.lock().await;
        let _ = tx
            .send(tokio_tungstenite::tungstenite::Message::Close(None))
            .await;
        Ok::<_, anyhow::Error>(())
    });

    tokio::select! {
        _ = &mut ws_task => stdin_task.abort(),
        _ = &mut stdin_task => {
            let mut tx = ws_tx.lock().await;
            let _ = tx.send(tokio_tungstenite::tungstenite::Message::Close(None)).await;
        }
    }

    drop(raw_mode);
    Ok(())
}
