//! Remote command execution using the /exec endpoint.
//!
//! This provides clean command output without shell noise (MOTD, prompts, logout).

use crate::{auth::AuthManager, config::Config, devices, util::shutdown::SHUTDOWN};
use anyhow::{anyhow, Result};
use futures::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use termion::raw::IntoRawMode;
use tokio::io::AsyncWriteExt;
use tokio::sync::{mpsc, Mutex};
use tokio_tungstenite::{connect_async, tungstenite::client::IntoClientRequest};

#[derive(Serialize)]
struct ExecRequest {
    command: String,
    tty: bool,
}

#[derive(Deserialize)]
struct ExecResult {
    exit_code: i32,
}

/// Run a command on a remote device.
///
/// Flags follow Docker's model:
/// - `stdin` (`-i`): Keep stdin open, forward input to remote (for prompts like Y/n)
/// - `tty` (`-t`): Allocate pseudo-TTY with raw mode (for TUI apps like vim, htop)
pub async fn run_cmd(device: &str, command: Vec<String>, stdin: bool, tty: bool) -> Result<()> {
    rustls::crypto::CryptoProvider::install_default(rustls::crypto::ring::default_provider()).ok();

    let config = Config::load()?;
    let base = config.get_server_hostname();
    let dev = devices::list_devices()
        .await?
        .into_iter()
        .find(|d| d.name == device)
        .ok_or_else(|| anyhow!("Device '{}' not found", device))?;

    // Use /exec endpoint for clean output
    let url = format!("wss://{}.{}/exec", dev.short_id, base);

    let token = AuthManager::get_cli_token().await?;
    let mut req = url.into_client_request()?;
    req.headers_mut()
        .insert("Sec-WebSocket-Protocol", format!("bearer.{token}").parse()?);

    let (ws_stream, _) = connect_async(req).await?;
    let (ws_tx, ws_rx) = ws_stream.split();

    // Join command into single string (shell will interpret operators like && |)
    let cmd_str = command.join(" ");

    match (stdin, tty) {
        (false, false) => run_output_only(ws_tx, ws_rx, cmd_str).await,
        (true, false) => run_with_stdin(ws_tx, ws_rx, cmd_str).await,
        (_, true) => run_with_tty(ws_tx, ws_rx, cmd_str).await, // tty implies stdin
    }
}

/// Try to parse exit code from a message (server sends JSON before close)
fn try_parse_exit_code(text: &str) -> Option<i32> {
    serde_json::from_str::<ExecResult>(text).ok().map(|r| r.exit_code)
}

/// No stdin, no tty: just send command config and stream output
async fn run_output_only<S, R>(mut ws_tx: S, mut ws_rx: R, cmd_str: String) -> Result<()>
where
    S: SinkExt<tokio_tungstenite::tungstenite::Message> + Unpin,
    R: StreamExt<Item = Result<tokio_tungstenite::tungstenite::Message, tokio_tungstenite::tungstenite::Error>>
        + Unpin,
{
    // Send command config
    let config = ExecRequest {
        command: cmd_str,
        tty: false,
    };
    ws_tx
        .send(tokio_tungstenite::tungstenite::Message::Text(
            serde_json::to_string(&config)?.into(),
        ))
        .await
        .map_err(|_| anyhow!("Failed to send command config"))?;

    let mut stdout = tokio::io::stdout();
    let mut exit_code = 0;

    // Stream output until connection closes or Ctrl+C
    loop {
        tokio::select! {
            _ = SHUTDOWN.cancelled() => {
                let _ = ws_tx.send(tokio_tungstenite::tungstenite::Message::Close(None)).await;
                std::process::exit(130);
            }
            msg = ws_rx.next() => {
                match msg {
                    Some(Ok(tokio_tungstenite::tungstenite::Message::Text(t))) => {
                        if let Some(code) = try_parse_exit_code(&t) {
                            exit_code = code;
                        } else {
                            stdout.write_all(t.as_bytes()).await?;
                            stdout.flush().await?;
                        }
                    }
                    Some(Ok(tokio_tungstenite::tungstenite::Message::Binary(b))) => {
                        stdout.write_all(&b).await?;
                        stdout.flush().await?;
                    }
                    Some(Ok(tokio_tungstenite::tungstenite::Message::Close(_))) => break,
                    Some(Err(_)) => break,
                    None => break,
                    _ => {}
                }
            }
        }
    }

    if exit_code != 0 {
        std::process::exit(exit_code);
    }
    Ok(())
}

/// Stdin forwarding without raw mode (line-buffered input for prompts)
async fn run_with_stdin<S, R>(ws_tx: S, ws_rx: R, cmd_str: String) -> Result<()>
where
    S: SinkExt<tokio_tungstenite::tungstenite::Message> + Unpin + Send + 'static,
    R: StreamExt<Item = Result<tokio_tungstenite::tungstenite::Message, tokio_tungstenite::tungstenite::Error>>
        + Unpin
        + Send
        + 'static,
{
    let ws_tx = Arc::new(Mutex::new(ws_tx));

    // Send command config
    {
        let config = ExecRequest {
            command: cmd_str,
            tty: false,
        };
        let mut tx = ws_tx.lock().await;
        tx.send(tokio_tungstenite::tungstenite::Message::Text(
            serde_json::to_string(&config)?.into(),
        ))
        .await
        .map_err(|_| anyhow!("Failed to send command config"))?;
    }

    let mut stdout = tokio::io::stdout();

    // Stdin reader thread (line-buffered, normal terminal mode)
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

    // Stdin -> WebSocket task
    let ws_tx_stdin = ws_tx.clone();
    let stdin_task = tokio::spawn(async move {
        while let Some(bytes) = stdin_rx.recv().await {
            if bytes.is_empty() {
                let mut tx = ws_tx_stdin.lock().await;
                let _ = tx
                    .send(tokio_tungstenite::tungstenite::Message::Close(None))
                    .await;
                break;
            }
            let mut tx = ws_tx_stdin.lock().await;
            let _ = tx
                .send(tokio_tungstenite::tungstenite::Message::Binary(bytes.into()))
                .await;
        }
    });

    // WebSocket -> Stdout (main task) with Ctrl+C handling
    let mut ws_rx = ws_rx;
    let mut exit_code = 0;
    loop {
        tokio::select! {
            _ = SHUTDOWN.cancelled() => {
                let mut tx = ws_tx.lock().await;
                let _ = tx.send(tokio_tungstenite::tungstenite::Message::Close(None)).await;
                std::process::exit(130);
            }
            msg = ws_rx.next() => {
                match msg {
                    Some(Ok(tokio_tungstenite::tungstenite::Message::Text(t))) => {
                        if let Some(code) = try_parse_exit_code(&t) {
                            exit_code = code;
                        } else {
                            stdout.write_all(t.as_bytes()).await?;
                            stdout.flush().await?;
                        }
                    }
                    Some(Ok(tokio_tungstenite::tungstenite::Message::Binary(b))) => {
                        stdout.write_all(&b).await?;
                        stdout.flush().await?;
                    }
                    Some(Ok(tokio_tungstenite::tungstenite::Message::Close(_))) => break,
                    Some(Err(_)) => break,
                    None => break,
                    _ => {}
                }
            }
        }
    }

    stdin_task.abort();

    if exit_code != 0 {
        std::process::exit(exit_code);
    }
    Ok(())
}

/// Full TTY mode: raw terminal, bidirectional stdin/stdout (for vim, htop, etc.)
async fn run_with_tty<S, R>(ws_tx: S, ws_rx: R, cmd_str: String) -> Result<()>
where
    S: SinkExt<tokio_tungstenite::tungstenite::Message> + Unpin + Send + 'static,
    R: StreamExt<Item = Result<tokio_tungstenite::tungstenite::Message, tokio_tungstenite::tungstenite::Error>>
        + Unpin
        + Send
        + 'static,
{
    let ws_tx = Arc::new(Mutex::new(ws_tx));

    // Enter raw mode
    let raw_mode = std::io::stdout().into_raw_mode()?;
    let mut stdout = tokio::io::stdout();

    // Send command config with tty: true
    {
        let config = ExecRequest {
            command: cmd_str,
            tty: true,
        };
        let mut tx = ws_tx.lock().await;
        tx.send(tokio_tungstenite::tungstenite::Message::Text(
            serde_json::to_string(&config)?.into(),
        ))
        .await
        .map_err(|_| anyhow!("Failed to send command config"))?;
    }

    // Stdin reader thread (raw mode - every keystroke sent immediately)
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

    // Stdin -> WebSocket task
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
            let _ = tx
                .send(tokio_tungstenite::tungstenite::Message::Binary(bytes.into()))
                .await;
        }
        Ok::<_, anyhow::Error>(())
    });

    // WebSocket -> Stdout task
    let ws_tx_close = ws_tx.clone();
    let mut ws_rx = ws_rx;
    let exit_code = Arc::new(Mutex::new(0i32));
    let exit_code_ws = exit_code.clone();

    let mut ws_task = tokio::spawn(async move {
        while let Some(m) = ws_rx.next().await {
            let m = m?;
            match m {
                tokio_tungstenite::tungstenite::Message::Binary(d) => {
                    stdout.write_all(&d).await?;
                    stdout.flush().await?;
                }
                tokio_tungstenite::tungstenite::Message::Text(t) => {
                    if let Some(code) = try_parse_exit_code(&t) {
                        *exit_code_ws.lock().await = code;
                    } else {
                        stdout.write_all(t.as_bytes()).await?;
                        stdout.flush().await?;
                    }
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

    // Wait for either task to complete or shutdown signal
    let final_code;
    tokio::select! {
        _ = SHUTDOWN.cancelled() => {
            ws_task.abort();
            stdin_task.abort();
            final_code = 130;
        }
        _ = &mut ws_task => {
            stdin_task.abort();
            final_code = *exit_code.lock().await;
        }
        _ = &mut stdin_task => {
            let mut tx = ws_tx.lock().await;
            let _ = tx.send(tokio_tungstenite::tungstenite::Message::Close(None)).await;
            final_code = *exit_code.lock().await;
        }
    }

    drop(raw_mode);

    if final_code != 0 {
        std::process::exit(final_code);
    }
    Ok(())
}
