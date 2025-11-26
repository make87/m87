use crate::{auth::AuthManager, config::Config, devices};
use anyhow::{anyhow, Result};
use futures::{SinkExt, StreamExt};
use std::sync::Arc;
use termion::raw::IntoRawMode;
use tokio::io::AsyncWriteExt;
use tokio::sync::{mpsc, Mutex};
use tokio_tungstenite::{connect_async, tungstenite::client::IntoClientRequest};

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

    let url = format!("wss://{}.{}/terminal", dev.short_id, base);

    let token = AuthManager::get_cli_token().await?;
    let mut req = url.into_client_request()?;
    req.headers_mut()
        .insert("Sec-WebSocket-Protocol", format!("bearer.{token}").parse()?);

    let (ws_stream, _) = connect_async(req).await?;
    let (ws_tx, mut ws_rx) = ws_stream.split();

    // Build command string - run command then exit with its exit code
    let cmd_str = format!("{}; exit $?\n", shell_escape(&command));

    // Wait for shell to be ready (skip init messages)
    loop {
        match ws_rx.next().await {
            Some(Ok(tokio_tungstenite::tungstenite::Message::Text(t))) => {
                if t.contains("Shell connected") {
                    break;
                }
            }
            Some(Err(e)) => return Err(anyhow!("WebSocket error during init: {}", e)),
            None => return Err(anyhow!("WebSocket closed during init")),
            _ => {}
        }
    }

    match (stdin, tty) {
        (false, false) => run_output_only(ws_tx, ws_rx, cmd_str).await,
        (true, false) => run_with_stdin(ws_tx, ws_rx, cmd_str).await,
        (_, true) => run_with_tty(ws_tx, ws_rx, cmd_str).await, // tty implies stdin
    }
}

/// No stdin, no tty: just send command and stream output
async fn run_output_only<S, R>(mut ws_tx: S, mut ws_rx: R, cmd_str: String) -> Result<()>
where
    S: SinkExt<tokio_tungstenite::tungstenite::Message> + Unpin,
    R: StreamExt<Item = Result<tokio_tungstenite::tungstenite::Message, tokio_tungstenite::tungstenite::Error>>
        + Unpin,
{
    // Send the command
    ws_tx
        .send(tokio_tungstenite::tungstenite::Message::Text(cmd_str.into()))
        .await
        .map_err(|_| anyhow!("Failed to send command"))?;

    let mut stdout = tokio::io::stdout();

    // Stream output until connection closes
    while let Some(msg) = ws_rx.next().await {
        match msg {
            Ok(tokio_tungstenite::tungstenite::Message::Text(t)) => {
                stdout.write_all(t.as_bytes()).await?;
                stdout.flush().await?;
            }
            Ok(tokio_tungstenite::tungstenite::Message::Binary(b)) => {
                stdout.write_all(&b).await?;
                stdout.flush().await?;
            }
            Ok(tokio_tungstenite::tungstenite::Message::Close(_)) => break,
            Err(_) => break,
            _ => {}
        }
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

    // Send the command first
    {
        let mut tx = ws_tx.lock().await;
        tx.send(tokio_tungstenite::tungstenite::Message::Text(cmd_str.into()))
            .await
            .map_err(|_| anyhow!("Failed to send command"))?;
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

    // WebSocket -> Stdout (main task)
    let mut ws_rx = ws_rx;
    loop {
        match ws_rx.next().await {
            Some(Ok(tokio_tungstenite::tungstenite::Message::Text(t))) => {
                stdout.write_all(t.as_bytes()).await?;
                stdout.flush().await?;
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

    stdin_task.abort();
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

    // Send the command first
    {
        let mut tx = ws_tx.lock().await;
        tx.send(tokio_tungstenite::tungstenite::Message::Text(cmd_str.into()))
            .await
            .map_err(|_| anyhow!("Failed to send command"))?;
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

    // Wait for either task to complete
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

/// Join command arguments for shell execution.
/// No escaping - let the remote shell interpret operators like && || ; |
fn shell_escape(args: &[String]) -> String {
    args.join(" ")
}
