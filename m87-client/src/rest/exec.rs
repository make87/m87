//! WebSocket endpoint for clean command execution.
//!
//! Unlike `/terminal` which spawns an interactive login shell,
//! this endpoint runs commands directly via `$SHELL -c "command"`,
//! producing clean output without MOTD, prompts, or logout messages.

use axum::extract::ws::{Message, Utf8Bytes, WebSocket};
use futures::{SinkExt, StreamExt};
use portable_pty::{native_pty_system, CommandBuilder, PtySize};
use serde::{Deserialize, Serialize};
use std::io::{Read, Write};
use std::process::Stdio;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::process::Command;
use tokio::sync::{mpsc, Mutex};
use tokio::{select, time::Duration};

#[derive(Deserialize)]
struct ExecRequest {
    command: String,
    #[serde(default)]
    tty: bool,
}

#[derive(Serialize)]
struct ExecResult {
    exit_code: i32,
}

/// Get the user's shell, falling back to /bin/sh
fn get_shell() -> String {
    std::env::var("SHELL").unwrap_or_else(|_| "/bin/sh".to_string())
}

pub async fn handle_exec_ws(socket: WebSocket) {
    let (mut ws_tx, mut ws_rx) = socket.split();

    // Wait for command config
    let config: ExecRequest = match ws_rx.next().await {
        Some(Ok(Message::Text(t))) => match serde_json::from_str(&t) {
            Ok(c) => c,
            Err(e) => {
                let _ = ws_tx
                    .send(Message::Text(
                        format!("Invalid request: {e}\n").into(),
                    ))
                    .await;
                return;
            }
        },
        Some(Ok(Message::Close(_))) | None => return,
        _ => {
            let _ = ws_tx
                .send(Message::Text("Expected JSON config message\n".into()))
                .await;
            return;
        }
    };

    if config.tty {
        run_with_pty(ws_tx, ws_rx, config).await;
    } else {
        run_piped(ws_tx, ws_rx, config).await;
    }
}

/// Run command with piped stdio (no PTY) - for simple commands and -i mode
async fn run_piped<S, R>(mut ws_tx: S, mut ws_rx: R, config: ExecRequest)
where
    S: SinkExt<Message> + Unpin + Send + 'static,
    R: StreamExt<Item = Result<Message, axum::Error>> + Unpin + Send + 'static,
{
    let shell = get_shell();

    let mut child = match Command::new(&shell)
        .arg("-c")
        .arg(&config.command)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
    {
        Ok(c) => c,
        Err(e) => {
            let _ = ws_tx
                .send(Message::Text(format!("Failed to spawn command: {e}\n").into()))
                .await;
            return;
        }
    };

    let stdin = child.stdin.take();
    let stdout = child.stdout.take().unwrap();
    let stderr = child.stderr.take().unwrap();

    // Channel for collecting output to send via WebSocket
    let (out_tx, mut out_rx) = mpsc::unbounded_channel::<Vec<u8>>();

    // Stdout reader task
    let out_tx_stdout = out_tx.clone();
    let stdout_task = tokio::spawn(async move {
        let mut stdout = stdout;
        let mut buf = [0u8; 4096];
        loop {
            match stdout.read(&mut buf).await {
                Ok(0) => break,
                Ok(n) => {
                    let _ = out_tx_stdout.send(buf[..n].to_vec());
                }
                Err(_) => break,
            }
        }
    });

    // Stderr reader task
    let out_tx_stderr = out_tx.clone();
    let stderr_task = tokio::spawn(async move {
        let mut stderr = stderr;
        let mut buf = [0u8; 4096];
        loop {
            match stderr.read(&mut buf).await {
                Ok(0) => break,
                Ok(n) => {
                    let _ = out_tx_stderr.send(buf[..n].to_vec());
                }
                Err(_) => break,
            }
        }
    });

    // Stdin writer task (forwards WebSocket input to child stdin)
    let stdin_task = if let Some(mut stdin) = stdin {
        Some(tokio::spawn(async move {
            while let Some(Ok(msg)) = ws_rx.next().await {
                match msg {
                    Message::Text(t) => {
                        if stdin.write_all(t.as_bytes()).await.is_err() {
                            break;
                        }
                        let _ = stdin.flush().await;
                    }
                    Message::Binary(b) => {
                        if stdin.write_all(&b).await.is_err() {
                            break;
                        }
                        let _ = stdin.flush().await;
                    }
                    Message::Close(_) => {
                        // Close stdin to signal EOF to child
                        drop(stdin);
                        break;
                    }
                    _ => {}
                }
            }
        }))
    } else {
        None
    };

    // Main loop: forward output to WebSocket
    let ws_tx = Arc::new(Mutex::new(ws_tx));
    let ws_tx_output = ws_tx.clone();

    let output_task = tokio::spawn(async move {
        while let Some(data) = out_rx.recv().await {
            let text = String::from_utf8_lossy(&data).to_string();
            let mut tx = ws_tx_output.lock().await;
            if tx.send(Message::Text(text.into())).await.is_err() {
                break;
            }
        }
    });

    // Wait for child to exit
    let status = child.wait().await;

    // Clean up tasks
    stdout_task.abort();
    stderr_task.abort();
    output_task.abort();
    if let Some(task) = stdin_task {
        task.abort();
    }

    // Send exit code
    let exit_code = status
        .ok()
        .and_then(|s| s.code())
        .unwrap_or(-1);

    let result = ExecResult { exit_code };
    let mut tx = ws_tx.lock().await;
    let _ = tx
        .send(Message::Text(serde_json::to_string(&result).unwrap().into()))
        .await;
    let _ = tx.send(Message::Close(None)).await;
}

/// Run command with PTY - for TUI applications (vim, htop, etc.)
async fn run_with_pty<S, R>(mut ws_tx: S, mut ws_rx: R, config: ExecRequest)
where
    S: SinkExt<Message> + Unpin + Send + 'static,
    R: StreamExt<Item = Result<Message, axum::Error>> + Unpin + Send + 'static,
{
    let shell = get_shell();

    // Create PTY
    let pty_system = native_pty_system();
    let pair = match pty_system.openpty(PtySize {
        rows: 24,
        cols: 80,
        pixel_width: 0,
        pixel_height: 0,
    }) {
        Ok(p) => p,
        Err(e) => {
            let _ = ws_tx
                .send(Message::Text(format!("Failed to create PTY: {e}\n").into()))
                .await;
            return;
        }
    };

    // Spawn command in PTY (via shell -c, not interactive shell)
    let mut cmd = CommandBuilder::new(&shell);
    cmd.args(&["-c", &config.command]);
    cmd.env("TERM", "xterm-256color");

    let mut child = match pair.slave.spawn_command(cmd) {
        Ok(c) => c,
        Err(e) => {
            let _ = ws_tx
                .send(Message::Text(format!("Failed to spawn command: {e}\n").into()))
                .await;
            return;
        }
    };

    // Get PTY master reader/writer
    let reader = match pair.master.try_clone_reader() {
        Ok(r) => r,
        Err(e) => {
            let _ = ws_tx
                .send(Message::Text(format!("Failed to get PTY reader: {e}\n").into()))
                .await;
            let _ = child.kill();
            return;
        }
    };
    let writer = match pair.master.take_writer() {
        Ok(w) => w,
        Err(e) => {
            let _ = ws_tx
                .send(Message::Text(format!("Failed to get PTY writer: {e}\n").into()))
                .await;
            let _ = child.kill();
            return;
        }
    };
    let writer = Arc::new(Mutex::new(writer));

    // PTY -> WebSocket reader task
    let (tx, mut rx) = mpsc::unbounded_channel::<Vec<u8>>();
    tokio::task::spawn_blocking(move || {
        let mut reader = reader;
        let mut buf = [0u8; 4096];
        loop {
            match reader.read(&mut buf) {
                Ok(0) => break,
                Ok(n) => {
                    let _ = tx.send(buf[..n].to_vec());
                }
                Err(_) => break,
            }
        }
    });

    let ws_tx = Arc::new(Mutex::new(ws_tx));
    let ws_tx_output = ws_tx.clone();

    // Main loop
    'outer: loop {
        select! {
            // WebSocket -> PTY
            Some(Ok(msg)) = ws_rx.next() => {
                match msg {
                    Message::Text(text) => {
                        let data = text.to_string();
                        let writer = Arc::clone(&writer);
                        if tokio::task::spawn_blocking(move || {
                            let mut w = writer.blocking_lock();
                            w.write_all(data.as_bytes())?;
                            w.flush()
                        }).await.is_err() {
                            break 'outer;
                        }
                    }
                    Message::Binary(bin) => {
                        let data = bin.to_vec();
                        let writer = Arc::clone(&writer);
                        if tokio::task::spawn_blocking(move || {
                            let mut w = writer.blocking_lock();
                            w.write_all(&data)?;
                            w.flush()
                        }).await.is_err() {
                            break 'outer;
                        }
                    }
                    Message::Close(_) => break 'outer,
                    _ => {}
                }
            }

            // PTY -> WebSocket
            Some(out) = rx.recv() => {
                let text = String::from_utf8_lossy(&out).to_string();
                let mut tx = ws_tx_output.lock().await;
                if tx.send(Message::Text(Utf8Bytes::from(text))).await.is_err() {
                    break 'outer;
                }
            }

            // Check if child exited
            _ = tokio::time::sleep(Duration::from_millis(50)) => {
                if let Some(status) = child.try_wait().unwrap_or(None) {
                    // Send exit code
                    let exit_code = status.exit_code() as i32;
                    let result = ExecResult { exit_code };
                    let mut tx = ws_tx.lock().await;
                    let _ = tx
                        .send(Message::Text(serde_json::to_string(&result).unwrap().into()))
                        .await;
                    break 'outer;
                }
            }

            else => break 'outer,
        }
    }

    // Cleanup
    let _ = child.kill();
    let mut tx = ws_tx.lock().await;
    let _ = tx.send(Message::Close(None)).await;
}
