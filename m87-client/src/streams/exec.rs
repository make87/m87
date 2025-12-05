//! Raw TCP/TLS endpoint for clean command execution.
//!
//! Unlike `/terminal` which spawns an interactive login shell,
//! this endpoint runs commands directly via `$SHELL -c "command"`,
//! producing clean output without MOTD, prompts, or logout messages.
//!
//! Protocol:
//! 1. Client sends JSON config line: {"command":"...", "tty":false}\n
//! 2. Bidirectional raw bytes for stdin/stdout
//! 3. Server sends exit code JSON before closing: {"exit_code":N}\n

use portable_pty::{CommandBuilder, PtySize, native_pty_system};
use serde::{Deserialize, Serialize};
use std::io::{Read, Write};
use std::process::Stdio;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::process::Command;
use tokio::sync::{Mutex, mpsc};
use tokio::{select, time::Duration};

use crate::streams::quic::QuicIo;

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

pub async fn handle_exec_io(io: QuicIo) {
    // Split into reader/writer
    let (reader, writer) = tokio::io::split(io);
    let mut reader = BufReader::new(reader);
    let writer = Arc::new(Mutex::new(writer));

    // Read first line as JSON config
    let mut config_line = String::new();
    if reader.read_line(&mut config_line).await.is_err() {
        return;
    }

    let config: ExecRequest = match serde_json::from_str(config_line.trim()) {
        Ok(c) => c,
        Err(e) => {
            let mut w = writer.lock().await;
            let _ = w
                .write_all(format!("Invalid request: {e}\n").as_bytes())
                .await;
            return;
        }
    };

    if config.tty {
        run_with_pty(reader, writer, config).await;
    } else {
        run_piped(reader, writer, config).await;
    }
}

/// Run command with piped stdio (no PTY) - for simple commands and -i mode
async fn run_piped<R, W>(mut reader: R, writer: Arc<Mutex<W>>, config: ExecRequest)
where
    R: AsyncReadExt + Unpin + Send + 'static,
    W: AsyncWriteExt + Unpin + Send + 'static,
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
            let mut w = writer.lock().await;
            let _ = w
                .write_all(format!("Failed to spawn command: {e}\n").as_bytes())
                .await;
            return;
        }
    };

    let stdin = child.stdin.take();
    let stdout = child.stdout.take().unwrap();
    let stderr = child.stderr.take().unwrap();

    // Channel for collecting output
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

    // Stdin writer task (forwards client input to child stdin)
    let stdin_task = if let Some(mut stdin) = stdin {
        Some(tokio::spawn(async move {
            let mut buf = [0u8; 4096];
            loop {
                match reader.read(&mut buf).await {
                    Ok(0) => {
                        // EOF - close stdin to signal end to child
                        drop(stdin);
                        break;
                    }
                    Ok(n) => {
                        if stdin.write_all(&buf[..n]).await.is_err() {
                            break;
                        }
                        let _ = stdin.flush().await;
                    }
                    Err(_) => break,
                }
            }
        }))
    } else {
        None
    };

    // Output forwarding task
    let writer_output = writer.clone();
    let output_task = tokio::spawn(async move {
        while let Some(data) = out_rx.recv().await {
            let mut w = writer_output.lock().await;
            if w.write_all(&data).await.is_err() {
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
    let exit_code = status.ok().and_then(|s| s.code()).unwrap_or(-1);
    let result = ExecResult { exit_code };
    let mut w = writer.lock().await;
    let _ = w
        .write_all(format!("{}\n", serde_json::to_string(&result).unwrap()).as_bytes())
        .await;
    let _ = w.shutdown().await;
}

/// Run command with PTY - for TUI applications (vim, htop, etc.)
async fn run_with_pty<R, W>(mut reader: R, writer: Arc<Mutex<W>>, config: ExecRequest)
where
    R: AsyncReadExt + Unpin + Send + 'static,
    W: AsyncWriteExt + Unpin + Send + 'static,
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
            let mut w = writer.lock().await;
            let _ = w
                .write_all(format!("Failed to create PTY: {e}\n").as_bytes())
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
            let mut w = writer.lock().await;
            let _ = w
                .write_all(format!("Failed to spawn command: {e}\n").as_bytes())
                .await;
            return;
        }
    };

    // Get PTY master reader/writer
    let pty_reader = match pair.master.try_clone_reader() {
        Ok(r) => r,
        Err(e) => {
            let mut w = writer.lock().await;
            let _ = w
                .write_all(format!("Failed to get PTY reader: {e}\n").as_bytes())
                .await;
            let _ = child.kill();
            return;
        }
    };
    let pty_writer = match pair.master.take_writer() {
        Ok(w) => w,
        Err(e) => {
            let mut w = writer.lock().await;
            let _ = w
                .write_all(format!("Failed to get PTY writer: {e}\n").as_bytes())
                .await;
            let _ = child.kill();
            return;
        }
    };
    let pty_writer = Arc::new(Mutex::new(pty_writer));

    // PTY -> channel (blocking reader thread)
    let (pty_tx, mut pty_rx) = mpsc::unbounded_channel::<Vec<u8>>();
    tokio::task::spawn_blocking(move || {
        let mut pty_reader = pty_reader;
        let mut buf = [0u8; 4096];
        loop {
            match pty_reader.read(&mut buf) {
                Ok(0) => break,
                Ok(n) => {
                    let _ = pty_tx.send(buf[..n].to_vec());
                }
                Err(_) => break,
            }
        }
    });

    // Main loop
    let mut read_buf = [0u8; 4096];
    'outer: loop {
        select! {
            // Client -> PTY
            r = reader.read(&mut read_buf) => {
                match r {
                    Ok(0) => break 'outer,
                    Ok(n) => {
                        let data = read_buf[..n].to_vec();
                        let pty_w = pty_writer.clone();
                        if tokio::task::spawn_blocking(move || {
                            let mut w = pty_w.blocking_lock();
                            w.write_all(&data)?;
                            w.flush()
                        }).await.is_err() {
                            break 'outer;
                        }
                    }
                    Err(_) => break 'outer,
                }
            }

            // PTY -> Client
            Some(out) = pty_rx.recv() => {
                let mut w = writer.lock().await;
                if w.write_all(&out).await.is_err() {
                    break 'outer;
                }
            }

            // Check if child exited
            _ = tokio::time::sleep(Duration::from_millis(50)) => {
                if let Some(status) = child.try_wait().unwrap_or(None) {
                    // Send exit code
                    let exit_code = status.exit_code() as i32;
                    let result = ExecResult { exit_code };
                    let mut w = writer.lock().await;
                    let _ = w
                        .write_all(format!("{}\n", serde_json::to_string(&result).unwrap()).as_bytes())
                        .await;
                    break 'outer;
                }
            }

            else => break 'outer,
        }
    }

    // Cleanup
    let _ = child.kill();
    let mut w = writer.lock().await;
    let _ = w.shutdown().await;
}
