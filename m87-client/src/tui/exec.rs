//! Remote command execution using the /exec endpoint.
//!
//! This provides clean command output without shell noise (MOTD, prompts, logout).

use crate::streams::quic::open_quic_io;
use crate::streams::stream_type::StreamType;
use crate::{auth::AuthManager, config::Config, devices, util::shutdown::SHUTDOWN};
use anyhow::{Context, Result, anyhow};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use termion::raw::IntoRawMode;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::sync::{Mutex, mpsc};

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
pub async fn run_exec(device: &str, command: Vec<String>, stdin: bool, tty: bool) -> Result<()> {
    rustls::crypto::CryptoProvider::install_default(rustls::crypto::ring::default_provider()).ok();

    let config = Config::load()?;
    let base = config.get_server_hostname();
    let dev = devices::list_devices()
        .await?
        .into_iter()
        .find(|d| d.name == device)
        .ok_or_else(|| anyhow!("Device '{}' not found", device))?;

    let token = AuthManager::get_cli_token().await?;

    let stream_type = StreamType::Exec {
        token: token.to_string(),
    };
    let (_, io) = open_quic_io(
        &base,
        &dev.short_id,
        stream_type,
        config.trust_invalid_server_cert,
    )
    .await
    .context("Failed to connect to RAW metrics stream")?;

    // Join command into single string (shell will interpret operators like && |)
    let cmd_str = command.join(" ");

    match (stdin, tty) {
        (false, false) => run_output_only(io, cmd_str).await,
        (true, false) => run_with_stdin(io, cmd_str).await,
        (false, true) => run_tty_readonly(io, cmd_str).await,
        (true, true) => run_with_tty(io, cmd_str).await,
    }
}

/// Try to parse exit code from a line (server sends JSON before close)
fn try_parse_exit_code(line: &str) -> Option<i32> {
    serde_json::from_str::<ExecResult>(line.trim())
        .ok()
        .map(|r| r.exit_code)
}

/// No stdin, no tty: just send command config and stream output
async fn run_output_only<IO>(io: IO, cmd_str: String) -> Result<()>
where
    IO: AsyncReadExt + AsyncWriteExt + Unpin + Send + 'static,
{
    let (reader, mut writer) = tokio::io::split(io);
    let mut reader = BufReader::new(reader);

    // Send command config as JSON line
    let config = ExecRequest {
        command: cmd_str,
        tty: false,
    };
    writer
        .write_all(format!("{}\n", serde_json::to_string(&config)?).as_bytes())
        .await?;
    writer.flush().await?;

    let mut stdout = tokio::io::stdout();
    let mut exit_code = 0;

    // Stream output until connection closes or Ctrl+C
    let mut line = String::new();
    loop {
        line.clear();
        tokio::select! {
            _ = SHUTDOWN.cancelled() => {
                std::process::exit(130);
            }
            result = reader.read_line(&mut line) => {
                match result {
                    Ok(0) => break, // EOF
                    Ok(_) => {
                        if let Some(code) = try_parse_exit_code(&line) {
                            exit_code = code;
                        } else {
                            stdout.write_all(line.as_bytes()).await?;
                            stdout.flush().await?;
                        }
                    }
                    Err(_) => break,
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
async fn run_with_stdin<IO>(io: IO, cmd_str: String) -> Result<()>
where
    IO: AsyncReadExt + AsyncWriteExt + Unpin + Send + 'static,
{
    let (reader, writer) = tokio::io::split(io);
    let mut reader = BufReader::new(reader);
    let writer = Arc::new(Mutex::new(writer));

    // Send command config as JSON line
    {
        let config = ExecRequest {
            command: cmd_str,
            tty: false,
        };
        let mut w = writer.lock().await;
        w.write_all(format!("{}\n", serde_json::to_string(&config)?).as_bytes())
            .await?;
        w.flush().await?;
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

    // Stdin -> remote task
    let writer_stdin = writer.clone();
    let stdin_task = tokio::spawn(async move {
        while let Some(bytes) = stdin_rx.recv().await {
            if bytes.is_empty() {
                let mut w = writer_stdin.lock().await;
                let _ = w.shutdown().await;
                break;
            }
            let mut w = writer_stdin.lock().await;
            let _ = w.write_all(&bytes).await;
            let _ = w.flush().await;
        }
    });

    // Remote -> Stdout (main task) with Ctrl+C handling
    let mut exit_code = 0;
    let mut line = String::new();
    loop {
        line.clear();
        tokio::select! {
            _ = SHUTDOWN.cancelled() => {
                std::process::exit(130);
            }
            result = reader.read_line(&mut line) => {
                match result {
                    Ok(0) => break, // EOF
                    Ok(_) => {
                        if let Some(code) = try_parse_exit_code(&line) {
                            exit_code = code;
                        } else {
                            stdout.write_all(line.as_bytes()).await?;
                            stdout.flush().await?;
                        }
                    }
                    Err(_) => break,
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

/// TTY mode without stdin: raw terminal output only (read-only view of TUI apps)
async fn run_tty_readonly<IO>(io: IO, cmd_str: String) -> Result<()>
where
    IO: AsyncReadExt + AsyncWriteExt + Unpin + Send + 'static,
{
    let (mut reader, mut writer) = tokio::io::split(io);

    // Enter raw mode for proper escape sequence rendering
    let raw_mode = std::io::stdout().into_raw_mode()?;
    let mut stdout = tokio::io::stdout();

    // Send command config with tty: true
    let config = ExecRequest {
        command: cmd_str,
        tty: true,
    };
    writer
        .write_all(format!("{}\n", serde_json::to_string(&config)?).as_bytes())
        .await?;
    writer.flush().await?;

    let mut exit_code = 0;
    let mut buf = [0u8; 4096];
    let mut pending = Vec::new();

    // Stream output until connection closes or Ctrl+C
    loop {
        tokio::select! {
            _ = SHUTDOWN.cancelled() => {
                drop(raw_mode);
                std::process::exit(130);
            }
            result = reader.read(&mut buf) => {
                match result {
                    Ok(0) => break, // EOF
                    Ok(n) => {
                        pending.extend_from_slice(&buf[..n]);

                        // Look for potential exit code JSON at the end
                        if let Some(newline_pos) = pending.iter().rposition(|&b| b == b'\n')
                            && let Some(last) = String::from_utf8_lossy(&pending[..=newline_pos]).lines().last()
                            && let Some(code) = try_parse_exit_code(last)
                        {
                            exit_code = code;
                            // Output everything except the JSON line
                            let output_end = pending.len() - last.len() - 1;
                            if output_end > 0 {
                                stdout.write_all(&pending[..output_end]).await?;
                                stdout.flush().await?;
                            }
                            pending.clear();
                            continue;
                        }

                        // No exit code found, output everything
                        stdout.write_all(&pending).await?;
                        stdout.flush().await?;
                        pending.clear();
                    }
                    Err(_) => break,
                }
            }
        }
    }

    drop(raw_mode);

    if exit_code != 0 {
        std::process::exit(exit_code);
    }
    Ok(())
}

/// Full TTY mode: raw terminal, bidirectional stdin/stdout (for vim, htop, etc.)
async fn run_with_tty<IO>(io: IO, cmd_str: String) -> Result<()>
where
    IO: AsyncReadExt + AsyncWriteExt + Unpin + Send + 'static,
{
    let (reader, writer) = tokio::io::split(io);
    let writer = Arc::new(Mutex::new(writer));

    // Enter raw mode
    let raw_mode = std::io::stdout().into_raw_mode()?;
    let mut stdout = tokio::io::stdout();

    // Send command config with tty: true
    {
        let config = ExecRequest {
            command: cmd_str,
            tty: true,
        };
        let mut w = writer.lock().await;
        w.write_all(format!("{}\n", serde_json::to_string(&config)?).as_bytes())
            .await?;
        w.flush().await?;
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

    // Stdin -> remote task
    let writer_stdin = writer.clone();
    let mut stdin_task = tokio::spawn(async move {
        while let Some(bytes) = stdin_rx.recv().await {
            if bytes.is_empty() {
                let mut w = writer_stdin.lock().await;
                let _ = w.shutdown().await;
                break;
            }
            let mut w = writer_stdin.lock().await;
            let _ = w.write_all(&bytes).await;
            let _ = w.flush().await;
        }
        Ok::<_, anyhow::Error>(())
    });

    // Remote -> Stdout task
    let exit_code = Arc::new(Mutex::new(0i32));
    let exit_code_reader = exit_code.clone();

    let mut reader_task = tokio::spawn(async move {
        let mut reader = reader;
        let mut buf = [0u8; 4096];
        let mut pending = Vec::new();

        loop {
            let n = reader.read(&mut buf).await?;
            if n == 0 {
                break;
            }

            // Check if this chunk ends with a JSON line (exit code)
            pending.extend_from_slice(&buf[..n]);

            // Look for potential exit code JSON at the end
            if let Some(newline_pos) = pending.iter().rposition(|&b| b == b'\n')
                && let Some(last) = String::from_utf8_lossy(&pending[..=newline_pos])
                    .lines()
                    .last()
                && let Some(code) = try_parse_exit_code(last)
            {
                *exit_code_reader.lock().await = code;
                // Output everything except the JSON line
                let output_end = pending.len() - last.len() - 1;
                if output_end > 0 {
                    stdout.write_all(&pending[..output_end]).await?;
                    stdout.flush().await?;
                }
                pending.clear();
                continue;
            }

            // No exit code found, output everything
            stdout.write_all(&pending).await?;
            stdout.flush().await?;
            pending.clear();
        }

        Ok::<_, anyhow::Error>(())
    });

    // Wait for either task to complete or shutdown signal
    let final_code;
    tokio::select! {
        _ = SHUTDOWN.cancelled() => {
            reader_task.abort();
            stdin_task.abort();
            final_code = 130;
        }
        _ = &mut reader_task => {
            stdin_task.abort();
            final_code = *exit_code.lock().await;
        }
        _ = &mut stdin_task => {
            final_code = *exit_code.lock().await;
        }
    }

    drop(raw_mode);

    if final_code != 0 {
        std::process::exit(final_code);
    }
    Ok(())
}
