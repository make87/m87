//! Raw TCP/TLS endpoint for clean command execution.
//!
//! Unlike `/terminal` which spawns an interactive login shell,
//! this endpoint runs commands via a detected shell with mode-appropriate
//! flags, producing clean output without MOTD, prompts, or logout messages.
//! Profile files are sourced on shells that support `-l` (bash, zsh, fish)
//! and PATH is hardened for minimal environments.
//!
//! Protocol:
//! 1. Client sends JSON config line: {"command":"...", "tty":false, "rows":24, "cols":80}\n
//! 2. Bidirectional raw bytes for stdin/stdout
//!    - In PTY mode, `0xFF` + 4 bytes = resize frame (same as terminal.rs)
//! 3. Server sends exit code JSON before closing: {"exit_code":N}\n

use portable_pty::{CommandBuilder, PtySize, native_pty_system};
use serde::{Deserialize, Serialize};
use std::io::{Read, Write};
use std::process::Stdio;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::process::Command;
use tokio::sync::{Mutex, mpsc};
use tokio::select;

use crate::streams::quic::QuicIo;
use crate::util::shell::{self, ShellMode};

#[derive(Deserialize)]
struct ExecRequest {
    command: String,
    #[serde(default)]
    tty: bool,
    #[serde(default)]
    rows: Option<u16>,
    #[serde(default)]
    cols: Option<u16>,
}

#[derive(Serialize)]
struct ExecResult {
    exit_code: i32,
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
    let shell = shell::detect_shell();
    let args = shell::build_shell_args(
        &shell,
        ShellMode::ExecPiped {
            command: config.command.clone(),
        },
    );
    let path = shell::ensure_minimal_path();

    let mut cmd = Command::new(&shell);
    cmd.args(&args)
        .env("PATH", &path)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());

    // Create a new session so the child has no controlling terminal.
    // This ensures programs like `sudo` that open /dev/tty will fall back
    // to using stderr for prompts (which we pipe back to the command line).
    #[cfg(unix)]
    {
        #[allow(unused_imports)]
        use std::os::unix::process::CommandExt;
        unsafe {
            cmd.pre_exec(|| {
                if libc::setsid() == -1 {
                    eprintln!("setsid() failed: {}", std::io::Error::last_os_error());
                }
                Ok(())
            });
        }
    }

    let mut child = match cmd.spawn() {
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

    // Drop the original sender so the output channel can close once
    // stdout_task and stderr_task finish draining their pipes.
    drop(out_tx);

    // Wait for stdout/stderr reader tasks to finish (they'll hit EOF quickly
    // after the child exits) — this prevents silently dropping buffered output.
    let _ = stdout_task.await;
    let _ = stderr_task.await;

    // Now the output_task will finish once all senders are dropped
    let _ = output_task.await;

    // Abort stdin task (no longer needed)
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
    let shell = shell::detect_shell();
    let args = shell::build_shell_args(
        &shell,
        ShellMode::ExecPty {
            command: config.command.clone(),
        },
    );
    let path = shell::ensure_minimal_path();

    let rows = config.rows.unwrap_or(24);
    let cols = config.cols.unwrap_or(80);

    // Create PTY
    let pty_system = native_pty_system();
    let pair = match pty_system.openpty(PtySize {
        rows,
        cols,
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

    // Spawn command in PTY with mode-appropriate flags
    let mut cmd = CommandBuilder::new(&shell);
    let args_refs: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
    cmd.args(&args_refs);
    cmd.env("TERM", "xterm-256color");
    cmd.env("PATH", &path);

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

    // Spawn a blocking task to wait for child exit — replaces 50ms polling
    let (exit_tx, mut exit_rx) = mpsc::channel::<i32>(1);
    let child_arc = Arc::new(std::sync::Mutex::new(child));
    let child_wait = child_arc.clone();
    tokio::task::spawn_blocking(move || {
        let mut guard = child_wait.lock().unwrap();
        let code = guard
            .wait()
            .ok()
            .map(|s| s.exit_code() as i32)
            .unwrap_or(-1);
        let _ = exit_tx.blocking_send(code);
    });

    // Main loop
    let mut read_buf = [0u8; 4096];
    let mut input_buf: Vec<u8> = Vec::new();
    let mut exit_code: Option<i32> = None;
    'outer: loop {
        select! {
            // Client -> PTY (with resize frame parsing)
            r = reader.read(&mut read_buf) => {
                match r {
                    Ok(0) => break 'outer,
                    Ok(n) => {
                        input_buf.extend_from_slice(&read_buf[..n]);

                        while !input_buf.is_empty() {
                            // Resize frame: 0xFF + 4 bytes (rows_hi, rows_lo, cols_hi, cols_lo)
                            if input_buf.len() >= 5 && input_buf[0] == 0xFF {
                                let new_rows = u16::from_be_bytes([input_buf[1], input_buf[2]]);
                                let new_cols = u16::from_be_bytes([input_buf[3], input_buf[4]]);

                                let _ = pair.master.resize(PtySize {
                                    rows: new_rows,
                                    cols: new_cols,
                                    pixel_width: 0,
                                    pixel_height: 0,
                                });

                                input_buf.drain(..5);
                                continue;
                            }

                            // Normal input — everything until next 0xFF or end
                            let next_resize = input_buf
                                .iter()
                                .position(|&b| b == 0xFF)
                                .unwrap_or(input_buf.len());

                            let payload: Vec<u8> = input_buf.drain(..next_resize).collect();

                            if !payload.is_empty() {
                                let pty_w = pty_writer.clone();
                                if tokio::task::spawn_blocking(move || {
                                    let mut w = pty_w.blocking_lock();
                                    w.write_all(&payload)?;
                                    w.flush()
                                }).await.is_err() {
                                    break 'outer;
                                }
                            }
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

            // Child exited
            Some(code) = exit_rx.recv() => {
                exit_code = Some(code);
                break 'outer;
            }

            else => break 'outer,
        }
    }

    // Cleanup — drop PTY master to send SIGHUP to the session
    drop(pair);

    // Kill process group if child didn't exit cleanly
    {
        let mut guard = child_arc.lock().unwrap();
        let _ = guard.kill();
    }

    // Send exit code
    let code = exit_code.unwrap_or(-1);
    let result = ExecResult { exit_code: code };
    let mut w = writer.lock().await;
    let _ = w
        .write_all(format!("{}\n", serde_json::to_string(&result).unwrap()).as_bytes())
        .await;
    let _ = w.shutdown().await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::util::shell;

    #[test]
    fn test_detect_shell_returns_valid_path() {
        let s = shell::detect_shell();
        assert!(!s.is_empty());
        #[cfg(unix)]
        assert!(s.starts_with('/') || s == "powershell.exe");
    }

    #[test]
    fn test_exec_request_deserialization() {
        let json = r#"{"command":"ls -la","tty":true}"#;
        let req: ExecRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.command, "ls -la");
        assert!(req.tty);
    }

    #[test]
    fn test_exec_request_tty_default_false() {
        let json = r#"{"command":"echo hello"}"#;
        let req: ExecRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.command, "echo hello");
        assert!(!req.tty);
    }

    #[test]
    fn test_exec_request_with_rows_cols() {
        let json = r#"{"command":"htop","tty":true,"rows":50,"cols":120}"#;
        let req: ExecRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.rows, Some(50));
        assert_eq!(req.cols, Some(120));
    }

    #[test]
    fn test_exec_request_rows_cols_default_none() {
        let json = r#"{"command":"echo hello"}"#;
        let req: ExecRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.rows, None);
        assert_eq!(req.cols, None);
    }

    #[test]
    fn test_exec_request_invalid_json() {
        let json = r#"{"invalid": true}"#;
        let result: Result<ExecRequest, _> = serde_json::from_str(json);
        assert!(result.is_err());
    }

    #[test]
    fn test_exec_result_serialization() {
        let result = ExecResult { exit_code: 0 };
        let json = serde_json::to_string(&result).unwrap();
        assert_eq!(json, r#"{"exit_code":0}"#);

        let result = ExecResult { exit_code: -1 };
        let json = serde_json::to_string(&result).unwrap();
        assert_eq!(json, r#"{"exit_code":-1}"#);
    }

    #[test]
    fn test_exec_result_serialization_various_codes() {
        for code in [0, 1, 127, 255, -1, -15] {
            let result = ExecResult { exit_code: code };
            let json = serde_json::to_string(&result).unwrap();
            assert!(json.contains(&format!("\"exit_code\":{}", code)));
        }
    }
}
