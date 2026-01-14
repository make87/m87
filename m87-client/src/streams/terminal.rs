use portable_pty::{CommandBuilder, PtySize, native_pty_system};
use tokio::sync::{Mutex, mpsc, oneshot};
use tokio::{
    io::AsyncReadExt,
    io::AsyncWriteExt,
    select,
    time::{Duration, timeout},
};

use std::path::Path;
use std::{io::Read, io::Write, sync::Arc};

use crate::streams::quic::QuicIo;

pub async fn handle_terminal_io(term: Option<String>, io: &mut QuicIo) {
    // Notify client that shell is initializing
    let _ = io.write_all(b"\n\rInitializing shell..").await;

    // --------------------------------------------------------------------
    // 1. Create PTY
    // --------------------------------------------------------------------
    let pty_system = native_pty_system();

    let mut buf = [0u8; 5];
    io.read_exact(&mut buf).await.ok();

    let (rows, cols) = if buf[0] == 0xFF {
        (
            u16::from_be_bytes([buf[1], buf[2]]),
            u16::from_be_bytes([buf[3], buf[4]]),
        )
    } else {
        (24, 80)
    };

    let pair = match pty_system.openpty(PtySize {
        rows,
        cols,
        pixel_width: 0,
        pixel_height: 0,
    }) {
        Ok(p) => p,
        Err(e) => {
            let _ = io
                .write_all(format!("Failed to create PTY: {e}\n").as_bytes())
                .await;
            return;
        }
    };

    // --------------------------------------------------------------------
    // 2. Spawn login shell
    // --------------------------------------------------------------------
    let shell = detect_shell();

    let mut cmd = CommandBuilder::new(shell);
    cmd.args(&["-l", "-i"]);
    let term = term.as_deref().unwrap_or("xterm-256color");
    cmd.env("TERM", term);
    cmd.env("COLORTERM", "truecolor");

    let mut child = match pair.slave.spawn_command(cmd) {
        Ok(c) => c,
        Err(e) => {
            let _ = io
                .write_all(format!("Failed to spawn shell: {e}\n").as_bytes())
                .await;
            return;
        }
    };

    // PTY reader & writer
    let reader = match pair.master.try_clone_reader() {
        Ok(r) => r,
        Err(e) => {
            let _ = io
                .write_all(format!("Failed to get PTY reader: {e}\n").as_bytes())
                .await;
            let _ = child.kill();
            return;
        }
    };

    let writer = match pair.master.take_writer() {
        Ok(w) => w,
        Err(e) => {
            let _ = io
                .write_all(format!("Failed to get PTY writer: {e}\n").as_bytes())
                .await;
            let _ = child.kill();
            return;
        }
    };

    let writer = Arc::new(Mutex::new(writer));

    // --------------------------------------------------------------------
    // 3. PTY → mpsc channel (blocking reader thread)
    // --------------------------------------------------------------------
    let (pty_tx, mut pty_rx) = mpsc::unbounded_channel::<Vec<u8>>();
    let (ready_tx, ready_rx) = oneshot::channel::<()>();

    tokio::task::spawn_blocking(move || {
        let mut reader = reader;
        let mut buf = [0u8; 1024];
        let mut ready_opt = Some(ready_tx);

        loop {
            match reader.read(&mut buf) {
                Ok(0) => break,
                Ok(n) => {
                    if let Some(tx) = ready_opt.take() {
                        let _ = tx.send(());
                    }
                    let _ = pty_tx.send(buf[..n].to_vec());
                }
                Err(_) => break,
            }
        }

        if let Some(tx) = ready_opt.take() {
            let _ = tx.send(());
        }
    });

    // --------------------------------------------------------------------
    // 4. Wait until shell produces first output
    // --------------------------------------------------------------------
    if timeout(Duration::from_secs(2), ready_rx).await.is_err() {
        let _ = io
            .write_all(b"Shell failed to start within timeout\n")
            .await;
        let _ = child.kill();
        return;
    }

    let _ = io.write_all(b"Shell connected successfully\r\n").await;

    // --------------------------------------------------------------------
    // 5. Main loop: IO <-> PTY
    // --------------------------------------------------------------------
    let mut io_read_buf = [0u8; 1024];
    let mut input_buf: Vec<u8> = Vec::new();
    'outer: loop {
        select! {
            // ---------- CLIENT → PTY ----------
            r = io.read(&mut io_read_buf) => {
                match r {
                    Ok(0) => break 'outer,
                    Ok(n) => {
                        input_buf.extend_from_slice(&io_read_buf[..n]);

                        while !input_buf.is_empty() {
                            // ----- RESIZE FRAME -----
                            if input_buf.len() >= 5 && input_buf[0] == 0xFF {
                                let rows = u16::from_be_bytes([input_buf[1], input_buf[2]]);
                                let cols = u16::from_be_bytes([input_buf[3], input_buf[4]]);

                                let _ = pair.master.resize(PtySize {
                                    rows,
                                    cols,
                                    pixel_width: 0,
                                    pixel_height: 0,
                                });

                                // consume resize frame
                                input_buf.drain(..5);
                                continue;
                            }

                            // ----- NORMAL INPUT -----
                            // everything until next resize marker or end
                            let next_resize = input_buf
                                .iter()
                                .position(|&b| b == 0xFF)
                                .unwrap_or(input_buf.len());

                            let payload: Vec<u8> = input_buf.drain(..next_resize).collect();

                            if !payload.is_empty() {
                                let writer = writer.clone();

                                if tokio::task::spawn_blocking(move || {
                                    let mut w = writer.blocking_lock();
                                    w.write_all(&payload)?;
                                    w.flush()
                                })
                                .await
                                .is_err()
                                {
                                    break 'outer;
                                }
                            }
                        }
                    }
                    Err(_) => break 'outer,
                }
            }

            // ---------- PTY → CLIENT ----------
            Some(out) = pty_rx.recv() => {
                if io.write_all(&out).await.is_err() {
                    break 'outer;
                }
            }

            // ---------- Shell exit ----------
            _ = tokio::time::sleep(Duration::from_millis(50)) => {
                if let Ok(Some(_)) = child.try_wait() {
                    break 'outer;
                }
            }
        }
    }

    // --------------------------------------------------------------------
    // 6. Cleanup
    // --------------------------------------------------------------------
    let _ = child.kill();
    let _ = io.shutdown().await;
}

fn detect_shell() -> String {
    if cfg!(windows) {
        return "powershell.exe".to_string();
    }

    if let Ok(shell) = std::env::var("SHELL") {
        if Path::new(&shell).exists() {
            return shell;
        }
    }
    for c in ["/bin/bash", "/bin/zsh", "/usr/bin/fish", "/bin/sh"] {
        if Path::new(c).exists() {
            return c.to_string();
        }
    }
    "/bin/sh".to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_detect_shell_returns_valid_path() {
        let shell = detect_shell();
        assert!(!shell.is_empty());

        #[cfg(unix)]
        {
            // On Unix, should be an absolute path
            assert!(shell.starts_with('/'));
            // Should point to a real shell
            assert!(Path::new(&shell).exists());
        }

        #[cfg(windows)]
        {
            assert_eq!(shell, "powershell.exe");
        }
    }

    #[test]
    #[cfg(unix)]
    fn test_detect_shell_fallback_exists() {
        // At least /bin/sh should exist on all Unix systems
        let shell = detect_shell();
        assert!(Path::new(&shell).exists());
    }

    #[test]
    fn test_detect_shell_common_shells() {
        let shell = detect_shell();
        // Should be one of the common shells
        let common_shells = [
            "/bin/bash",
            "/bin/zsh",
            "/usr/bin/fish",
            "/bin/sh",
            "powershell.exe",
        ];

        // Either from env var or one of the common shells
        let is_common = common_shells.iter().any(|s| shell.ends_with(s) || *s == shell);
        let from_env = std::env::var("SHELL").is_ok();

        assert!(is_common || from_env);
    }

    #[test]
    fn test_pty_size_parsing() {
        // Test the PTY size parsing logic used in handle_terminal_io
        // Frame format: [0xFF, rows_high, rows_low, cols_high, cols_low]
        let buf: [u8; 5] = [0xFF, 0x00, 0x18, 0x00, 0x50]; // 24 rows, 80 cols

        let (rows, cols) = if buf[0] == 0xFF {
            (
                u16::from_be_bytes([buf[1], buf[2]]),
                u16::from_be_bytes([buf[3], buf[4]]),
            )
        } else {
            (24, 80)
        };

        assert_eq!(rows, 24);
        assert_eq!(cols, 80);
    }

    #[test]
    fn test_pty_size_parsing_large_terminal() {
        // Test larger terminal size
        let buf: [u8; 5] = [0xFF, 0x00, 0x64, 0x01, 0x00]; // 100 rows, 256 cols

        let rows = u16::from_be_bytes([buf[1], buf[2]]);
        let cols = u16::from_be_bytes([buf[3], buf[4]]);

        assert_eq!(rows, 100);
        assert_eq!(cols, 256);
    }

    #[test]
    fn test_pty_size_parsing_default_on_invalid() {
        // If first byte is not 0xFF, use defaults
        let buf: [u8; 5] = [0x00, 0x00, 0x00, 0x00, 0x00];

        let (rows, cols) = if buf[0] == 0xFF {
            (
                u16::from_be_bytes([buf[1], buf[2]]),
                u16::from_be_bytes([buf[3], buf[4]]),
            )
        } else {
            (24, 80)
        };

        assert_eq!(rows, 24);
        assert_eq!(cols, 80);
    }
}
