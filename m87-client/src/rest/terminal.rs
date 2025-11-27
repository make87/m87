use portable_pty::{native_pty_system, CommandBuilder, PtySize};
use tokio::sync::{mpsc, oneshot, Mutex};
use tokio::{
    io::AsyncReadExt,
    io::AsyncWriteExt,
    select,
    time::{timeout, Duration},
};

use std::env;
use std::path::Path;
use std::{io::Read, io::Write, sync::Arc};

use crate::rest::upgrade::BoxedIo;

pub async fn handle_terminal_io(_: (), mut io: BoxedIo) {
    // Notify client that shell is initializing
    let _ = io.write_all(b"Initializing shell...\r\n").await;

    // --------------------------------------------------------------------
    // 1. Create PTY
    // --------------------------------------------------------------------
    let pty_system = native_pty_system();

    let pair = match pty_system.openpty(PtySize {
        rows: 24,
        cols: 80,
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
    cmd.env("TERM", "xterm-256color");

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

    'outer: loop {
        select! {
            // ---------- CLIENT → PTY ----------
            r = io.read(&mut io_read_buf) => {
                match r {
                    Ok(0) => break 'outer,
                    Ok(n) => {
                        let data = io_read_buf[..n].to_vec();
                        let writer = writer.clone();
                        if tokio::task::spawn_blocking(move || {
                            let mut w = writer.blocking_lock();
                            w.write_all(&data)?;
                            w.flush()
                        }).await.is_err() {
                            break 'outer;
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

            else => break 'outer,
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
