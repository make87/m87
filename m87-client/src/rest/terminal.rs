use axum::extract::ws::{Message, Utf8Bytes, WebSocket};
use futures::{SinkExt, StreamExt};
use portable_pty::{native_pty_system, CommandBuilder, PtySize};
use tokio::sync::{mpsc, oneshot, Mutex};
use tokio::{
    select,
    time::{timeout, Duration},
};
use tracing::{error, info};

use std::{io::Read, io::Write, sync::Arc};

pub async fn handle_terminal_ws(socket: WebSocket) {
    let (mut ws_tx, mut ws_rx) = socket.split();

    if let Err(e) = ws_tx
        .send(Message::Text("Initializing shell...\n\r".into()))
        .await
    {
        error!("Failed to send initialization message: {}", e);
        return;
    }

    // --------------------------------------------------------
    // 1. Create PTY pair
    // --------------------------------------------------------
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
                .send(Message::Text(Utf8Bytes::from(format!(
                    "Failed to create PTY: {e}\n"
                ))))
                .await;
            return;
        }
    };

    // --------------------------------------------------------
    // 2. Spawn shell into the PTY
    // --------------------------------------------------------
    let shell = if cfg!(windows) {
        "powershell.exe"
    } else {
        "/bin/bash"
    };

    let mut cmd = CommandBuilder::new(shell);
    cmd.args(&["-l", "-i"]);
    cmd.env("TERM", "xterm-256color");

    let mut child = match pair.slave.spawn_command(cmd) {
        Ok(c) => c,
        Err(e) => {
            let _ = ws_tx
                .send(Message::Text(Utf8Bytes::from(format!(
                    "Failed to spawn shell: {e}\n"
                ))))
                .await;
            return;
        }
    };

    // Master side: reader + writer (sync I/O)
    let reader = match pair.master.try_clone_reader() {
        Ok(r) => r,
        Err(e) => {
            let _ = ws_tx
                .send(Message::Text(Utf8Bytes::from(format!(
                    "Failed to get PTY reader: {e}\n"
                ))))
                .await;
            let _ = child.kill();
            return;
        }
    };
    let writer = match pair.master.take_writer() {
        Ok(w) => w,
        Err(e) => {
            let _ = ws_tx
                .send(Message::Text(Utf8Bytes::from(format!(
                    "Failed to get PTY writer: {e}\n"
                ))))
                .await;
            let _ = child.kill();
            return;
        }
    };
    let writer = Arc::new(Mutex::new(writer));

    // --------------------------------------------------------
    // 3. PTY → WS reader task (blocking thread)
    // --------------------------------------------------------
    let (tx, mut rx) = mpsc::unbounded_channel::<Vec<u8>>();
    let (ready_tx, ready_rx) = oneshot::channel::<()>();
    tokio::task::spawn_blocking(move || {
        let mut reader = reader;
        let mut buf = [0u8; 1024];

        // wrap Sender in Option so we can send exactly once
        let mut ready_opt = Some(ready_tx);

        loop {
            match reader.read(&mut buf) {
                Ok(0) => {
                    info!("closed pty");
                    break;
                }
                Ok(n) => {
                    if let Some(tx) = ready_opt.take() {
                        // send “ready” exactly once
                        let _ = tx.send(());
                    }
                    let _ = tx.send(buf[..n].to_vec());
                }
                Err(_) => {
                    info!("error reading pty");
                    break;
                }
            }
        }

        // if shell produced no output before EOF,
        // we still ensure ready is sent exactly once
        if let Some(tx) = ready_opt.take() {
            let _ = tx.send(());
        }
    });

    // --------------------------------------------------------
    // 4. Wait for shell readiness
    // --------------------------------------------------------
    if timeout(Duration::from_secs(2), ready_rx).await.is_err() {
        let _ = ws_tx
            .send(Message::Text(Utf8Bytes::from_static(
                "Shell failed to start within timeout\n",
            )))
            .await;
        let _ = child.kill();
        return;
    }

    let _ = ws_tx
        .send(Message::Text(Utf8Bytes::from_static(
            "Shell connected successfully\n\r",
        )))
        .await;

    // --------------------------------------------------------
    // 5. Main loop: WebSocket <-> PTY
    // --------------------------------------------------------
    'outer: loop {
        select! {
            // WebSocket → PTY
            Some(Ok(msg)) = ws_rx.next() => {
                match msg {
                    Message::Text(text) => {
                        let data = text.clone();
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

            // PTY → WebSocket
            Some(out) = rx.recv() => {
                let text = String::from_utf8_lossy(&out).to_string();
                if ws_tx.send(Message::Text(Utf8Bytes::from(text))).await.is_err() {
                    break 'outer;
                }
            }

            _ = tokio::time::sleep(Duration::from_millis(50)) => {
                if let Some(_status) = child.try_wait().unwrap_or(None) {
                    // bash is gone
                    break 'outer;
                }
            }

            else => {
                // PTY thread ended OR websocket ended
                break 'outer;
            }
        }
    }

    // --------------------------------------------------------
    // 6. Cleanup
    // --------------------------------------------------------
    let _ = child.kill();

    // Explicitly close the websocket so clients can exit cleanly
    let _ = ws_tx.send(Message::Close(None)).await;

    // Dropping ws_tx also helps ensure the stream terminates
    drop(ws_tx);
}
