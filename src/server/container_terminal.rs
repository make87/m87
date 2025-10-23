use crate::server::auth::validate_token_via_ws;
use futures::{SinkExt, StreamExt};
use pty_process::Size;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tracing::{error, info};
use warp::ws::{Message, WebSocket, Ws};

/// Per-connection interactive container shell (not shared on purpose).
pub async fn container_terminal_handler(container_name: String, ws: WebSocket) {
    let (mut ws_tx, mut ws_rx) = ws.split();

    if let Err(e) = validate_token_via_ws(&mut ws_tx, &mut ws_rx, true).await {
        error!("auth failed: {}", e);
        return;
    }

    let candidate_cmds = ["/bin/bash", "/bin/sh", "bash", "sh"];
    let mut child_result = None;
    let mut final_pty = None;
    for shell in candidate_cmds {
        let (pty, pts) = pty_process::open().expect("open PTY");
        pty.resize(Size::new(24, 80)).ok(); // set initial terminal size (24x80)
        let mut cmd = pty_process::Command::new("docker");
        cmd = cmd.args(["exec", "-it", &container_name, shell]);
        let mut child = cmd.spawn(pts);

        match child {
            Ok(chld) => {
                child_result = Some(chld);
                final_pty = Some(pty);
            }
            Err(_) => {
                error!(
                    "Failed to start `{}` in container `{}`",
                    shell, container_name
                );
                // Try the next shell command
                continue;
            }
        }
    }

    let (mut child, pty) = match (child_result, final_pty) {
        (Some(c), Some(p)) => (c, p),
        (None, _) => {
            let _ = ws_tx
                .send(Message::text(format!(
                    "Could not start a shell in container `{}`.\nTried: {:?}\n\
            Hint: Install one of these in your image: /bin/sh, /bin/bash",
                    container_name, candidate_cmds
                )))
                .await;
            return;
        }
        (_, None) => {
            let _ = ws_tx
                .send(Message::text(format!(
                    "Failed to open PTY for container `{}`",
                    container_name
                )))
                .await;
            return;
        }
    };

    // 3. Split the PTY into a read half and write half for concurrent I/O
    let (mut pty_reader, mut pty_writer) = pty.into_split();

    // Spawn a task to forward data from PTY to WebSocket
    tokio::spawn(async move {
        let mut buffer = [0u8; 128];
        loop {
            match pty_reader.read(&mut buffer).await {
                Ok(0) => break, // PTY EOF
                Ok(n) => {
                    // Send the output chunk as binary WebSocket message
                    let data = buffer[..n].to_vec();
                    let data_str = String::from_utf8_lossy(&data);
                    if ws_tx.send(warp::ws::Message::text(data_str)).await.is_err() {
                        break; // WS connection closed or errored
                    }
                }
                Err(e) => {
                    error!("PTY read error: {:?}", e);
                    break;
                }
            }
        }
    });

    // Main loop: forward incoming WebSocket messages to the PTY
    while let Some(result) = ws_rx.next().await {
        match result {
            Ok(msg) => {
                if msg.is_close() {
                    break;
                }
                // Combine text or binary frames into bytes and write to PTY
                let data = msg.into_bytes();
                if let Err(e) = pty_writer.write_all(&data).await {
                    error!("PTY write error: {:?}", e);
                    break;
                }
            }
            Err(e) => {
                error!("WebSocket error: {:?}", e);
                break;
            }
        }
    }

    // 4. Cleanup: if websocket closes, kill the child and let PTY drop
    let _ = child.kill(); // ensure the container shell is terminated
    info!("Closed PTY for `{}`", container_name);
}

pub async fn handle_container_terminal_ws(
    container_name: String,
    ws: Ws,
) -> Result<impl warp::Reply, std::convert::Infallible> {
    Ok(ws.on_upgrade(move |socket| container_terminal_handler(container_name, socket)))
}
