use axum::extract::ws::{Message, WebSocket};
use futures::{SinkExt, StreamExt};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;
use tracing::{error, info};

pub async fn handle_docker_ws(socket: WebSocket) {
    let (ws_tx, mut ws_rx) = socket.split();
    let ws_tx = std::sync::Arc::new(tokio::sync::Mutex::new(ws_tx));

    // Connect to Docker socket
    let docker_sock = match UnixStream::connect("/var/run/docker.sock").await {
        Ok(sock) => sock,
        Err(e) => {
            error!("Failed to connect to Docker socket: {}", e);
            let _ = ws_tx
                .lock()
                .await
                .send(Message::Text(format!("Failed to connect to Docker: {}\n", e).into()))
                .await;
            return;
        }
    };

    let (mut docker_read, mut docker_write) = docker_sock.into_split();

    info!("Docker WebSocket proxy established");

    // Task 1: Docker socket → WebSocket
    let ws_tx_clone = ws_tx.clone();
    let docker_to_ws = tokio::spawn(async move {
        let mut buf = vec![0u8; 8192];

        loop {
            match docker_read.read(&mut buf).await {
                Ok(0) => {
                    info!("Docker socket closed (read)");
                    break;
                }
                Ok(n) => {
                    let data = buf[..n].to_vec();
                    if ws_tx_clone
                        .lock()
                        .await
                        .send(Message::Binary(data.into()))
                        .await
                        .is_err()
                    {
                        error!("Failed to send to WebSocket");
                        break;
                    }
                }
                Err(e) => {
                    error!("Error reading from Docker socket: {}", e);
                    break;
                }
            }
        }
    });

    // Task 2: WebSocket → Docker socket
    let ws_to_docker = tokio::spawn(async move {
        while let Some(Ok(msg)) = ws_rx.next().await {
            match msg {
                Message::Binary(data) => {
                    if docker_write.write_all(&data).await.is_err() {
                        error!("Failed to write to Docker socket");
                        break;
                    }
                }
                Message::Text(text) => {
                    if docker_write.write_all(text.as_bytes()).await.is_err() {
                        error!("Failed to write to Docker socket");
                        break;
                    }
                }
                Message::Close(_) => {
                    info!("WebSocket closed");
                    break;
                }
                _ => {}
            }
        }
    });

    // Wait for either direction to complete
    tokio::select! {
        _ = docker_to_ws => {
            info!("Docker → WebSocket task completed");
        }
        _ = ws_to_docker => {
            info!("WebSocket → Docker task completed");
        }
    }

    // Cleanup
    let _ = ws_tx.lock().await.send(Message::Close(None)).await;
    info!("Docker WebSocket proxy closed");
}
