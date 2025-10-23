use crate::server::auth::validate_token_via_ws;
use crate::server::shared::acquire_process_task;
use futures::{SinkExt, StreamExt};
use warp::ws::{Message, WebSocket, Ws};

pub async fn container_logs_handler(container: String, ws: WebSocket) {
    let (mut ws_tx, mut ws_rx) = ws.split();
    if let Err(e) = validate_token_via_ws(&mut ws_tx, &mut ws_rx, true).await {
        eprintln!("auth failed: {}", e);
        return;
    }

    let (task, mut rx) = acquire_process_task(
        &format!("container-logs:{}", container),
        "docker",
        &["logs", "-f", "--tail", "1000", &container],
    )
    .await;

    let forward = tokio::spawn(async move {
        while let Ok(line) = rx.recv().await {
            if ws_tx
                .send(Message::text(format!("{}\r\n", line)))
                .await
                .is_err()
            {
                break;
            }
        }
    });

    while let Some(msg) = ws_rx.next().await {
        if let Ok(m) = msg {
            if m.is_close() {
                break;
            }
        } else {
            break;
        }
    }

    forward.abort();
    task.dec_or_shutdown();
}

pub async fn handle_container_logs_ws(
    container_name: String,
    ws: Ws,
) -> Result<impl warp::Reply, std::convert::Infallible> {
    Ok(ws.on_upgrade(move |socket| container_logs_handler(container_name, socket)))
}
