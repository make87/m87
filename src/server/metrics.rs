use crate::server::shared::acquire_metrics_task;

use futures::{SinkExt, StreamExt};
use warp::ws::{Message, WebSocket, Ws};

pub async fn system_metrics_handler(ws: WebSocket) {
    // If you want auth, call validate_token_via_ws here.
    let (mut ws_tx, mut ws_rx) = ws.split();

    let (task, mut rx) = acquire_metrics_task("system-metrics").await;

    let forward = tokio::spawn(async move {
        while let Ok(json) = rx.recv().await {
            if ws_tx.send(Message::text(json)).await.is_err() {
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

pub async fn handle_system_metrics_ws(
    ws: Ws,
) -> Result<impl warp::Reply, std::convert::Infallible> {
    Ok(ws.on_upgrade(move |socket| system_metrics_handler(socket)))
}
