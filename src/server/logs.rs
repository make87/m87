use crate::server::auth::validate_token_via_ws;
use crate::util::logging::get_log_rx;
use futures::{SinkExt, StreamExt};
use warp::ws::{Message, WebSocket, Ws};
use tracing::error;

pub async fn logs_handler(ws: WebSocket) {
    let (mut ws_tx, mut ws_rx) = ws.split();

    if let Err(e) = validate_token_via_ws(&mut ws_tx, &mut ws_rx, true).await {
        error!("auth failed: {}", e);
        return;
    }

    // subscribe to tracing log broadcast
    let mut rx = match get_log_rx() {
        Some(r) => r,
        None => {
            let _ = ws_tx.send(Message::text("logging not initialized")).await;
            return;
        }
    };

    // forward logs to WebSocket
    let forward = tokio::spawn(async move {
        while let Ok(line) = rx.recv().await {
            if ws_tx.send(Message::text(line)).await.is_err() {
                break;
            }
        }
    });

    // wait for close
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
}


pub async fn handle_logs_ws(ws: Ws) -> Result<impl warp::Reply, std::convert::Infallible> {
    Ok(ws.on_upgrade(move |socket| logs_handler(socket)))
}
