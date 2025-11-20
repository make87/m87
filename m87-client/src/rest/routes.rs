use anyhow::Result;
use axum::{
    extract::{
        ws::{WebSocket, WebSocketUpgrade},
        Path,
    },
    response::{IntoResponse, Response},
    routing::get,
    Router,
};
use std::{future::Future, net::SocketAddr, pin::Pin};
use tokio::net::TcpListener;

// import your handlers
use crate::rest::{
    container_logs::handle_container_logs_ws, container_terminal::handle_container_terminal_ws,
    logs::handle_logs_ws, metrics::handle_system_metrics_ws, terminal::handle_terminal_ws,
};

pub fn build_router() -> Router {
    Router::new()
        .route("/logs", get(ws_upgrade(handle_logs_ws)))
        .route("/terminal", get(ws_upgrade(handle_terminal_ws)))
        .route("/metrics", get(ws_upgrade(handle_system_metrics_ws)))
        .route(
            "/container/{name}",
            get(ws_upgrade_with_param(handle_container_terminal_ws)),
        )
        .route(
            "/container-logs/{name}",
            get(ws_upgrade_with_param(handle_container_logs_ws)),
        )
}

/// WebSocket upgrade helper (no path params)
fn ws_upgrade<H>(
    handler: fn(WebSocket) -> H,
) -> impl Fn(WebSocketUpgrade) -> Pin<Box<dyn Future<Output = Response> + Send>> + Clone + Send + 'static
where
    H: Future<Output = ()> + Send + 'static,
{
    move |ws: WebSocketUpgrade| {
        Box::pin(async move {
            let resp = ws.on_upgrade(handler);
            resp.into_response()
        })
    }
}

/// WebSocket upgrade helper (with path params)
fn ws_upgrade_with_param<H>(
    handler: fn(String, WebSocket) -> H,
) -> impl Fn(Path<String>, WebSocketUpgrade) -> Pin<Box<dyn Future<Output = Response> + Send>>
       + Clone
       + Send
       + 'static
where
    H: Future<Output = ()> + Send + 'static,
{
    move |Path(param): Path<String>, ws: WebSocketUpgrade| {
        Box::pin(async move {
            let resp = ws.on_upgrade(move |socket| handler(param.clone(), socket));
            resp.into_response()
        })
    }
}

/// Start the Axum server (safe to call in a spawn loop)
pub async fn serve_server(port: u16) -> Result<()> {
    let app = build_router();
    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    let listener = TcpListener::bind(addr).await?;
    axum::serve(listener, app.into_make_service()).await?;
    Ok(())
}
