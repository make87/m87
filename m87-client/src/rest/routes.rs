use anyhow::Result;
use axum::{
    extract::{
        ws::{WebSocket, WebSocketUpgrade},
        Path, Query,
    },
    http::HeaderMap,
    response::{IntoResponse, Response},
    routing::get,
    Router,
};
use reqwest::header;
use serde::Deserialize;
use std::{future::Future, net::SocketAddr, pin::Pin};
use tokio::net::TcpListener;

#[derive(Deserialize)]
struct PortForwardParams {
    host: Option<String>,
}

use crate::rest::auth::validate_token;
use crate::rest::{
    container_logs::handle_container_logs_ws, container_terminal::handle_container_terminal_ws,
    docker::handle_docker_ws, exec::handle_exec_ws, logs::handle_logs_ws,
    metrics::handle_system_metrics_ws, port::handle_port_forward_ws, terminal::handle_terminal_ws,
};

pub fn build_router() -> Router {
    Router::new()
        .route("/docker", get(ws_upgrade(handle_docker_ws)))
        .route("/exec", get(ws_upgrade(handle_exec_ws)))
        .route("/logs", get(ws_upgrade(handle_logs_ws)))
        .route("/terminal", get(ws_upgrade(handle_terminal_ws)))
        .route("/metrics", get(ws_upgrade(handle_system_metrics_ws)))
        .route(
            "/port/{port}",
            get(ws_upgrade_port_forward(handle_port_forward_ws)),
        )
        .route(
            "/container/{name}",
            get(ws_upgrade_with_param(handle_container_terminal_ws)),
        )
        .route(
            "/container-logs/{name}",
            get(ws_upgrade_with_param(handle_container_logs_ws)),
        )
}

fn ws_upgrade<H>(
    handler: fn(WebSocket) -> H,
) -> impl Fn(WebSocketUpgrade, HeaderMap) -> Pin<Box<dyn Future<Output = Response> + Send>>
       + Clone
       + Send
       + 'static
where
    H: Future<Output = ()> + Send + 'static,
{
    move |ws: WebSocketUpgrade, headers: HeaderMap| {
        Box::pin(async move {
            // Extract protocol: "bearer.<jwt>"
            let proto = headers
                .get(header::SEC_WEBSOCKET_PROTOCOL)
                .and_then(|h| h.to_str().ok());

            let jwt = match &proto {
                Some(p) if p.starts_with("bearer.") => p.trim_start_matches("bearer.").to_string(),
                _ => {
                    return (
                        axum::http::StatusCode::UNAUTHORIZED,
                        "Missing or invalid WebSocket protocol",
                    )
                        .into_response();
                }
            };

            // Validate token
            match validate_token(&jwt).await {
                Ok(c) => c,
                Err(_) => {
                    return (axum::http::StatusCode::UNAUTHORIZED, "Invalid auth token")
                        .into_response();
                }
            };
            let protocol_string = format!("bearer.{jwt}");
            let ws = ws.protocols([protocol_string]);

            ws.on_upgrade(move |socket| handler(socket)).into_response()
        })
    }
}

/// WebSocket upgrade helper for port forwarding (path + optional query params)
fn ws_upgrade_port_forward<H>(
    handler: fn(String, Option<String>, WebSocket) -> H,
) -> impl Fn(
    Path<String>,
    Query<PortForwardParams>,
    WebSocketUpgrade,
    HeaderMap,
) -> Pin<Box<dyn Future<Output = Response> + Send>>
       + Clone
       + Send
       + 'static
where
    H: Future<Output = ()> + Send + 'static,
{
    move |Path(port): Path<String>,
          Query(params): Query<PortForwardParams>,
          ws: WebSocketUpgrade,
          headers: HeaderMap| {
        Box::pin(async move {
            // Extract protocol: "bearer.<jwt>"
            let proto = headers
                .get(header::SEC_WEBSOCKET_PROTOCOL)
                .and_then(|h| h.to_str().ok());

            let jwt = match &proto {
                Some(p) if p.starts_with("bearer.") => p.trim_start_matches("bearer.").to_string(),
                _ => {
                    return (
                        axum::http::StatusCode::UNAUTHORIZED,
                        "Missing or invalid WebSocket protocol",
                    )
                        .into_response();
                }
            };

            // Validate token
            match validate_token(&jwt).await {
                Ok(c) => c,
                Err(_) => {
                    return (axum::http::StatusCode::UNAUTHORIZED, "Invalid auth token")
                        .into_response();
                }
            };
            let protocol_string = format!("bearer.{jwt}");
            let ws = ws.protocols([protocol_string]);

            let resp = ws.on_upgrade(move |socket| handler(port.clone(), params.host, socket));
            resp.into_response()
        })
    }
}

/// WebSocket upgrade helper (with path params)
fn ws_upgrade_with_param<H>(
    handler: fn(String, WebSocket) -> H,
) -> impl Fn(
    Path<String>,
    WebSocketUpgrade,
    HeaderMap,
) -> Pin<Box<dyn Future<Output = Response> + Send>>
       + Clone
       + Send
       + 'static
where
    H: Future<Output = ()> + Send + 'static,
{
    move |Path(param): Path<String>, ws: WebSocketUpgrade, headers: HeaderMap| {
        Box::pin(async move {
            // Extract protocol: "bearer.<jwt>"
            let proto = headers
                .get(header::SEC_WEBSOCKET_PROTOCOL)
                .and_then(|h| h.to_str().ok());

            let jwt = match &proto {
                Some(p) if p.starts_with("bearer.") => p.trim_start_matches("bearer.").to_string(),
                _ => {
                    return (
                        axum::http::StatusCode::UNAUTHORIZED,
                        "Missing or invalid WebSocket protocol",
                    )
                        .into_response();
                }
            };

            // Validate token
            match validate_token(&jwt).await {
                Ok(c) => c,
                Err(_) => {
                    return (axum::http::StatusCode::UNAUTHORIZED, "Invalid auth token")
                        .into_response();
                }
            };
            let protocol_string = format!("bearer.{jwt}");
            let ws = ws.protocols([protocol_string]);

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
