use anyhow::Result;
use axum::{routing::any, Router};
use std::net::SocketAddr;
use tokio::net::TcpListener;

use crate::rest::upgrade::io_upgrade;
use crate::rest::{
    docker::handle_docker_io, logs::handle_logs_io, metrics::handle_system_metrics_io,
    port::handle_port_forward_io, ssh::handle_ssh_io, terminal::handle_terminal_io,
};

pub fn build_router() -> Router {
    Router::new()
        .route("/docker", any(io_upgrade(handle_docker_io)))
        .route("/logs", any(io_upgrade(handle_logs_io)))
        .route("/terminal", any(io_upgrade(handle_terminal_io)))
        .route("/metrics", any(io_upgrade(handle_system_metrics_io)))
        .route("/ssh", any(io_upgrade(handle_ssh_io)))
        .route("/port/{port}", any(io_upgrade(handle_port_forward_io)))

}

/// Start the Axum server (safe to call in a spawn loop)
pub async fn serve_server(port: u16) -> Result<()> {
    let app = build_router();
    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    let listener = TcpListener::bind(addr).await?;
    axum::serve(listener, app.into_make_service()).await?;
    Ok(())
}
