mod auth;
mod container_logs;
mod container_terminal;
mod logs;
mod metrics;
mod routes;
mod shared;
mod terminal;
use crate::{config::Config, server::routes::run_server};
use anyhow::Result;

pub async fn serve_server() -> Result<()> {
    let config = Config::load()?;
    run_server(config.server_port).await
}
