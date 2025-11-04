mod api;
mod auth;
mod config;
mod db;
mod models;
mod relay;
mod response;
mod util;

use std::sync::Arc;
use tracing::info;
use util::logging::init_tracing;

use crate::{relay::relay_state::RelayState, response::ServerResult};

#[tokio::main]
async fn main() -> ServerResult<()> {
    println!("Booting Nexus...");
    init_tracing();
    info!("Starting nexus");

    let config = config::AppConfig::from_env().unwrap_or_else(|e| {
        eprintln!("Failed to load config: {:?}", e);
        std::process::exit(1);
    });

    let mongo_uri = config.mongo_uri.clone();
    let db_name = config.mongo_db.clone();

    info!("Connecting to database");
    let db = Arc::new(db::Mongo::connect(&mongo_uri, &db_name).await?);
    db.ensure_indexes().await?;
    let config = Arc::new(config);
    // Shared relay state
    let relay_state = Arc::new(RelayState::new()?);

    info!("nexus started");
    if let Err(e) = api::serve::serve(db, relay_state, config).await {
        tracing::error!("Server exited: {:?}", e);
    }
    Ok(())
}
