use std::sync::Arc;

use crate::{config::AppConfig, db::Mongo, relay::relay_state::RelayState};

#[derive(Clone)]
pub struct AppState {
    pub db: Arc<Mongo>,
    pub config: Arc<AppConfig>,
    pub relay: Arc<RelayState>,
}
