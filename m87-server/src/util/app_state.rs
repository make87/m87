use std::sync::Arc;

use m87_shared::iroh_ticket::IrohTicketSigner;

use crate::{config::AppConfig, db::Mongo, relay::relay_state::RelayState};

#[derive(Clone)]
pub struct AppState {
    pub db: Arc<Mongo>,
    pub config: Arc<AppConfig>,
    pub relay: Arc<RelayState>,
    /// Signs short-lived tickets authorizing direct CLI→device iroh
    /// connections. The matching public key is advertised to devices in the
    /// heartbeat so they can verify the tickets CLIs present.
    pub iroh_ticket_signer: Arc<IrohTicketSigner>,
}
