use std::fmt::Write;
use std::sync::OnceLock;
use tokio::sync::broadcast;
use tracing::{Event, Subscriber};
use tracing_subscriber::layer::{Context, Layer};
use tracing_subscriber::prelude::*;

static LOG_TX: OnceLock<broadcast::Sender<String>> = OnceLock::new();

/// A tracing layer that forwards log messages to a broadcast channel.
/// Can be used by any component (e.g. WebSocket handlers) to stream logs live.
pub struct LogBroadcastLayer {
    tx: broadcast::Sender<String>,
}

impl LogBroadcastLayer {
    pub fn new(tx: broadcast::Sender<String>) -> Self {
        Self { tx }
    }
}

/// Layer implementation that receives all tracing events and forwards them.
impl<S> Layer<S> for LogBroadcastLayer
where
    S: Subscriber,
{
    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        let mut buf = String::new();
        let _ = write!(&mut buf, "{:?}", event);
        let _ = self.tx.send(buf);
    }
}

/// Initialize global tracing with both fmt and WebSocket broadcast layers.
pub fn init_tracing_with_log_layer() -> broadcast::Sender<String> {
    let (tx, _rx) = broadcast::channel(1000);
    LOG_TX.set(tx.clone()).ok();

    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer())
        .with(LogBroadcastLayer::new(tx.clone()))
        .init();

    tx
}

/// Retrieve a receiver for streaming live log messages.
pub fn get_log_rx() -> Option<broadcast::Receiver<String>> {
    LOG_TX.get().map(|tx| tx.subscribe())
}
