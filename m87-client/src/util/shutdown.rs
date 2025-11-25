//! Global shutdown signal for graceful termination.

use once_cell::sync::Lazy;
use tokio_util::sync::CancellationToken;

/// Global cancellation token for Ctrl+C handling.
pub static SHUTDOWN: Lazy<CancellationToken> = Lazy::new(CancellationToken::new);
