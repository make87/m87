// Shared modules (used by both m87 runtime and m87 command line)
pub mod iroh_p2p;
pub mod quic;
pub mod stream_type;

// auth uses jsonwebtoken — only needed for runtime token validation
#[cfg(feature = "runtime")]
pub mod auth;

// Runtime-specific: These modules handle incoming streams on the device side
// Only compiled when runtime feature is enabled
#[cfg(feature = "runtime")]
mod docker;
#[cfg(feature = "runtime")]
mod exec;
#[cfg(feature = "runtime")]
mod forward;
#[cfg(feature = "runtime")]
mod logs;
#[cfg(feature = "runtime")]
mod metrics;
#[cfg(feature = "runtime")]
pub mod router;
#[cfg(feature = "runtime")]
mod serial;
#[cfg(feature = "runtime")]
mod shared;
#[cfg(feature = "runtime")]
mod ssh;
#[cfg(feature = "runtime")]
mod terminal;
#[cfg(feature = "runtime")]
pub mod udp_manager;
