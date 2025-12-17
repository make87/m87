// Shared modules (used by both agent and manager)
pub mod quic;
pub mod stream_type;

// Agent-specific: These modules handle incoming streams on the device side
// Only compiled when agent feature is enabled
#[cfg(feature = "agent")]
mod docker;
#[cfg(feature = "agent")]
mod exec;
#[cfg(feature = "agent")]
mod logs;
#[cfg(feature = "agent")]
mod metrics;
#[cfg(feature = "agent")]
pub mod router;
#[cfg(feature = "agent")]
mod serial;
#[cfg(feature = "agent")]
mod shared;
#[cfg(feature = "agent")]
mod ssh;
#[cfg(feature = "agent")]
mod terminal;
#[cfg(feature = "agent")]
mod tunnel;
#[cfg(feature = "agent")]
pub mod udp_manager;
