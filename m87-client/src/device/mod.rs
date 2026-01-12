// Agent-specific modules (Linux-only via build.rs)
#[cfg(feature = "agent")]
pub mod agent;

#[cfg(feature = "agent")]
pub mod deployment_manager;
#[cfg(feature = "agent")]
pub mod log_manager;
#[cfg(feature = "agent")]
pub mod system_metrics;

pub mod docker;
pub mod fs;
pub mod tunnel;

#[cfg(feature = "agent")]
mod control_tunnel;

#[cfg(unix)] // won't compile on Windows because no PTY
pub mod serial;
pub mod ssh;

pub mod deploy;
