// Agent-specific modules (Linux-only via build.rs)
#[cfg(feature = "agent")]
pub mod agent;

#[cfg(feature = "agent")]
pub mod services;

#[cfg(feature = "agent")]
pub mod system_metrics;

pub mod docker;
pub mod fs;
pub mod tunnel;

#[cfg(unix)]  // won't compile on Windows because no PTY
pub mod serial;
pub mod ssh;
