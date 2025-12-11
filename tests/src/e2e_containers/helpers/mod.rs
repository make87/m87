//! Helper utilities for E2E tests

mod exec;
mod parsing;
mod polling;
mod sni;

pub use exec::*;
pub use parsing::*;
pub use polling::*;
pub use sni::*;

/// Error type for E2E tests
#[derive(Debug, thiserror::Error)]
pub enum E2EError {
    #[error("Setup failed: {0}")]
    Setup(String),

    #[error("Exec failed: {0}")]
    Exec(String),

    #[error("Timeout waiting for: {0}")]
    Timeout(String),

    #[error("Device not found")]
    DeviceNotFound,

    #[error("Agent crashed: {0}")]
    AgentCrashed(String),

    #[error("Parse error: {0}")]
    Parse(String),
}

pub type E2EResult<T> = Result<T, E2EError>;
