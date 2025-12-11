//! E2E test containers and infrastructure

pub mod containers;
mod device_registration;
pub mod fixtures;
pub mod helpers;
mod fs;
mod setup;
mod tunnel;

// Re-export commonly used items
pub use containers::E2EInfra;
pub use fixtures::{AgentRunner, DeviceRegistration, RegisteredDevice};
pub use helpers::{E2EError, E2EResult};
