//! E2E test containers and infrastructure

mod runtime_args;
pub mod containers;
mod device_registration;
mod docker;
mod exec;
pub mod fixtures;
mod fs;
pub mod helpers;
mod install;
mod ls;
mod misc;
mod monitoring;
pub mod setup;
mod forward;

// Re-export commonly used items
pub use containers::E2EInfra;
pub use fixtures::{RuntimeRunner, DeviceRegistration, RegisteredDevice, TestSetup};
pub use helpers::{E2EError, E2EResult};
