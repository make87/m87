//! E2E test containers and infrastructure

mod runtime_args;
mod backward_compat;
pub mod containers;
mod deployment;
mod device_registration;
mod docker;
mod iroh_p2p;
mod logs_status;
mod exec;
pub mod fixtures;
mod fs;
pub mod helpers;
mod install;
mod ls;
mod misc;
mod mongo_indexes;
mod monitoring;
pub mod setup;
mod forward;

// Re-export commonly used items
pub use containers::E2EInfra;
pub use fixtures::{RuntimeRunner, DeviceRegistration, RegisteredDevice, TestSetup};
pub use helpers::{E2EError, E2EResult};
