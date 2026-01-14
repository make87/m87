//! Test fixtures for E2E tests
//!
//! Fixtures provide reusable setup patterns for common test scenarios.

mod runtime;
mod device;
mod setup;

pub use runtime::*;
pub use device::*;
pub use setup::*;
