// === Core modules ===
pub mod device;
pub mod devices;
pub mod app;
pub mod auth;
pub mod config;

// Agent-specific modules (Linux-only via build.rs)
#[cfg(feature = "agent")]
pub mod rest;

pub mod server;
pub mod stack;
pub mod update;
pub mod util;

// === CLI entrypoint ===
pub mod cli;

/// Entrypoint used by `main.rs` and tests to run the full CLI.
pub async fn run_cli() -> anyhow::Result<()> {
    cli::cli().await
}
