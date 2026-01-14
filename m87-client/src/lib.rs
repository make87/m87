pub mod auth;
pub mod config;
pub mod device;
pub mod devices;

// Runtime module (Linux-only via build.rs)
#[cfg(feature = "runtime")]
pub mod runtime;

pub mod streams;

pub mod server;
pub mod update;
pub mod util;

pub mod tui;
// === CLI entrypoint ===
pub mod cli;

/// Entrypoint used by `main.rs` and tests to run the full CLI.
pub async fn run_cli() -> anyhow::Result<()> {
    cli::cli().await
}
