mod audit;
mod executor;
mod handler;
mod policy;

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use tokio::net::UnixListener;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

const DEFAULT_SOCKET_PATH: &str = "/run/m87/privileged.sock";
const DEFAULT_POLICY_PATH: &str = "/etc/m87/privileged-policy.json";
const DEFAULT_AUDIT_PATH: &str = "/var/log/m87/privileged.log";

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    // Parse CLI args (simple — just --socket-path override).
    let socket_path = parse_socket_path();
    let policy_path = PathBuf::from(DEFAULT_POLICY_PATH);
    let audit_path = PathBuf::from(DEFAULT_AUDIT_PATH);

    // Assert running as root.
    if !nix::unistd::Uid::effective().is_root() {
        bail!("m87-privileged must run as root");
    }

    info!("m87-privileged starting");

    // Load policy store.
    let store = policy::load_policy(&policy_path)
        .await
        .context("failed to load policy store")?;
    let policy = Arc::new(Mutex::new(store));

    // Init audit logger.
    let audit = Arc::new(
        audit::AuditLogger::new(&audit_path)
            .await
            .context("failed to init audit logger")?,
    );

    // Prepare socket directory.
    let sock_dir = PathBuf::from(socket_path.parent().unwrap_or(std::path::Path::new("/run/m87")));
    tokio::fs::create_dir_all(&sock_dir).await?;

    // Remove stale socket.
    if socket_path.exists() {
        tokio::fs::remove_file(&socket_path).await?;
    }

    // Bind Unix listener.
    let listener = UnixListener::bind(&socket_path)
        .context("failed to bind unix socket")?;

    // Set socket permissions to 0o660.
    std::fs::set_permissions(&socket_path, std::os::unix::fs::PermissionsExt::from_mode(0o660))
        .context("failed to set socket permissions")?;

    info!(path = %socket_path.display(), "listening on unix socket");

    // Graceful shutdown via CancellationToken.
    let cancel = CancellationToken::new();

    // Spawn signal handler.
    let cancel_clone = cancel.clone();
    tokio::spawn(async move {
        let ctrl_c = tokio::signal::ctrl_c();
        let mut sigterm =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .expect("failed to register SIGTERM handler");

        tokio::select! {
            _ = ctrl_c => info!("received SIGINT"),
            _ = sigterm.recv() => info!("received SIGTERM"),
        }
        cancel_clone.cancel();
    });

    // Accept loop.
    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                info!("shutting down");
                break;
            }
            result = listener.accept() => {
                match result {
                    Ok((stream, _addr)) => {
                        let policy = Arc::clone(&policy);
                        let audit = Arc::clone(&audit);
                        let policy_path = policy_path.clone();
                        tokio::spawn(async move {
                            handler::handle_connection(stream, policy, audit, policy_path).await;
                        });
                    }
                    Err(e) => {
                        error!("failed to accept connection: {e}");
                    }
                }
            }
        }
    }

    // Cleanup socket.
    let _ = tokio::fs::remove_file(&socket_path).await;
    info!("m87-privileged stopped");

    Ok(())
}

fn parse_socket_path() -> PathBuf {
    let args: Vec<String> = std::env::args().collect();
    let mut i = 1;
    while i < args.len() {
        if args[i] == "--socket-path" && i + 1 < args.len() {
            return PathBuf::from(&args[i + 1]);
        }
        i += 1;
    }
    PathBuf::from(DEFAULT_SOCKET_PATH)
}
