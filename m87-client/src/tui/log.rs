use crate::streams::quic::open_device_io;
use crate::streams::stream_type::StreamType;
use crate::{auth::AuthManager, config::Config, devices, util::shutdown::SHUTDOWN};
use anyhow::{Context, Result};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;

/// Stream live logs from a device using RAW upgraded connection.
pub async fn run_logs(device: &str) -> Result<()> {
    let config = Config::load()?;

    let resolved = devices::resolve_device_cached(device).await?;

    let token = AuthManager::get_cli_token().await?;

    println!("Connecting to logs of {} ...", device);

    let stream_type = StreamType::Logs {
        token: token.to_string(),
    };
    let (conn, mut io) = open_device_io(
        &resolved.host,
        &resolved.url,
        &token,
        &resolved.short_id,
        &resolved.id,
        stream_type,
        config.trust_invalid_server_cert,
    )
    .await
    .context("Failed to connect to RAW metrics stream")?;

    println!("Connected. Press Ctrl+C to exit.\n");

    let mut stdout = tokio::io::stdout();

    // Channel to detect stdin EOF (pressing Ctrl+D)
    let (stdin_tx, mut stdin_rx) = mpsc::unbounded_channel::<()>();

    std::thread::spawn(move || {
        use std::io::Read;
        let mut stdin = std::io::stdin();
        let mut buf = [0u8; 1];

        loop {
            match stdin.read(&mut buf) {
                Ok(0) => {
                    let _ = stdin_tx.send(());
                    break;
                }
                Ok(_) => {}
                Err(_) => break,
            }
        }
    });

    // Task: device logs → stdout
    let mut read_task = tokio::spawn(async move {
        let mut buf = [0u8; 8192];
        loop {
            let n = io.read(&mut buf).await?;
            if n == 0 {
                break; // remote closed
            }
            stdout.write_all(&buf[..n]).await?;
            stdout.flush().await?;
        }
        Ok::<_, anyhow::Error>(())
    });

    // Exit on one of:
    // - device closed stream
    // - stdin EOF
    // - global shutdown (Ctrl+C)
    tokio::select! {
        _ = &mut read_task => {},
        _ = stdin_rx.recv() => {},
        _ = SHUTDOWN.cancelled() => {},
    }

    // Stop reading, then tear the connection down gracefully so iroh doesn't log
    // an "Aborting ungracefully" endpoint-drop error on exit.
    read_task.abort();
    conn.close_gracefully(b"logs closed").await;

    println!("\nLogs stream closed.");
    Ok(())
}
