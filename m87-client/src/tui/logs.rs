use crate::streams::quic::open_quic_io;
use crate::streams::stream_type::StreamType;
use crate::{auth::AuthManager, config::Config, devices, util::shutdown::SHUTDOWN};
use anyhow::{Context, Result, anyhow};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;

/// Stream live logs from a device using RAW upgraded connection.
pub async fn run_logs(device: &str) -> Result<()> {
    let config = Config::load()?;
    let base = config.get_server_hostname();

    let dev = devices::list_devices()
        .await?
        .into_iter()
        .find(|d| d.name == device)
        .ok_or_else(|| anyhow!("Device '{}' not found", device))?;

    let token = AuthManager::get_cli_token().await?;

    println!("Connecting to logs of {} ...", device);

    let stream_type = StreamType::Logs {
        token: token.to_string(),
    };
    let (_, mut io) = open_quic_io(
        &base,
        &dev.short_id,
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

    println!("\nLogs stream closed.");
    Ok(())
}
