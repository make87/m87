use crate::streams::quic::open_quic_io;
use crate::streams::stream_type::StreamType;
use crate::util::shutdown::SHUTDOWN;
use crate::{auth::AuthManager, config::Config, devices};
use anyhow::Result;
use termion::{raw::IntoRawMode, terminal_size};
use tokio::io::{AsyncWriteExt};
use tokio::signal::unix::{SignalKind, signal};
use tokio::sync::mpsc;

pub async fn run_shell(device: &str) -> Result<()> {
    let config = Config::load()?;
    let base = config.get_server_hostname();
    let dev = devices::list_devices()
        .await?
        .into_iter()
        .find(|d| d.name == device)
        .ok_or_else(|| anyhow::anyhow!("Device not found"))?;

    let token = AuthManager::get_cli_token().await?;

    // --- open QUIC terminal stream ---
    let stream_type = StreamType::Terminal {
        token: token.to_string(),
    };
    let (_, io) = open_quic_io(
        &base,
        &token,
        &dev.short_id,
        stream_type,
        config.trust_invalid_server_cert,
    )
    .await?;

    let mut reader = io.recv;
    let mut writer = io.send;

    // --- raw mode ---
    let _raw_mode = std::io::stdout().into_raw_mode()?;

    // --- send initial terminal size ---
    if let Ok((cols, rows)) = terminal_size() {
        let mut buf = [0u8; 5];
        buf[0] = 0xFF;
        buf[1] = (rows >> 8) as u8;
        buf[2] = rows as u8;
        buf[3] = (cols >> 8) as u8;
        buf[4] = cols as u8;

        writer.write_all(&buf).await?;
        writer.flush().await?;
    }

    tracing::info!("[done] Connected.");
    tracing::info!("\r");

    // === stdin → channel ===
    let (stdin_tx, mut stdin_rx) = mpsc::unbounded_channel::<Vec<u8>>();

    std::thread::spawn(move || {
        use std::io::Read;
        let mut stdin = std::io::stdin();
        let mut buf = [0u8; 1024];

        loop {
            match stdin.read(&mut buf) {
                Ok(0) => {
                    let _ = stdin_tx.send(Vec::new());
                    break;
                }
                Ok(n) => {
                    let _ = stdin_tx.send(buf[..n].to_vec());
                }
                Err(_) => break,
            }
        }
    });

    // === writer task: stdin + resize (fused) ===
    let mut sigwinch = signal(SignalKind::window_change())?;

    let mut writer_task = tokio::spawn(async move {
        loop {
            tokio::select! {
                // ----- stdin -----
                Some(bytes) = stdin_rx.recv() => {
                    if bytes.is_empty() {
                        let _ = writer.shutdown().await;
                        break;
                    }

                    writer.write_all(&bytes).await?;
                    writer.flush().await?;
                }

                // ----- resize -----
                _ = sigwinch.recv() => {
                    if let Ok((cols, rows)) = terminal_size() {
                        let mut buf = [0u8; 5];
                        buf[0] = 0xFF;
                        buf[1] = (rows >> 8) as u8;
                        buf[2] = rows as u8;
                        buf[3] = (cols >> 8) as u8;
                        buf[4] = cols as u8;

                        writer.write_all(&buf).await?;
                        writer.flush().await?;
                    }
                }
            }
        }

        Ok::<_, anyhow::Error>(())
    });

    // === reader → stdout ===
    let mut reader_task = tokio::spawn(async move {
        let mut stdout = tokio::io::stdout();
        let mut buf = [0u8; 8192];

        loop {
            let n = match reader.read(&mut buf).await? {
                Some(n) => n,
                None => break,
            };
            if n == 0 {
                break;
            }
            stdout.write_all(&buf[..n]).await?;
            stdout.flush().await?;
        }

        Ok::<_, anyhow::Error>(())
    });

    // === shutdown ===
    tokio::select! {
        _ = &mut reader_task => writer_task.abort(),
        _ = &mut writer_task => {},
        _ = SHUTDOWN.cancelled() => {},
    }

    Ok(())
}
