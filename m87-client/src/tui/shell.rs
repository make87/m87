use crate::util::raw_connection::open_raw_io;
use crate::{auth::AuthManager, config::Config, devices};
use anyhow::Result;
use tokio::io::{self, AsyncReadExt, AsyncWriteExt};
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

    // --- open raw upgraded TLS stream ---
    let io = open_raw_io(
        &base,
        &dev.short_id,
        "/terminal",
        &token,
        config.trust_invalid_server_cert,
    )
    .await?;

    // --- split for bidirectional tasks ---
    let (mut reader, mut writer) = io::split(io);

    println!("Connected. Press Ctrl+C to exit.\n\r");

    // === Spawn task: stdin → writer ===
    let (stdin_tx, mut stdin_rx) = mpsc::unbounded_channel::<Vec<u8>>();

    // Blocking thread to read raw stdin (termion)
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

    // async writer task
    let mut writer_task = tokio::spawn(async move {
        while let Some(bytes) = stdin_rx.recv().await {
            if bytes.is_empty() {
                // EOF / Ctrl+D → shutdown
                let _ = writer.shutdown().await;
                break;
            }
            writer.write_all(&bytes).await?;
            writer.flush().await?;
        }
        Ok::<_, anyhow::Error>(())
    });

    // === Spawn task: reader → stdout ===
    let mut reader_task = tokio::spawn(async move {
        let mut stdout = tokio::io::stdout();
        let mut buf = [0u8; 8192];

        loop {
            let n = reader.read(&mut buf).await?;
            if n == 0 {
                break; // remote closed
            }
            stdout.write_all(&buf[..n]).await?;
            stdout.flush().await?;
        }

        Ok::<_, anyhow::Error>(())
    });

    // === Wait until one side closes ===
    tokio::select! {
        _ = &mut reader_task => writer_task.abort(),
        _ = &mut writer_task => {},
    }

    Ok(())
}
