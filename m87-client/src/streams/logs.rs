use tokio::io::AsyncWriteExt;

use crate::{streams::quic::QuicIo, util::logging::get_log_rx};

pub async fn handle_logs_io(io: &mut QuicIo) {
    let mut rx = match get_log_rx() {
        Some(r) => r,
        None => {
            let _ = io.write_all(b"logging not initialized\n").await;
            return;
        }
    };

    // Forward log lines until IO is closed
    while let Ok(line) = rx.recv().await {
        if io.write_all(line.as_bytes()).await.is_err() {
            break;
        }
        if io.write_all(b"\n").await.is_err() {
            break;
        }
    }
}
