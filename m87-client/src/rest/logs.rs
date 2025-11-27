use tokio::io::AsyncWriteExt;

use crate::{rest::upgrade::BoxedIo, util::logging::get_log_rx};

pub async fn handle_logs_io(_: (), mut io: BoxedIo) {
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
