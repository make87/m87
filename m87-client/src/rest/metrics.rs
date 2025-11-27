use tokio::io::AsyncWriteExt;

use crate::rest::{shared::acquire_metrics_task, upgrade::BoxedIo};

pub async fn handle_system_metrics_io(_: (), mut io: BoxedIo) {
    // Start metrics subscription task
    let (task, mut rx) = acquire_metrics_task("system-metrics").await;

    // Forward metrics until client disconnects or task ends
    while let Ok(json) = rx.recv().await {
        if io.write_all(json.as_bytes()).await.is_err() {
            break;
        }
        if io.write_all(b"\n").await.is_err() {
            break;
        }
    }

    // Shutdown metrics producer
    task.dec_or_shutdown();

    // Explicitly flush & close
    let _ = io.shutdown().await;
}
