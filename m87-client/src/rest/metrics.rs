use tokio::io::AsyncWriteExt;

use crate::rest::{
    shared::{acquire_metrics_task, SharedReceiver},
    upgrade::BoxedIo,
};

pub async fn handle_system_metrics_io(_: (), mut io: BoxedIo) {
    // Start metrics subscription task
    let (_task, mut rx): (_, SharedReceiver) = acquire_metrics_task("system-metrics").await;

    // Forward metrics until client disconnects or producer shuts down
    while let Ok(json) = rx.inner_mut().recv().await {
        if io.write_all(json.as_bytes()).await.is_err() {
            break;
        }
        if io.write_all(b"\n").await.is_err() {
            break;
        }
    }

    // No manual ref decrement — dropping rx handles it
    drop(rx);

    // Flush & close
    let _ = io.shutdown().await;
}
