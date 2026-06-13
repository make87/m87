use anyhow::{Result, anyhow};
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio::sync::broadcast::error::RecvError;

use crate::util::format;
use crate::{
    device::deployment_manager::DeploymentManager, streams::quic::QuicIo, util::logging::get_log_rx,
};

pub async fn handle_logs_io(io: &mut QuicIo, unit_manager: Arc<DeploymentManager>) -> Result<()> {
    let _ = unit_manager.start_log_follow().await?;
    let mut app_rx = match get_log_rx() {
        Some(r) => r,
        None => {
            let _ = io.write_all(b"logging not initialized\n").await;
            return Err(anyhow!("logging not initialized"));
        }
    };

    loop {
        tokio::select! {
            res = app_rx.recv() => {
                let line = match res {
                    Ok(line) => line,
                    Err(RecvError::Lagged(n)) => {
                        // You fell behind; old messages were dropped.
                        // Don't break; just keep streaming the newest.

                        let _ = io.write_all(format!("(dropped {n} log lines)\n").as_bytes()).await;
                        continue;
                    }
                    Err(RecvError::Closed) => {
                        break;
                    }
                };

                let formatted_msg = if line.trim().contains("[observe]") {
                    line.replace("[observe]", "")
                } else {
                    format::format_log("m87", &line, true)
                };

                if io.write_all(formatted_msg.as_bytes()).await.is_err() { break; }
                if io.write_all(b"\n").await.is_err() { break; }
            }
        }
    }

    // stop_log_follow now requires a unit_id; log streaming cleanup is handled per-unit elsewhere

    Ok(())
}
