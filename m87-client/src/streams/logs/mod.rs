use anyhow::{Result, anyhow};
use std::sync::Arc;
use tokio::io::AsyncWriteExt;

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
            Ok(line) = app_rx.recv() => {
                // only format if line does not start with [
                let formatted_msg = match line.trim().contains("[observe]") {
                    true => line.to_string().replace("[observe]", ""),
                    false => format::format_log("m87", &line, true),
                };

                if io.write_all(formatted_msg.as_bytes()).await.is_err() {
                    break;
                }
                if io.write_all(b"\n").await.is_err() {
                    break;
                }
            }
        }
    }
    let _ = unit_manager.stop_log_follow().await?;

    Ok(())
}
