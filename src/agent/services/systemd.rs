use anyhow::Result;
use std::time::Duration;
use tracing::warn;

use crate::agent::services::service_info::{ServiceInfo, ServiceKind};
use crate::util::command::{binary_exists, safe_run_command};

pub async fn collect_systemd_services() -> Result<Vec<ServiceInfo>> {
    if !binary_exists("systemctl") {
        return Ok(Vec::new());
    }

    let mut cmd = tokio::process::Command::new("systemctl");
    cmd.args(&["list-units", "--type=service", "--no-pager", "--no-legend"]);

    let output = safe_run_command(cmd, Duration::from_secs(3)).await?;

    if !output.status.success() {
        warn!("`systemctl list-units` exited with {:?}", output.status);
        return Ok(Vec::new());
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    Ok(parse_systemd_services(stdout.as_ref()))
}

fn parse_systemd_services(output: &str) -> Vec<ServiceInfo> {
    output
        .lines()
        .filter_map(|line| {
            let parts: Vec<_> = line.split_whitespace().collect();
            if parts.len() >= 4 {
                Some(ServiceInfo {
                    name: parts[0].to_string(),
                    kind: ServiceKind::Systemd,
                    status: parts[3].to_string(),
                    uptime_secs: 0, // Not provided by systemctl
                    restart_count: 0,
                })
            } else {
                None
            }
        })
        .collect()
}
