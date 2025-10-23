use anyhow::Result;
use serde::Deserialize;
use std::time::Duration;
use tracing::warn;

use crate::agent::services::service_info::{ServiceInfo, ServiceKind};
use crate::util::command::{binary_exists, safe_run_command};

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct ContainerJson {
    names: Option<Vec<String>>,
    state: Option<String>,
    running_for: Option<String>,
    restart_count: Option<u32>,
}

pub async fn collect_docker_services() -> Result<Vec<ServiceInfo>> {
    if !binary_exists("docker") {
        return Ok(Vec::new());
    }

    let mut cmd = tokio::process::Command::new("docker");
    cmd.args(&["ps", "--all", "--format", "{{json .}}"]);

    let output = safe_run_command(cmd, Duration::from_secs(3)).await?;

    if !output.status.success() {
        warn!("`docker ps` exited with {:?}", output.status);
        return Ok(Vec::new());
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    Ok(parse_containers(stdout.as_ref(), ServiceKind::Docker))
}

fn parse_containers(output: &str, kind: ServiceKind) -> Vec<ServiceInfo> {
    output
        .lines()
        .filter_map(|line| serde_json::from_str::<ContainerJson>(line).ok())
        .map(|c| ServiceInfo {
            name: c
                .names
                .as_ref()
                .and_then(|n| n.first())
                .cloned()
                .unwrap_or_default(),
            kind: kind.clone(),
            status: c.state.unwrap_or_else(|| "unknown".into()),
            uptime_secs: parse_running_for(&c.running_for),
            restart_count: c.restart_count.unwrap_or(0),
        })
        .collect()
}

fn parse_running_for(val: &Option<String>) -> u64 {
    if let Some(v) = val {
        let parts: Vec<_> = v.split_whitespace().collect();
        if parts.len() >= 2 {
            if let Ok(n) = parts[0].parse::<u64>() {
                return match parts[1] {
                    "seconds" | "second" => n,
                    "minutes" | "minute" => n * 60,
                    "hours" | "hour" => n * 3600,
                    "days" | "day" => n * 86400,
                    _ => 0,
                };
            }
        }
    }
    0
}
