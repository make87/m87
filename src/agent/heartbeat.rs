use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use tracing::{error, info};

use crate::agent::services::collect_all_services;
use crate::agent::services::service_info::ServiceInfo;
use crate::agent::system_metrics::{collect_system_metrics, SystemMetrics};

#[derive(Serialize, Deserialize, Debug)]
pub struct HeartbeatRequest {
    pub last_instruction_hash: String,
    pub system: SystemMetrics,
    pub services: Vec<ServiceInfo>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct HeartbeatResponse {
    pub up_to_date: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compose_ref: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub digests: Option<Digests>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Digests {
    pub compose: Option<String>,
    pub secrets: Option<String>,
    pub ssh: Option<String>,
    pub config: Option<String>,
    pub combined: String,
}

pub async fn send_heartbeat(
    last_instruction_hash: &str,
    node_id: &str,
    api_url: &str,
    token: &str,
) -> Result<HeartbeatResponse> {
    let metrics = collect_system_metrics().await?;
    let services = collect_all_services().await?;
    let req = HeartbeatRequest {
        last_instruction_hash: last_instruction_hash.to_string(),
        system: metrics,
        services,
    };

    let client = reqwest::Client::new();
    let url = format!("{}/api/v0/nodes/{}/heartbeat", api_url, node_id);

    let resp = client
        .post(&url)
        .bearer_auth(token)
        .json(&req)
        .send()
        .await
        .context("Failed to send heartbeat")?;

    let status = resp.status();
    let text = resp
        .text()
        .await
        .context("Failed to read heartbeat response body")?;

    if !status.is_success() {
        error!("Heartbeat request failed with status {}: {}", status, text);
        return Err(anyhow::anyhow!(
            "Heartbeat failed with status {}: {}",
            status,
            text
        ));
    }

    // Try to decode JSON, log the body in case it fails
    match serde_json::from_str::<HeartbeatResponse>(&text) {
        Ok(decoded) => {
            info!("Heartbeat sent successfully: {:?}", decoded);
            Ok(decoded)
        }
        Err(err) => {
            error!(
                "Failed to decode heartbeat response: {}\nRaw response: {}",
                err, text
            );
            Err(anyhow::anyhow!("Invalid heartbeat response: {}", err))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json;

    // simple helper to silence tracing output during tests
    fn init_tracing() {
        let _ = tracing_subscriber::fmt::try_init();
    }

    #[tokio::test]
    async fn test_heartbeat_construction() -> Result<()> {
        init_tracing();

        // Fake identifiers
        let last_instruction_hash = "sha256:testhash";

        // Call the internal parts only — no backend
        let system = collect_system_metrics().await?;
        let services = collect_all_services().await?;

        // Basic validation
        assert!(!system.hostname.is_empty(), "hostname should not be empty");
        assert!(
            system.memory.total_mb > 0,
            "memory total should be greater than zero"
        );

        // Build request object
        let hb = HeartbeatRequest {
            last_instruction_hash: last_instruction_hash.to_string(),
            system,
            services,
        };

        // Serialize it to JSON to ensure it's valid
        let json = serde_json::to_string_pretty(&hb)?;
        println!("{}", json);

        // It should include at least the system section
        assert!(json.contains("system"));
        assert!(json.contains("hostname"));
        assert!(json.contains("memory"));

        Ok(())
    }

    #[tokio::test]
    async fn test_heartbeat_response_deserialize() -> Result<()> {
        let json = r#"
        {
            "up_to_date": false,
            "compose_ref": "ghcr.io/make87/insector:latest",
            "digests": {
                "compose": "sha256:a",
                "secrets": "sha256:b",
                "ssh": "sha256:c",
                "config": "sha256:d",
                "combined": "sha256:final"
            }
        }
        "#;

        let resp: HeartbeatResponse = serde_json::from_str(json)?;
        assert!(!resp.up_to_date);
        assert_eq!(
            resp.digests.unwrap().combined,
            "sha256:final",
            "combined digest must match"
        );
        Ok(())
    }
}
