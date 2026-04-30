use serde::{Deserialize, Serialize};

use crate::config::DeviceClientConfig;
use crate::deploy_spec::{DeployReportKind, DeploymentRevision, JobRun, LifecycleUpdate};
use crate::device::DeviceSystemInfo;
use crate::metrics::SystemMetrics;

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct HeartbeatRequest {
    pub last_instruction_hash: String,
    #[serde(default)]
    pub system_info: Option<DeviceSystemInfo>,
    #[serde(default)]
    pub client_version: Option<String>,
    #[serde(default)]
    pub metrics: Option<SystemMetrics>,
    pub active_revision: String,
    #[serde(default)]
    pub deploy_report: Option<DeployReportKind>,
    /// Revision format version this device supports.
    /// `None` / `1` = legacy (`jobs: Vec<RunSpec>` flat list).
    /// `2`         = new format (`services` / `observers` / `job_defs`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub supported_revision_format: Option<u8>,
    /// iroh `EndpointAddr` (JSON) advertised by the device for direct P2P.
    #[serde(default)]
    pub iroh_node_addr: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct HeartbeatResponse {
    pub up_to_date: bool,
    #[serde(default)]
    pub config: Option<DeviceClientConfig>,
    pub instruction_hash: String,
    /// Full desired revision to apply on the device.
    #[serde(default)]
    pub target_revision: Option<DeploymentRevision>,
    /// Report hashes the server has received and persisted.
    #[serde(default)]
    pub received_report_hashes: Option<Vec<String>>,
    /// Runtime lifecycle overrides to apply without a revision change.
    /// e.g. pause / resume a service or observer.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub lifecycle_updates: Vec<LifecycleUpdate>,
    /// Job runs that are `Queued` and waiting to be executed on this device.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub pending_job_runs: Vec<JobRun>,
    /// Whether the server-side relay understands iroh signalling.
    #[serde(default)]
    pub iroh_supported: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── HeartbeatRequest backward-compat ────────────────────────────────────

    /// A payload produced by an *old* client (no iroh_node_addr field) must
    /// still deserialise cleanly; the new field should default to None.
    #[test]
    fn test_request_missing_iroh_addr_defaults_to_none() {
        let old_json = r#"{
            "last_instruction_hash": "abc",
            "active_revision": "rev1"
        }"#;
        let req: HeartbeatRequest = serde_json::from_str(old_json).unwrap();
        assert_eq!(req.iroh_node_addr, None);
        assert_eq!(req.last_instruction_hash, "abc");
    }

    /// A new client that sets iroh_node_addr should round-trip cleanly.
    #[test]
    fn test_request_iroh_addr_round_trips() {
        let original = HeartbeatRequest {
            last_instruction_hash: "hash".into(),
            active_revision: "rev".into(),
            iroh_node_addr: Some("{\"nodeId\":\"test\"}".into()),
            ..Default::default()
        };
        let json = serde_json::to_string(&original).unwrap();
        let decoded: HeartbeatRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.iroh_node_addr, original.iroh_node_addr);
    }

    // ── HeartbeatResponse backward-compat ───────────────────────────────────

    /// Old server responses (pre-iroh) must still deserialise; iroh_supported
    /// should default to false so old devices aren't confused.
    #[test]
    fn test_response_missing_iroh_supported_defaults_false() {
        let old_json = r#"{
            "up_to_date": true,
            "instruction_hash": "hash123"
        }"#;
        let resp: HeartbeatResponse = serde_json::from_str(old_json).unwrap();
        assert!(
            !resp.iroh_supported,
            "iroh_supported should default to false"
        );
        assert!(resp.up_to_date);
    }

    /// A response that explicitly sets iroh_supported = true should round-trip.
    #[test]
    fn test_response_iroh_supported_round_trips() {
        let original = HeartbeatResponse {
            up_to_date: true,
            instruction_hash: "hash".into(),
            iroh_supported: true,
            ..Default::default()
        };
        let json = serde_json::to_string(&original).unwrap();
        let decoded: HeartbeatResponse = serde_json::from_str(&json).unwrap();
        assert!(decoded.iroh_supported);
    }
}
