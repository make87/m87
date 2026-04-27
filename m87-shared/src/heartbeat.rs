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
}

#[derive(Serialize, Deserialize, Debug)]
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
}
