use serde::{Deserialize, Serialize};

use crate::config::DeviceClientConfig;
use crate::deploy_spec::{DeployReportKind, DeploymentRevision};
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
    #[serde(default)]
    pub target_revision: Option<DeploymentRevision>,
    #[serde(default)]
    pub received_report_hashes: Option<Vec<String>>,
}
