use serde::{Deserialize, Serialize};

use crate::metrics::SystemMetrics;
use crate::services::ServiceInfo;

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
