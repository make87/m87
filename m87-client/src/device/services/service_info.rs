use serde::{Deserialize, Serialize};

/// General representation of any service the agent monitors:
/// Docker container, Podman container, systemd unit, etc.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ServiceInfo {
    pub name: String,
    pub kind: ServiceKind,
    pub status: String,
    pub uptime_secs: u64,
    pub restart_count: u32,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ServiceKind {
    Docker,
    Podman,
    Systemd,
    Other,
}

impl Default for ServiceKind {
    fn default() -> Self {
        ServiceKind::Other
    }
}
