use std::{fmt::Display, hash::Hash};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{config::DeviceClientConfig, roles::Role};

/// Compute short device ID (first 6 chars of SHA256 hash)
/// Used for tunnel routing - must be consistent across server and client
pub fn short_device_id(device_id: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(device_id.as_bytes());
    let hash = hex::encode(hasher.finalize());
    hash[..6].to_string()
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct PublicDevice {
    pub id: String,
    pub name: String,
    pub short_id: String,
    pub updated_at: String,
    pub created_at: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_connection: Option<String>,
    pub online: bool,
    pub version: String,
    pub target_version: String,
    #[serde(default)]
    pub config: DeviceClientConfig,
    pub system_info: DeviceSystemInfo,
    #[serde(default)]
    pub role: Role, // the role of the requestor
}

impl Display for PublicDevice {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let json = serde_json::to_string_pretty(self).unwrap();
        write!(f, "{}", json)
    }
}

fn default_architecture() -> String {
    "unknown".to_string()
}

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct DeviceSystemInfo {
    pub hostname: String,
    pub username: String,
    pub public_ip_address: Option<String>,
    pub operating_system: String,
    #[serde(default = "default_architecture")]
    pub architecture: String,
    #[serde(default)]
    pub cores: Option<u32>,
    pub cpu_name: String,
    #[serde(default)]
    /// Memory in GB
    pub memory: Option<f64>,
    #[serde(default)]
    pub gpus: Vec<String>,
}

impl Hash for DeviceSystemInfo {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.hostname.hash(state);
        self.username.hash(state);
        self.public_ip_address.hash(state);
        self.operating_system.hash(state);
        self.architecture.hash(state);
        if let Some(cores) = &self.cores {
            cores.hash(state);
        }
        if let Some(memory) = &self.memory {
            memory.to_bits().hash(state);
        }
        self.cpu_name.hash(state);
        self.gpus.hash(state);
    }
}

#[derive(Deserialize, Serialize, Default)]
pub struct UpdateDeviceBody {
    pub system_info: Option<DeviceSystemInfo>,
    pub client_version: Option<String>,
    pub config: Option<DeviceClientConfig>,
}

#[derive(Deserialize, Serialize, Default, Clone)]
pub struct ObserveStatus {
    pub name: String,
    pub alive: bool,
    pub healthy: bool,
    pub crashes: u32,
    pub unhealthy_checks: u32,
}

#[derive(Deserialize, Serialize, Default)]
pub struct IncidentInfo {
    pub id: String,
    pub start_time: String,
    pub end_time: String,
}

#[derive(Deserialize, Serialize, Default)]
pub struct DeviceStatus {
    // current livelyness status
    pub observations: Vec<ObserveStatus>,

    // incidents
    #[serde(default)]
    pub incidents: Vec<IncidentInfo>,

    #[serde(default)]
    pub device_id: Option<String>,
}

#[derive(Deserialize, Serialize, Default)]
pub struct AuditLog {
    pub user_name: String,
    pub user_email: String,
    pub timestamp: String,
    pub action: String,
    pub details: String,
    pub device_id: Option<String>,
}

#[derive(Deserialize, Serialize, Default)]
pub struct AddDeviceAccessBody {
    pub email_or_org_id: String,
    pub role: Role,
}

impl Display for AddDeviceAccessBody {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let json = serde_json::to_string_pretty(self).unwrap();
        write!(f, "{}", json)
    }
}

// remove and uupdate bodies
