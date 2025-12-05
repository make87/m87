use std::hash::Hash;

use serde::{Deserialize, Serialize};

use crate::config::DeviceClientConfig;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct PublicDevice {
    pub id: String,
    pub name: String,
    pub short_id: String,
    pub updated_at: String,
    pub created_at: String,
    pub last_connection: String,
    pub online: bool,
    pub version: String,
    pub target_version: String,
    #[serde(default)]
    pub config: DeviceClientConfig,
    pub system_info: DeviceSystemInfo,
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
}
