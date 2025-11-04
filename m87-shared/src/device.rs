use serde::{Deserialize, Serialize};

use crate::config::DeviceClientConfig;

#[derive(Debug, Deserialize, Clone)]
pub struct Device {
    pub id: String,
    pub name: String,
    pub updated_at: String,
    pub created_at: String,
    pub last_connection: String,
    pub online: bool,
    pub device_version: String,
    pub target_device_version: String,
    #[serde(default)]
    pub system_info: DeviceSystemInfo,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct PublicDevice {
    pub id: String,
    pub name: String,
    pub updated_at: String,
    pub created_at: String,
    pub last_connection: String,
    pub online: bool,
    pub client_version: String,
    pub target_client_version: String,
    #[serde(default)]
    pub client_config: DeviceClientConfig,
    pub system_info: DeviceSystemInfo,
}

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct DeviceSystemInfo {
    pub hostname: String,
    pub username: String,
    pub public_ip_address: Option<String>,
    pub operating_system: String,
    pub architecture: String,
    #[serde(default)]
    pub cores: Option<u32>,
    pub cpu_name: String,
    #[serde(default)]
    /// Memory in GB
    pub memory: Option<f64>,
    #[serde(default)]
    pub gpus: Vec<String>,
    #[serde(default)]
    pub latitude: Option<f64>,
    #[serde(default)]
    pub longitude: Option<f64>,
    #[serde(default)]
    pub country_code: Option<String>,
}

#[derive(Deserialize, Serialize, Default)]
pub struct UpdateDeviceBody {
    pub system_info: Option<DeviceSystemInfo>,
    pub client_version: Option<String>,
}
