use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DeviceClientConfig {
    #[serde(default)]
    pub heartbeat_interval_secs: Option<u32>,
    #[serde(default)]
    pub update_check_interval_secs: Option<u32>,
    pub server_port: u32,
}

impl Default for DeviceClientConfig {
    fn default() -> Self {
        DeviceClientConfig {
            heartbeat_interval_secs: Some(30),
            update_check_interval_secs: Some(60),
            server_port: 8337,
        }
    }
}
