use serde::{Deserialize, Serialize};

use crate::device::DeviceSystemInfo;

#[derive(Serialize, Deserialize)]
pub struct DeviceAuthRequestBody {
    pub device_info: DeviceSystemInfo,
    pub owner_scope: String,
    pub device_id: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DeviceAuthRequestCheckResponse {
    pub state: String,
    pub api_key: Option<String>,
}

#[derive(Serialize, Deserialize)]
pub struct CheckAuthRequest {
    pub request_id: String,
}

#[derive(Serialize, Deserialize)]
pub struct AuthRequestAction {
    pub accept: bool,
    pub request_id: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DeviceAuthRequest {
    pub request_id: String,
    pub device_info: DeviceSystemInfo,
    pub created_at: String,
}
