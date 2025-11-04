use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct DeviceAuthRequestBody {
    pub device_info: String,
    pub hostname: String,
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
    pub device_info: String,
    pub created_at: String,
}
