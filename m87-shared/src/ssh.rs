use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SSHKey {
    pub id: String,
    pub key: String,
    pub owner_scope: String,
    pub allowed_scopes: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateSSHKeyRequest {
    pub key: String,
    pub owner_scope: String,
    pub allowed_scopes: Vec<String>,
}
