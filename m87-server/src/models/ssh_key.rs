use std::sync::Arc;

use mongodb::bson::{doc, oid::ObjectId};

use serde::{Deserialize, Serialize};

use crate::{
    auth::access_control::AccessControlled,
    db::Mongo,
    response::{ServerError, ServerResult},
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SSHPubKeyDoc {
    #[serde(rename = "_id", skip_serializing_if = "Option::is_none")]
    pub id: Option<ObjectId>,
    /// SSH public key
    pub key: String,
    pub owner_scope: String,
    pub allowed_scopes: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SSHPubKeyCreateRequest {
    pub key: String,
    pub owner_scope: String,
    pub allowed_scopes: Vec<String>,
}

impl SSHPubKeyDoc {
    pub async fn create(db: &Arc<Mongo>, body: SSHPubKeyCreateRequest) -> ServerResult<()> {
        // split the owner scope by : and take second part as owner id. If ss:ownerid is not in allwerd scopes, add it
        let owner_id = body.owner_scope.split(':').nth(1).unwrap_or("").to_string();
        let self_access_scope = format!("ssh:{}", owner_id);
        let allowed_scopes = match body.allowed_scopes.contains(&self_access_scope) {
            true => body.allowed_scopes,
            false => {
                let mut allowed_scopes = body.allowed_scopes.clone();
                allowed_scopes.push(self_access_scope);
                allowed_scopes
            }
        };

        let request = SSHPubKeyDoc {
            id: None,
            key: body.key,
            owner_scope: body.owner_scope,
            allowed_scopes,
        };
        let _ = db.ssh_keys().insert_one(request).await.map_err(|err| {
            tracing::error!("Failed to create SSH public key: {}", err);
            ServerError::internal_error("Failed to create SSH public key")
        })?;

        Ok(())
    }
}

impl AccessControlled for SSHPubKeyDoc {
    fn owner_scope_field() -> &'static str {
        "owner_scope"
    }

    fn allowed_scopes_field() -> Option<&'static str> {
        Some("allowed_scopes")
    }
}
