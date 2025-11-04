use std::sync::Arc;

use mongodb::bson::{doc, oid::ObjectId, DateTime};

use serde::{Deserialize, Serialize};

// Import shared types
pub use m87_shared::auth::{
    AuthRequestAction, CheckAuthRequest, DeviceAuthRequest, DeviceAuthRequestBody,
    DeviceAuthRequestCheckResponse,
};

use crate::{
    auth::access_control::AccessControlled,
    db::Mongo,
    response::{ServerError, ServerResult},
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeviceAuthRequestDoc {
    #[serde(rename = "_id", skip_serializing_if = "Option::is_none")]
    pub id: Option<ObjectId>,
    /// uuid of the request
    pub request_id: String,
    /// fastfetch string output
    pub device_info: String,
    /// Time when the entry was created
    pub created_at: DateTime,
    pub device_id: String,
    pub hostname: String,
    pub owner_scope: String,
    pub approved: bool,
}

impl DeviceAuthRequestDoc {
    pub async fn create(db: &Arc<Mongo>, body: DeviceAuthRequestBody) -> ServerResult<String> {
        let request_uuid = uuid::Uuid::new_v4().to_string();
        let request = DeviceAuthRequestDoc {
            id: None,
            request_id: request_uuid.clone(),
            created_at: DateTime::now(),
            device_info: body.device_info.to_string(),
            device_id: body.device_id.to_string(),
            owner_scope: body.owner_scope.to_string(),
            hostname: body.hostname.to_string(),
            approved: false,
        };
        let _ = db
            .device_auth_requests()
            .insert_one(request)
            .await
            .map_err(|err| {
                tracing::error!("Failed to create device auth request: {}", err);
                ServerError::internal_error("Failed to create device auth request")
            })?;

        Ok(request_uuid)
    }
}

impl AccessControlled for DeviceAuthRequestDoc {
    fn owner_scope_field() -> &'static str {
        "owner_scope"
    }

    fn allowed_scopes_field() -> Option<&'static str> {
        None
    }
}

impl From<DeviceAuthRequestDoc> for DeviceAuthRequest {
    fn from(request: DeviceAuthRequestDoc) -> Self {
        DeviceAuthRequest {
            request_id: request.request_id,
            device_info: request.device_info,
            created_at: request.created_at.try_to_rfc3339_string().unwrap(),
        }
    }
}

pub fn from_vec(docs: Vec<DeviceAuthRequestDoc>) -> Vec<DeviceAuthRequest> {
    docs.into_iter().map(Into::into).collect()
}
