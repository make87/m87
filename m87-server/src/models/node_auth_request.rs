use std::sync::Arc;

use mongodb::bson::{doc, oid::ObjectId, DateTime};

use serde::{Deserialize, Serialize};

use crate::{
    auth::access_control::AccessControlled,
    db::Mongo,
    response::{NexusError, NexusResult},
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeAuthRequestDoc {
    #[serde(rename = "_id", skip_serializing_if = "Option::is_none")]
    pub id: Option<ObjectId>,
    /// uuid of the request
    pub request_id: String,
    /// fastfetch string output
    pub node_info: String,
    /// Time when the entry was created
    pub created_at: DateTime,
    pub node_id: String,
    pub hostname: String,
    pub owner_scope: String,
    pub approved: bool,
}

impl NodeAuthRequestDoc {
    pub async fn create(db: &Arc<Mongo>, body: NodeAuthRequestBody) -> NexusResult<String> {
        let request_uuid = uuid::Uuid::new_v4().to_string();
        let request = NodeAuthRequestDoc {
            id: None,
            request_id: request_uuid.clone(),
            created_at: DateTime::now(),
            node_info: body.node_info.to_string(),
            node_id: body.node_id.to_string(),
            owner_scope: body.owner_scope.to_string(),
            hostname: body.hostname.to_string(),
            approved: false,
        };
        let _ = db
            .node_auth_requests()
            .insert_one(request)
            .await
            .map_err(|err| {
                tracing::error!("Failed to create node auth request: {}", err);
                NexusError::internal_error("Failed to create node auth request")
            })?;

        Ok(request_uuid)
    }
}

impl AccessControlled for NodeAuthRequestDoc {
    fn owner_scope_field() -> &'static str {
        "owner_scope"
    }

    fn allowed_scopes_field() -> Option<&'static str> {
        None
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct PublicNodeAuthRequest {
    pub request_id: String,
    pub node_info: String,
    pub created_at: String,
}

impl From<NodeAuthRequestDoc> for PublicNodeAuthRequest {
    fn from(request: NodeAuthRequestDoc) -> Self {
        PublicNodeAuthRequest {
            request_id: request.request_id,
            node_info: request.node_info,
            created_at: request.created_at.try_to_rfc3339_string().unwrap(),
        }
    }
}

impl PublicNodeAuthRequest {
    pub fn from_vec(nodes: Vec<NodeAuthRequestDoc>) -> Vec<Self> {
        nodes.into_iter().map(Into::into).collect()
    }
}

#[derive(Serialize, Deserialize)]
pub struct NodeAuthRequestBody {
    pub node_info: String,
    pub hostname: String,
    pub owner_scope: String,
    pub node_id: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct NodeAuthRequestCheckResponse {
    pub state: String,
    pub api_key: Option<String>,
}

#[derive(Deserialize)]
pub struct AuthRequestAction {
    pub accept: bool,
}
