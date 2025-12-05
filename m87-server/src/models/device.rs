use std::sync::Arc;

use mongodb::bson::{DateTime, Document, doc, oid::ObjectId};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

// Import shared types
pub use m87_shared::config::DeviceClientConfig;
pub use m87_shared::device::{DeviceSystemInfo, PublicDevice};
pub use m87_shared::heartbeat::{HeartbeatRequest, HeartbeatResponse};

use crate::{
    auth::{access_control::AccessControlled, claims::Claims},
    db::Mongo,
    response::{ServerError, ServerResult},
};

fn default_stable_version() -> String {
    "latest".to_string()
}

#[derive(Deserialize, Serialize, Default)]
pub struct UpdateDeviceBody {
    pub system_info: Option<DeviceSystemInfo>,
    pub version: Option<String>,
    pub target_version: Option<String>,
    #[serde(default)]
    pub config: Option<DeviceClientConfig>,
    #[serde(default)]
    pub owner_scope: Option<String>,
    #[serde(default)]
    pub allowed_scopes: Option<Vec<String>>,
}

impl UpdateDeviceBody {
    pub fn to_update_doc(&self) -> Document {
        let mut update_fields = doc! {};

        if let Some(system_info) = &self.system_info {
            update_fields.insert("system_info", mongodb::bson::to_bson(system_info).unwrap());
        }

        if let Some(owner_scope) = &self.owner_scope {
            update_fields.insert("owner_scope", owner_scope);
        }

        if let Some(allowed_scopes) = &self.allowed_scopes {
            update_fields.insert("allowed_scopes", allowed_scopes);
        }

        if let Some(version) = &self.version {
            update_fields.insert("version", version);
        }

        if let Some(target_version) = &self.target_version {
            update_fields.insert("target_version", target_version);
        }

        if let Some(config) = &self.config {
            update_fields.insert("config", mongodb::bson::to_bson(config).unwrap());
            // Force a compose recheck when config changes
            update_fields.insert("current_compose_hash", mongodb::bson::Bson::Null);
        }

        // Always set these system timestamps
        update_fields.insert("last_connection", DateTime::now());
        update_fields.insert("updated_at", DateTime::now());

        doc! { "$set": update_fields }
    }
}

#[derive(Deserialize, Serialize, Default)]
pub struct CreateDeviceBody {
    pub id: Option<String>,
    pub name: String,
    pub target_version: Option<String>,
    pub owner_scope: String,
    pub allowed_scopes: Vec<String>,
    pub api_key_id: ObjectId,
    pub system_info: DeviceSystemInfo,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeviceDoc {
    #[serde(rename = "_id", skip_serializing_if = "Option::is_none")]
    pub id: Option<ObjectId>,
    pub short_id: String,
    pub name: String,
    pub updated_at: DateTime,
    pub created_at: DateTime,
    pub last_connection: DateTime,
    #[serde(default = "String::new")]
    pub version: String,
    #[serde(default = "default_stable_version")]
    pub target_version: String,
    #[serde(default)]
    pub config: DeviceClientConfig,
    pub owner_scope: String,
    pub allowed_scopes: Vec<String>,
    pub system_info: DeviceSystemInfo,
    pub instruction_hash: i64,
    pub api_key_id: ObjectId,
}

impl DeviceDoc {
    pub async fn create_from(db: &Arc<Mongo>, create_body: CreateDeviceBody) -> ServerResult<()> {
        let device_id = match create_body.id {
            Some(id) => ObjectId::parse_str(&id)?,
            None => ObjectId::new(),
        };
        let self_scope = format!("device:{}", device_id.to_string());
        let allowed_scopes = match create_body.allowed_scopes.contains(&self_scope) {
            true => create_body.allowed_scopes,
            false => {
                let mut allowed_scopes = create_body.allowed_scopes;
                allowed_scopes.push(self_scope);
                allowed_scopes
            }
        };

        let now = DateTime::now();
        let node = DeviceDoc {
            id: Some(device_id.clone()),
            short_id: short_device_id(device_id.to_string()),
            name: create_body.name,
            updated_at: now,
            created_at: now,
            last_connection: now,
            version: "".to_string(),
            target_version: create_body
                .target_version
                .unwrap_or(default_stable_version()),
            config: DeviceClientConfig::default(),
            owner_scope: create_body.owner_scope,
            allowed_scopes,
            system_info: create_body.system_info,
            instruction_hash: 0,
            api_key_id: create_body.api_key_id,
        };
        let _ = db.devices().insert_one(node.clone()).await?;
        Ok(())
    }

    pub async fn remove_device(&self, claims: &Claims, db: &Arc<Mongo>) -> ServerResult<()> {
        let api_keys_col = db.api_keys();
        let roles_col = db.roles();

        // Delete associated API keys
        api_keys_col
            .delete_many(doc! { "_id": self.api_key_id })
            .await
            .map_err(|_| ServerError::internal_error("Failed to delete API keys"))?;

        // Delete any roles scoped to this node
        roles_col
            .delete_many(doc! { "reference_id": self.api_key_id })
            .await
            .map_err(|_| ServerError::internal_error("Failed to delete roles"))?;

        // Check access and delete device
        let success = claims
            .delete_one_with_access(&db.devices(), doc! { "_id": &self.id.clone().unwrap() })
            .await?;

        if !success {
            return Err(ServerError::not_found(
                "Node you are trying to remove does not exist",
            ));
        }
        Ok(())
    }

    pub async fn handle_heartbeat(
        &self,
        _claims: Claims,
        _db: &Arc<Mongo>,
        _payload: HeartbeatRequest,
    ) -> ServerResult<HeartbeatResponse> {
        // TODO: Process system metrics and services from payload
        // TODO: Store or log the metrics for monitoring/alerting

        // For now, return a basic response
        let resp = HeartbeatResponse {
            up_to_date: true,
            compose_ref: None,
            digests: None,
        };

        Ok(resp)
    }
}

fn short_device_id(device_id: String) -> String {
    let mut hasher = Sha256::new();
    hasher.update(device_id.as_bytes());
    let hash = hex::encode(&hasher.finalize());
    let short = &hash[..6]; // 24 bits — should be enough entropy
    short.to_string()
}

impl Into<PublicDevice> for DeviceDoc {
    fn into(self) -> PublicDevice {
        let now_ms = DateTime::now().timestamp_millis();
        let last_ms = self.last_connection.timestamp_millis();
        let heartbeat_secs = self.config.heartbeat_interval_secs.clone().unwrap_or(30);
        // convert u32 to i64
        let heartbeat_secs = heartbeat_secs as i64;

        let online = now_ms - last_ms < 3 * heartbeat_secs * 1000;
        PublicDevice {
            id: self.id.unwrap().to_string(),
            name: self.name.clone(),
            short_id: self.short_id.clone(),
            updated_at: self.updated_at.try_to_rfc3339_string().unwrap(),
            created_at: self.created_at.try_to_rfc3339_string().unwrap(),
            last_connection: self.last_connection.try_to_rfc3339_string().unwrap(),
            online,
            version: self.version.clone(),
            target_version: self.target_version.clone(),
            config: self.config.clone(),
            system_info: self.system_info.clone(),
        }
    }
}

impl DeviceDoc {
    pub fn to_public_devices(devices: Vec<DeviceDoc>) -> Vec<PublicDevice> {
        devices.into_iter().map(|device| device.into()).collect()
    }
}

impl AccessControlled for DeviceDoc {
    fn owner_scope_field() -> &'static str {
        "owner_scope"
    }
    fn allowed_scopes_field() -> Option<&'static str> {
        Some("allowed_scopes")
    }
}
