use std::{
    hash::{DefaultHasher, Hash, Hasher},
    sync::Arc,
};

use mongodb::bson::{doc, oid::ObjectId, Bson, DateTime, Document};

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
    util::app_state::AppState,
};

fn default_stable_version() -> String {
    "latest".to_string()
}

// Helper functions to convert shared types to Bson (can't use From trait due to orphan rules)
pub fn device_config_to_bson(config: &DeviceClientConfig) -> Bson {
    mongodb::bson::to_bson(config).unwrap()
}

pub fn device_system_info_to_bson(info: &DeviceSystemInfo) -> Bson {
    mongodb::bson::to_bson(info).unwrap()
}

// Helper function to hash DeviceSystemInfo (used for caching)
pub fn hash_device_system_info(info: &DeviceSystemInfo) -> u64 {
    let mut hasher = DefaultHasher::new();
    info.hostname.hash(&mut hasher);
    info.public_ip_address.hash(&mut hasher);
    info.operating_system.hash(&mut hasher);
    info.architecture.hash(&mut hasher);
    if let Some(cores) = &info.cores {
        cores.hash(&mut hasher);
    }
    if let Some(memory) = &info.memory {
        memory.to_bits().hash(&mut hasher);
    }
    if let Some(latitude) = &info.latitude {
        latitude.to_bits().hash(&mut hasher);
    }
    if let Some(longitude) = &info.longitude {
        longitude.to_bits().hash(&mut hasher);
    }
    info.country_code.hash(&mut hasher);
    info.cpu_name.hash(&mut hasher);
    info.gpus.hash(&mut hasher);
    hasher.finish()
}

#[derive(Deserialize, Serialize, Default)]
pub struct UpdateDeviceBody {
    pub system_info: Option<DeviceSystemInfo>,
    pub client_version: Option<String>,
    pub target_client_version: Option<String>,
    #[serde(default)]
    pub client_config: Option<DeviceClientConfig>,
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

        if let Some(client_version) = &self.client_version {
            update_fields.insert("client_version", client_version);
        }

        if let Some(target_client_version) = &self.target_client_version {
            update_fields.insert("target_client_version", target_client_version);
        }

        if let Some(client_config) = &self.client_config {
            update_fields.insert(
                "client_config",
                mongodb::bson::to_bson(client_config).unwrap(),
            );
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
    pub target_client_version: Option<String>,
    pub owner_scope: String,
    pub allowed_scopes: Vec<String>,
    pub api_key_id: ObjectId,
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
    pub client_version: String,
    #[serde(default = "default_stable_version")]
    pub target_client_version: String,
    #[serde(default)]
    pub client_config: DeviceClientConfig,
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
        let self_scope = format!("node:{}", device_id.to_string());
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
            client_version: "".to_string(),
            target_client_version: "latest".to_string(),
            client_config: DeviceClientConfig::default(),
            owner_scope: create_body.owner_scope,
            allowed_scopes,
            system_info: DeviceSystemInfo::default(),
            instruction_hash: 0,
            api_key_id: create_body.api_key_id,
        };
        let _ = db.devices().insert_one(node).await?;

        Ok(())
    }

    pub async fn remove_device(&self, claims: &Claims, db: &Arc<Mongo>) -> ServerResult<()> {
        let nodes_col = db.devices();
        let api_keys_col = db.api_keys();
        let roles_col = db.roles();

        // Check access and delete node
        claims
            .delete_one_with_access(&nodes_col, doc! { "_id": self.id.clone().unwrap() })
            .await?;

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

        let success = claims
            .delete_one_with_access(&db.devices(), doc! { "_id": &self.id.clone().unwrap() })
            .await?;

        if success {
            return Err(ServerError::not_found(
                "Node you are trying to remove does not exist",
            ));
        }
        Ok(())
    }

    pub async fn request_public_url(
        &self,
        name: &str,
        port: u16,
        url_prefix: &str,
        allowed_source_ips: Option<Vec<String>>,
        state: &AppState,
    ) -> ServerResult<String> {
        let device_id = self.id.clone().unwrap().to_string();
        let sni_host = match name.len() {
            0 => format!("{}.{}", self.short_id, state.config.public_address),
            _ => format!("{}.{}.{}", name, self.short_id, state.config.public_address),
        };
        let _ = state
            .relay
            .register_forward(sni_host.clone(), device_id, port, allowed_source_ips);
        let url = format!("{}{}", url_prefix, sni_host,);
        Ok(url)
    }

    pub async fn request_ssh_command(&self, state: &AppState) -> ServerResult<String> {
        let url = self.request_public_url("ssh", 22, "", None, state).await?;
        let url = format!("ssh -p 443 make87@{}", url);
        Ok(url)
    }

    async fn get_device_client_rest_url(
        &self,
        allowed_source_ips: Option<Vec<String>>,
        state: &AppState,
    ) -> ServerResult<String> {
        let port = self.client_config.server_port as u16;
        let url = self
            .request_public_url("", port, "https://", allowed_source_ips, state)
            .await?;
        Ok(url)
    }

    pub async fn get_logs_url(
        &self,
        allowed_source_ips: Option<Vec<String>>,
        state: &AppState,
    ) -> ServerResult<String> {
        let url = self
            .get_device_client_rest_url(allowed_source_ips, state)
            .await?;
        let url = format!("{}/logs", url);
        Ok(url)
    }

    pub async fn get_terminal_url(
        &self,
        allowed_source_ips: Option<Vec<String>>,
        state: &AppState,
    ) -> ServerResult<String> {
        let url = self
            .get_device_client_rest_url(allowed_source_ips, state)
            .await?;
        let url = format!("{}/terminal", url);
        Ok(url)
    }

    pub async fn get_container_terminal_url(
        &self,
        container_name: &str,
        allowed_source_ips: Option<Vec<String>>,
        state: &AppState,
    ) -> ServerResult<String> {
        let url = self
            .get_device_client_rest_url(allowed_source_ips, state)
            .await?;
        let url = format!("{}/container/{}", url, container_name);
        Ok(url)
    }

    pub async fn get_container_logs_url(
        &self,
        container_name: &str,
        allowed_source_ips: Option<Vec<String>>,
        state: &AppState,
    ) -> ServerResult<String> {
        let url = self
            .get_device_client_rest_url(allowed_source_ips, state)
            .await?;
        let url = format!("{}/container-logs/{}", url, container_name);
        Ok(url)
    }

    pub async fn get_metrics_url(
        &self,
        allowed_source_ips: Option<Vec<String>>,
        state: &AppState,
    ) -> ServerResult<String> {
        let url = self
            .get_device_client_rest_url(allowed_source_ips, state)
            .await?;
        let url = format!("{}/metrics", url);
        Ok(url)
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

// Helper functions for converting DeviceDoc to PublicDevice (can't use impl due to orphan rules)
pub fn device_doc_to_public(node: &DeviceDoc) -> PublicDevice {
    let now_ms = DateTime::now().timestamp_millis();
    let last_ms = node.last_connection.timestamp_millis();
    let heartbeat_secs = node
        .client_config
        .heartbeat_interval_secs
        .clone()
        .unwrap_or(30);
    // convert u32 to i64
    let heartbeat_secs = heartbeat_secs as i64;

    let online = now_ms - last_ms < 3 * heartbeat_secs * 1000;
    PublicDevice {
        id: node.id.unwrap().to_string(),
        name: node.name.clone(),
        updated_at: node.updated_at.try_to_rfc3339_string().unwrap(),
        created_at: node.created_at.try_to_rfc3339_string().unwrap(),
        last_connection: node.last_connection.try_to_rfc3339_string().unwrap(),
        online,
        client_version: node.client_version.clone(),
        target_client_version: node.target_client_version.clone(),
        client_config: node.client_config.clone(),
        system_info: node.system_info.clone(),
    }
}

pub fn device_docs_to_public(nodes: &Vec<DeviceDoc>) -> Vec<PublicDevice> {
    nodes.iter().map(device_doc_to_public).collect()
}

impl AccessControlled for DeviceDoc {
    fn owner_scope_field() -> &'static str {
        "owner_scope"
    }
    fn allowed_scopes_field() -> Option<&'static str> {
        Some("allowed_scopes")
    }
}
