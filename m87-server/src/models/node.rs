use std::{
    hash::{DefaultHasher, Hash, Hasher},
    sync::Arc,
};

use mongodb::bson::{doc, oid::ObjectId, Bson, DateTime, Document};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{
    auth::{access_control::AccessControlled, claims::Claims},
    db::Mongo,
    response::{NexusError, NexusResult},
    util::{app_state::AppState, pagination::RequestPagination},
};

fn default_stable_version() -> String {
    "latest".to_string()
}

fn default_architecture() -> String {
    "unknown".to_string()
}

#[derive(Debug, Serialize, Deserialize, Clone, Hash)]
pub struct NodeClientConfig {
    #[serde(default)]
    pub heartbeat_interval_secs: Option<u32>,
    #[serde(default)]
    pub update_check_interval_secs: Option<u32>,
    pub server_port: u32,
}

impl From<NodeClientConfig> for Bson {
    fn from(state: NodeClientConfig) -> Self {
        mongodb::bson::to_bson(&state).unwrap()
    }
}

impl Default for NodeClientConfig {
    fn default() -> Self {
        NodeClientConfig {
            heartbeat_interval_secs: Some(30),
            update_check_interval_secs: Some(60),
            server_port: 8337,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct NodeSystemInfo {
    pub hostname: String,
    pub public_ip_address: Option<String>,
    pub node_info: Option<String>,
    pub operating_system: String,
    pub client_version: String,
    pub target_client_version: String,
    pub is_managed_node: bool,
    pub icon_url: String,
    #[serde(default = "default_architecture")]
    pub architecture: String,
    #[serde(default)]
    pub cores: Option<u32>,
    #[serde(default)]
    /// Memory in GB
    pub memory: Option<f64>,
    #[serde(default)]
    pub latitude: Option<f64>,
    #[serde(default)]
    pub longitude: Option<f64>,
    #[serde(default)]
    pub country_code: Option<String>,
}

impl From<NodeSystemInfo> for Bson {
    fn from(state: NodeSystemInfo) -> Self {
        mongodb::bson::to_bson(&state).unwrap()
    }
}

impl Hash for NodeSystemInfo {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.hostname.hash(state);
        self.public_ip_address.hash(state);
        self.node_info.hash(state);
        self.operating_system.hash(state);
        self.client_version.hash(state);
        self.target_client_version.hash(state);
        self.is_managed_node.hash(state);
        self.icon_url.hash(state);
        self.architecture.hash(state);
        if let Some(cores) = &self.cores {
            cores.hash(state);
        }
        if let Some(memory) = &self.memory {
            memory.to_bits().hash(state);
        }
        if let Some(latitude) = &self.latitude {
            latitude.to_bits().hash(state);
        }
        if let Some(longitude) = &self.longitude {
            longitude.to_bits().hash(state);
        }
        self.country_code.hash(state);
    }
}

#[derive(Deserialize, Serialize, Hash, Default)]
pub struct UpdateNodeBody {
    pub system_info: Option<NodeSystemInfo>,
    pub client_version: Option<String>,
    pub managed_node_reference: Option<String>,
    pub target_client_version: Option<String>,
    #[serde(default)]
    pub client_config: Option<NodeClientConfig>,
    #[serde(default)]
    pub owner_scope: Option<String>,
    #[serde(default)]
    pub allowed_scopes: Option<Vec<String>>,
}

impl UpdateNodeBody {
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

        if let Some(managed_node_reference) = &self.managed_node_reference {
            update_fields.insert("managed_node_reference", managed_node_reference);
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
pub struct CreateNodeBody {
    pub id: Option<String>,
    pub name: String,
    pub target_client_version: Option<String>,
    pub owner_scope: String,
    pub allowed_scopes: Vec<String>,
    pub api_key_id: ObjectId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeDoc {
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
    pub client_config: NodeClientConfig,
    pub owner_scope: String,
    pub allowed_scopes: Vec<String>,
    pub system_info: NodeSystemInfo,
    pub instruction_hash: i64,
    pub api_key_id: ObjectId,
}

impl NodeDoc {
    pub async fn create_from(db: &Arc<Mongo>, create_body: CreateNodeBody) -> NexusResult<()> {
        let node_id = match create_body.id {
            Some(id) => ObjectId::parse_str(&id)?,
            None => ObjectId::new(),
        };
        // if format!("node:{}", node_id.to_string()) not in create_body.allowed_scopes add it
        let self_scope = format!("node:{}", node_id.to_string());
        // if !create_body.allowed_scopes.contains(&self_scope) {
        // create_body.allowed_scopes.push(self_scope);
        // }
        let allowed_scopes = match create_body.allowed_scopes.contains(&self_scope) {
            true => create_body.allowed_scopes,
            false => {
                let mut allowed_scopes = create_body.allowed_scopes;
                allowed_scopes.push(self_scope);
                allowed_scopes
            }
        };

        let now = DateTime::now();
        let node = NodeDoc {
            id: Some(node_id.clone()),
            short_id: short_node_id(node_id.to_string()),
            name: create_body.name,
            updated_at: now,
            created_at: now,
            last_connection: now,
            client_version: "".to_string(),
            target_client_version: "latest".to_string(),
            client_config: NodeClientConfig::default(),
            owner_scope: create_body.owner_scope,
            allowed_scopes,
            system_info: NodeSystemInfo::default(),
            instruction_hash: 0,
            api_key_id: create_body.api_key_id,
        };
        let _ = db.nodes().insert_one(node).await?;

        Ok(())
    }

    pub async fn remove_node(&self, claims: &Claims, db: &Arc<Mongo>) -> NexusResult<()> {
        let nodes_col = db.nodes();
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
            .map_err(|_| NexusError::internal_error("Failed to delete API keys"))?;

        // Delete any roles scoped to this node
        roles_col
            .delete_many(doc! { "reference_id": self.api_key_id })
            .await
            .map_err(|_| NexusError::internal_error("Failed to delete roles"))?;

        let success = claims
            .delete_one_with_access(&db.nodes(), doc! { "_id": &self.id.clone().unwrap() })
            .await?;

        if success {
            return Err(NexusError::not_found(
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
    ) -> NexusResult<String> {
        let node_id = self.id.clone().unwrap().to_string();
        let sni_host = match name.len() {
            0 => format!("{}.{}", self.short_id, state.config.public_address),
            _ => format!("{}.{}.{}", name, self.short_id, state.config.public_address),
        };
        let _ = state
            .relay
            .register_forward(sni_host.clone(), node_id, port, allowed_source_ips);
        let url = format!("{}{}", url_prefix, sni_host,);
        Ok(url)
    }

    pub async fn request_ssh_command(&self, state: &AppState) -> NexusResult<String> {
        let url = self.request_public_url("ssh", 22, "", None, state).await?;
        let url = format!("ssh -p 443 make87@{}", url);
        Ok(url)
    }

    async fn get_node_client_rest_url(
        &self,
        allowed_source_ips: Option<Vec<String>>,
        state: &AppState,
    ) -> NexusResult<String> {
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
    ) -> NexusResult<String> {
        let url = self
            .get_node_client_rest_url(allowed_source_ips, state)
            .await?;
        let url = format!("{}/logs", url);
        Ok(url)
    }

    pub async fn get_terminal_url(
        &self,
        allowed_source_ips: Option<Vec<String>>,
        state: &AppState,
    ) -> NexusResult<String> {
        let url = self
            .get_node_client_rest_url(allowed_source_ips, state)
            .await?;
        let url = format!("{}/terminal", url);
        Ok(url)
    }

    pub async fn get_container_terminal_url(
        &self,
        container_name: &str,
        allowed_source_ips: Option<Vec<String>>,
        state: &AppState,
    ) -> NexusResult<String> {
        let url = self
            .get_node_client_rest_url(allowed_source_ips, state)
            .await?;
        let url = format!("{}/container/{}", url, container_name);
        Ok(url)
    }

    pub async fn get_container_logs_url(
        &self,
        container_name: &str,
        allowed_source_ips: Option<Vec<String>>,
        state: &AppState,
    ) -> NexusResult<String> {
        let url = self
            .get_node_client_rest_url(allowed_source_ips, state)
            .await?;
        let url = format!("{}/container-logs/{}", url, container_name);
        Ok(url)
    }

    pub async fn get_metrics_url(
        &self,
        allowed_source_ips: Option<Vec<String>>,
        state: &AppState,
    ) -> NexusResult<String> {
        let url = self
            .get_node_client_rest_url(allowed_source_ips, state)
            .await?;
        let url = format!("{}/metrics", url);
        Ok(url)
    }

    pub async fn handle_heartbeat(
        &self,
        claims: Claims,
        db: &Arc<Mongo>,
        payload: HeartbeatRequest,
    ) -> NexusResult<HeartbeatResponse> {
        let last_hash = payload.last_instruction_hash as i64;
        if self.instruction_hash == last_hash {
            return Ok(HeartbeatResponse::default());
        }

        let ssh_keys = claims
            .list_with_access(&db.ssh_keys(), &RequestPagination::max_limit())
            .await?;

        let ssh_keys = ssh_keys.into_iter().map(|key| key.key).collect();

        let config = self.client_config.clone();
        let resp = HeartbeatResponse {
            compose_ref: None,
            client_config: Some(config),
            ssh_keys: Some(ssh_keys),
        };

        let new_hash = resp.get_hash() as i64;
        db.nodes()
            .update_one(
                doc! {"_id": self.id.unwrap()},
                doc! {"$set": {"instruction_hash": new_hash}},
            )
            .await?;

        Ok(resp)
    }
}

fn short_node_id(node_id: String) -> String {
    let mut hasher = Sha256::new();
    hasher.update(node_id.as_bytes());
    let hash = hex::encode(&hasher.finalize());
    let short = &hash[..6]; // 24 bits — should be enough entropy
    short.to_string()
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct PublicNode {
    pub id: String,
    pub name: String,
    pub updated_at: String,
    pub created_at: String,
    pub last_connection: String,
    pub online: bool,
    pub client_version: String,
    pub target_client_version: String,
    #[serde(default)]
    pub client_config: NodeClientConfig,
    pub system_info: NodeSystemInfo,
}

impl PublicNode {
    pub fn from_node(node: &NodeDoc) -> Self {
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
        Self {
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

    pub fn from_nodes(nodes: &Vec<NodeDoc>) -> Vec<Self> {
        nodes.iter().map(Self::from_node).collect()
    }
}

impl AccessControlled for NodeDoc {
    fn owner_scope_field() -> &'static str {
        "owner_scope"
    }
    fn allowed_scopes_field() -> Option<&'static str> {
        Some("allowed_scopes")
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct HeartbeatRequest {
    pub last_instruction_hash: u64,
    pub needs_nexus_token: bool,
    // pub system: SystemMetrics,
    // pub services: Vec<ServiceInfo>,
}

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct HeartbeatResponse {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compose_ref: Option<String>,
    // pub digests: Option<Digests>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_config: Option<NodeClientConfig>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ssh_keys: Option<Vec<String>>,
}

impl Hash for HeartbeatResponse {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.compose_ref.hash(state);
        self.client_config.hash(state);
        self.ssh_keys.hash(state);
    }
}

impl HeartbeatResponse {
    pub fn get_hash(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        self.hash(&mut hasher);
        hasher.finish()
    }
}
