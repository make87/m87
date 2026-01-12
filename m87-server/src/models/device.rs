use std::sync::Arc;
use std::time::{Duration, SystemTime};

use m87_shared::deploy_spec::DeploymentRevision;
use m87_shared::device::DeviceStatus;
use mongodb::bson::{DateTime, Document, doc, oid::ObjectId};

use serde::{Deserialize, Serialize};

// Import shared types
pub use m87_shared::config::DeviceClientConfig;
pub use m87_shared::device::{DeviceSystemInfo, PublicDevice, short_device_id};
pub use m87_shared::heartbeat::{HeartbeatRequest, HeartbeatResponse};

use crate::config::AppConfig;
use crate::models::deploy_spec::{CreateDeployReportBody, DeployReportDoc, DeployRevisionDoc};
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

            let new_hash = config.get_hash().to_string();
            update_fields.insert("last_instruction_hash", new_hash);
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
    pub api_key_id: ObjectId,
    #[serde(default)]
    pub last_config_hash: String,
    #[serde(default)]
    pub last_deployment_hash: String,
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
            short_id: short_device_id(&device_id.to_string()),
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
            api_key_id: create_body.api_key_id,
            last_config_hash: "".to_string(),
            last_deployment_hash: "".to_string(),
        };
        let _ = db.devices().insert_one(node.clone()).await?;
        Ok(())
    }

    pub async fn invalidate_deployment_hash(
        db: &Arc<Mongo>,
        device_oid: &ObjectId,
    ) -> ServerResult<()> {
        let _ = db
            .devices()
            .update_one(
                doc! { "_id": device_oid },
                doc! { "$set": { "last_deployment_hash": "" } },
            )
            .await?;
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
        db: &Arc<Mongo>,
        payload: HeartbeatRequest,
        config: &Arc<AppConfig>,
    ) -> ServerResult<HeartbeatResponse> {
        let mut update_fields = doc! {};
        if let Some(sys_info) = payload.system_info {
            update_fields.insert("system_info", mongodb::bson::to_bson(&sys_info).unwrap());
        }
        if let Some(client_version) = payload.client_version {
            update_fields.insert("client_version", client_version);
        }

        if !update_fields.is_empty() {
            update_fields.insert("updated_at", DateTime::now());
        }

        update_fields.insert("last_connection", DateTime::now());
        let _ = db
            .devices()
            .update_one(
                doc! {
                    "_id": &self.id.unwrap()
                },
                doc! {
                    "$set": update_fields
                },
            )
            .await;

        if let Some(deploy_report) = payload.deploy_report {
            let body = CreateDeployReportBody {
                device_id: self.id.clone().unwrap(),
                revision_id: deploy_report.get_revision_id().to_string(),
                kind: deploy_report,
                expires_at: Some(DateTime::from_system_time(
                    SystemTime::now()
                        + Duration::from_hours(24 * config.report_retention_days as u64),
                )),
            };
            let res = DeployReportDoc::create_or_update(db, body).await;
            if let Err(err) = res {
                tracing::error!("Failed to create deploy report: {}", err);
            }
        }

        let target_hash = format!("{}-{}", self.last_deployment_hash, self.last_config_hash);
        if payload.last_instruction_hash == target_hash {
            return Ok(HeartbeatResponse {
                up_to_date: true,
                config: None,
                instruction_hash: target_hash.clone(),
                target_revision: None,
            });
        }

        let out = DeployRevisionDoc::get_active_device_deployment(&db, self.id.unwrap()).await;
        let target_revision = match out {
            Ok(Some(revision)) => Some(revision.revision),
            Ok(None) => Some(DeploymentRevision::empty()),
            _ => None,
        };

        let new_deployment_hash = match &target_revision {
            Some(revision) => revision.get_hash(),
            None => "".to_string(),
        };
        let config_hash = self.config.get_hash().to_string();
        // update last_deployment_hash in database
        let _ = db
            .devices()
            .update_one(
                doc! {
                    "_id": &self.id.unwrap()
                },
                doc! {
                    "$set": doc! {
                        "last_deployment_hash": &new_deployment_hash,
                        "last_config_hash": &config_hash,
                    }
                },
            )
            .await;

        let resp = HeartbeatResponse {
            up_to_date: false,
            config: Some(self.config.clone()),
            instruction_hash: format!("{}-{}", new_deployment_hash, config_hash,),
            target_revision,
        };
        Ok(resp)
    }

    pub async fn get_status(&self, db: &Arc<Mongo>, since: u32) -> ServerResult<DeviceStatus> {
        let active_revision =
            DeployRevisionDoc::get_active_device_deployment(db, self.id.clone().unwrap()).await?;
        let observations = match active_revision {
            Some(revision) => {
                let observations = DeployReportDoc::get_device_observations_since(
                    db,
                    &revision.revision.id.unwrap(),
                    &self.id.clone().unwrap(),
                    since,
                )
                .await?;
                observations
            }
            None => vec![],
        };
        let status = DeviceStatus {
            incidents: vec![],
            observations,
        };
        Ok(status)
    }
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
